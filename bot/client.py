"""Custom Discord Bot Client with lifecycle hooks and Backloggd integration."""

import asyncio
import logging
import math
import socket
import time
from pathlib import Path
from typing import Any, Callable, Optional

import aiohttp
import discord
from backloggd import BackloggdClient
from discord import app_commands
from discord.ext import commands

from bot.command_errors import handle_app_command_error
from bot.config import ALERT_OWNER_DM, ALERT_WEBHOOK_URL, DATABASE_PATH, DEV_GUILD_ID
from bot.db import Database
from bot.services.cooldowns import GuildCooldownService
from bot.services.guild_membership import GuildMembershipService
from bot.services.library_sync import LibrarySyncService
from bot.services.session_health import UpstreamSessionHealth
from bot.services.task_lifecycle import await_owned_task

logger = logging.getLogger("backloggd.bot")

_DEFAULT_COMMAND_SYNC_RETRY_DELAYS = (5.0, 15.0)
_DEFAULT_COMMAND_SYNC_RETRY_BUDGET = 60.0
_RECONNECT_GRACE_SECONDS = 60.0


def _retry_after_seconds(error: Exception) -> Optional[float]:
    value = getattr(error, "retry_after", None)
    if value is None:
        response = getattr(error, "response", None)
        headers = getattr(response, "headers", None) or getattr(error, "headers", None) or {}
        value = headers.get("Retry-After", headers.get("retry-after"))
    if value is None or isinstance(value, bool):
        return None
    try:
        seconds = float(value)
    except (ValueError, TypeError):
        return None
    return seconds if math.isfinite(seconds) and seconds >= 0 else None


def _is_transient_command_sync_error(error: Exception) -> bool:
    if isinstance(error, (discord.RateLimited, discord.DiscordServerError)):
        return True
    if isinstance(error, (discord.HTTPException, aiohttp.ClientResponseError)):
        return error.status in (408, 429) or 500 <= error.status < 600
    return isinstance(error, (aiohttp.ClientError, TimeoutError, ConnectionError, socket.gaierror))


class BackloggdBot(commands.Bot):
    def __init__(
        self,
        *,
        command_sync_retry_delays: tuple[float, ...] = _DEFAULT_COMMAND_SYNC_RETRY_DELAYS,
        command_sync_retry_budget: float = _DEFAULT_COMMAND_SYNC_RETRY_BUDGET,
        monotonic_clock: Optional[Callable[[], float]] = None,
        alert_webhook_url: Optional[str] = None,
        alert_owner_dm: Optional[bool] = None,
    ):
        intents = discord.Intents.default()
        intents.members = True          # Required for server-specific member queries
        intents.message_content = False  # Slash commands only; message content is unused

        super().__init__(
            command_prefix="!",
            intents=intents,
            help_command=None,
            allowed_mentions=discord.AllowedMentions.none(),
        )

        self.db = Database(DATABASE_PATH)
        self.backloggd = BackloggdClient(
            request_delay=1.0,
            profile_ttl=3600,
            cache_path=Path(DATABASE_PATH).parent / 'sdk-cache' / 'backloggd_cache.db',
        )
        self.library_sync = LibrarySyncService(max_workers=2)
        self.guild_membership = GuildMembershipService()
        self.cooldowns = GuildCooldownService(db_provider=lambda: self.db)
        self._alert_webhook_url: str = (
            ALERT_WEBHOOK_URL if alert_webhook_url is None else str(alert_webhook_url).strip()
        )
        self._alert_owner_dm: bool = (
            ALERT_OWNER_DM if alert_owner_dm is None else bool(alert_owner_dm)
        )
        self.session_health = UpstreamSessionHealth(
            alert_callback=self._dispatch_session_health_alert,
        )
        self._close_task: Optional[asyncio.Task] = None
        self._command_sync_task: Optional[asyncio.Task] = None
        self._closing: bool = False
        self._has_been_ready: bool = False
        self._disconnected_since: Optional[float] = None
        self._monotonic_clock: Callable[[], float] = monotonic_clock or time.monotonic
        self._reconnect_grace_seconds: float = _RECONNECT_GRACE_SECONDS
        self._command_sync_retry_delays = tuple(float(delay) for delay in command_sync_retry_delays)
        self._command_sync_retry_budget = float(command_sync_retry_budget)
        self.tree.error(self.on_app_command_error)

    async def on_app_command_error(
        self,
        interaction: discord.Interaction,
        error: app_commands.AppCommandError,
    ) -> None:
        await handle_app_command_error(interaction, error, log=logger)

    async def _start_command_sync(self) -> None:
        if self._closing or (self._command_sync_task is not None and not self._command_sync_task.done()):
            return

        sync_guild = discord.Object(id=DEV_GUILD_ID) if DEV_GUILD_ID else None
        scope = f"dev guild {sync_guild.id}" if sync_guild else "global"
        if not self.application_id:
            logger.warning(
                "Skipped slash command sync (%s): application_id is unavailable; registration stopped.",
                scope,
            )
            return
        await self._run_command_sync(sync_guild, scope)

    async def _run_command_sync(
        self,
        sync_guild: Optional[discord.Object],
        scope: str,
        *,
        attempt: int = 1,
        delay: float = 0.0,
        deadline: Optional[float] = None,
    ) -> None:
        loop = asyncio.get_running_loop()
        delays = self._command_sync_retry_delays
        max_attempts = 1 + len(delays)
        while not self._closing:
            budget = asyncio.timeout_at(deadline)
            try:
                async with budget:
                    if deadline is not None:
                        await asyncio.sleep(delay)
                        if self._closing:
                            return
                        if loop.time() >= deadline:
                            raise TimeoutError("retry budget elapsed")
                    elif sync_guild is not None:
                        self.tree.copy_global_to(guild=sync_guild)
                    sync_options = {"guild": sync_guild} if sync_guild is not None else {}
                    synced = await self.tree.sync(**sync_options)
            except asyncio.CancelledError:
                logger.info("Slash command sync (%s) cancelled; registration stopped.", scope)
                raise
            except Exception as error:
                if budget.expired() or (deadline is not None and loop.time() >= deadline):
                    reason = "retry budget expired"
                elif not _is_transient_command_sync_error(error):
                    reason = "failed permanently"
                elif attempt == max_attempts:
                    reason = "retries exhausted"
                else:
                    if deadline is None:
                        deadline = loop.time() + self._command_sync_retry_budget
                    delay = max(delays[attempt - 1], _retry_after_seconds(error) or 0.0)
                    if self._closing:
                        return
                    if delay < deadline - loop.time():
                        logger.warning(
                            "Slash command sync (%s) attempt %d/%d failed (%s); scheduling retry attempt %d/%d in %.2fs.",
                            scope, attempt, max_attempts, error, attempt + 1, max_attempts, delay,
                        )
                        if attempt == 1:
                            # Let startup proceed while the scheduled retries run.
                            self._command_sync_task = asyncio.create_task(
                                self._run_command_sync(sync_guild, scope, attempt=2, delay=delay, deadline=deadline)
                            )
                            return
                        attempt += 1
                        continue
                    reason = f"retry skipped because {delay:.2f}s delay exceeds retry budget"

                logger.error(
                    "Slash command sync (%s) attempt %d/%d: %s (%s); registration stopped.",
                    scope, attempt, max_attempts, reason, error,
                )
                return

            logger.info(
                "Synced %d slash commands (%s) on attempt %d/%d; registration complete.",
                len(synced), scope, attempt, max_attempts,
            )
            return

    async def _close_command_sync(self) -> None:
        task = self._command_sync_task
        if task is not None:
            if not task.done() and not task.cancelling():
                task.cancel()
            # The bot's owned shutdown task already shields this drain from callers.
            result, = await asyncio.gather(task, return_exceptions=True)
            if isinstance(result, Exception):
                logger.debug("Command sync task raised during shutdown", exc_info=result)
            self._command_sync_task = None

    async def setup_hook(self) -> None:
        """Called automatically before bot logs into Discord."""
        # 1. Connect SQLite database
        await self.db.connect()

        # 2. Load Cogs
        initial_extensions = [
            "bot.cogs.general",
            "bot.cogs.account",
            "bot.cogs.games",
            "bot.cogs.users",
            "bot.cogs.social",
            "bot.cogs.discovery",
            "bot.cogs.stats",
            "bot.cogs.feed",
        ]
        for ext in initial_extensions:
            try:
                await self.load_extension(ext)
                logger.info("Loaded extension: %s", ext)
            except Exception as e:
                logger.error("Failed to load required extension %s: %s", ext, e, exc_info=True)
                raise RuntimeError(f"Required extension {ext} failed to load") from e

        # 3. Slash Command Sync
        await self._start_command_sync()

    async def close(self) -> None:
        """Idempotently drain command sync, cogs, shared services, SDK, DB, and Discord client."""
        self._closing = True
        if self._close_task is None:
            logger.info("Shutting down BackloggdBot...")
            discord_close = super().close

            async def _shutdown() -> None:
                errors: list[Exception] = []
                stages: list[tuple[str, Any]] = [
                    ("command_sync", self._close_command_sync),
                ]
                for cog_name in ("Feed", "Stats", "Account", "Social"):
                    cog = self.get_cog(cog_name)
                    close_fn: Any = getattr(cog, "close", None)
                    if cog is not None and callable(close_fn):
                        stages.append((f"cog:{cog_name}", close_fn))
                stages.extend([
                    ("session_health", self.session_health.close),
                    ("guild_membership", self.guild_membership.close),
                    ("library_sync", self.library_sync.close),
                    ("backloggd", self.backloggd.close),
                    ("db", self.db.close),
                    ("discord", discord_close),
                ])
                for stage_name, closer in stages:
                    try:
                        await closer()
                    except asyncio.CancelledError:
                        raise
                    except Exception as exc:
                        logger.error(
                            "Error during bot shutdown stage %s",
                            stage_name,
                            exc_info=exc,
                        )
                        errors.append(exc)
                if errors:
                    raise errors[0]

            self._close_task = asyncio.create_task(_shutdown())

        await await_owned_task(self._close_task)

    async def _dispatch_session_health_alert(self, event: str, details: dict) -> None:
        """Send a one-shot, redacted alert via optional webhook and/or owner DM."""
        if self._closing:
            return
        webhook_url = self._alert_webhook_url
        owner_dm = self._alert_owner_dm
        if not webhook_url and not owner_dm:
            return

        kind = str(details.get("kind") or "upstream")
        if event == "recovered":
            failures = int(details.get("failure_count") or 0)
            message = (
                f"✅ **Backloggd bot recovered:** upstream scraping health returned to normal "
                f"(previous state: `{kind}`, {failures} error(s))."
            )
        else:
            error_type = str(details.get("error_type") or "Error")
            hint = (
                " Check or refresh `BACKLOGGD_SESSION` if this persists."
                if details.get("is_cookie_issue")
                else " Feed polling will retry automatically after backoff."
            )
            message = (
                f"⚠️ **Backloggd bot upstream degraded (`{kind}`):** "
                f"detected `{error_type}`.{hint}"
            )

        if webhook_url:
            try:
                timeout = aiohttp.ClientTimeout(total=5.0)
                async with aiohttp.ClientSession(timeout=timeout) as session:
                    async with session.post(webhook_url, json={"content": message}) as resp:
                        if resp.status >= 400:
                            logger.warning(
                                "Session health alert webhook returned HTTP %d",
                                resp.status,
                            )
            except asyncio.CancelledError:
                raise
            except Exception as error:
                logger.warning(
                    "Could not send session health alert webhook: %s",
                    type(error).__name__,
                )

        if owner_dm:
            try:
                app_info = await self.application_info()
                owner = getattr(app_info, "owner", None)
                team = getattr(app_info, "team", None)
                if owner is None and team is not None:
                    owner = getattr(team, "owner", None)
                if owner is not None:
                    await owner.send(message)
            except asyncio.CancelledError:
                raise
            except Exception as error:
                logger.warning(
                    "Could not send session health owner DM alert: %s",
                    type(error).__name__,
                )

    def get_readiness_status(self) -> tuple[int, str, bool]:
        """Return (http_status, state_name, ready) from local bot and DB state."""
        if self._closing or self.is_closed():
            return 503, "stopping", False
        if not self._has_been_ready:
            return 503, "starting", False
        if not getattr(self.db, "is_connected", False):
            return 503, "unavailable", False
        if self._disconnected_since is not None:
            elapsed = max(0.0, self._monotonic_clock() - self._disconnected_since)
            if elapsed < self._reconnect_grace_seconds:
                return 200, "reconnecting", False
            return 503, "unavailable", False
        return 200, "online", True

    async def on_ready(self) -> None:
        """Triggered when connection is established and cache is ready."""
        self._has_been_ready = True
        self._disconnected_since = None
        if self.user is not None:
            logger.info("Logged in as %s (ID: %d)", self.user.name, self.user.id)
        feed_cog: Any = self.get_cog("Feed")
        if feed_cog is not None:
            await feed_cog.pipeline.reconcile_legacy_enrollments()
        activity = discord.Activity(
            type=discord.ActivityType.watching,
            name="games on Backloggd | /game"
        )
        await self.change_presence(activity=activity)

    async def on_disconnect(self) -> None:
        """Record first disconnect time after initial readiness for reconnect grace tracking."""
        if self._has_been_ready and self._disconnected_since is None:
            self._disconnected_since = self._monotonic_clock()

    async def on_resumed(self) -> None:
        """Restore readiness when an existing Discord session resumes."""
        self._disconnected_since = None

    async def on_guild_remove(self, guild: discord.Guild) -> None:
        """Pause feed polling and evict in-memory caches when the bot leaves a server."""
        guild_id = getattr(guild, "id", None)
        if guild_id is None:
            return
        cooldowns = getattr(self, "cooldowns", None)
        if cooldowns is not None and callable(getattr(cooldowns, "invalidate_guild", None)):
            cooldowns.invalidate_guild(guild_id)
        membership = getattr(self, "guild_membership", None)
        if membership is not None and callable(getattr(membership, "evict_guild", None)):
            membership.evict_guild(guild_id)
        feed_cog = self.get_cog("Feed")
        pipeline = getattr(feed_cog, "pipeline", None)
        if pipeline is not None and callable(getattr(pipeline, "forget_guild", None)):
            pipeline.forget_guild(guild_id)
        db = getattr(self, "db", None)
        if db is None or not getattr(db, "is_connected", False):
            return
        try:
            paused = await db.unset_feed_channel(guild_id)
            digest_removed = False
            if hasattr(db, "unset_guild_digest"):
                digest_removed = await db.unset_guild_digest(guild_id)
            logger.info(
                "Removed from guild %d; feed paused=%s digest_removed=%s",
                guild_id,
                bool(paused),
                bool(digest_removed),
            )
        except Exception:
            logger.warning(
                "Failed to pause feed subscriptions after removal from guild %d",
                guild_id,
                exc_info=True,
            )
