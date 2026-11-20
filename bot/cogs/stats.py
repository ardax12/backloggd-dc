"""StatsCog: Handles /server-top (with 5 specialty leaderboards) and /server-stats commands."""

import asyncio
import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, List, Literal, Optional, Sequence

import discord
from backloggd.exceptions import ResourceNotFoundError, UserNotFoundError
from discord import app_commands
from discord.ext import commands

from bot.command_errors import defer_interaction, send_private_followup, send_public_followup
from bot.services.cooldowns import guild_cooldown
from bot.services.freshness import (
    FRESHNESS_WINDOW_SECONDS,
    resolve_guild_aggregate_freshness,
    valid_played_count,
)
from bot.services.guild_membership import (
    GUILD_MEMBERSHIP_UNAVAILABLE_MESSAGE,
    resolve_guild_member_ids,
)
from bot.services.library_sync import parse_sync_timestamp
from bot.services.session_health import report_upstream_failure, report_upstream_success
from bot.services.task_lifecycle import TaskDrainGroup
from bot.ui.embeds import (
    create_playing_embed,
    create_server_recap_embed,
    create_server_stats_embed,
    create_server_top_embed,
)
from bot.ui.views import PaginatedEmbedView, ServerTopView

logger = logging.getLogger("backloggd.cogs.stats")

LeaderboardMode = Literal[
    "Highest Rated",
    "Most Played",
    "Most Polarizing",
    "Hidden Gems",
    "Server Backlog Shame List"
]
RecapPeriod = Literal["7 days", "30 days"]

_LEADERBOARD_MODES: tuple[tuple[str, str, str, str], ...] = (
    ("Highest Rated", "highest_rated", "⭐ Highest Rated Games", "⭐"),
    ("Most Played", "most_played", "🎮 Most Played Games", "🎮"),
    ("Most Polarizing", "polarizing", "🔥 Most Polarizing / Divisive Games", "🔥"),
    ("Hidden Gems", "hidden_gems", "💎 Hidden Gems (Server Rating 4.5+)", "💎"),
    ("Server Backlog Shame List", "shame_list", "🪦 Server Backlog Shame List (Most Unplayed)", "🪦"),
)
_LEADERBOARD_MODE_SPECS: dict[str, tuple[str, str, str]] = {
    name: (mode_key, title, emoji)
    for name, mode_key, title, emoji in _LEADERBOARD_MODES
}
_SERVER_TOP_SELECT_MODES: tuple[tuple[str, str], ...] = tuple(
    (name, emoji) for name, _, _, emoji in _LEADERBOARD_MODES
)

class StatsCog(commands.Cog, name="Stats"):
    def __init__(
        self,
        bot,
        *,
        clock=None,
        played_count_timeout: float = 15.0,
        profile_count_concurrency: int = 2,
        played_count_freshness_seconds: float = FRESHNESS_WINDOW_SECONDS,
    ):
        self.bot = bot
        self.clock = clock or (lambda: datetime.now(timezone.utc))
        self.played_count_timeout = max(0.0, float(played_count_timeout))
        self.played_count_freshness_seconds = max(0.0, float(played_count_freshness_seconds))
        self.profile_count_concurrency = max(1, int(profile_count_concurrency))
        self._profile_count_slots = asyncio.Semaphore(self.profile_count_concurrency)
        self._drain_group = TaskDrainGroup()
        self._profile_count_workers: set[asyncio.Task] = self._drain_group.tasks
        self._active_count_operations: set[asyncio.Task] = set()

    @property
    def _closing(self) -> bool:
        return self._drain_group.closing

    @_closing.setter
    def _closing(self, value: bool) -> None:
        self._drain_group.closing = bool(value)

    async def close(self) -> None:
        """Stop count work and wait for all SDK workers to unwind."""
        # Each count operation owns cancellation and gathering of its workers.
        # Drain active operations once first before cancelling any orphan workers.
        await self._drain_group.close(
            before_drain=lambda caller: TaskDrainGroup.cancel_and_drain(
                self._active_count_operations, exclude=(caller,), repeat=False
            ),
            task_source=lambda: self._profile_count_workers,
        )

    async def cog_unload(self) -> None:
        await self.close()

    async def _played_tab_counts(
        self,
        member_ids: List[int],
        *,
        profiles: Optional[Sequence[dict]] = None,
        timeout: Optional[float] = None,
    ) -> dict:
        if self._closing:
            raise asyncio.CancelledError
        operation = asyncio.current_task()
        if operation is not None:
            self._active_count_operations.add(operation)
        try:
            return await self._run_played_tab_counts(
                member_ids, profiles=profiles, timeout=timeout,
            )
        finally:
            if operation is not None:
                self._active_count_operations.discard(operation)

    async def _fetch_authoritative_played_count(self, username: str) -> Optional[int]:
        # The SDK's cached profile has no observation timestamp.
        public_profile = await self.bot.backloggd.get_user_profile(username, force_refresh=True)
        if public_profile is None:
            return None
        report_upstream_success(self.bot)
        count = valid_played_count(getattr(public_profile, "played_count", None))
        if count is not None:
            return count
        counts_method: Optional[Callable[..., Any]] = getattr(
            self.bot.backloggd, "get_user_collection_counts", None
        )
        if callable(counts_method):
            collection_counts = await counts_method(username)
            report_upstream_success(self.bot)
            if isinstance(collection_counts, dict):
                return valid_played_count(collection_counts.get("played"))
        return None

    async def _run_played_tab_counts(
        self,
        member_ids: List[int],
        *,
        profiles: Optional[Sequence[dict]] = None,
        timeout: Optional[float] = None,
    ) -> dict:
        """Sum public profile Played-tab totals, reusing fresh stored observations."""
        resolved_profiles: Sequence[dict] = (
            profiles
            if profiles is not None
            else (await self.bot.db.get_guild_linked_profiles(member_ids) or ())
        )
        deduped_profiles: list[tuple[str, dict]] = []
        seen_keys: set[str] = set()
        for row in resolved_profiles:
            username = row["display_username"]
            profile_key = str(row.get("profile_key") or username).strip().lower()
            if not profile_key or profile_key in seen_keys:
                continue
            seen_keys.add(profile_key)
            deduped_profiles.append((profile_key, row))

        if not deduped_profiles:
            return {
                "total_logged": 0,
                "logged_profiles_counted": 0,
                "logged_profiles_missing": 0,
                "logged_profiles_stale": 0,
            }

        current_now = parse_sync_timestamp(self.clock()) or datetime.now(timezone.utc)
        results: dict[str, int] = {}
        stale_candidates: dict[str, int] = {}
        fetch_targets: list[tuple[str, str]] = []

        for profile_key, row in deduped_profiles:
            username = row["display_username"]
            stored_count = valid_played_count(row.get("played_count"))
            observed_at = parse_sync_timestamp(row.get("played_count_observed_at"))
            if (
                stored_count is not None
                and observed_at is not None
                and (current_now - observed_at).total_seconds() < self.played_count_freshness_seconds
            ):
                results[profile_key] = stored_count
            else:
                if stored_count is not None:
                    stale_candidates[profile_key] = stored_count
                fetch_targets.append((profile_key, username))

        if not fetch_targets:
            return {
                "total_logged": sum(results.values()),
                "logged_profiles_counted": len(results),
                "logged_profiles_missing": 0,
                "logged_profiles_stale": 0,
            }

        deadline = self.played_count_timeout if timeout is None else max(0.0, float(timeout))
        newly_observed: dict[str, int] = {}
        next_idx = 0
        stop_pulling = False

        async def worker() -> None:
            nonlocal next_idx, stop_pulling
            while not stop_pulling:
                idx = next_idx
                if idx >= len(fetch_targets):
                    return
                next_idx += 1
                profile_key, username = fetch_targets[idx]
                try:
                    async with self._profile_count_slots:
                        if stop_pulling:
                            return
                        count = await self._fetch_authoritative_played_count(username)
                    if count is not None:
                        results[profile_key] = count
                        newly_observed[profile_key] = count
                except asyncio.CancelledError:
                    raise
                except Exception as error:
                    if not isinstance(error, (UserNotFoundError, ResourceNotFoundError)):
                        if report_upstream_failure(self.bot, error) is not None:
                            stop_pulling = True
                    logger.warning("Could not read Played-tab count for %s: %s", username, type(error).__name__)

        worker_count = min(self.profile_count_concurrency, len(fetch_targets))
        workers = [asyncio.create_task(worker()) for _ in range(worker_count)]
        self._profile_count_workers.update(workers)
        for task in workers:
            task.add_done_callback(self._profile_count_workers.discard)
        try:
            _, pending = await asyncio.wait(workers, timeout=deadline)
            if pending:
                stop_pulling = True
                for task in pending:
                    task.cancel()
            await asyncio.gather(*workers, return_exceptions=True)
        except BaseException:
            stop_pulling = True
            for task in workers:
                if not task.done():
                    task.cancel()
            await asyncio.gather(*workers, return_exceptions=True)
            raise

        updater: Optional[Callable[..., Any]] = getattr(
            self.bot.db, "update_profiles_played_counts", None
        )
        if newly_observed and callable(updater):
            try:
                await updater(newly_observed.items(), observed_at=current_now)
            except Exception:
                logger.warning("Could not persist Played-tab counts", exc_info=True)

        stale_used = 0
        for profile_key, stale_value in stale_candidates.items():
            if profile_key not in results:
                results[profile_key] = stale_value
                stale_used += 1

        counted = len(results)
        return {
            "total_logged": sum(results.values()),
            "logged_profiles_counted": counted,
            "logged_profiles_missing": len(deduped_profiles) - counted,
            "logged_profiles_stale": stale_used,
        }

    async def _load_server_top_pages(
        self,
        guild: Optional[discord.Guild],
        mode: str,
        *,
        min_ratings: int = 2,
    ) -> tuple[Optional[list[discord.Embed]], Optional[str]]:
        if not guild:
            return None, "❌ This command must be run inside a Discord server."

        spec = _LEADERBOARD_MODE_SPECS.get(mode)
        if spec is None:
            raise ValueError(f"Unsupported leaderboard mode: {mode!r}")
        mode_key, title, _ = spec

        if isinstance(min_ratings, bool) or not isinstance(min_ratings, int) or not (1 <= min_ratings <= 50):
            raise ValueError("min_ratings must be an integer between 1 and 50")

        guild_member_ids = await resolve_guild_member_ids(self.bot, guild)
        if guild_member_ids is None:
            return None, GUILD_MEMBERSHIP_UNAVAILABLE_MESSAGE

        guild_name = guild.name
        context = await self.bot.db.get_guild_stats_context(guild_member_ids)
        profile_keys = context["profile_keys"]
        freshness_note = await resolve_guild_aggregate_freshness(
            self.bot,
            guild_member_ids,
            now=self.clock(),
            profiles=context["profiles"],
            profiles_are_linked=True,
            require_subscribed=True,
        )

        page_size = 10
        max_entries = 50
        if mode == "Highest Rated":
            entries = await self.bot.db.get_server_top_rated_for_profiles(
                profile_keys, limit=max_entries, min_ratings=min_ratings
            )
        elif mode == "Most Played":
            entries = await self.bot.db.get_server_most_played_for_profiles(
                profile_keys, limit=max_entries
            )
        elif mode == "Most Polarizing":
            entries = await self.bot.db.get_server_polarizing_for_profiles(
                profile_keys, limit=max_entries, min_ratings=min_ratings
            )
        elif mode == "Hidden Gems":
            entries = await self.bot.db.get_server_hidden_gems_for_profiles(
                profile_keys, limit=max_entries
            )
        elif mode == "Server Backlog Shame List":
            entries = await self.bot.db.get_server_shame_list_for_profiles(
                profile_keys, limit=max_entries
            )
        else:
            raise ValueError(f"Unsupported leaderboard mode: {mode!r}")

        total_pages = max(1, (len(entries) + page_size - 1) // page_size)
        pages = [
            create_server_top_embed(
                title=title,
                entries=entries,
                mode=mode_key,
                guild_name=guild_name,
                freshness_note=freshness_note,
                page=page_num,
                per_page=page_size,
                min_ratings=min_ratings,
            )
            for page_num in range(1, total_pages + 1)
        ]
        return pages, None

    async def _load_server_top_embed(
        self,
        guild: Optional[discord.Guild],
        mode: str,
        *,
        min_ratings: int = 2,
    ) -> tuple[Optional[discord.Embed], Optional[str]]:
        pages, error_message = await self._load_server_top_pages(guild, mode, min_ratings=min_ratings)
        if error_message is not None or not pages:
            return None, error_message
        return pages[0], None

    @app_commands.command(name="server-top", description="View server game rankings and specialty leaderboards")
    @app_commands.describe(
        mode="Choose leaderboard mode (e.g. Highest Rated, Most Polarizing, Shame List)",
        min_ratings="Optional: Minimum ratings for Highest Rated & Most Polarizing (1-50, default 2)",
    )
    @app_commands.guild_only()
    @guild_cooldown("server-top")
    async def server_top(
        self,
        interaction: discord.Interaction,
        mode: LeaderboardMode = "Highest Rated",
        min_ratings: app_commands.Range[int, 1, 50] = 2,
    ):
        await defer_interaction(interaction)

        pages, error_message = await self._load_server_top_pages(
            interaction.guild, mode, min_ratings=min_ratings
        )
        if error_message is not None or not pages:
            await send_private_followup(
                interaction,
                error_message or "❌ This command must be run inside a Discord server.",
                log=logger,
            )
            return

        async def on_mode_change(
            select_interaction: discord.Interaction,
            requested_mode: str,
        ) -> tuple[Optional[list[discord.Embed]], Optional[str]]:
            return await self._load_server_top_pages(
                select_interaction.guild, requested_mode, min_ratings=min_ratings
            )

        author_id = getattr(getattr(interaction, "user", None), "id", None)
        view = ServerTopView(
            author_id=author_id,
            current_mode=mode,
            modes=_SERVER_TOP_SELECT_MODES,
            on_mode_change=on_mode_change,
            pages=pages,
        )
        view.message = await send_public_followup(interaction, embed=pages[0], view=view, wait=True)

    @app_commands.command(name="server-stats", description="View gaming community statistics for this Discord server")
    @app_commands.guild_only()
    @guild_cooldown("server-stats")
    async def server_stats(self, interaction: discord.Interaction):
        await defer_interaction(interaction)

        if not interaction.guild:
            await send_private_followup(interaction, "❌ This command must be run inside a Discord server.", log=logger)
            return

        guild_member_ids = await resolve_guild_member_ids(self.bot, interaction.guild)
        if guild_member_ids is None:
            await send_private_followup(interaction, GUILD_MEMBERSHIP_UNAVAILABLE_MESSAGE, log=logger)
            return
        context = await self.bot.db.get_guild_stats_context(guild_member_ids)
        freshness_note = await resolve_guild_aggregate_freshness(
            self.bot,
            guild_member_ids,
            now=self.clock(),
            profiles=context["profiles"],
            profiles_are_linked=True,
            require_subscribed=True,
        )
        stats = await self.bot.db.get_server_stats_for_profiles(
            context["profile_keys"],
            linked_members=context["linked_members"],
            include_library_row_counts=False,
        )
        # Cached library rows include wishlist/backlog entries and can have
        # viewer-specific statuses. The displayed total comes from profile tabs.
        stats.update(await self._played_tab_counts(guild_member_ids, profiles=context["profiles"]))

        embed = create_server_stats_embed(
            stats,
            guild_name=interaction.guild.name,
            freshness_note=freshness_note,
        )
        await send_public_followup(interaction, embed=embed)

    @app_commands.command(
        name="playing",
        description="See what server members are currently playing on Backloggd",
    )
    @app_commands.guild_only()
    @guild_cooldown("playing")
    async def playing(self, interaction: discord.Interaction):
        await defer_interaction(interaction)

        if not interaction.guild:
            await send_private_followup(
                interaction,
                "❌ This command must be run inside a Discord server.",
                log=logger,
            )
            return

        guild_member_ids = await resolve_guild_member_ids(self.bot, interaction.guild)
        if guild_member_ids is None:
            await send_private_followup(interaction, GUILD_MEMBERSHIP_UNAVAILABLE_MESSAGE, log=logger)
            return

        guild_name = interaction.guild.name
        context = await self.bot.db.get_guild_stats_context(guild_member_ids)
        profile_keys = context["profile_keys"]
        profiles = context["profiles"]

        if not profile_keys:
            embed = create_playing_embed(
                [],
                guild_name=guild_name,
                total_games=0,
                total_players=0,
                empty_message="*No one in this server has linked a Backloggd profile yet. Use `/link` to get started!*",
            )
            await send_public_followup(interaction, embed=embed)
            return

        freshness_note = await resolve_guild_aggregate_freshness(
            self.bot,
            guild_member_ids,
            now=self.clock(),
            profiles=profiles,
            profiles_are_linked=True,
            require_subscribed=True,
        )

        entries = await self.bot.db.get_server_currently_playing_for_profiles(profile_keys, limit=50)
        if not entries:
            if profiles and all(p.get("last_synced_at") is None for p in profiles):
                empty_msg = (
                    "*Linked Backloggd libraries are still syncing for the first time. "
                    "Check back in a moment or run `/refresh`.*"
                )
            else:
                empty_msg = (
                    "*No one in this server currently has any games marked as Playing on Backloggd.*"
                )
            embed = create_playing_embed(
                [],
                guild_name=guild_name,
                total_games=0,
                total_players=0,
                freshness_note=freshness_note,
                empty_message=empty_msg,
            )
            await send_public_followup(interaction, embed=embed)
            return

        distinct_players = {
            str(player).lower()
            for entry in entries
            for player in entry.get("players", ())
        }
        total_games = len(entries)
        total_players = len(distinct_players)
        page_size = 10
        total_pages = max(1, (total_games + page_size - 1) // page_size)
        pages = [
            create_playing_embed(
                entries[(page_num - 1) * page_size : page_num * page_size],
                guild_name=guild_name,
                total_games=total_games,
                total_players=total_players,
                page=page_num,
                total_pages=total_pages,
                freshness_note=freshness_note,
            )
            for page_num in range(1, total_pages + 1)
        ]

        if total_pages > 1:
            author_id = getattr(getattr(interaction, "user", None), "id", None)
            view = PaginatedEmbedView(author_id=author_id, pages=pages, command_name="/playing")
            view.message = await send_public_followup(interaction, embed=pages[0], view=view, wait=True)
        else:
            await send_public_followup(interaction, embed=pages[0])

    @app_commands.command(
        name="server-recap",
        description="View a recap of recent Backloggd activity announced in this server",
    )
    @app_commands.describe(period="Time window to summarize (default: 7 days)")
    @app_commands.guild_only()
    @guild_cooldown("server-stats")
    async def server_recap(
        self,
        interaction: discord.Interaction,
        period: RecapPeriod = "7 days",
    ):
        await defer_interaction(interaction)

        if not interaction.guild:
            await send_private_followup(
                interaction,
                "❌ This command must be run inside a Discord server.",
                log=logger,
            )
            return

        days = 30 if period == "30 days" else 7
        now = self.clock()
        if now.tzinfo is None:
            now = now.replace(tzinfo=timezone.utc)
        since_utc = now.astimezone(timezone.utc) - timedelta(days=days)

        recap = await self.bot.db.get_guild_activity_recap(
            interaction.guild.id,
            since_utc=since_utc,
        )
        empty_message: Optional[str] = None
        if int(recap.get("total_announcements", 0) or 0) == 0:
            feed_channel = (
                await self.bot.db.get_feed_channel(interaction.guild.id)
                if hasattr(self.bot.db, "get_feed_channel")
                else None
            )
            if feed_channel is None:
                empty_message = (
                    "*No activity feed channel is configured for this server yet. "
                    "An admin can run `/setchannel` to start recording community recaps.*"
                )
            else:
                empty_message = (
                    f"*No feed announcements were recorded in this server in the last {period}.*"
                )

        embed = create_server_recap_embed(
            recap,
            guild_name=interaction.guild.name,
            period_label=f"Last {period}",
            empty_message=empty_message,
        )
        await send_public_followup(interaction, embed=embed)


async def setup(bot):
    await bot.add_cog(StatsCog(bot))
