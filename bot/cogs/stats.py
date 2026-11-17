"""StatsCog: Handles /server-top (with 5 specialty leaderboards) and /server-stats commands."""

from datetime import datetime, timezone
import asyncio
import discord
from discord import app_commands
from discord.ext import commands
import logging
from typing import List, Literal, Optional, Sequence
from bot.ui.embeds import create_server_top_embed, create_server_stats_embed
from bot.services.guild_membership import (
    GUILD_MEMBERSHIP_UNAVAILABLE_MESSAGE,
    resolve_guild_member_ids,
)
from bot.command_errors import defer_interaction, send_private_followup, send_public_followup
from bot.services.freshness import resolve_guild_aggregate_freshness

logger = logging.getLogger("backloggd.cogs.stats")

LeaderboardMode = Literal[
    "Highest Rated",
    "Most Played",
    "Most Polarizing",
    "Hidden Gems",
    "Server Backlog Shame List"
]

class StatsCog(commands.Cog, name="Stats"):
    def __init__(
        self,
        bot,
        *,
        clock=None,
        played_count_timeout: float = 15.0,
        profile_count_concurrency: int = 2,
    ):
        self.bot = bot
        self.clock = clock or (lambda: datetime.now(timezone.utc))
        self.played_count_timeout = max(0.0, float(played_count_timeout))
        self.profile_count_concurrency = max(1, int(profile_count_concurrency))
        self._profile_count_slots = asyncio.Semaphore(self.profile_count_concurrency)
        self._closing = False
        self._profile_count_workers: set[asyncio.Task] = set()
        self._active_count_operations: set[asyncio.Task] = set()
        self._close_task: asyncio.Task | None = None

    async def close(self) -> None:
        """Stop count work and wait for all SDK workers to unwind."""
        self._closing = True
        current = asyncio.current_task()
        if self._close_task is None:
            self._close_task = asyncio.create_task(self._drain_count_work())
        drain_task = self._close_task
        if drain_task is current:
            return
        cancelled: Optional[asyncio.CancelledError] = None
        while not drain_task.done():
            try:
                await asyncio.shield(drain_task)
            except asyncio.CancelledError as exc:
                cancelled = exc
        if cancelled is not None:
            raise cancelled

    async def _drain_count_work(self) -> None:
        current = asyncio.current_task()
        operations = [
            task for task in self._active_count_operations
            if task is not current and not task.done()
        ]
        for task in operations:
            task.cancel()
        if operations:
            # Each count operation owns cancellation and gathering of its
            # workers. Let that cleanup run once before handling any orphan.
            await asyncio.gather(*operations, return_exceptions=True)

        workers = [
            task for task in self._profile_count_workers
            if task is not current and not task.done()
        ]
        for task in workers:
            task.cancel()
        if workers:
            await asyncio.gather(*workers, return_exceptions=True)

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

    async def _run_played_tab_counts(
        self,
        member_ids: List[int],
        *,
        profiles: Optional[Sequence[dict]] = None,
        timeout: Optional[float] = None,
    ) -> dict:
        """Sum public profile Played-tab totals with a bounded worker pool and deadline."""
        if profiles is None:
            profiles = await self.bot.db.get_guild_linked_profiles(member_ids)
        profile_list = list(profiles)
        if not profile_list:
            return {
                "total_logged": 0,
                "logged_profiles_counted": 0,
                "logged_profiles_missing": 0,
            }

        deadline = self.played_count_timeout if timeout is None else max(0.0, float(timeout))
        results: dict[int, int] = {}
        next_idx = 0
        stop_pulling = False

        async def worker() -> None:
            nonlocal next_idx
            while not stop_pulling:
                idx = next_idx
                if idx >= len(profile_list):
                    return
                next_idx += 1
                username = profile_list[idx]["display_username"]
                try:
                    async with self._profile_count_slots:
                        if stop_pulling:
                            return
                        public_profile = await self.bot.backloggd.get_user_profile(username)
                    count = getattr(public_profile, "played_count", None)
                    if isinstance(count, int) and not isinstance(count, bool) and count >= 0:
                        results[idx] = count
                except asyncio.CancelledError:
                    raise
                except Exception as error:
                    logger.warning("Could not read Played-tab count for %s: %s", username, type(error).__name__)

        worker_count = min(self.profile_count_concurrency, len(profile_list))
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

        counted = len(results)
        return {
            "total_logged": sum(results.values()),
            "logged_profiles_counted": counted,
            "logged_profiles_missing": len(profile_list) - counted,
        }

    async def _resolve_stats_context(self, member_ids: List[int]) -> dict:
        context_getter = getattr(self.bot.db, "get_guild_stats_context", None)
        if context_getter is not None:
            return await context_getter(member_ids)
        profile_getter = (
            getattr(self.bot.db, "get_guild_linked_profiles_with_sync_info", None)
            or getattr(self.bot.db, "get_guild_linked_profiles", None)
        )
        if profile_getter is None:
            return {"linked_members": None, "profile_keys": None, "profiles": []}
        profiles = list(await profile_getter(member_ids))
        return {
            "linked_members": None,
            "profile_keys": [row["profile_key"] for row in profiles],
            "profiles": profiles,
        }

    @app_commands.command(name="server-top", description="View server game rankings and specialty leaderboards")
    @app_commands.describe(mode="Choose leaderboard mode (e.g. Highest Rated, Most Polarizing, Shame List)")
    async def server_top(
        self,
        interaction: discord.Interaction,
        mode: LeaderboardMode = "Highest Rated"
    ):
        await defer_interaction(interaction)

        if not interaction.guild:
            await send_private_followup(interaction, "❌ This command must be run inside a Discord server.", log=logger)
            return

        guild_member_ids = await resolve_guild_member_ids(self.bot, interaction.guild)
        if guild_member_ids is None:
            await send_private_followup(interaction, GUILD_MEMBERSHIP_UNAVAILABLE_MESSAGE, log=logger)
            return
        guild_name = interaction.guild.name
        context = await self._resolve_stats_context(guild_member_ids)
        profile_keys = context["profile_keys"]
        freshness_note = await resolve_guild_aggregate_freshness(
            self.bot,
            guild_member_ids,
            now=self.clock(),
            profiles=context["profiles"],
            profiles_are_linked=True,
            require_subscribed=True,
        )

        entries = []
        mode_key = "highest_rated"
        title = "Highest Rated Games"

        if mode == "Highest Rated":
            mode_key = "highest_rated"
            title = "⭐ Highest Rated Games"
            helper = getattr(self.bot.db, "_get_server_top_rated_for_profiles", None)
            if helper is not None and profile_keys is not None:
                entries = await helper(profile_keys, limit=10, min_ratings=2)
            else:
                entries = await self.bot.db.get_server_top_rated(guild_member_ids, limit=10, min_ratings=2)

        elif mode == "Most Played":
            mode_key = "most_played"
            title = "🎮 Most Played Games"
            helper = getattr(self.bot.db, "_get_server_most_played_for_profiles", None)
            if helper is not None and profile_keys is not None:
                entries = await helper(profile_keys, limit=10)
            else:
                entries = await self.bot.db.get_server_most_played(guild_member_ids, limit=10)

        elif mode == "Most Polarizing":
            mode_key = "polarizing"
            title = "🔥 Most Polarizing / Divisive Games"
            helper = getattr(self.bot.db, "_get_server_polarizing_for_profiles", None)
            if helper is not None and profile_keys is not None:
                entries = await helper(profile_keys, limit=10, min_ratings=2)
            else:
                entries = await self.bot.db.get_server_polarizing(guild_member_ids, limit=10, min_ratings=2)

        elif mode == "Hidden Gems":
            mode_key = "hidden_gems"
            title = "💎 Hidden Gems (Server Rating 4.5+)"
            helper = getattr(self.bot.db, "_get_server_hidden_gems_for_profiles", None)
            if helper is not None and profile_keys is not None:
                entries = await helper(profile_keys, limit=10)
            else:
                entries = await self.bot.db.get_server_hidden_gems(guild_member_ids, limit=10)

        elif mode == "Server Backlog Shame List":
            mode_key = "shame_list"
            title = "🪦 Server Backlog Shame List (Most Unplayed)"
            helper = getattr(self.bot.db, "_get_server_shame_list_for_profiles", None)
            if helper is not None and profile_keys is not None:
                entries = await helper(profile_keys, limit=10)
            else:
                entries = await self.bot.db.get_server_shame_list(guild_member_ids, limit=10)

        embed = create_server_top_embed(
            title=title,
            entries=entries,
            mode=mode_key,
            guild_name=guild_name,
            freshness_note=freshness_note,
        )

        await send_public_followup(interaction, embed=embed)

    @app_commands.command(name="server-stats", description="View gaming community statistics for this Discord server")
    async def server_stats(self, interaction: discord.Interaction):
        await defer_interaction(interaction)

        if not interaction.guild:
            await send_private_followup(interaction, "❌ This command must be run inside a Discord server.", log=logger)
            return

        guild_member_ids = await resolve_guild_member_ids(self.bot, interaction.guild)
        if guild_member_ids is None:
            await send_private_followup(interaction, GUILD_MEMBERSHIP_UNAVAILABLE_MESSAGE, log=logger)
            return
        context = await self._resolve_stats_context(guild_member_ids)
        freshness_note = await resolve_guild_aggregate_freshness(
            self.bot,
            guild_member_ids,
            now=self.clock(),
            profiles=context["profiles"],
            profiles_are_linked=True,
            require_subscribed=True,
        )
        stats_helper = getattr(self.bot.db, "_get_server_stats_for_profiles", None)
        if (
            stats_helper is not None
            and context["profile_keys"] is not None
            and context["linked_members"] is not None
        ):
            stats = await stats_helper(
                context["profile_keys"],
                linked_members=context["linked_members"],
                include_library_row_counts=False,
            )
        else:
            stats = await self.bot.db.get_server_stats(guild_member_ids)
        # Cached library rows include wishlist/backlog entries and can have
        # viewer-specific statuses. The displayed total comes from profile tabs.
        stats.update(await self._played_tab_counts(guild_member_ids, profiles=context["profiles"]))

        embed = create_server_stats_embed(
            stats,
            guild_name=interaction.guild.name,
            freshness_note=freshness_note,
        )
        await send_public_followup(interaction, embed=embed)

async def setup(bot):
    await bot.add_cog(StatsCog(bot))
