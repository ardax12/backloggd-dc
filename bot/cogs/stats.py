"""StatsCog: Handles /server-top (with 5 specialty leaderboards) and /server-stats commands."""

from datetime import datetime, timezone
import asyncio
import discord
from discord import app_commands
from discord.ext import commands
import logging
from typing import List, Literal, Optional
from bot.ui.embeds import create_server_top_embed, create_server_stats_embed
from bot.services.guild_membership import (
    GUILD_MEMBERSHIP_UNAVAILABLE_MESSAGE,
    resolve_guild_member_ids,
)
from bot.services.freshness import (
    ensure_profiles_fresh,
    format_aggregate_freshness,
    summarize_aggregate_freshness,
)

logger = logging.getLogger("backloggd.cogs.stats")

LeaderboardMode = Literal[
    "Highest Rated",
    "Most Played",
    "Most Polarizing",
    "Hidden Gems",
    "Server Backlog Shame List"
]

class StatsCog(commands.Cog, name="Stats"):
    def __init__(self, bot, *, clock=None):
        self.bot = bot
        self.clock = clock or (lambda: datetime.now(timezone.utc))
        self._profile_count_slots = asyncio.Semaphore(2)

    async def _played_tab_counts(self, member_ids: List[int]) -> dict:
        """Sum public profile Played-tab totals, independently of library status parsing."""
        profiles = await self.bot.db.get_guild_linked_profiles(member_ids)

        async def count_profile(profile):
            username = profile["display_username"]
            try:
                async with self._profile_count_slots:
                    public_profile = await self.bot.backloggd.get_user_profile(username)
                count = getattr(public_profile, "played_count", None)
                if isinstance(count, int) and not isinstance(count, bool) and count >= 0:
                    return count
            except Exception as error:
                logger.warning("Could not read Played-tab count for %s: %s", username, type(error).__name__)
            return None

        counts = await asyncio.gather(*(count_profile(profile) for profile in profiles))
        return {
            "total_logged": sum(count for count in counts if count is not None),
            "logged_profiles_counted": sum(count is not None for count in counts),
            "logged_profiles_missing": sum(count is None for count in counts),
        }

    async def _server_freshness_note(self, member_ids: List[int]) -> Optional[str]:
        profile_getter = (
            getattr(self.bot.db, "get_guild_linked_profiles_with_sync_info", None)
            or getattr(self.bot.db, "get_guild_linked_profiles", None)
        )
        if profile_getter is None:
            return None
        profiles = await profile_getter(member_ids)
        if not profiles:
            return "No linked Backloggd profiles in cache yet."
        statuses = await ensure_profiles_fresh(
            self.bot, profiles, now=self.clock(), require_subscribed=True
        )
        return format_aggregate_freshness(summarize_aggregate_freshness(statuses.values()))

    @app_commands.command(name="server-top", description="View server game rankings and specialty leaderboards")
    @app_commands.describe(mode="Choose leaderboard mode (e.g. Highest Rated, Most Polarizing, Shame List)")
    async def server_top(
        self,
        interaction: discord.Interaction,
        mode: LeaderboardMode = "Highest Rated"
    ):
        await interaction.response.defer()

        if not interaction.guild:
            await interaction.followup.send("❌ This command must be run inside a Discord server.", ephemeral=True)
            return

        guild_member_ids = await resolve_guild_member_ids(self.bot, interaction.guild)
        if guild_member_ids is None:
            await interaction.followup.send(GUILD_MEMBERSHIP_UNAVAILABLE_MESSAGE, ephemeral=True)
            return
        guild_name = interaction.guild.name
        freshness_note = await self._server_freshness_note(guild_member_ids)

        entries = []
        mode_key = "highest_rated"
        title = "Highest Rated Games"

        if mode == "Highest Rated":
            mode_key = "highest_rated"
            title = "⭐ Highest Rated Games"
            entries = await self.bot.db.get_server_top_rated(guild_member_ids, limit=10, min_ratings=2)

        elif mode == "Most Played":
            mode_key = "most_played"
            title = "🎮 Most Played Games"
            entries = await self.bot.db.get_server_most_played(guild_member_ids, limit=10)

        elif mode == "Most Polarizing":
            mode_key = "polarizing"
            title = "🔥 Most Polarizing / Divisive Games"
            entries = await self.bot.db.get_server_polarizing(guild_member_ids, limit=10, min_ratings=2)

        elif mode == "Hidden Gems":
            mode_key = "hidden_gems"
            title = "💎 Hidden Gems (Server Rating 4.5+)"
            entries = await self.bot.db.get_server_hidden_gems(guild_member_ids, limit=10)

        elif mode == "Server Backlog Shame List":
            mode_key = "shame_list"
            title = "🪦 Server Backlog Shame List (Most Unplayed)"
            entries = await self.bot.db.get_server_shame_list(guild_member_ids, limit=10)

        embed = create_server_top_embed(
            title=title,
            entries=entries,
            mode=mode_key,
            guild_name=guild_name,
            freshness_note=freshness_note,
        )

        await interaction.followup.send(embed=embed)

    @app_commands.command(name="server-stats", description="View gaming community statistics for this Discord server")
    async def server_stats(self, interaction: discord.Interaction):
        await interaction.response.defer()

        if not interaction.guild:
            await interaction.followup.send("❌ This command must be run inside a Discord server.", ephemeral=True)
            return

        guild_member_ids = await resolve_guild_member_ids(self.bot, interaction.guild)
        if guild_member_ids is None:
            await interaction.followup.send(GUILD_MEMBERSHIP_UNAVAILABLE_MESSAGE, ephemeral=True)
            return
        freshness_note = await self._server_freshness_note(guild_member_ids)
        stats = await self.bot.db.get_server_stats(guild_member_ids)
        # Cached library rows include wishlist/backlog entries and can have
        # viewer-specific statuses. The displayed total comes from profile tabs.
        stats.update(await self._played_tab_counts(guild_member_ids))

        embed = create_server_stats_embed(
            stats,
            guild_name=interaction.guild.name,
            freshness_note=freshness_note,
        )
        await interaction.followup.send(embed=embed)

async def setup(bot):
    await bot.add_cog(StatsCog(bot))
