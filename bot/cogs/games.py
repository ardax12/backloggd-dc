"""GamesCog: Handles /game with live Discord autocomplete and server score shelf."""

from datetime import datetime, timezone
import discord
from discord import app_commands
from discord.ext import commands
import logging
from typing import List, Optional
from uuid import uuid4
from bot.ui.embeds import create_game_embed, create_all_ratings_embed
from bot.ui.views import LinkButtonView, GameCardView, RatingsView
from bot.ui.helpers import build_backloggd_game_url
from bot.services.freshness import (
    ensure_profiles_fresh,
    format_aggregate_freshness,
    summarize_aggregate_freshness,
)

logger = logging.getLogger("backloggd.cogs.games")

class GamesCog(commands.Cog, name="Games"):
    def __init__(self, bot, *, clock=None):
        self.bot = bot
        self.clock = clock or (lambda: datetime.now(timezone.utc))
        # Lightweight in-memory cache for autocomplete queries to avoid hammering search
        self._autocomplete_cache = {}
        self._autocomplete_slugs = {}

    async def game_autocomplete(
        self,
        interaction: discord.Interaction,
        current: str
    ) -> List[app_commands.Choice[str]]:
        """Provides live search suggestions as the user types in /game."""
        clean_query = current.strip()
        if len(clean_query) < 2:
            return []

        # Check in-memory autocomplete cache
        if clean_query.lower() in self._autocomplete_cache:
            return self._autocomplete_cache[clean_query.lower()]

        # Prevent unbounded memory growth by pruning cache if it grows too large
        if len(self._autocomplete_cache) > 500:
            self._autocomplete_cache.clear()
            self._autocomplete_slugs.clear()

        try:
            results = await self.bot.backloggd.search_games(clean_query)
            choices = []
            for g in results[:15]:  # Discord limit is 25, 15 keeps it fast
                name = f"{g.title} ({g.release_year})" if g.release_year else g.title
                # Discord choice name limit is 100 characters
                if len(name) > 100:
                    name = name[:97] + "..."
                token = f"g{uuid4().hex}"
                self._autocomplete_slugs[token] = g.slug
                choices.append(app_commands.Choice(name=name, value=token))

            self._autocomplete_cache[clean_query.lower()] = choices
            return choices
        except Exception as e:
            logger.warning("Autocomplete search error for '%s': %s", clean_query, e)
            return []

    async def _guild_freshness_note(self, member_ids: List[int]) -> Optional[str]:
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

    @app_commands.command(name="game", description="Search for a game and view Backloggd ratings & server member scores")
    @app_commands.describe(title="Type game title to search with live autocomplete")
    @app_commands.autocomplete(title=game_autocomplete)
    async def game(self, interaction: discord.Interaction, title: str):
        await interaction.response.defer()

        # Autocomplete values are opaque tokens so free text is always searched.
        query = title.strip()
        try:
            slug = self._autocomplete_slugs.get(query)
            if not slug:
                search_results = await self.bot.backloggd.search_games(query)
                if not search_results:
                    await interaction.followup.send(
                        f"❌ No games found matching **{query}** on Backloggd.",
                        ephemeral=True
                    )
                    return
                if len(search_results) > 1:
                    candidates = [
                        f"{game.title} ({game.release_year})" if getattr(game, "release_year", None) else game.title
                        for game in search_results[:5]
                    ]
                    options = "; ".join(candidates)
                    await interaction.followup.send(
                        f"I found several games matching **{query}**: {options}. Select the game from the autocomplete list and run `/game` again.",
                        ephemeral=True,
                    )
                    return
                slug = search_results[0].slug

            game_data = await self.bot.backloggd.get_game(slug)
            if not game_data:
                await interaction.followup.send(
                    f"❌ Could not retrieve details for game: **{slug}**.",
                    ephemeral=True
                )
                return

            # Cache the game metadata in SQLite
            await self.bot.db.upsert_game(
                slug=game_data.slug,
                title=game_data.title,
                cover_url=game_data.cover_url,
                release_year=game_data.release_year,
                community_rating=game_data.community_rating
            )

            # Retrieve scores from server members who linked their Backloggd account
            guild_member_ids = [m.id for m in interaction.guild.members] if interaction.guild else [interaction.user.id]
            freshness_note = await self._guild_freshness_note(guild_member_ids)
            server_ratings = await self.bot.db.get_server_ratings_for_game(guild_member_ids, game_data.slug)

            guild_name = interaction.guild.name if interaction.guild else "Server"
            embed = create_game_embed(
                game_data,
                server_ratings,
                guild_name=guild_name,
                freshness_note=freshness_note,
            )
            game_url = build_backloggd_game_url(game_data.slug)

            async def on_view_all(btn_interaction: discord.Interaction):
                ratings_view = RatingsView(game_data.title, game_data.slug, guild_name, server_ratings)
                await btn_interaction.response.send_message(
                    embed=ratings_view.embed(), view=ratings_view, ephemeral=True
                )
                ratings_view.message = await btn_interaction.original_response()

            view = GameCardView(
                game_url=game_url,
                total_ratings=len(server_ratings),
                on_view_all=on_view_all if len(server_ratings) > 12 else None
            )

            view.message = await interaction.followup.send(embed=embed, view=view, wait=True)

        except Exception as e:
            logger.error("Error looking up game '%s': %s", title, e, exc_info=True)
            await interaction.followup.send(
                f"❌ An error occurred while fetching information for **{title}**.",
                ephemeral=True
            )

async def setup(bot):
    await bot.add_cog(GamesCog(bot))
