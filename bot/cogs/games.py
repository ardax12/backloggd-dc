"""GamesCog: Handles /game with live Discord autocomplete and server score shelf."""

import discord
from discord import app_commands
from discord.ext import commands
import logging
from typing import List
from bot.ui.embeds import create_game_embed
from bot.ui.views import LinkButtonView
from bot.ui.helpers import build_backloggd_game_url

logger = logging.getLogger("backloggd.cogs.games")

class GamesCog(commands.Cog, name="Games"):
    def __init__(self, bot):
        self.bot = bot
        # Lightweight in-memory cache for autocomplete queries to avoid hammering search
        self._autocomplete_cache = {}

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

        try:
            results = await self.bot.backloggd.search_games(clean_query)
            choices = []
            for g in results[:15]:  # Discord limit is 25, 15 keeps it fast
                name = f"{g.title} ({g.release_year})" if g.release_year else g.title
                # Discord choice name limit is 100 characters
                if len(name) > 100:
                    name = name[:97] + "..."
                choices.append(app_commands.Choice(name=name, value=g.slug))

            self._autocomplete_cache[clean_query.lower()] = choices
            return choices
        except Exception as e:
            logger.warning("Autocomplete search error for '%s': %s", clean_query, e)
            return []

    @app_commands.command(name="game", description="Search for a game and view Backloggd ratings & server member scores")
    @app_commands.describe(title="Type game title to search with live autocomplete")
    @app_commands.autocomplete(title=game_autocomplete)
    async def game(self, interaction: discord.Interaction, title: str):
        await interaction.response.defer()

        # Resolve slug or run fallback search
        slug = title.strip()
        try:
            # Check if title is directly a slug or title
            if " " in slug or not slug.replace("-", "").isalnum():
                search_results = await self.bot.backloggd.search_games(slug)
                if not search_results:
                    await interaction.followup.send(
                        f"❌ No games found matching **{slug}** on Backloggd.",
                        ephemeral=True
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
            server_ratings = await self.bot.db.get_server_ratings_for_game(guild_member_ids, game_data.slug)

            guild_name = interaction.guild.name if interaction.guild else "Server"
            embed = create_game_embed(game_data, server_ratings, guild_name=guild_name)
            view = LinkButtonView(label="View on Backloggd", url=build_backloggd_game_url(game_data.slug))

            await interaction.followup.send(embed=embed, view=view)

        except Exception as e:
            logger.error("Error looking up game '%s': %s", title, e, exc_info=True)
            await interaction.followup.send(
                f"❌ An error occurred while fetching information for **{title}**.",
                ephemeral=True
            )

async def setup(bot):
    await bot.add_cog(GamesCog(bot))
