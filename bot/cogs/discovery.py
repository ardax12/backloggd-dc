"""DiscoveryCog: Handles /recommend (server taste-matched suggestions) and /roulette (backlog picker with Reroll button)."""

import discord
from discord import app_commands
from discord.ext import commands
import logging
import random
from typing import Optional
from backloggd.analytics import calculate_similarity_raw
from bot.ui.embeds import create_roulette_embed
from bot.ui.views import RouletteView, LinkButtonView
from bot.ui.helpers import build_backloggd_game_url, rating_to_stars
from bot.config import COLOR_PRIMARY, COLOR_GOLD, COLOR_PLAYED

logger = logging.getLogger("backloggd.cogs.discovery")

class DiscoveryCog(commands.Cog, name="Discovery"):
    def __init__(self, bot):
        self.bot = bot

    # =========================================================================
    # /roulette (Interactive Backlog Decision Picker)
    # =========================================================================

    @app_commands.command(name="roulette", description="Pick a random game from your Backlog to decide what to play next")
    @app_commands.describe(user="Optional: Backloggd user or mention (defaults to you)")
    async def roulette(self, interaction: discord.Interaction, user: Optional[discord.Member] = None):
        await interaction.response.defer()

        target_discord_id = user.id if user else interaction.user.id
        linked = await self.bot.db.get_linked_user(target_discord_id)

        if not linked:
            who = user.display_name if user else "You"
            await interaction.followup.send(
                f"❌ {who} do not have a linked Backloggd account! Use `/link <username>` first.",
                ephemeral=True
            )
            return

        username = linked["backloggd_username"]
        backlog_games = await self.bot.db.get_user_backlog(target_discord_id)

        if not backlog_games:
            await interaction.followup.send(
                f"ℹ️ **{username}** has no games with status `Backlog` in the local cache. Try running `/refresh` to sync your latest library!",
                ephemeral=True
            )
            return

        chosen_game = random.choice(backlog_games)
        remaining = len(backlog_games)

        # Reroll callback definition for the interactive view
        async def on_reroll(btn_interaction: discord.Interaction):
            new_choice = random.choice(backlog_games)
            new_embed = create_roulette_embed(username, new_choice, remaining)
            new_view = RouletteView(
                author_id=interaction.user.id,
                game_url=build_backloggd_game_url(new_choice["slug"]),
                on_reroll=on_reroll
            )
            await btn_interaction.response.edit_message(embed=new_embed, view=new_view)

        embed = create_roulette_embed(username, chosen_game, remaining)
        view = RouletteView(
            author_id=interaction.user.id,
            game_url=build_backloggd_game_url(chosen_game["slug"]),
            on_reroll=on_reroll
        )

        await interaction.followup.send(embed=embed, view=view)

    # =========================================================================
    # /recommend (Taste-Matched Server Recommendations)
    # =========================================================================

    @app_commands.command(name="recommend", description="Get game recommendations from server members who share your taste")
    @app_commands.describe(user="Optional: Server member to get recommendations for (defaults to you)")
    async def recommend(self, interaction: discord.Interaction, user: Optional[discord.Member] = None):
        await interaction.response.defer()

        target_discord_id = user.id if user else interaction.user.id
        linked = await self.bot.db.get_linked_user(target_discord_id)

        if not linked:
            who = user.display_name if user else "You"
            await interaction.followup.send(
                f"❌ {who} do not have a linked Backloggd account. Use `/link <username>` first.",
                ephemeral=True
            )
            return

        username = linked["backloggd_username"]
        my_ratings = await self.bot.db.get_user_ratings_dict(target_discord_id)

        if not my_ratings:
            await interaction.followup.send(
                f"❌ **{username}** has no cached ratings yet. Use `/refresh` to sync your library.",
                ephemeral=True
            )
            return

        # Fetch other linked members in this server
        if not interaction.guild:
            await interaction.followup.send("❌ This command must be run inside a Discord server.", ephemeral=True)
            return

        guild_member_ids = [m.id for m in interaction.guild.members if m.id != target_discord_id]
        other_linked = await self.bot.db.get_guild_linked_users(guild_member_ids)

        if not other_linked:
            await interaction.followup.send(
                "ℹ️ No other server members have linked their Backloggd accounts yet! Invite them to use `/link`.",
                ephemeral=True
            )
            return

        # Calculate taste match with each server member
        taste_matches = []
        for other in other_linked:
            other_id = other["discord_id"]
            other_username = other["backloggd_username"]
            other_ratings = await self.bot.db.get_user_ratings_dict(other_id)

            if len(other_ratings) < 5:
                continue

            final_score, _, _ = calculate_similarity_raw(my_ratings, other_ratings)
            taste_matches.append((other_id, other_username, final_score, other_ratings))

        if not taste_matches:
            await interaction.followup.send(
                "ℹ️ Not enough library overlap with other server members yet. Try logging more games!",
                ephemeral=True
            )
            return

        # Sort by similarity score descending
        taste_matches.sort(key=lambda x: x[2], reverse=True)

        # Recommend games rated >= 4.0 by taste twins that the user hasn't played
        my_slugs = set(my_ratings.keys())
        recommendations = []

        for other_id, other_name, match_score, other_ratings in taste_matches:
            match_pct = int(round(match_score))
            for slug, rating in other_ratings.items():
                if rating is not None and rating >= 4.0 and slug not in my_slugs:
                    recommendations.append({
                        "slug": slug,
                        "title": slug.replace("-", " ").title(),
                        "rating": rating,
                        "recommender": other_name,
                        "match_pct": match_pct
                    })
                if len(recommendations) >= 5:
                    break
            if len(recommendations) >= 5:
                break

        if not recommendations:
            await interaction.followup.send(
                f"🎉 You have already played all the top-rated games from your server taste matches!",
                ephemeral=True
            )
            return

        # Build recommendation embed
        embed = discord.Embed(
            title=f"💡 Recommendations for {username}",
            description="Hand-picked games based on the highest taste agreement with your fellow server members:\n",
            color=COLOR_PLAYED
        )

        for rec in recommendations:
            url = build_backloggd_game_url(rec["slug"])
            stars = rating_to_stars(rec["rating"])
            embed.add_field(
                name=f"🎮 {rec['title']}",
                value=(
                    f"• Recommended by **{rec['recommender']}** ({rec['match_pct']}% taste match)\n"
                    f"• Their Rating: {stars} (`{rec['rating']:.1f}`)\n"
                    f"• [View on Backloggd]({url})"
                ),
                inline=False
            )

        embed.set_footer(text="Based on server similarity scores and Backloggd ratings")
        await interaction.followup.send(embed=embed)

async def setup(bot):
    await bot.add_cog(DiscoveryCog(bot))
