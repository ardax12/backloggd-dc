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

    @app_commands.command(
        name="recommend",
        description="Get taste-matched recommendations or recommend your top games to a friend"
    )
    @app_commands.describe(
        user="Optional server member to recommend your games to (defaults to recommendations for you)"
    )
    async def recommend(self, interaction: discord.Interaction, user: Optional[discord.Member] = None):
        await interaction.response.defer()

        if not interaction.guild:
            await interaction.followup.send("❌ This command must be run inside a Discord server.", ephemeral=True)
            return

        caller_discord_id = interaction.user.id
        caller_link = await self.bot.db.get_linked_user(caller_discord_id)

        # =====================================================================
        # MODE 2: Recommend caller's games to a specific friend (/recommend @Friend)
        # =====================================================================
        if user and user.id != caller_discord_id:
            if not caller_link:
                await interaction.followup.send(
                    "❌ You haven't linked your Backloggd account yet! Use `/link <username>` to recommend your games to others.",
                    ephemeral=True
                )
                return

            caller_username = caller_link["backloggd_username"]
            caller_ratings = await self.bot.db.get_user_ratings_dict(caller_discord_id)
            if not caller_ratings:
                await interaction.followup.send(
                    f"❌ **{caller_username}** has no cached ratings yet. Use `/refresh` to sync your library first.",
                    ephemeral=True
                )
                return

            friend_discord_id = user.id
            friend_link = await self.bot.db.get_linked_user(friend_discord_id)
            friend_username = friend_link["backloggd_username"] if friend_link else user.display_name
            friend_ratings = (
                await self.bot.db.get_user_ratings_dict(friend_discord_id) if friend_link else {}
            )
            friend_slugs = set(friend_ratings.keys())

            # Optional compatibility score if friend is linked and has ratings
            taste_pct = None
            if friend_ratings and len(friend_ratings) >= 5:
                sim_score, _, _ = calculate_similarity_raw(caller_ratings, friend_ratings)
                taste_pct = int(round(sim_score))

            # Pick caller's top-rated games (rated >= 4.0) that friend hasn't logged yet
            eligible_games = [
                (slug, rating)
                for slug, rating in caller_ratings.items()
                if rating is not None and rating >= 4.0 and slug not in friend_slugs
            ]
            eligible_games.sort(key=lambda x: x[1], reverse=True)

            if not eligible_games:
                if friend_link:
                    await interaction.followup.send(
                        f"🎉 **{friend_username}** has already played or logged all of your 4.0+ rated games!",
                        ephemeral=True
                    )
                else:
                    await interaction.followup.send(
                        f"ℹ️ You do not have any games rated 4.0 or above in your cached library to recommend.",
                        ephemeral=True
                    )
                return

            top_picks = eligible_games[:5]
            cand_slugs = [slug for slug, _ in top_picks]
            cached_meta = await self.bot.db.get_games_by_slugs(cand_slugs) if cand_slugs else {}

            comp_str = f" • `{taste_pct}%` taste compatibility" if taste_pct is not None else ""
            desc = f"Hand-picked games from **[{caller_username}](https://www.backloggd.com/u/{caller_username}/)**'s library{comp_str}:"
            if not friend_link:
                desc += f"\n*(Note: {user.display_name} hasn't linked Backloggd yet, showing your all-time highest ratings)*"

            embed = discord.Embed(
                title=f"💡 Recommendations for {friend_username}",
                description=desc,
                color=COLOR_PLAYED
            )

            for slug, rating in top_picks:
                meta = cached_meta.get(slug)
                title = meta["title"] if meta and meta["title"] else slug.replace("-", " ").title()
                url = build_backloggd_game_url(slug)
                stars = rating_to_stars(rating)
                embed.add_field(
                    name=f"🎮 {title}",
                    value=(
                        f"• {caller_username}'s Rating: {stars} (`{rating:.1f}`)\n"
                        f"• [View on Backloggd]({url})"
                    ),
                    inline=False
                )

            embed.set_footer(text=f"Recommended by {caller_username} via Backloggd Discord Bot")
            await interaction.followup.send(embed=embed)
            return

        # =====================================================================
        # MODE 1: Discover recommendations for the caller from server taste twins
        # (Guarantees diversity: 1 recommendation per user across different members)
        # =====================================================================
        if not caller_link:
            await interaction.followup.send(
                "❌ You do not have a linked Backloggd account. Use `/link <username>` first.",
                ephemeral=True
            )
            return

        caller_username = caller_link["backloggd_username"]
        my_ratings = await self.bot.db.get_user_ratings_dict(caller_discord_id)

        if not my_ratings:
            await interaction.followup.send(
                f"❌ **{caller_username}** has no cached ratings yet. Use `/refresh` to sync your library.",
                ephemeral=True
            )
            return

        guild_member_ids = [m.id for m in interaction.guild.members if m.id != caller_discord_id]
        other_linked = await self.bot.db.get_guild_linked_users(guild_member_ids)

        if not other_linked:
            await interaction.followup.send(
                "ℹ️ No other server members have linked their Backloggd accounts yet! Invite them to use `/link`.",
                ephemeral=True
            )
            return

        # Batch fetch ratings for all candidate server members in 1 single query (fixes N+1)
        other_ids = [other["discord_id"] for other in other_linked]
        all_other_ratings = await self.bot.db.get_multi_user_ratings_dict(other_ids)

        taste_matches = []
        for other in other_linked:
            other_id = other["discord_id"]
            other_username = other["backloggd_username"]
            other_ratings = all_other_ratings.get(other_id, {})

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

        # Sort members by similarity score descending
        taste_matches.sort(key=lambda x: x[2], reverse=True)

        my_slugs = set(my_ratings.keys())
        raw_candidates = []
        chosen_slugs = set()

        # PASS 1: Strict diversity — exactly 1 top recommendation per member
        for other_id, other_name, match_score, other_ratings in taste_matches:
            match_pct = int(round(match_score))
            # Sort this member's eligible games by rating descending
            user_top_games = [
                (slug, rating)
                for slug, rating in other_ratings.items()
                if rating is not None and rating >= 4.0 and slug not in my_slugs and slug not in chosen_slugs
            ]
            user_top_games.sort(key=lambda x: x[1], reverse=True)

            if user_top_games:
                best_slug, best_rating = user_top_games[0]
                raw_candidates.append((best_slug, best_rating, other_name, match_pct))
                chosen_slugs.add(best_slug)
                if len(raw_candidates) >= 5:
                    break

        # PASS 2: If fewer than 5 recommendations found (e.g. only 2 or 3 taste twins in server),
        # round-robin additional top games from highest matches to fill up to 5
        if len(raw_candidates) < 5:
            for other_id, other_name, match_score, other_ratings in taste_matches:
                match_pct = int(round(match_score))
                user_top_games = [
                    (slug, rating)
                    for slug, rating in other_ratings.items()
                    if rating is not None and rating >= 4.0 and slug not in my_slugs and slug not in chosen_slugs
                ]
                user_top_games.sort(key=lambda x: x[1], reverse=True)

                for slug, rating in user_top_games:
                    raw_candidates.append((slug, rating, other_name, match_pct))
                    chosen_slugs.add(slug)
                    if len(raw_candidates) >= 5:
                        break
                if len(raw_candidates) >= 5:
                    break

        if not raw_candidates:
            await interaction.followup.send(
                f"🎉 You have already played all the top-rated games from your server taste matches!",
                ephemeral=True
            )
            return

        # Batch resolve authentic game titles from cache
        cand_slugs = [c[0] for c in raw_candidates]
        cached_meta = await self.bot.db.get_games_by_slugs(cand_slugs) if cand_slugs else {}

        recommendations = []
        for slug, rating, other_name, match_pct in raw_candidates:
            meta = cached_meta.get(slug)
            title = meta["title"] if meta and meta["title"] else slug.replace("-", " ").title()
            recommendations.append({
                "slug": slug,
                "title": title,
                "rating": rating,
                "recommender": other_name,
                "match_pct": match_pct
            })

        embed = discord.Embed(
            title=f"💡 Recommendations for {caller_username}",
            description="Hand-picked games from different server members who share your taste:\n",
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

        embed.set_footer(text="Diverse recommendations curated across your server taste matches")
        await interaction.followup.send(embed=embed)

async def setup(bot):
    await bot.add_cog(DiscoveryCog(bot))
