"""DiscoveryCog: Handles profile-based recommendations and an unplayed-game roulette."""

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
from bot.recommendation_rules import numeric_ratings

logger = logging.getLogger("backloggd.cogs.discovery")

class DiscoveryCog(commands.Cog, name="Discovery"):
    def __init__(self, bot):
        self.bot = bot

    # =========================================================================
    # /roulette (Interactive Backlog Decision Picker)
    # =========================================================================

    @app_commands.command(name="roulette", description="Pick an unrated game from your Backlog or Wishlist")
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
                f"ℹ️ **{username}** has no unrated games in `Backlog` or `Wishlist` in the local cache. Try `/refresh` to sync your latest library.",
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
            new_view.message = btn_interaction.message
            await btn_interaction.response.edit_message(embed=new_embed, view=new_view)

        embed = create_roulette_embed(username, chosen_game, remaining)
        view = RouletteView(
            author_id=interaction.user.id,
            game_url=build_backloggd_game_url(chosen_game["slug"]),
            on_reroll=on_reroll
        )

        view.message = await interaction.followup.send(embed=embed, view=view, wait=True)

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
            caller_ratings = await self.bot.db.get_profile_ratings_dict(caller_link["profile_key"])
            if not caller_ratings:
                await interaction.followup.send(
                    f"❌ **{caller_username}** has no cached ratings yet. Use `/refresh` to sync your library first.",
                    ephemeral=True
                )
                return

            friend_discord_id = user.id
            friend_link = await self.bot.db.get_linked_user(friend_discord_id)
            if friend_link and friend_link["profile_key"] == caller_link["profile_key"]:
                await interaction.followup.send(
                    "ℹ️ Both Discord members link to the same Backloggd profile, so there is no separate profile to recommend to.",
                    ephemeral=True,
                )
                return
            friend_username = friend_link["backloggd_username"] if friend_link else user.display_name
            friend_ratings = (
                await self.bot.db.get_profile_ratings_dict(friend_link["profile_key"]) if friend_link else {}
            )
            friend_slugs = set(friend_ratings.keys())

            # Optional compatibility score if friend is linked and has ratings
            taste_pct = None
            caller_numeric = numeric_ratings(caller_ratings)
            friend_numeric = numeric_ratings(friend_ratings)
            if len(caller_numeric) >= 5 and len(friend_numeric) >= 5:
                sim_score, _, _ = calculate_similarity_raw(caller_numeric, friend_numeric)
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
                        f"🎉 **{friend_username}** already has all of your 4.0+ rated games in their library.",
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
        # MODE 1: Discover recommendations from distinct server profiles with similar taste.
        # =====================================================================
        if not caller_link:
            await interaction.followup.send(
                "❌ You do not have a linked Backloggd account. Use `/link <username>` first.",
                ephemeral=True
            )
            return

        caller_username = caller_link["backloggd_username"]
        my_ratings = await self.bot.db.get_profile_ratings_dict(caller_link["profile_key"])

        if not my_ratings:
            await interaction.followup.send(
                f"❌ **{caller_username}** has no cached ratings yet. Use `/refresh` to sync your library.",
                ephemeral=True
            )
            return

        my_numeric_ratings = numeric_ratings(my_ratings)
        if not my_numeric_ratings:
            await interaction.followup.send(
                f"ℹ️ **{caller_username}** has no numeric game ratings in the cached library yet. Similarity needs rated games; unrated backlog and wishlist entries do not count.",
                ephemeral=True,
            )
            return

        guild_member_ids = [m.id for m in interaction.guild.members if m.id != caller_discord_id]
        other_linked = await self.bot.db.get_guild_linked_profiles(
            guild_member_ids, exclude_profile_key=caller_link["profile_key"]
        )

        if not other_linked:
            await interaction.followup.send(
                "ℹ️ No other distinct Backloggd profiles are linked in this server yet. Invite members to use `/link`.",
                ephemeral=True
            )
            return

        # Batch fetch ratings once per distinct profile.
        other_profile_keys = [other["profile_key"] for other in other_linked]
        all_other_ratings = await self.bot.db.get_multi_profile_ratings_dict(other_profile_keys)

        taste_matches = []
        enough_rated_profiles = False
        for other in other_linked:
            other_profile_key = other["profile_key"]
            other_username = other["display_username"]
            other_ratings = all_other_ratings.get(other_profile_key, {})
            other_numeric_ratings = numeric_ratings(other_ratings)

            if len(other_numeric_ratings) < 5:
                continue
            enough_rated_profiles = True

            common_rated = set(my_numeric_ratings) & set(other_numeric_ratings)
            if len(common_rated) < 2:
                continue

            final_score, _, _ = calculate_similarity_raw(my_numeric_ratings, other_numeric_ratings)
            taste_matches.append((other_profile_key, other_username, final_score, other_ratings))

        if not taste_matches:
            if enough_rated_profiles:
                reason = "No other profile shares at least two numeric ratings with you yet."
            else:
                reason = "Other profiles need at least five numeric ratings before we can compare taste."
            await interaction.followup.send(
                f"ℹ️ {reason} Unrated backlog and wishlist entries do not count toward similarity.",
                ephemeral=True
            )
            return

        # Sort profiles by similarity score descending.
        taste_matches.sort(key=lambda x: x[2], reverse=True)

        my_slugs = set(my_ratings.keys())
        raw_candidates = []
        chosen_slugs = set()

        # PASS 1: Strict diversity — exactly 1 top recommendation per profile.
        for other_profile_key, other_name, match_score, other_ratings in taste_matches:
            match_pct = int(round(match_score))
            # Sort this profile's eligible games by rating descending.
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
            for other_profile_key, other_name, match_score, other_ratings in taste_matches:
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
                "ℹ️ There are no 4.0+ recommendations from similar profiles that are missing from your library.",
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
            description="Hand-picked games from different server profiles that share your taste:\n",
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
