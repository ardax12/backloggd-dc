"""SocialCog: Handles /compare taste duel and similarity between users."""

import discord
from discord import app_commands
from discord.ext import commands
import logging
from typing import Optional, Dict
from backloggd.analytics import calculate_similarity_raw
from bot.ui.embeds import create_compare_embed, create_taste_twins_embed
from bot.ui.views import LinkButtonView, TasteTwinsView
from bot.ui.helpers import build_backloggd_user_url

logger = logging.getLogger("backloggd.cogs.social")

class SocialCog(commands.Cog, name="Social"):
    def __init__(self, bot):
        self.bot = bot

    async def _resolve_username(self, interaction: discord.Interaction, target: Optional[str]) -> Optional[str]:
        """Resolve a parameter into a Backloggd username (supports Discord mention or username)."""
        if not target:
            linked = await self.bot.db.get_linked_user(interaction.user.id)
            return linked["backloggd_username"] if linked else None

        clean = target.strip().lstrip("<@!").rstrip(">")
        if clean.isdigit():
            linked = await self.bot.db.get_linked_user(int(clean))
            return linked["backloggd_username"] if linked else None

        return clean.lstrip("@")

    async def _get_or_fetch_ratings(self, username: str) -> Dict[str, Optional[float]]:
        """Fetch user's library ratings mapping (slug -> score)."""
        # Fast indexed lookup in DB first (O(1) instead of scanning all users)
        linked = await self.bot.db.get_linked_user_by_username(username)
        if linked:
            cached = await self.bot.db.get_user_ratings_dict(linked["discord_id"])
            if cached:
                return cached

        # Otherwise fetch library via backloggd-py
        lib = await self.bot.backloggd.get_user_library(username)
        return lib.to_ratings_dict()

    @app_commands.command(name="compare", description="Compare video game tastes and ratings between two Backloggd users")
    @app_commands.describe(
        user1="First user (Backloggd username or @mention)",
        user2="Optional second user (defaults to you if comparing with user1)"
    )
    async def compare(
        self,
        interaction: discord.Interaction,
        user1: str,
        user2: Optional[str] = None
    ):
        await interaction.response.defer()

        # Target resolution
        if user2 is None:
            # user1 is target, user2 is caller
            u1_name = await self._resolve_username(interaction, user1)
            u2_name = await self._resolve_username(interaction, None)
            if not u2_name:
                await interaction.followup.send(
                    "❌ You haven't linked your Backloggd account yet! Use `/link <username>` or specify two users: `/compare <user1> <user2>`.",
                    ephemeral=True
                )
                return
        else:
            u1_name = await self._resolve_username(interaction, user1)
            u2_name = await self._resolve_username(interaction, user2)

        if not u1_name or not u2_name:
            await interaction.followup.send(
                "❌ Could not resolve one or both users. Please check usernames or mentions.",
                ephemeral=True
            )
            return

        if u1_name.lower() == u2_name.lower():
            await interaction.followup.send(
                "❌ You cannot compare a user to themselves!",
                ephemeral=True
            )
            return

        try:
            import asyncio
            ratings1, ratings2 = await asyncio.gather(
                self._get_or_fetch_ratings(u1_name),
                self._get_or_fetch_ratings(u2_name)
            )

            if not ratings1 or not ratings2:
                empty_user = u1_name if not ratings1 else u2_name
                await interaction.followup.send(
                    f"❌ **{empty_user}** does not have any logged games in their library to compare.",
                    ephemeral=True
                )
                return

            # Calculate similarity score using backloggd-py
            final_score, library_score, rating_score = calculate_similarity_raw(ratings1, ratings2)
            match_pct = final_score / 100.0

            # Find common rated games
            common_slugs = set(ratings1.keys()) & set(ratings2.keys())
            shared_loves = []
            disagreements = []

            for slug in common_slugs:
                r1 = ratings1.get(slug)
                r2 = ratings2.get(slug)
                if r1 is not None and r2 is not None:
                    delta = abs(r1 - r2)
                    title = slug.replace("-", " ").title()
                    # Shared loves (both rated 4.0+)
                    if r1 >= 4.0 and r2 >= 4.0:
                        shared_loves.append({"title": title, "r1": r1, "r2": r2, "avg": (r1 + r2) / 2})
                    # Disagreements (delta >= 1.0)
                    if delta >= 1.0:
                        disagreements.append({"title": title, "r1": r1, "r2": r2, "delta": delta})

            shared_loves.sort(key=lambda x: x["avg"], reverse=True)
            disagreements.sort(key=lambda x: x["delta"], reverse=True)

            embed = create_compare_embed(
                user1=u1_name,
                user2=u2_name,
                match_score=match_pct,
                shared_loves=shared_loves,
                disagreements=disagreements
            )
            view = LinkButtonView(label=f"View {u1_name}'s Profile", url=build_backloggd_user_url(u1_name))

            await interaction.followup.send(embed=embed, view=view)

        except Exception as e:
            logger.error("Error comparing %s and %s: %s", u1_name, u2_name, e, exc_info=True)
            await interaction.followup.send(
                f"❌ An error occurred while comparing **{u1_name}** and **{u2_name}**.",
                ephemeral=True
            )


    # =========================================================================
    # /taste-twins (Server Taste Twins & Nemesis Finder)
    # =========================================================================

    @app_commands.command(
        name="taste-twins",
        description="Find your closest gaming taste twins (and arch-nemesis) in this server"
    )
    @app_commands.describe(
        user="Optional server member or Backloggd user to analyze (defaults to you)"
    )
    async def taste_twins(
        self,
        interaction: discord.Interaction,
        user: Optional[str] = None
    ):
        await interaction.response.defer()

        if not interaction.guild:
            await interaction.followup.send("❌ This command must be run inside a Discord server.", ephemeral=True)
            return

        # 1. Target resolution
        target_username = None
        target_discord_id = None
        target_avatar = None

        if user:
            clean_arg = user.strip().lstrip("<@!").rstrip(">")
            if clean_arg.isdigit():
                target_discord_id = int(clean_arg)
                linked = await self.bot.db.get_linked_user(target_discord_id)
                if not linked:
                    await interaction.followup.send(
                        "❌ That user has not linked their Backloggd account yet.",
                        ephemeral=True
                    )
                    return
                target_username = linked["backloggd_username"]
                target_avatar = linked["avatar_url"]
            else:
                clean_name = user.strip().lstrip("@")
                linked = await self.bot.db.get_linked_user_by_username(clean_name)
                if linked:
                    target_discord_id = linked["discord_id"]
                    target_username = linked["backloggd_username"]
                    target_avatar = linked["avatar_url"]
                else:
                    target_username = clean_name
                    target_avatar = None
        else:
            target_discord_id = interaction.user.id
            linked = await self.bot.db.get_linked_user(target_discord_id)
            if not linked:
                await interaction.followup.send(
                    "❌ You haven't linked your Backloggd account yet! Use `/link <username>` first.",
                    ephemeral=True
                )
                return
            target_username = linked["backloggd_username"]
            target_avatar = linked["avatar_url"]

        try:
            # 2. Get target ratings
            if target_discord_id:
                my_ratings = await self.bot.db.get_user_ratings_dict(target_discord_id)
            else:
                my_ratings = await self._get_or_fetch_ratings(target_username)

            if not my_ratings:
                await interaction.followup.send(
                    f"❌ **{target_username}** does not have any cached ratings yet. Use `/refresh` to sync your library.",
                    ephemeral=True
                )
                return

            # 3. Fetch candidate members in this guild
            guild_member_ids = [m.id for m in interaction.guild.members if m.id != target_discord_id]
            other_linked = await self.bot.db.get_guild_linked_users(guild_member_ids)

            if not other_linked:
                await interaction.followup.send(
                    "ℹ️ No other server members have linked their Backloggd accounts yet! Invite them to use `/link`.",
                    ephemeral=True
                )
                return

            # 4. Batch fetch ratings for all candidate members in 1 single query (zero N+1)
            other_ids = [o["discord_id"] for o in other_linked]
            all_other_ratings = await self.bot.db.get_multi_user_ratings_dict(other_ids)

            candidates = []
            slugs_to_lookup = set()

            for other in other_linked:
                other_id = other["discord_id"]
                other_username = other["backloggd_username"]
                other_avatar = other["avatar_url"]
                other_ratings = all_other_ratings.get(other_id, {})

                # Find common rated games
                common_slugs = [
                    s for s in (set(my_ratings.keys()) & set(other_ratings.keys()))
                    if my_ratings.get(s) is not None and other_ratings.get(s) is not None
                ]
                if len(common_slugs) < 2:
                    continue

                final_score, _, _ = calculate_similarity_raw(my_ratings, other_ratings)
                match_pct = int(round(final_score))

                # Find top shared love (both rated >= 3.5)
                shared_loves = [
                    (s, (my_ratings[s] + other_ratings[s]) / 2.0)
                    for s in common_slugs
                    if my_ratings[s] >= 3.5 and other_ratings[s] >= 3.5
                ]
                shared_loves.sort(key=lambda x: x[1], reverse=True)
                top_fav_slug = shared_loves[0][0] if shared_loves else None

                # Find biggest disagreement
                disagreements = [
                    (s, abs(my_ratings[s] - other_ratings[s]))
                    for s in common_slugs
                ]
                disagreements.sort(key=lambda x: x[1], reverse=True)
                top_disagree_slug, max_delta = (disagreements[0][0], disagreements[0][1]) if disagreements else (None, 0.0)

                if top_fav_slug:
                    slugs_to_lookup.add(top_fav_slug)
                if top_disagree_slug:
                    slugs_to_lookup.add(top_disagree_slug)

                candidates.append({
                    "discord_id": other_id,
                    "username": other_username,
                    "avatar_url": other_avatar,
                    "match_pct": match_pct,
                    "shared_count": len(common_slugs),
                    "top_fav_slug": top_fav_slug,
                    "top_disagree_slug": top_disagree_slug,
                    "delta": max_delta,
                })

            if not candidates:
                await interaction.followup.send(
                    "ℹ️ Not enough library overlap with other server members yet. "
                    "Members need at least a few shared rated games to calculate compatibility!",
                    ephemeral=True
                )
                return

            # 5. Batch resolve authentic game titles from cache
            cached_meta = await self.bot.db.get_games_by_slugs(list(slugs_to_lookup)) if slugs_to_lookup else {}
            for c in candidates:
                if c["top_fav_slug"]:
                    meta = cached_meta.get(c["top_fav_slug"])
                    c["top_fav"] = meta["title"] if meta and meta["title"] else c["top_fav_slug"].replace("-", " ").title()
                else:
                    c["top_fav"] = None

                if c["top_disagree_slug"]:
                    meta = cached_meta.get(c["top_disagree_slug"])
                    c["top_disagree"] = meta["title"] if meta and meta["title"] else c["top_disagree_slug"].replace("-", " ").title()
                else:
                    c["top_disagree"] = None

            # 6. Sort candidates by similarity descending
            candidates.sort(key=lambda x: (x["match_pct"], x["shared_count"]), reverse=True)

            twins = candidates[:3]
            # Nemesis is lowest match candidate if score is divergent (< 65%)
            nemesis = candidates[-1] if len(candidates) > 1 and candidates[-1]["match_pct"] < 65 else None
            server_avg_match = sum(c["match_pct"] for c in candidates) / len(candidates)

            top_twin_name = twins[0]["username"] if twins else None

            # Interactive duel callback
            async def on_compare(btn_interaction: discord.Interaction):
                if btn_interaction.user.id != interaction.user.id:
                    await btn_interaction.response.send_message(
                        "❌ Only the member who initiated `/taste-twins` can duel with their twin!",
                        ephemeral=True
                    )
                    return
                await btn_interaction.response.defer()
                t1_ratings = my_ratings
                t2_ratings = all_other_ratings.get(twins[0]["discord_id"], {})
                final_s, _, _ = calculate_similarity_raw(t1_ratings, t2_ratings)
                c_slugs = set(t1_ratings.keys()) & set(t2_ratings.keys())
                c_loves = []
                c_dis = []
                for s in c_slugs:
                    r1 = t1_ratings.get(s)
                    r2 = t2_ratings.get(s)
                    if r1 is not None and r2 is not None:
                        d = abs(r1 - r2)
                        meta_lookup = cached_meta.get(s)
                        t_title = meta_lookup["title"] if meta_lookup and meta_lookup["title"] else s.replace("-", " ").title()
                        if r1 >= 4.0 and r2 >= 4.0:
                            c_loves.append({"title": t_title, "r1": r1, "r2": r2, "avg": (r1 + r2) / 2})
                        if d >= 1.0:
                            c_dis.append({"title": t_title, "r1": r1, "r2": r2, "delta": d})

                c_loves.sort(key=lambda x: x["avg"], reverse=True)
                c_dis.sort(key=lambda x: x["delta"], reverse=True)
                comp_embed = create_compare_embed(
                    user1=target_username,
                    user2=top_twin_name,
                    match_score=final_s / 100.0,
                    shared_loves=c_loves,
                    disagreements=c_dis
                )
                comp_view = LinkButtonView(label=f"View {top_twin_name}'s Profile", url=build_backloggd_user_url(top_twin_name))
                await btn_interaction.followup.send(embed=comp_embed, view=comp_view)

            embed = create_taste_twins_embed(
                username=target_username,
                avatar_url=target_avatar,
                guild_name=interaction.guild.name,
                twins=twins,
                nemesis=nemesis,
                server_avg_match=server_avg_match
            )
            view = TasteTwinsView(
                user_url=build_backloggd_user_url(target_username),
                top_twin_name=top_twin_name,
                on_compare=on_compare if top_twin_name else None
            )

            await interaction.followup.send(embed=embed, view=view)

        except Exception as e:
            logger.error("Error running taste-twins for %s: %s", target_username, e, exc_info=True)
            await interaction.followup.send(
                f"❌ An error occurred while computing taste twins for **{target_username}**.",
                ephemeral=True
            )

async def setup(bot):
    await bot.add_cog(SocialCog(bot))
