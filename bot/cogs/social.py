"""SocialCog: Handles /compare taste duel and similarity between users."""

import discord
from discord import app_commands
from discord.ext import commands
import logging
from typing import Optional, Dict
from backloggd.analytics import calculate_similarity_raw
from bot.ui.embeds import create_compare_embed
from bot.ui.views import LinkButtonView
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
        # Try finding linked user in DB first
        for row in await self.bot.db.get_all_linked_users():
            if row["backloggd_username"].lower() == username.lower():
                cached = await self.bot.db.get_user_ratings_dict(row["discord_id"])
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
            ratings1 = await self._get_or_fetch_ratings(u1_name)
            ratings2 = await self._get_or_fetch_ratings(u2_name)

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

async def setup(bot):
    await bot.add_cog(SocialCog(bot))
