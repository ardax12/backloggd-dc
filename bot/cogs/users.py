"""UsersCog: Handles /profile command displaying stats, metrics, and favorite games."""

import discord
from discord import app_commands
from discord.ext import commands
import logging
from typing import Optional
from bot.ui.embeds import create_profile_embed
from bot.ui.views import LinkButtonView
from bot.ui.helpers import build_backloggd_user_url

logger = logging.getLogger("backloggd.cogs.users")

class UsersCog(commands.Cog, name="Users"):
    def __init__(self, bot):
        self.bot = bot

    @app_commands.command(name="profile", description="View a Backloggd user's profile, library stats, and favorite games")
    @app_commands.describe(user="Optional: Mention a server member or provide a Backloggd username (defaults to you)")
    async def profile(self, interaction: discord.Interaction, user: Optional[str] = None):
        await interaction.response.defer()

        target_username = None
        target_discord_id = None

        # 1. Check if user is a mention or discord ID
        if user:
            clean_arg = user.strip().lstrip("<@!").rstrip(">")
            if clean_arg.isdigit():
                target_discord_id = int(clean_arg)
                linked = await self.bot.db.get_linked_user(target_discord_id)
                if linked:
                    target_username = linked["backloggd_username"]
                else:
                    await interaction.followup.send(
                        f"❌ That user has not linked their Backloggd account yet.",
                        ephemeral=True
                    )
                    return
            else:
                target_username = user.strip().lstrip("@")
                linked = await self.bot.db.get_linked_user_by_username(target_username)
                if linked:
                    target_discord_id = linked["discord_id"]
        else:
            # Check caller's own linked account
            target_discord_id = interaction.user.id
            linked = await self.bot.db.get_linked_user(target_discord_id)
            if linked:
                target_username = linked["backloggd_username"]
            else:
                await interaction.followup.send(
                    "❌ You haven't linked your Backloggd account yet! Use `/link <username>` first.",
                    ephemeral=True
                )
                return

        try:
            profile_data = await self.bot.backloggd.get_user_profile(target_username)
            if not profile_data or not profile_data.username:
                await interaction.followup.send(
                    f"❌ Backloggd user **{target_username}** could not be found.",
                    ephemeral=True
                )
                return

            # Fetch cached ratings stats from database if user is linked
            avg_rating = None
            rated_count = None
            if target_discord_id:
                stats = await self.bot.db.get_user_stats(target_discord_id)
                if stats and stats.get("rated_count", 0) > 0:
                    avg_rating = stats.get("average_rating")
                    rated_count = stats.get("rated_count")

            embed = create_profile_embed(profile_data, avg_rating=avg_rating, rated_count=rated_count)
            view = LinkButtonView(label="View Profile on Backloggd", url=build_backloggd_user_url(profile_data.username))

            await interaction.followup.send(embed=embed, view=view)

        except Exception as e:
            logger.error("Error looking up profile for '%s': %s", target_username, e, exc_info=True)
            await interaction.followup.send(
                f"❌ An error occurred while retrieving profile for **{target_username}**.",
                ephemeral=True
            )

async def setup(bot):
    await bot.add_cog(UsersCog(bot))
