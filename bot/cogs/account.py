"""AccountCog: Handles /link, /unlink, and /refresh with background library sync."""

import discord
from discord import app_commands
from discord.ext import commands
import logging
import asyncio
from typing import Optional
from bot.config import COLOR_PRIMARY, COLOR_PLAYED, COLOR_ERROR

logger = logging.getLogger("backloggd.cogs.account")

class AccountCog(commands.Cog, name="Account"):
    def __init__(self, bot):
        self.bot = bot

    async def _sync_user_library(self, discord_id: int, username: str) -> int:
        """Fetch full user library from Backloggd and sync games & ratings to SQLite."""
        try:
            profile = await self.bot.backloggd.get_user_profile(username, force_refresh=True)
            avatar_url = profile.avatar_url if profile else None
            await self.bot.db.link_user(discord_id, username, avatar_url)

            # Fetch library (all pages)
            library = await self.bot.backloggd.get_user_library(username, force_refresh=True)
            
            # Save games & ratings in SQLite
            for game in library:
                await self.bot.db.upsert_game(
                    slug=game.slug,
                    title=game.title,
                    cover_url=game.cover_url
                )
                await self.bot.db.upsert_user_rating(
                    discord_id=discord_id,
                    game_slug=game.slug,
                    rating=game.rating,
                    status=game.status.value if hasattr(game.status, "value") else str(game.status),
                    is_liked=game.is_liked
                )

            # Seed journal logs to avoid spamming past logs on auto feed
            try:
                journal = await self.bot.backloggd.get_user_journal(username, max_pages=1)
                for entry in journal:
                    if entry.entry_id:
                        await self.bot.db.mark_journal_entry_seen(entry.entry_id, discord_id)
            except Exception as e:
                logger.warning("Failed to seed journal logs for %s: %s", username, e)

            logger.info("Synced library for %s (%d games)", username, len(library))
            return len(library)
        except Exception as e:
            logger.error("Error syncing library for %s: %s", username, e, exc_info=True)
            return 0

    @app_commands.command(name="link", description="Link your Discord account to your Backloggd username")
    @app_commands.describe(username="Your Backloggd username (e.g. Gamer123)")
    async def link(self, interaction: discord.Interaction, username: str):
        await interaction.response.defer()

        clean_user = username.strip().lstrip("@")
        try:
            profile = await self.bot.backloggd.get_user_profile(clean_user)
            if not profile or not profile.username:
                await interaction.followup.send(
                    f"❌ Could not find a Backloggd user with the username **{clean_user}**.",
                    ephemeral=True
                )
                return
        except Exception as e:
            logger.error("Failed to verify user %s: %s", clean_user, e)
            await interaction.followup.send(
                f"❌ Error verifying username **{clean_user}**. Please check the spelling.",
                ephemeral=True
            )
            return

        # Save basic link immediately
        await self.bot.db.link_user(
            discord_id=interaction.user.id,
            username=profile.username,
            avatar_url=profile.avatar_url
        )

        embed = discord.Embed(
            title="🔗 Account Linked!",
            description=(
                f"Successfully linked **{interaction.user.mention}** to Backloggd user **[{profile.username}](https://www.backloggd.com/u/{profile.username}/)**.\n\n"
                f"🔄 *Syncing your library in the background so your ratings show up on `/game`...*"
            ),
            color=COLOR_PLAYED
        )
        if profile.avatar_url:
            embed.set_thumbnail(url=profile.avatar_url)

        await interaction.followup.send(embed=embed)

        # Background sync
        asyncio.create_task(self._sync_user_library(interaction.user.id, profile.username))

    @app_commands.command(name="unlink", description="Unlink your Backloggd account from Discord")
    async def unlink(self, interaction: discord.Interaction):
        success = await self.bot.db.unlink_user(interaction.user.id)
        if success:
            await interaction.response.send_message(
                "✅ Successfully unlinked your Backloggd account. Your cached ratings have been removed.",
                ephemeral=True
            )
        else:
            await interaction.response.send_message(
                "ℹ️ You do not currently have a Backloggd account linked.",
                ephemeral=True
            )

    @app_commands.command(name="refresh", description="Refresh your Backloggd library and ratings")
    @app_commands.describe(user="Optional: Mention a server member or provide a Backloggd username to refresh")
    async def refresh(self, interaction: discord.Interaction, user: Optional[discord.Member] = None):
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
        total_synced = await self._sync_user_library(target_discord_id, username)

        embed = discord.Embed(
            title="🔄 Library Refreshed!",
            description=(
                f"Successfully refreshed library for **[{username}](https://www.backloggd.com/u/{username}/)**.\n"
                f"• Total Games Cached: **{total_synced}**\n"
                f"• All ratings and statuses are up to date!"
            ),
            color=COLOR_PLAYED
        )
        if linked["avatar_url"]:
            embed.set_thumbnail(url=linked["avatar_url"])

        await interaction.followup.send(embed=embed)

async def setup(bot):
    await bot.add_cog(AccountCog(bot))
