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
        self._sync_tasks: dict[int, asyncio.Task] = {}

    def cog_unload(self) -> None:
        for task in self._sync_tasks.values():
            task.cancel()
        self._sync_tasks.clear()

    def _start_sync_task(self, discord_id: int, username: str, status_message=None) -> asyncio.Task:
        """Keep only the newest library sync active for each Discord account."""
        previous = self._sync_tasks.get(discord_id)
        if previous and not previous.done():
            previous.cancel()

        task = asyncio.create_task(self._sync_user_library(discord_id, username))
        self._sync_tasks[discord_id] = task

        def clear_task(completed: asyncio.Task) -> None:
            if self._sync_tasks.get(discord_id) is completed:
                self._sync_tasks.pop(discord_id, None)

        task.add_done_callback(clear_task)
        if status_message:
            task.add_done_callback(
                lambda completed: asyncio.create_task(
                    self._update_link_status(completed, username, status_message)
                )
            )
        return task

    async def _update_link_status(self, task: asyncio.Task, username: str, status_message) -> None:
        try:
            total_synced = task.result()
        except asyncio.CancelledError:
            title = "Library sync stopped"
            description = f"The sync for **{username}** was interrupted. Run `/refresh` to try again."
            color = COLOR_PRIMARY
        else:
            if total_synced is None:
                title = "Library sync failed"
                description = f"Backloggd could not sync **{username}** right now. Your previous library was kept; try `/refresh` shortly."
                color = COLOR_ERROR
            else:
                title = "Library sync complete"
                description = f"Synced **{total_synced}** games for **{username}**. Your ratings are ready to use with `/game`."
                color = COLOR_PLAYED
        embed = discord.Embed(title=title, description=description, color=color)
        try:
            await status_message.edit(embed=embed)
        except discord.HTTPException:
            logger.info("Could not update link status message for %s", username, exc_info=True)

    def _cancel_sync_task(self, discord_id: int) -> None:
        task = self._sync_tasks.pop(discord_id, None)
        if task and not task.done():
            task.cancel()

    async def _sync_user_library(self, discord_id: int, username: str) -> Optional[int]:
        """Fetch and atomically replace the linked user's cached library."""
        try:
            library = await self.bot.backloggd.get_user_library(username, force_refresh=True)
            replaced = await self.bot.db.replace_user_library(
                discord_id, username, library
            )
            if not replaced:
                logger.info("Discarded stale library sync for %s (%d)", username, discord_id)
                return None

            logger.info("Synced library for %s (%d games)", username, len(library))
            return len(library)
        except Exception as e:
            logger.error("Error syncing library for %s: %s", username, e, exc_info=True)
            return None

    @app_commands.command(name="link", description="Link your Discord account to your Backloggd username")
    @app_commands.describe(username="Your Backloggd username (e.g. Gamer123)")
    async def link(self, interaction: discord.Interaction, username: str):
        await interaction.response.defer()

        clean_user = username.strip().lstrip("@")

        # 1. Check if the user is already linked to an account
        existing_link = await self.bot.db.get_linked_user(interaction.user.id)
        if existing_link:
            linked_name = existing_link["backloggd_username"]
            if linked_name.lower() == clean_user.lower():
                await interaction.followup.send(
                    f"ℹ️ You are already linked to Backloggd user **[{linked_name}](https://www.backloggd.com/u/{linked_name}/)**.\n"
                    f"If you want to sync your latest games and ratings, use `/refresh`.",
                    ephemeral=True
                )
            else:
                await interaction.followup.send(
                    f"⚠️ You are currently linked to Backloggd user **[{linked_name}](https://www.backloggd.com/u/{linked_name}/)**.\n"
                    f"Please run `/unlink` first before linking a different account.",
                    ephemeral=True
                )
            return

        # 2. Check if the Backloggd account is already claimed by another Discord user
        other_user = await self.bot.db.get_linked_user_by_username(clean_user)
        if other_user and other_user["discord_id"] != interaction.user.id:
            await interaction.followup.send(
                f"❌ The Backloggd username **{clean_user}** is already linked to another Discord user (<@{other_user['discord_id']}>).",
                ephemeral=True
            )
            return

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
                f"❌ Backloggd could not verify **{clean_user}** right now. Please try again shortly.",
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

        status_message = await interaction.followup.send(embed=embed, wait=True)

        # Background sync. An older in-flight sync cannot write after a relink.
        self._start_sync_task(interaction.user.id, profile.username, status_message=status_message)

    @app_commands.command(name="unlink", description="Unlink your Backloggd account from Discord")
    async def unlink(self, interaction: discord.Interaction):
        self._cancel_sync_task(interaction.user.id)
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
        task = self._start_sync_task(target_discord_id, username)
        try:
            total_synced = await task
        except asyncio.CancelledError:
            await interaction.followup.send(
                "❌ This refresh was superseded by a newer account sync. Please try again.",
                ephemeral=True,
            )
            return

        if total_synced is None:
            await interaction.followup.send(
                f"❌ Could not refresh **{username}**. The previous cached library was kept; try again shortly.",
                ephemeral=True,
            )
            return

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
