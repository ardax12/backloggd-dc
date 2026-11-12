"""AccountCog: Handles /link, /unlink, and /refresh with background library sync."""

import discord
from discord import app_commands
from discord.ext import commands
import logging
import asyncio
from datetime import datetime, timezone
from typing import Optional
from bot.config import COLOR_PRIMARY, COLOR_PLAYED, COLOR_ERROR
from bot.services.library_sync import LibrarySyncService, SyncOutcome

logger = logging.getLogger("backloggd.cogs.account")

class AccountCog(commands.Cog, name="Account"):
    def __init__(self, bot):
        self.bot = bot
        self.library_sync = getattr(bot, "library_sync", None) or LibrarySyncService()

    def _start_sync_task(
        self,
        discord_id: int,
        username: str,
        status_message=None,
        *,
        manual: bool = False,
        freshness_window_seconds: Optional[float] = None,
    ) -> asyncio.Task:
        """Request one shared refresh for the normalized Backloggd profile."""
        profile_key = username.strip().lower()
        task = self.library_sync.request(
            profile_key,
            lambda: self._sync_user_library(discord_id, username),
            manual=manual,
            freshness_window_seconds=freshness_window_seconds,
        )
        if status_message:
            task.add_done_callback(
                lambda completed: asyncio.create_task(
                    self._update_link_status(completed, username, status_message)
                )
            )
        return task

    async def _seed_sync_state(self, username: str) -> None:
        """Seed the in-memory coordinator from the persisted complete snapshot."""
        getter = getattr(self.bot.db, "get_profile_sync_info", None)
        if getter is None:
            return
        info = await getter(username)
        if not info or not info["last_synced_at"]:
            return
        try:
            synced_at = info["last_synced_at"]
            if not isinstance(synced_at, datetime):
                synced_at = datetime.fromisoformat(str(synced_at).replace("Z", "+00:00"))
            if synced_at.tzinfo is None:
                synced_at = synced_at.replace(tzinfo=timezone.utc)
            age_seconds = (datetime.now(timezone.utc) - synced_at.astimezone(timezone.utc)).total_seconds()
            self.library_sync.seed_success(
                username.strip().lower(),
                age_seconds=max(0.0, age_seconds),
                game_count=int(info["game_count"] or 0),
            )
        except (TypeError, ValueError, OverflowError):
            logger.info("Could not seed library refresh age for %s", username, exc_info=True)

    async def _update_link_status(self, task: asyncio.Task, username: str, status_message) -> None:
        try:
            result = task.result()
        except asyncio.CancelledError:
            title = "Library sync stopped"
            description = f"The sync for **{username}** was interrupted. Run `/refresh` to try again."
            color = COLOR_PRIMARY
        else:
            if not isinstance(result, SyncOutcome):
                result = SyncOutcome(status="failed" if result is None else "success", game_count=result)
            if result.status == "failed":
                title = "Library sync failed"
                description = f"Backloggd could not sync **{username}** right now. Your previous library was kept; try `/refresh` shortly."
                color = COLOR_ERROR
            elif result.status == "skipped" and result.reason == "fresh":
                title = "Shared library is current"
                description = (
                    f"Using the shared library for **{username}** ({result.game_count or 0} games); "
                    "it was synced within the last hour. Use `/refresh` to force a new sync."
                )
                color = COLOR_PRIMARY
            elif result.status == "skipped":
                title = "Using the shared library"
                description = (
                    f"A recent refresh for **{username}** is still within its automatic cooldown. "
                    "The previous library was kept; use `/refresh` to force a retry."
                )
                color = COLOR_PRIMARY
            else:
                title = "Library sync complete"
                description = f"Synced **{result.game_count or 0}** games for **{username}**. Your ratings are ready to use with `/game`."
                color = COLOR_PLAYED
        embed = discord.Embed(title=title, description=description, color=color)
        try:
            await status_message.edit(embed=embed)
        except discord.HTTPException:
            logger.info("Could not update link status message for %s", username, exc_info=True)

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

        await self.bot.db.link_user(
            discord_id=interaction.user.id,
            username=profile.username,
            avatar_url=profile.avatar_url,
        )
        await self._seed_sync_state(profile.username)

        embed = discord.Embed(
            title="🔗 Account Linked!",
            description=(
                f"Connected your Discord account to the public Backloggd profile **[{profile.username}](https://www.backloggd.com/u/{profile.username}/)**. "
                f"This link does not verify profile ownership.\n\n"
                f"Syncing your library in the background so your ratings show up on `/game`..."
            ),
            color=COLOR_PLAYED
        )
        if profile.avatar_url:
            embed.set_thumbnail(url=profile.avatar_url)

        duplicate_names = []
        if interaction.guild:
            members = {member.id: member for member in interaction.guild.members}
            connections = await self.bot.db.get_profile_connections(
                profile.username,
                list(members),
                exclude_discord_id=interaction.user.id,
            )
            duplicate_names = [
                discord.utils.escape_markdown(members[row["discord_id"]].display_name)
                for row in connections
                if row["discord_id"] in members
            ]
        if duplicate_names:
            shown_names = duplicate_names[:8]
            if len(duplicate_names) > len(shown_names):
                shown_names.append(f"and {len(duplicate_names) - len(shown_names)} more")
            embed.add_field(
                name="Also linked in this server",
                value=(
                    ", ".join(shown_names)
                    + "\nThese are separate public-profile links; they do not prove who owns the profile."
                ),
                inline=False,
            )

        status_message = await interaction.followup.send(
            embed=embed,
            wait=True,
            allowed_mentions=discord.AllowedMentions.none(),
        )

        # The profile-keyed service shares work across members and servers.
        self._start_sync_task(
            interaction.user.id,
            profile.username,
            status_message=status_message,
            freshness_window_seconds=3600,
        )

    @app_commands.command(name="unlink", description="Unlink your Backloggd account from Discord")
    async def unlink(self, interaction: discord.Interaction):
        success = await self.bot.db.unlink_user(interaction.user.id)
        if success:
            await interaction.response.send_message(
                "Successfully disconnected your Backloggd profile. Shared library data remains available to other linked members, and past feed posts stay in place.",
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
        task = self._start_sync_task(target_discord_id, username, manual=True)
        try:
            result = await task
        except asyncio.CancelledError:
            await interaction.followup.send(
                "❌ This refresh was superseded by a newer account sync. Please try again.",
                ephemeral=True,
            )
            return

        if result.status == "failed":
            await interaction.followup.send(
                f"❌ Could not refresh **{username}**. The previous cached library was kept; try again shortly.",
                ephemeral=True,
            )
            return

        total_synced = result.game_count or 0

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
