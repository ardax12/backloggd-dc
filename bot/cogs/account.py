"""AccountCog: Handles /link, /unlink, and /refresh with background library sync."""

import discord
from discord import app_commands
from discord.ext import commands
import logging
import asyncio
from typing import Optional
from backloggd.exceptions import ResourceNotFoundError, UserNotFoundError
from bot.config import COLOR_PRIMARY, COLOR_PLAYED, COLOR_ERROR
from bot.input_validation import (
    InvalidUsernameError,
    escape_display_text,
    normalize_backloggd_username,
)
from bot.services.guild_membership import get_membership_service
from bot.services.library_sync import (
    LibrarySyncService,
    SyncOutcome,
    seed_profile_sync_state,
    sync_profile_library,
)
from bot.ui.helpers import build_backloggd_user_url

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
        dirty: bool = False,
        freshness_window_seconds: Optional[float] = None,
    ) -> asyncio.Task:
        """Request one shared refresh for the normalized Backloggd profile."""
        profile_key = username.strip().lower()
        task = self.library_sync.request(
            profile_key,
            lambda: self._sync_user_library(discord_id, username),
            manual=manual,
            dirty=dirty,
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
        await seed_profile_sync_state(self.bot, username, service=self.library_sync)

    async def _update_link_status(self, task: asyncio.Task, username: str, status_message) -> None:
        safe_username = escape_display_text(username)
        try:
            result = task.result()
        except asyncio.CancelledError:
            title = "Library sync stopped"
            description = f"The sync for **{safe_username}** was interrupted. Run `/refresh` to try again."
            color = COLOR_PRIMARY
        else:
            if not isinstance(result, SyncOutcome):
                result = SyncOutcome(status="failed" if result is None else "success", game_count=result)
            if result.status == "failed":
                title = "Library sync failed"
                description = f"Backloggd could not sync **{safe_username}** right now. Your previous library was kept; try `/refresh` shortly."
                color = COLOR_ERROR
            elif result.status == "skipped" and result.reason == "fresh":
                title = "Shared library is current"
                description = (
                    f"Using the shared library for **{safe_username}** ({result.game_count or 0} games); "
                    "it was synced within the last hour. Use `/refresh` to force a new sync."
                )
                color = COLOR_PRIMARY
            elif result.status == "skipped":
                title = "Using the shared library"
                description = (
                    f"A recent refresh for **{safe_username}** is still within its automatic cooldown. "
                    "The previous library was kept; use `/refresh` to force a retry."
                )
                color = COLOR_PRIMARY
            else:
                title = "Library sync complete"
                description = f"Synced **{result.game_count or 0}** games for **{safe_username}**. Your ratings are ready to use with `/game`."
                color = COLOR_PLAYED
        embed = discord.Embed(title=title, description=description, color=color)
        try:
            await status_message.edit(embed=embed)
        except discord.HTTPException:
            logger.info("Could not update link status message for %s", username, exc_info=True)

    async def _sync_user_library(self, discord_id: int, username: str) -> Optional[int]:
        """Fetch and atomically replace the linked profile's cached library."""
        return await sync_profile_library(self.bot, username, discord_id=discord_id)

    @app_commands.command(name="link", description="Link your Discord account to your Backloggd username")
    @app_commands.describe(username="Your Backloggd username (e.g. Gamer123)")
    async def link(self, interaction: discord.Interaction, username: str):
        await interaction.response.defer()

        try:
            clean_user = normalize_backloggd_username(username)
        except InvalidUsernameError as err:
            await interaction.followup.send(
                f"❌ {err}",
                ephemeral=True,
                allowed_mentions=discord.AllowedMentions.none(),
            )
            return

        # 1. Check if the user is already linked to an account
        existing_link = await self.bot.db.get_linked_user(interaction.user.id)
        if existing_link:
            linked_name = existing_link["backloggd_username"]
            safe_linked = escape_display_text(linked_name)
            linked_url = build_backloggd_user_url(linked_name)
            if linked_name.lower() == clean_user.lower():
                if interaction.guild is not None and hasattr(self.bot.db, "set_guild_feed_preference"):
                    await self.bot.db.set_guild_feed_preference(
                        interaction.guild_id, interaction.user.id, True
                    )
                    participation = " Your profile is also enabled for this server's activity feed."
                else:
                    participation = (
                        " Use `/feed on` in each server where you want activity announcements."
                        if interaction.guild is None else ""
                    )
                await interaction.followup.send(
                    f"ℹ️ You are already linked to Backloggd user **[{safe_linked}]({linked_url})**.\n"
                    f"If you want to sync your latest games and ratings, use `/refresh`." + participation,
                    ephemeral=True,
                    allowed_mentions=discord.AllowedMentions.none(),
                )
            else:
                await interaction.followup.send(
                    f"⚠️ You are currently linked to Backloggd user **[{safe_linked}]({linked_url})**.\n"
                    f"Please run `/unlink` first before linking a different account.",
                    ephemeral=True,
                    allowed_mentions=discord.AllowedMentions.none(),
                )
            return

        safe_clean = escape_display_text(clean_user)
        try:
            profile = await self.bot.backloggd.get_user_profile(clean_user)
            if not profile or not profile.username:
                await interaction.followup.send(
                    f"❌ Could not find a Backloggd user with the username **{safe_clean}**.",
                    ephemeral=True,
                    allowed_mentions=discord.AllowedMentions.none(),
                )
                return
        except (UserNotFoundError, ResourceNotFoundError):
            await interaction.followup.send(
                f"❌ Could not find a Backloggd user with the username **{safe_clean}**.",
                ephemeral=True,
                allowed_mentions=discord.AllowedMentions.none(),
            )
            return
        except Exception as e:
            logger.error("Failed to verify user %s: %s", clean_user, e)
            await interaction.followup.send(
                f"❌ Backloggd could not verify **{safe_clean}** right now. Please try again shortly.",
                ephemeral=True,
                allowed_mentions=discord.AllowedMentions.none(),
            )
            return

        await self.bot.db.link_user(
            discord_id=interaction.user.id,
            username=profile.username,
            avatar_url=profile.avatar_url,
        )
        if interaction.guild is not None and hasattr(self.bot.db, "set_guild_feed_preference"):
            await self.bot.db.set_guild_feed_preference(
                interaction.guild_id, interaction.user.id, True
            )
        await self._seed_sync_state(profile.username)

        safe_profile_name = escape_display_text(profile.username)
        profile_url = build_backloggd_user_url(profile.username)
        embed = discord.Embed(
            title="🔗 Account Linked!",
            description=(
                f"Connected your Discord account to the public Backloggd profile **[{safe_profile_name}]({profile_url})**. "
                f"This link does not verify profile ownership.\n\n"
                f"Syncing your library in the background so your ratings show up on `/game`..."
            ),
            color=COLOR_PLAYED
        )
        if profile.avatar_url:
            embed.set_thumbnail(url=profile.avatar_url)

        duplicate_names = []
        if interaction.guild:
            guild_members = await get_membership_service(self.bot).get_members(interaction.guild)
            if guild_members is not None:
                members = {member.id: member for member in guild_members}
                connections = await self.bot.db.get_profile_connections(
                    profile.username,
                    list(members),
                    exclude_discord_id=interaction.user.id,
                )
                duplicate_names = [
                    escape_display_text(members[row["discord_id"]].display_name)
                    for row in connections
                    if row["discord_id"] in members
                ]
        if duplicate_names:
            disclaimer = "\nThese are separate public-profile links; they do not prove who owns the profile."
            shown_names = duplicate_names[:8]
            if len(duplicate_names) > len(shown_names):
                shown_names.append(f"and {len(duplicate_names) - len(shown_names)} more")
            max_names_len = 1024 - len(disclaimer)
            names_text = ", ".join(shown_names)
            if len(names_text) > max_names_len:
                names_text = names_text[: max(0, max_names_len - 3)].rstrip(", \\") + "..."
            embed.add_field(
                name="Also linked in this server",
                value=names_text + disclaimer,
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
    @app_commands.describe(user="Optional: Server member whose linked library to refresh (defaults to you)")
    async def refresh(self, interaction: discord.Interaction, user: Optional[discord.Member] = None):
        await interaction.response.defer()

        target_discord_id = user.id if user else interaction.user.id
        linked = await self.bot.db.get_linked_user(target_discord_id)

        if not linked:
            if user:
                safe_name = escape_display_text(user.display_name)
                msg = f"❌ **{safe_name}** does not have a linked Backloggd account yet. They can link one with `/link <username>`."
            else:
                msg = "❌ You do not have a linked Backloggd account. Use `/link <username>` first."
            await interaction.followup.send(
                msg,
                ephemeral=True,
                allowed_mentions=discord.AllowedMentions.none(),
            )
            return

        username = linked["backloggd_username"]
        safe_username = escape_display_text(username)
        user_url = build_backloggd_user_url(username)
        profile_key = username.strip().lower()
        already_running = self.library_sync.is_refreshing(profile_key)
        embed = discord.Embed(
            title="🔄 Refresh In Progress..." if already_running else "🔄 Library Refresh Queued",
            description=(
                (
                    f"A refresh for **[{safe_username}]({user_url})** is already running; "
                    "this message will update when it finishes."
                )
                if already_running
                else (
                    f"Queued a library refresh for **[{safe_username}]({user_url})** in the background. "
                    "This message will update when it finishes."
                )
            )[:4096],
            color=COLOR_PRIMARY,
        )
        if linked["avatar_url"]:
            embed.set_thumbnail(url=linked["avatar_url"])

        status_message = await interaction.followup.send(
            embed=embed,
            wait=True,
            allowed_mentions=discord.AllowedMentions.none(),
        )
        self._start_sync_task(
            target_discord_id,
            username,
            status_message=status_message,
            manual=True,
        )

async def setup(bot):
    await bot.add_cog(AccountCog(bot))
