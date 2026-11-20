"""AccountCog: Handles /link, /unlink, and /refresh with background library sync."""

import asyncio
import logging
from typing import Optional

import discord
from backloggd.exceptions import ResourceNotFoundError, UserNotFoundError
from discord import app_commands
from discord.ext import commands

from bot.command_errors import defer_interaction, send_private_followup, send_public_followup
from bot.config import COLOR_ERROR, COLOR_PLAYED, COLOR_PRIMARY
from bot.input_validation import (
    InvalidUsernameError,
    escape_display_text,
    normalize_backloggd_username,
)
from bot.services.cooldowns import (
    COOLDOWN_SPECS,
    CooldownCommandName,
    GuildCooldownService,
    get_cooldown_spec,
    guild_cooldown,
    is_server_moderator,
)
from bot.services.guild_membership import get_membership_service
from bot.services.library_sync import (
    LibrarySyncService,
    SyncOutcome,
    seed_profile_sync_state,
    sync_profile_library,
)
from bot.services.session_health import report_upstream_failure, report_upstream_success
from bot.services.task_lifecycle import TaskDrainGroup
from bot.ui.helpers import build_backloggd_user_url, clamp_escaped_text, clean_image_url

logger = logging.getLogger("backloggd.cogs.account")

class AccountCog(commands.Cog, name="Account"):
    cooldown = app_commands.Group(
        name="cooldown",
        description="View or adjust per-command cooldowns for this server",
        guild_only=True,
    )

    def __init__(self, bot):
        self.bot = bot
        self.library_sync: LibrarySyncService = bot.library_sync
        self._fallback_cooldowns: Optional[GuildCooldownService] = None
        self._drain_group = TaskDrainGroup()
        self._status_tasks: set[asyncio.Task] = self._drain_group.tasks

    @property
    def _closing(self) -> bool:
        return self._drain_group.closing

    @_closing.setter
    def _closing(self, value: bool) -> None:
        self._drain_group.closing = bool(value)

    def _cooldown_service(self) -> GuildCooldownService:
        service = getattr(self.bot, "cooldowns", None)
        if isinstance(service, GuildCooldownService):
            return service
        if self._fallback_cooldowns is None:
            self._fallback_cooldowns = GuildCooldownService(
                db_provider=lambda: getattr(self.bot, "db", None)
            )
        return self._fallback_cooldowns

    async def close(self) -> None:
        """Stop and drain account-owned status edits; the bot owns library syncs."""
        await self._drain_group.close(
            task_source=lambda: self._status_tasks,
            clear_tasks=True,
        )

    async def cog_unload(self) -> None:
        """Drain account-owned status-edit tasks when unloaded."""
        await self.close()

    def _on_status_task_done(self, task: asyncio.Task, username: str) -> None:
        self._status_tasks.discard(task)
        if task.cancelled():
            return
        exc = task.exception()
        if exc is not None:
            logger.error(
                "Unexpected error updating library sync status for %s",
                username,
                exc_info=exc,
            )

    def _schedule_status_edit(
        self,
        completed: asyncio.Task,
        username: str,
        status_message,
    ) -> None:
        if self._closing:
            if not completed.cancelled():
                completed.exception()
            return
        edit_task = asyncio.create_task(
            self._update_link_status(completed, username, status_message)
        )
        self._status_tasks.add(edit_task)
        edit_task.add_done_callback(
            lambda finished, name=username: self._on_status_task_done(finished, name)
        )

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
        if self._closing:
            async def _cancelled():
                raise asyncio.CancelledError("Account cog is shutting down")
            return asyncio.create_task(_cancelled())

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
                lambda completed, name=username, msg=status_message: self._schedule_status_edit(
                    completed, name, msg
                )
            )
        return task

    async def _seed_sync_state(self, username: str) -> None:
        """Seed the in-memory coordinator from the persisted complete snapshot."""
        await seed_profile_sync_state(self.bot, username, service=self.library_sync)

    async def _update_link_status(self, task: asyncio.Task, username: str, status_message) -> None:
        if self._closing:
            if not task.cancelled():
                task.exception()
            return
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
        if self._closing:
            return
        embed = discord.Embed(title=title, description=description, color=color)
        try:
            await status_message.edit(embed=embed)
        except discord.HTTPException:
            logger.info("Could not update link status message for %s", username, exc_info=True)

    async def _sync_user_library(self, discord_id: int, username: str) -> Optional[int]:
        """Fetch and atomically replace the linked profile's cached library."""
        return await sync_profile_library(self.bot, username)

    async def _build_same_profile_link_notice(
        self, interaction: discord.Interaction, linked_name: str
    ) -> str:
        safe_linked = escape_display_text(linked_name)
        linked_url = build_backloggd_user_url(linked_name)
        if interaction.guild is not None and hasattr(self.bot.db, "set_guild_feed_preference"):
            guild_members = await get_membership_service(self.bot).get_members(interaction.guild)
            member_ids = [member.id for member in guild_members] if guild_members is not None else None
            await self.bot.db.set_guild_feed_preference(
                interaction.guild_id, interaction.user.id, True, member_ids=member_ids,
            )
            participation = " Your profile is also enabled for this server's activity feed."
        else:
            participation = (
                " Use `/feed on` in each server where you want activity announcements."
                if interaction.guild is None
                else ""
            )
        return (
            f"ℹ️ You are already linked to Backloggd user **[{safe_linked}]({linked_url})**.\n"
            f"If you want to sync your latest games and ratings, use `/refresh`.{participation}"
        )

    @app_commands.command(name="link", description="Link your Discord account to your Backloggd username")
    @app_commands.describe(username="Your Backloggd username (e.g. Gamer123)")
    async def link(self, interaction: discord.Interaction, username: str):
        await defer_interaction(interaction)

        try:
            clean_user = normalize_backloggd_username(username)
        except InvalidUsernameError as err:
            await send_private_followup(interaction, f"❌ {err}", log=logger)
            return

        # 1. Check if the user is already linked to an account
        existing_link = await self.bot.db.get_linked_user(interaction.user.id)
        if existing_link:
            linked_name = existing_link["backloggd_username"]
            if linked_name.strip().lower() == clean_user.lower():
                message = await self._build_same_profile_link_notice(interaction, linked_name)
                await send_private_followup(interaction, message, log=logger)
            else:
                safe_linked = escape_display_text(linked_name)
                linked_url = build_backloggd_user_url(linked_name)
                await send_private_followup(
                    interaction,
                    f"⚠️ You are currently linked to Backloggd user **[{safe_linked}]({linked_url})**.\n"
                    f"Please run `/unlink` first before linking a different account.",
                    log=logger,
                )
            return

        safe_clean = escape_display_text(clean_user)
        cache = getattr(self.bot.backloggd, "cache", None)
        get_cached_profile = getattr(cache, "get_profile", None)
        was_cached = callable(get_cached_profile) and get_cached_profile(clean_user) is not None
        try:
            profile = await self.bot.backloggd.get_user_profile(clean_user)
            if not profile or not profile.username:
                await send_private_followup(
                    interaction,
                    f"❌ Could not find a Backloggd user with the username **{safe_clean}**.",
                    log=logger,
                )
                return
            if not was_cached:
                report_upstream_success(self.bot)
        except (UserNotFoundError, ResourceNotFoundError):
            await send_private_followup(
                interaction,
                f"❌ Could not find a Backloggd user with the username **{safe_clean}**.",
                log=logger,
            )
            return
        except Exception as e:
            report_upstream_failure(self.bot, e)
            logger.error("Failed to verify user %s: %s", clean_user, e)
            await send_private_followup(
                interaction,
                f"❌ Backloggd could not verify **{safe_clean}** right now. Please try again shortly.",
                log=logger,
            )
            return

        guild_members = None
        member_ids = None
        if interaction.guild is not None:
            guild_members = await get_membership_service(self.bot).get_members(interaction.guild)
            if guild_members is not None:
                member_ids = [member.id for member in guild_members]

        linked = await self.bot.db.link_user_if_unlinked(
            discord_id=interaction.user.id,
            username=profile.username,
            avatar_url=profile.avatar_url,
        )
        if not linked:
            current_link = await self.bot.db.get_linked_user(interaction.user.id)
            if current_link is None:
                conflict_message = (
                    "ℹ️ Your account connection changed while verifying this profile. "
                    "Please run `/link` again."
                )
            elif current_link["backloggd_username"].strip().lower() == profile.username.strip().lower():
                conflict_message = await self._build_same_profile_link_notice(
                    interaction, current_link["backloggd_username"]
                )
            else:
                conflict_message = (
                    "⚠️ Your Discord account was linked while this Backloggd profile was being verified. "
                    "Run `/unlink` before linking a different account."
                )
            await send_private_followup(interaction, conflict_message, log=logger)
            return

        if interaction.guild is not None and hasattr(self.bot.db, "set_guild_feed_preference"):
            await self.bot.db.set_guild_feed_preference(
                interaction.guild_id, interaction.user.id, True, member_ids=member_ids,
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
        clean_avatar = clean_image_url(profile.avatar_url)
        if clean_avatar:
            embed.set_thumbnail(url=clean_avatar)

        duplicate_names = []
        if interaction.guild and guild_members is not None:
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
            names_text = clamp_escaped_text(", ".join(shown_names), max_names_len)
            embed.add_field(
                name="Also linked in this server",
                value=names_text + disclaimer,
                inline=False,
            )

        status_message = await send_public_followup(
            interaction,
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
        await defer_interaction(interaction, ephemeral=True)
        success = await self.bot.db.unlink_user(interaction.user.id)
        if success:
            await send_private_followup(
                interaction,
                "Successfully disconnected your Backloggd profile. Shared library data remains available to other linked members, and past feed posts stay in place.",
                log=logger,
            )
        else:
            await send_private_followup(
                interaction,
                "ℹ️ You do not currently have a Backloggd account linked.",
                log=logger,
            )

    @app_commands.command(name="refresh", description="Refresh your Backloggd library and ratings")
    @app_commands.describe(user="Optional: Server member whose linked library to refresh (defaults to you)")
    @guild_cooldown("refresh")
    async def refresh(self, interaction: discord.Interaction, user: Optional[discord.Member] = None):
        await defer_interaction(interaction)

        target_discord_id = user.id if user else interaction.user.id
        linked = await self.bot.db.get_linked_user(target_discord_id)

        if not linked:
            if user:
                safe_name = escape_display_text(user.display_name)
                msg = f"❌ **{safe_name}** does not have a linked Backloggd account yet. They can link one with `/link <username>`."
            else:
                msg = "❌ You do not have a linked Backloggd account. Use `/link <username>` first."
            await send_private_followup(interaction, msg, log=logger)
            return

        username = linked["backloggd_username"]
        safe_username = escape_display_text(username)
        user_url = build_backloggd_user_url(username)
        profile_key = username.strip().lower()
        already_running = self.library_sync.is_refreshing(profile_key)
        embed = discord.Embed(
            title="🔄 Refresh In Progress..." if already_running else "🔄 Library Refresh Queued",
            description=clamp_escaped_text(
                (
                    f"A refresh for **[{safe_username}]({user_url})** is already running; "
                    "this message will update when it finishes."
                )
                if already_running
                else (
                    f"Queued a library refresh for **[{safe_username}]({user_url})** in the background. "
                    "This message will update when it finishes."
                ),
                4096,
            ),
            color=COLOR_PRIMARY,
        )
        clean_avatar = clean_image_url(linked["avatar_url"])
        if clean_avatar:
            embed.set_thumbnail(url=clean_avatar)

        status_message = await send_public_followup(
            interaction,
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

    @cooldown.command(name="view", description="View command cooldowns configured for this server")
    @is_server_moderator()
    async def cooldown_view(self, interaction: discord.Interaction):
        if interaction.guild_id is None:
            await send_private_followup(interaction, "This command can only be used in a server.", log=logger)
            return
        await defer_interaction(interaction, ephemeral=True)
        service = self._cooldown_service()
        effective = await service.get_all_guild_cooldowns(interaction.guild_id, db=self.bot.db)
        overrides = await service.get_guild_overrides(interaction.guild_id, db=self.bot.db)

        lines = []
        for name, spec in COOLDOWN_SPECS.items():
            current = effective[name]
            tag = "custom" if name in overrides else "default"
            lines.append(
                f"`/{name}`: **{current}s** ({tag} · default {spec.default_seconds}s · "
                f"range {spec.min_seconds}–{spec.max_seconds}s · {spec.scope_label})"
            )

        embed = discord.Embed(
            title="⏱️ Server Command Cooldowns",
            description="\n".join(lines),
            color=COLOR_PRIMARY,
        )
        embed.set_footer(text="Use /cooldown set <command> <seconds> or /cooldown reset [command].")
        await send_private_followup(interaction, embed=embed, log=logger)

    @cooldown.command(name="set", description="Set a custom cooldown duration (in seconds) for a command in this server")
    @app_commands.describe(
        command="The command whose cooldown you want to change",
        seconds="Cooldown duration in seconds (within allowed command bounds)",
    )
    @is_server_moderator()
    async def cooldown_set(
        self,
        interaction: discord.Interaction,
        command: CooldownCommandName,
        seconds: app_commands.Range[int, 3, 3600],
    ):
        if interaction.guild_id is None:
            await send_private_followup(interaction, "This command can only be used in a server.", log=logger)
            return
        await defer_interaction(interaction, ephemeral=True)
        service = self._cooldown_service()
        try:
            updated = await service.set_cooldown(
                interaction.guild_id, command, seconds, db=self.bot.db
            )
        except ValueError as err:
            await send_private_followup(interaction, f"❌ {err}", log=logger)
            return

        spec = get_cooldown_spec(command)
        await send_private_followup(
            interaction,
            f"✅ Set `/{spec.command_name}` cooldown to **{updated}s** "
            f"({spec.scope_label}; default: {spec.default_seconds}s) for this server.",
            log=logger,
        )

    @cooldown.command(name="reset", description="Reset one or all server command cooldowns back to defaults")
    @app_commands.describe(command="Optional command to reset (leave empty to reset all commands)")
    @is_server_moderator()
    async def cooldown_reset(
        self,
        interaction: discord.Interaction,
        command: Optional[CooldownCommandName] = None,
    ):
        if interaction.guild_id is None:
            await send_private_followup(interaction, "This command can only be used in a server.", log=logger)
            return
        await defer_interaction(interaction, ephemeral=True)
        service = self._cooldown_service()
        try:
            removed = await service.reset_cooldown(
                interaction.guild_id, command, db=self.bot.db
            )
        except ValueError as err:
            await send_private_followup(interaction, f"❌ {err}", log=logger)
            return

        if command is not None:
            spec = get_cooldown_spec(command)
            if removed:
                msg = (
                    f"✅ Reset `/{spec.command_name}` cooldown to default "
                    f"(**{spec.default_seconds}s**, {spec.scope_label})."
                )
            else:
                msg = (
                    f"ℹ️ `/{spec.command_name}` is already using its default cooldown "
                    f"(**{spec.default_seconds}s**, {spec.scope_label})."
                )
        else:
            if removed:
                msg = f"✅ Reset {removed} custom cooldown override(s) in this server back to defaults."
            else:
                msg = "ℹ️ All commands in this server are already using their default cooldowns."

        await send_private_followup(interaction, msg, log=logger)

async def setup(bot):
    await bot.add_cog(AccountCog(bot))
