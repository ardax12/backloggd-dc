"""FeedCog: Handles /setchannel, /unsetchannel, and the periodic playthrough journal feed loop."""

import discord
from discord import app_commands
from discord.ext import commands, tasks
import logging
import asyncio
from backloggd.exceptions import ResourceNotFoundError, UserNotFoundError
from bot.command_errors import (
    defer_interaction,
    mark_interaction_error_handled,
    send_ephemeral_error,
    send_private_followup,
)
from bot.input_validation import (
    InvalidUsernameError,
    escape_display_text,
    normalize_backloggd_username,
)
from bot.services.library_sync import parse_sync_timestamp
from bot.ui.views import FeedListView
from bot.ui.helpers import build_backloggd_user_url, clamp_escaped_text, format_health_error_excerpt
from bot.services.feed_pipeline import FeedPipeline
from bot.services.task_lifecycle import await_owned_task

logger = logging.getLogger("backloggd.cogs.feed")

class FeedCog(commands.Cog, name="Feed"):
    feed = app_commands.Group(name="feed", description="Manage your server's Backloggd activity feed")

    def __init__(self, bot, *, clock=None):
        self.bot = bot
        self.pipeline = FeedPipeline(bot, clock=clock)
        self._closing = False
        self._draining_task: asyncio.Task | None = None

    async def cog_load(self):
        # Start loop when cog is loaded and bot event loop is active
        if not self._closing and not self.journal_feed_loop.is_running():
            self.journal_feed_loop.start()

    async def close(self) -> None:
        """Stop accepting polls, cancel the loop and wait for its work to unwind."""
        self._closing = True
        current = asyncio.current_task()
        if self._draining_task is not None:
            await await_owned_task(self._draining_task)
            return

        async def _drain() -> None:
            drain_task = asyncio.current_task()
            task = self.journal_feed_loop.get_task()
            if task is not None and task is not current and task is not drain_task:
                if not task.done() and not task.cancelling():
                    self.journal_feed_loop.cancel()
                await asyncio.gather(task, return_exceptions=True)
            await self.pipeline.close(exclude_task=current)

        self._draining_task = asyncio.create_task(_drain())
        try:
            await await_owned_task(self._draining_task)
        finally:
            if self._draining_task.done():
                self._draining_task = None

    async def cog_unload(self) -> None:
        await self.close()

    @staticmethod
    def _feed_enrollment_state_copy(status: dict) -> str:
        """Describe baseline state without treating missing membership as proof of a gap."""
        if status.get("pending_reconciliation"):
            return (
                "Source continuity is still being verified. No baseline reset is assumed; "
                "a complete member snapshot is required to resolve it."
            )
        if status.get("active"):
            initialized = set(status.get("initialized_sources") or ())
            expected = {"activity-display-v1", "journal-display-v1"}
            if expected.issubset(initialized):
                return "This profile is already active with initialized baselines; this command does not reset them."
            if initialized:
                return "This profile is already active; any uninitialized source will be baselined without resetting existing sources."
            return "This profile is active and its source baseline is pending the first successful check."
        if status.get("last_active_at") is not None:
            return "This profile is resuming after a proven inactive period; existing history will be skipped as a fresh baseline is established."
        return "This is the first enrollment; existing history will be skipped as the initial baseline is established."

    # =========================================================================
    # Channel Configuration Slash Commands (Admin Only)
    # =========================================================================

    @staticmethod
    def _format_rel_timestamp(value) -> str:
        dt = parse_sync_timestamp(value)
        if dt is None:
            return "Never"
        return f"<t:{int(dt.timestamp())}:R>"

    @classmethod
    def _describe_profile_feed_state(
        cls,
        *,
        channel_id,
        status: dict,
        is_candidate: bool,
        target_issue: str | None = None,
    ) -> str:
        if channel_id is None:
            return "Pending channel"
        if not is_candidate:
            return "Inactive"
        if target_issue:
            return f"Unavailable ({target_issue})"
        health_rows = status.get("source_health") or []
        recipient_gap = status.get("recipient_gap")
        has_attributed_recipient_gaps = bool(status.get("has_attributed_recipient_gaps"))

        def _is_masked_shared_recipient_gap(row) -> bool:
            if recipient_gap is not None or not has_attributed_recipient_gaps:
                return False
            keys = row.keys() if hasattr(row, "keys") else ()
            gap_reason = row["gap_reason"] if "gap_reason" in keys else None
            return gap_reason in {"unavailable_feed_target", "delivery_failure"}

        has_error = any(row["last_error"] for row in health_rows)
        has_gap = any(
            not _is_masked_shared_recipient_gap(row)
            and (
                row["unresolved_gap_since"] is not None
                or row["coverage"] in {"limited", "unsupported"}
            )
            for row in health_rows
        )
        initialized = status.get("initialized_sources") or []
        pending_reconciliation = bool(status.get("pending_reconciliation"))
        if not status.get("active") or not initialized:
            if pending_reconciliation:
                return (
                    "Degraded (pending baseline; continuity unverified)"
                    if has_error
                    else "Pending baseline (continuity unverified)"
                )
            return "Degraded (pending baseline)" if has_error else "Pending baseline"
        if pending_reconciliation:
            reasons = []
            if len(initialized) < 2:
                reasons.append("partially checked")
            if has_error:
                reasons.append("degraded")
            if recipient_gap is not None:
                if recipient_gap.get("reason") == "delivery_failure":
                    reasons.append("recipient delivery degraded")
                else:
                    reasons.append("recipient recovery pending")
            elif has_gap:
                reasons.append("incomplete coverage")
            if reasons:
                return f"Pending continuity verification ({'; '.join(reasons)})"
            return "Pending continuity verification"
        if len(initialized) < 2:
            return "Degraded (partially checked)" if has_error else "Partially checked"
        if has_error:
            return "Degraded"
        if recipient_gap is not None:
            if recipient_gap.get("reason") == "delivery_failure":
                return "Degraded (recipient delivery)"
            return "Active (recipient recovery pending)"
        if has_gap:
            return "Active (incomplete coverage)"
        return "Active"

    @app_commands.command(name="setchannel", description="Set the channel where automatic Backloggd playthrough logs are posted")
    @app_commands.describe(channel="The text channel for game log announcements")
    @app_commands.guild_only()
    @app_commands.checks.has_permissions(manage_channels=True)
    async def setchannel(self, interaction: discord.Interaction, channel: discord.TextChannel):
        if interaction.guild is None:
            await interaction.response.send_message("❌ Choose a text channel in a server to set up the activity feed.", ephemeral=True)
            return

        await defer_interaction(interaction, ephemeral=True)

        me = getattr(interaction.guild, "me", None)
        if me is None:
            await send_private_followup(
                interaction,
                "❌ I could not verify my permissions in that channel right now because Discord's member cache is unavailable. Please try `/setchannel` again shortly.",
                log=logger,
            )
            return

        permissions = channel.permissions_for(me)
        missing = [
            name for attribute, name in (
                ("view_channel", "View Channel"),
                ("send_messages", "Send Messages"),
                ("embed_links", "Embed Links"),
            )
            if not getattr(permissions, attribute, False)
        ]
        if missing:
            await send_private_followup(
                interaction,
                "❌ I need **" + "**, **".join(missing) + "** permission in that channel before I can post activity.",
                log=logger,
            )
            return

        await self.bot.db.set_feed_channel(interaction.guild_id, channel.id)
        await self.pipeline.reconcile_legacy_guild(interaction.guild)
        member_ids = await self.pipeline.authoritative_member_ids(interaction.guild)
        if member_ids is not None:
            linked_profiles = await self.bot.db.get_guild_feed_candidates(
                interaction.guild_id, member_ids
            )
            await self.bot.db.reconcile_guild_feed_activity(
                interaction.guild_id,
                [row["profile_key"] for row in linked_profiles],
                now=self.pipeline.clock(),
                present_member_ids=member_ids,
            )
            note = (
                "Each active profile source will initialize on its first successful poll and "
                "skip existing history so only future activity is announced."
            )
        else:
            note = (
                "Discord membership could not be verified right now; active profiles will "
                "enroll and initialize on their first successful poll (skipping existing history, "
                "so only future activity is announced) once a complete member snapshot is available."
            )

        await send_private_followup(
            interaction,
            f"✅ Set {channel.mention} as the Backloggd activity feed channel. {note}",
            log=logger,
        )

    @app_commands.command(name="unsetchannel", description="Disable the automatic Backloggd activity feed for this server")
    @app_commands.guild_only()
    @app_commands.checks.has_permissions(manage_channels=True)
    async def unsetchannel(self, interaction: discord.Interaction):
        if interaction.guild is None:
            await interaction.response.send_message("This command can only be used in a server.", ephemeral=True)
            return
        await defer_interaction(interaction, ephemeral=True)
        removed = await self.bot.db.unset_feed_channel(interaction.guild_id)
        if removed:
            await send_private_followup(
                interaction,
                "✅ Automatic activity feed has been disabled for this server.",
                log=logger,
            )
        else:
            await send_private_followup(
                interaction,
                "ℹ️ No activity feed channel was currently set for this server.",
                log=logger,
            )

    @feed.command(name="on", description="Include your linked Backloggd profile in this server's feed")
    async def feed_on(self, interaction: discord.Interaction):
        if interaction.guild is None:
            await interaction.response.send_message("Use `/feed on` in each server where you want activity announcements.", ephemeral=True)
            return
        await defer_interaction(interaction, ephemeral=True)
        linked = await self.bot.db.get_linked_user(interaction.user.id)
        if not linked:
            await send_private_followup(
                interaction,
                "Link your Backloggd profile with `/link` first. This command does not ask for or change your username.",
                log=logger,
            )
            return
        member_ids = await self.pipeline.authoritative_member_ids(interaction.guild)
        await self.bot.db.set_guild_feed_preference(
            interaction.guild_id, interaction.user.id, True, member_ids=member_ids,
        )
        status = await self.bot.db.get_guild_profile_feed_status(
            interaction.guild_id, linked["profile_key"],
        )
        channel_id = await self.bot.db.get_feed_channel(interaction.guild_id)
        message = f"Your linked profile **{escape_display_text(linked['backloggd_username'])}** is enabled for this server."
        if channel_id is None:
            message += " The feed is pending until an admin runs `/setchannel`; no Discord delivery can happen before a channel is configured."
        if member_ids is None:
            message += " No Discord delivery will happen until a complete member snapshot is available."
        message += " " + self._feed_enrollment_state_copy(status)
        await send_private_followup(interaction, message, log=logger)

    @feed.command(name="off", description="Stop your linked profile from participating in this server's feed")
    async def feed_off(self, interaction: discord.Interaction):
        if interaction.guild is None:
            await interaction.response.send_message("This command can only be used in a server.", ephemeral=True)
            return
        await defer_interaction(interaction, ephemeral=True)
        linked = await self.bot.db.get_linked_user(interaction.user.id)
        if not linked:
            await send_private_followup(
                interaction,
                "You do not have a linked Backloggd profile. Your server preference is unchanged.",
                log=logger,
            )
            return
        member_ids = await self.pipeline.authoritative_member_ids(interaction.guild)
        result = await self.bot.db.disable_guild_feed_preference_and_reconcile(
            interaction.guild_id,
            interaction.user.id,
            member_ids=member_ids,
        )
        if result is None:
            await send_private_followup(
                interaction,
                "You do not have a linked Backloggd profile. Your server preference is unchanged.",
                log=logger,
            )
            return
        if result["deactivated"]:
            message = "Your participation is off. Existing announcements stay archived. Use `/feed on` or `/link` in this server to participate again."
        elif result["pending_reconciliation"]:
            message = "Your opt-out is saved. I could not verify whether another current member keeps this profile active."
        elif result["has_admin"] and not result["remaining_members"]:
            message = "Your participation is off, but this profile remains in this server's feed through an admin follow."
        else:
            message = "Your participation is off, but this profile remains in this server's feed through another linked member."
        await send_private_followup(interaction, message, log=logger)

    @feed.command(name="status", description="Show your profile's feed participation in this server")
    async def feed_status(self, interaction: discord.Interaction):
        if interaction.guild is None:
            await interaction.response.send_message("This command can only be used in a server.", ephemeral=True)
            return
        await defer_interaction(interaction, ephemeral=True)
        linked = await self.bot.db.get_linked_user(interaction.user.id)
        if not linked:
            await send_private_followup(
                interaction,
                "No Backloggd profile is linked to your Discord account. Use `/link` first.",
                log=logger,
            )
            return
        profile_key = linked["profile_key"]
        username = linked["backloggd_username"]
        enabled = await self.bot.db.get_guild_feed_preference(interaction.guild_id, interaction.user.id)
        channel_id = await self.bot.db.get_feed_channel(interaction.guild_id)
        member_ids = await self.pipeline.authoritative_member_ids(interaction.guild)
        sources = (
            await self.bot.db.get_guild_profile_member_sources(
                interaction.guild_id, profile_key, member_ids
            ) if member_ids is not None else []
        )
        admin_source = await self.bot.db.has_guild_admin_follow(
            interaction.guild_id, profile_key
        )
        status = await self.bot.db.get_guild_profile_feed_status(interaction.guild_id, profile_key)
        is_candidate = bool(sources or admin_source) if member_ids is not None else bool(enabled or admin_source)
        _, _, target_issue = self.pipeline.inspect_target_channel(
            interaction.guild_id,
            channel_id,
            guild=interaction.guild,
            require_channel_lookup=False,
        )
        effective_state = self._describe_profile_feed_state(
            channel_id=channel_id,
            status=status,
            is_candidate=is_candidate,
            target_issue=target_issue,
        )

        if channel_id is None:
            channel_text = "Not configured"
        elif target_issue:
            channel_text = f"<#{channel_id}> (Unavailable — {target_issue})"
        else:
            channel_text = f"<#{channel_id}>"
        if sources and admin_source:
            source_summary = "Member + Admin follow"
        elif sources:
            source_summary = "Member (you)" if enabled else "Another linked member"
        elif admin_source:
            source_summary = "Admin follow"
        else:
            source_summary = "None"

        desc_lines = [
            f"**Profile:** [{escape_display_text(username)}]({build_backloggd_user_url(username)})",
            f"**Your participation:** {'On' if enabled else 'Off'}",
            f"**In this server's feed:** {'Yes' if is_candidate else 'No'} ({source_summary})",
            f"**Feed channel:** {channel_text}",
            f"**Effective state:** {effective_state}",
        ]
        if member_ids is None:
            desc_lines.append("⚠️ Current member participation could not be verified from Discord's member cache.")
        elif admin_source and not sources and not enabled:
            desc_lines.append("An admin follow keeps this public profile active in this server's feed.")
        elif sources and not enabled:
            desc_lines.append("Another linked server member keeps this public profile active in this server's feed.")
        if status.get("pending_reconciliation"):
            pending_continuity = status.get("pending_continuity")
            had_admin = bool(pending_continuity.get("had_admin_source")) if isinstance(pending_continuity, dict) else False
            candidate_count = int(pending_continuity.get("candidate_member_count") or 0) if isinstance(pending_continuity, dict) else 0
            if had_admin and candidate_count > 0:
                desc_lines.append(
                    f"Continuity unverified: awaiting a complete member snapshot to check prior admin and {candidate_count} candidate member source(s); neither continuity nor a reset has been confirmed."
                )
            elif had_admin:
                desc_lines.append(
                    "Continuity unverified: awaiting a complete member snapshot to check a prior admin follow; neither continuity nor a reset has been confirmed."
                )
            elif candidate_count > 0:
                desc_lines.append(
                    f"Continuity unverified: awaiting a complete member snapshot to check {candidate_count} prior member candidate(s); neither continuity nor a reset has been confirmed."
                )
            else:
                desc_lines.append(
                    "Continuity unverified: awaiting a complete member snapshot; no prior member candidates remain, pending final snapshot confirmation."
                )

        embed = discord.Embed(
            title="Server Feed Status",
            description="\n".join(desc_lines),
            color=discord.Color.blurple(),
        )

        initialized = set(status.get("initialized_sources") or [])
        health_by_source = {row["source"]: row for row in (status.get("source_health") or [])}
        recipient_gap = status.get("recipient_gap")
        has_attributed_recipient_gaps = bool(status.get("has_attributed_recipient_gaps"))
        source_lines = []
        for source_name, display_name in (("activity", "Activity"), ("journal", "Journal")):
            marker = f"{source_name}-display-v1"
            baselined = "Initialized" if marker in initialized else "Pending baseline"
            row = health_by_source.get(source_name)
            if not row:
                line = f"**{display_name}:** {baselined} · Last check: Never"
                if source_name == "activity" and recipient_gap is not None:
                    rg_since = self._format_rel_timestamp(recipient_gap.get("gap_since"))
                    line += f" · Recipient gap: {rg_since} \u2192 present"
                    if recipient_gap.get("last_error"):
                        err = format_health_error_excerpt(recipient_gap["last_error"], max_length=120)
                        line += f" · Error: {err}"
                source_lines.append(line)
                continue
            last_ok = self._format_rel_timestamp(row["last_success_at"])
            cov = str(row["coverage"] or "unknown").replace("`", "")
            line = f"**{display_name}:** {baselined} · Last OK: {last_ok} · Coverage: `{cov}`"
            health_keys = row.keys()
            gap_since_raw = row["unresolved_gap_since"]
            gap_until_raw = (
                row["unresolved_gap_until"]
                if "unresolved_gap_until" in health_keys else None
            )
            gap_reason = row["gap_reason"] if "gap_reason" in health_keys else None
            is_recipient_reason = gap_reason in {"unavailable_feed_target", "delivery_failure"}
            mask_shared_recipient_gap = (
                source_name == "activity"
                and recipient_gap is None
                and has_attributed_recipient_gaps
                and is_recipient_reason
            )
            if mask_shared_recipient_gap:
                gap_since_raw = None
                gap_until_raw = None
                gap_reason = None
            gap_since_at = parse_sync_timestamp(gap_since_raw)
            gap_until_at = parse_sync_timestamp(gap_until_raw)
            last_boundary_at = (
                row["last_continuous_boundary_at"]
                if "last_continuous_boundary_at" in health_keys else None
            )
            if gap_since_at and gap_until_at and gap_until_at < gap_since_at:
                line += " · Gap bounds conflict; extent unknown"
            elif gap_since_raw:
                gap_label = {
                    "unavailable_feed_target": "Recipient gap",
                    "delivery_failure": "Recipient gap",
                    "source_failure": "Continuity gap",
                }.get(gap_reason, "Coverage gap")
                gap_since = self._format_rel_timestamp(gap_since_raw)
                gap_until = self._format_rel_timestamp(gap_until_raw) if gap_until_raw else "present"
                line += f" · {gap_label}: {gap_since} \u2192 {gap_until}"
            elif gap_reason or cov in {"limited", "unsupported"}:
                line += " · Coverage extent unknown (no continuous boundary)"
            if (
                cov in {"limited", "unsupported"}
                and last_boundary_at is None
                and gap_since_raw
            ):
                line += " · Source extent unknown (no continuous boundary)"
            if (
                source_name == "activity"
                and recipient_gap is not None
                and not (gap_since_raw and is_recipient_reason)
            ):
                rg_since = self._format_rel_timestamp(recipient_gap.get("gap_since"))
                line += f" · Recipient gap: {rg_since} \u2192 present"
            if row["last_error"]:
                err = format_health_error_excerpt(row["last_error"], max_length=120)
                line += f" · Error: {err}"
            elif source_name == "activity" and recipient_gap is not None and recipient_gap.get("last_error"):
                err = format_health_error_excerpt(recipient_gap["last_error"], max_length=120)
                line += f" · Error: {err}"
            source_lines.append(line)
        embed.add_field(
            name="Source Health & Coverage",
            value=clamp_escaped_text("\n".join(source_lines), 1024),
            inline=False,
        )

        suppressions = status.get("suppression_counts") or {}
        deliveries = status.get("delivery_counts") or {}
        policy_parts = [
            f"Old (>7d): {suppressions.get('suppressed_old', 0)}",
            f"Pre-activation: {suppressions.get('suppressed_pre_activation', 0)}",
            f"Uncertain time: {suppressions.get('held_uncertain', 0)}",
            f"Future-dated: {suppressions.get('held_future', 0)}",
            f"Ambiguous edits: {suppressions.get('held_ambiguous', 0)}",
        ]
        unknown_deliveries = deliveries.get("unknown", 0)
        pending_deliveries = deliveries.get("pending", 0)
        if unknown_deliveries or pending_deliveries:
            policy_parts.append(f"Unknown send outcomes: {unknown_deliveries}")
            policy_parts.append(f"Pending sends: {pending_deliveries}")
        embed.add_field(
            name="Suppressed & Held Items",
            value=clamp_escaped_text(" · ".join(policy_parts), 1024),
            inline=False,
        )
        embed.set_footer(text="Feed source health is separate from library cache freshness (/profile, /refresh).")
        await send_private_followup(interaction, embed=embed, log=logger)

    def _feed_list_pages(self, candidates, channel_id):
        pages = []
        page_size = 20
        for start in range(0, len(candidates), page_size):
            embed = discord.Embed(
                title="Backloggd feed profiles",
                color=discord.Color.blurple(),
            )
            lines = []
            for candidate in candidates[start:start + page_size]:
                username = clamp_escaped_text(escape_display_text(candidate["display_username"]), 120)
                source = candidate["sources"].title()
                state = candidate.get("feed_state", "Pending baseline")
                lines.append(f"**{username}** — {source} · {state}")
            embed.description = clamp_escaped_text("\n".join(lines), 4000)
            embed.set_footer(text=f"{start + 1}–{min(start + page_size, len(candidates))} of {len(candidates)} profiles")
            pages.append(embed)
        return pages

    @feed.command(name="add", description="Follow a public Backloggd profile in this server's feed")
    @app_commands.describe(username="A public Backloggd username")
    @app_commands.guild_only()
    @app_commands.checks.has_permissions(manage_channels=True)
    async def feed_add(self, interaction: discord.Interaction, username: str):
        if interaction.guild is None:
            await interaction.response.send_message("This command can only be used in a server.", ephemeral=True)
            return
        await defer_interaction(interaction, ephemeral=True)
        try:
            requested = normalize_backloggd_username(username)
        except InvalidUsernameError as error:
            await send_private_followup(interaction, f"❌ {error}", log=logger)
            return
        safe_requested = escape_display_text(requested)
        try:
            profile = await self.bot.backloggd.get_user_profile(requested)
        except (UserNotFoundError, ResourceNotFoundError):
            await send_private_followup(
                interaction,
                f"No public Backloggd profile was found for **{safe_requested}**.",
                log=logger,
            )
            return
        except Exception as error:
            logger.warning("Could not validate feed profile %s: %s", requested, error)
            await send_private_followup(
                interaction,
                "Backloggd is temporarily unavailable. No feed subscription was added; try again shortly.",
                log=logger,
            )
            return
        if not profile or not profile.username:
            await send_private_followup(
                interaction,
                f"No public Backloggd profile was found for **{safe_requested}**.",
                log=logger,
            )
            return
        member_ids = await self.pipeline.authoritative_member_ids(interaction.guild)
        created = await self.bot.db.add_guild_admin_follow(
            interaction.guild_id,
            profile.username,
            profile.avatar_url,
            interaction.user.id,
            member_ids=member_ids,
        )
        channel_id = await self.bot.db.get_feed_channel(interaction.guild_id)
        if member_ids is not None and channel_id is not None:
            candidates = await self.bot.db.get_guild_feed_candidates(interaction.guild_id, member_ids)
            await self.bot.db.reconcile_guild_feed_activity(
                interaction.guild_id,
                [row["profile_key"] for row in candidates],
                now=self.pipeline.clock(),
                present_member_ids=member_ids,
            )
        status = await self.bot.db.get_guild_profile_feed_status(
            interaction.guild_id, profile.username.lower(),
        )
        safe_profile = escape_display_text(profile.username)
        if not created:
            message = f"**{safe_profile}** is already followed in this server."
        else:
            message = f"Added **{safe_profile}** to this server's activity feed."
        if channel_id is None:
            message += " The follow is pending until an admin runs `/setchannel`; no Discord delivery can happen before a channel is configured."
        if member_ids is None:
            message += " No Discord delivery will happen until Discord provides a complete member snapshot."
        message += " " + self._feed_enrollment_state_copy(status)
        await send_private_followup(interaction, message, log=logger)

    @feed.command(name="remove", description="Remove a public profile follow from this server's feed")
    @app_commands.describe(username="The Backloggd username to stop following")
    @app_commands.guild_only()
    @app_commands.checks.has_permissions(manage_channels=True)
    async def feed_remove(self, interaction: discord.Interaction, username: str):
        if interaction.guild is None:
            await interaction.response.send_message("This command can only be used in a server.", ephemeral=True)
            return
        await defer_interaction(interaction, ephemeral=True)
        try:
            requested = normalize_backloggd_username(username)
        except InvalidUsernameError as error:
            await send_private_followup(interaction, f"❌ {error}", log=logger)
            return
        safe_requested = escape_display_text(requested)
        if not await self.bot.db.has_guild_admin_follow(interaction.guild_id, requested):
            await send_private_followup(
                interaction,
                f"**{safe_requested}** does not have an admin follow in this server.",
                log=logger,
            )
            return
        member_ids = await self.pipeline.authoritative_member_ids(interaction.guild)
        result = await self.bot.db.remove_guild_admin_follow_and_reconcile(
            interaction.guild_id, requested, member_ids=member_ids
        )
        if not result["removed"]:
            await send_private_followup(
                interaction,
                f"**{safe_requested}** does not have an admin follow in this server.",
                log=logger,
            )
            return
        message = f"Removed the admin follow for **{safe_requested}**."
        if result["pending_reconciliation"]:
            message += " Current member participation could not be verified."
        elif not result["deactivated"] and result["remaining_members"]:
            message += " The profile remains active through member participation."
        else:
            message += " Existing announcements remain archived; a later re-add will skip inactive history."
        await send_private_followup(interaction, message, log=logger)

    @feed.command(name="list", description="List profiles included in this server's activity feed")
    @app_commands.guild_only()
    @app_commands.checks.has_permissions(manage_channels=True)
    async def feed_list(self, interaction: discord.Interaction):
        if interaction.guild is None:
            await interaction.response.send_message("This command can only be used in a server.", ephemeral=True)
            return
        await defer_interaction(interaction, ephemeral=True)
        member_ids = await self.pipeline.authoritative_member_ids(interaction.guild)
        if member_ids is None:
            await send_private_followup(
                interaction,
                "Discord has not provided a complete member snapshot yet. Try again shortly.",
                log=logger,
            )
            return
        candidates = await self.bot.db.get_guild_feed_candidates(interaction.guild_id, member_ids)
        if not candidates:
            await send_private_followup(
                interaction,
                "No member profiles are enabled and no admin profiles are followed in this server.",
                log=logger,
            )
            return
        channel_id = await self.bot.db.get_feed_channel(interaction.guild_id)
        _, _, target_issue = self.pipeline.inspect_target_channel(
            interaction.guild_id,
            channel_id,
            guild=interaction.guild,
            require_channel_lookup=False,
        )
        statuses = await self.bot.db.get_guild_feed_statuses(
            interaction.guild_id,
            [candidate["profile_key"] for candidate in candidates],
        )
        for candidate in candidates:
            status = statuses.get(candidate["profile_key"], {})
            candidate["feed_state"] = self._describe_profile_feed_state(
                channel_id=channel_id,
                status=status,
                is_candidate=True,
                target_issue=target_issue,
            )
        pages = self._feed_list_pages(candidates, channel_id)
        view = FeedListView(interaction.user.id, pages)
        message = await interaction.followup.send(
            embed=pages[0], view=view, ephemeral=True, wait=True
        )
        view.message = message

    @setchannel.error
    @unsetchannel.error
    @feed_add.error
    @feed_remove.error
    @feed_list.error
    async def channel_command_error(self, interaction: discord.Interaction, error: app_commands.AppCommandError):
        if isinstance(error, app_commands.MissingPermissions):
            reply = "❌ You need the **Manage Channels** permission to configure or manage the server activity feed."
        elif isinstance(error, app_commands.NoPrivateMessage):
            reply = "This command can only be used in a server."
        else:
            return
        mark_interaction_error_handled(interaction)
        await send_ephemeral_error(interaction, reply, log=logger)

    # =========================================================================
    # Background Polling Task Loop (Filmlinkd-style)
    # =========================================================================

    @tasks.loop(minutes=5.0)
    async def journal_feed_loop(self):
        if not self._closing:
            await self.pipeline.poll()

    @journal_feed_loop.before_loop
    async def before_feed_loop(self):
        """Wait until bot is fully logged in before running the background feed."""
        try:
            await self.bot.wait_until_ready()
        except RuntimeError:
            # Raised if testing offline without bot.start()
            pass

async def setup(bot):
    await bot.add_cog(FeedCog(bot))
