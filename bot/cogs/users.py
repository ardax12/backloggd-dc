"""UsersCog: Handles /profile command displaying stats, metrics, and favorite games."""

import asyncio
import logging
from datetime import datetime, timezone
from typing import Any, Callable, Optional

import discord
from backloggd.exceptions import ResourceNotFoundError, UserNotFoundError
from discord import app_commands
from discord.ext import commands

from bot.command_errors import defer_interaction, send_private_followup, send_public_followup
from bot.input_validation import (
    InvalidUsernameError,
    escape_display_text,
    normalize_backloggd_username,
    parse_discord_member_id,
)
from bot.services.cooldowns import guild_cooldown
from bot.services.freshness import (
    ensure_profiles_fresh,
    format_profile_freshness_line,
    valid_played_count,
)
from bot.services.session_health import report_upstream_failure, report_upstream_success
from bot.ui.embeds import create_profile_embed
from bot.ui.helpers import build_backloggd_review_url, build_backloggd_user_url
from bot.ui.views import ProfileCardView

logger = logging.getLogger("backloggd.cogs.users")

class UsersCog(commands.Cog, name="Users"):
    def __init__(self, bot, *, clock=None):
        self.bot = bot
        self.clock = clock or (lambda: datetime.now(timezone.utc))
        self._profile_context_menu = app_commands.ContextMenu(
            name="View Backloggd Profile",
            callback=self._context_view_profile,
        )
        app_commands.guild_only()(self._profile_context_menu)
        guild_cooldown("profile")(self._profile_context_menu)

    async def cog_load(self) -> None:
        tree = getattr(self.bot, "tree", None)
        if tree is not None:
            tree.add_command(self._profile_context_menu, override=True)

    async def cog_unload(self) -> None:
        tree = getattr(self.bot, "tree", None)
        if tree is not None:
            tree.remove_command(
                self._profile_context_menu.name,
                type=self._profile_context_menu.type,
            )

    async def _load_profile(self, username: str):
        sdk = self.bot.backloggd
        cached = sdk.cache.get_profile(username)
        if cached is not None:
            # The SDK does not expose when this cached profile was fetched.
            return cached, None
        profile = await sdk.get_user_profile(username, force_refresh=True)
        return profile, self.clock()

    async def _context_view_profile(
        self,
        interaction: discord.Interaction,
        member: discord.Member,
    ) -> None:
        if getattr(member, "bot", False):
            msg = "🤖 Bots don't have Backloggd profiles."
            is_done = getattr(getattr(interaction, "response", None), "is_done", None)
            if callable(is_done) and is_done():
                await send_private_followup(interaction, msg, log=logger)
            else:
                await interaction.response.send_message(msg, ephemeral=True)
            return
        await self._execute_profile(interaction, f"<@{member.id}>")

    @app_commands.command(name="profile", description="View a Backloggd user's profile, library stats, and favorite games")
    @app_commands.describe(user="Optional: Mention a server member or provide a Backloggd username (defaults to you)")
    @guild_cooldown("profile")
    async def profile(self, interaction: discord.Interaction, user: Optional[str] = None):
        await self._execute_profile(interaction, user)

    async def _execute_profile(self, interaction: discord.Interaction, user: Optional[str] = None):
        is_done = getattr(getattr(interaction, "response", None), "is_done", None)
        if not (callable(is_done) and is_done()):
            await defer_interaction(interaction)

        target_username = None
        target_discord_id = None

        if user is not None:
            member_id = parse_discord_member_id(user)
            if member_id is not None:
                target_discord_id = member_id
                linked = await self.bot.db.get_linked_user(target_discord_id)
                if linked:
                    target_username = linked["backloggd_username"]
                else:
                    await send_private_followup(
                        interaction,
                        "❌ That user has not linked their Backloggd account yet.",
                        log=logger,
                    )
                    return
            else:
                try:
                    target_username = normalize_backloggd_username(user)
                except InvalidUsernameError as err:
                    await send_private_followup(interaction, f"❌ {err}", log=logger)
                    return
        else:
            target_discord_id = interaction.user.id
            linked = await self.bot.db.get_linked_user(target_discord_id)
            if linked:
                target_username = linked["backloggd_username"]
            else:
                await send_private_followup(
                    interaction,
                    "❌ You haven't linked your Backloggd account yet! Use `/link <username>` first.",
                    log=logger,
                )
                return

        safe_target = escape_display_text(target_username)
        try:
            # Fetch profile, recent journal, and latest reviews concurrently
            profile_task = self._load_profile(target_username)
            journal_task = self.bot.backloggd.get_user_journal(target_username, max_pages=1)
            reviews_task = self.bot.backloggd.get_user_reviews(target_username, max_pages=1)

            results = await asyncio.gather(profile_task, journal_task, reviews_task, return_exceptions=True)
            profile_error = results[0] if isinstance(results[0], Exception) else None
            profile_data, profile_observed_at = results[0] if profile_error is None else (None, None)
            journal_entries = results[1] if not isinstance(results[1], Exception) and isinstance(results[1], list) else []
            user_reviews = results[2] if not isinstance(results[2], Exception) and isinstance(results[2], list) else []

            upstream_failed = False
            for item in results:
                if isinstance(item, Exception) and not isinstance(item, (UserNotFoundError, ResourceNotFoundError)):
                    if report_upstream_failure(self.bot, item) is not None:
                        upstream_failed = True
            if not upstream_failed and (
                profile_observed_at is not None
                or not isinstance(results[1], Exception)
                or not isinstance(results[2], Exception)
            ):
                report_upstream_success(self.bot)

            if profile_error:
                if isinstance(profile_error, (UserNotFoundError, ResourceNotFoundError)):
                    await send_private_followup(
                        interaction,
                        f"❌ Backloggd user **{safe_target}** could not be found.",
                        log=logger,
                    )
                    return
                logger.warning("Could not load Backloggd profile for %s: %s", target_username, profile_error)
                await send_private_followup(
                    interaction,
                    f"❌ Backloggd could not load **{safe_target}**'s profile right now. Please try again shortly.",
                    log=logger,
                )
                return

            if not profile_data or not profile_data.username:
                await send_private_followup(
                    interaction,
                    f"❌ Backloggd user **{safe_target}** could not be found.",
                    log=logger,
                )
                return

            # Read stats from the shared profile cache, without choosing a Discord link.
            avg_rating = None
            rated_count = None
            recent_played = journal_entries[:5] if journal_entries else []

            canonical_username = profile_data.username.strip()
            canonical_key = canonical_username.lower()
            collection_counts = {}
            counts_observed_at = None
            collections = ("played", "playing", "backlog", "wishlist")
            missing_counts = {name for name in collections if getattr(profile_data, f"{name}_count", None) is None}
            counts_method = getattr(self.bot.backloggd, "get_user_collection_counts", None)
            if missing_counts and counts_method is not None:
                try:
                    collection_counts = await asyncio.wait_for(counts_method(canonical_username), timeout=5.0)
                    counts_observed_at = self.clock()
                    report_upstream_success(self.bot)
                except Exception as error:
                    report_upstream_failure(self.bot, error)
                    logger.debug("Collection counts unavailable for %s: %s", canonical_username, type(error).__name__)
            played_observed_at = profile_observed_at
            authoritative_played = None
            if played_observed_at is not None:
                authoritative_played = valid_played_count(getattr(profile_data, "played_count", None))
            if authoritative_played is None and counts_observed_at is not None and isinstance(collection_counts, dict):
                authoritative_played = valid_played_count(collection_counts.get("played"))
                played_observed_at = counts_observed_at
            if authoritative_played is not None:
                played_updater: Optional[Callable[..., Any]] = getattr(
                    self.bot.db, "update_profile_played_count", None
                )
                if callable(played_updater):
                    try:
                        await played_updater(
                            canonical_key,
                            authoritative_played,
                            observed_at=played_observed_at,
                        )
                    except Exception:
                        logger.warning(
                            "Could not persist Played-tab count for %s",
                            canonical_username,
                            exc_info=True,
                        )

            sub_checker = getattr(self.bot.db, "is_profile_library_subscribed", None)
            subscribed = (
                bool(await sub_checker(canonical_key))
                if sub_checker is not None
                else bool(target_discord_id)
            )
            statuses = await ensure_profiles_fresh(
                self.bot,
                [(canonical_key, canonical_username)],
                now=self.clock(),
                require_subscribed=True,
                subscribed=subscribed,
            )
            status = statuses.get(canonical_key)

            stats = await self.bot.db.get_profile_stats(canonical_username)
            if stats and stats.get("rated_count", 0) > 0:
                avg_rating = stats.get("average_rating")
                rated_count = stats.get("rated_count")

            has_cached_stats = bool(stats and (stats.get("rated_count", 0) > 0 or stats.get("total_games", 0) > 0))
            if status is not None and (has_cached_stats or subscribed or status.synced_at is not None):
                freshness_note = format_profile_freshness_line(status, label="Library stats")
            else:
                freshness_note = "No synced library stats yet"

            latest_review = user_reviews[0] if user_reviews else None

            # Determine fallback avatar from Discord if available
            fallback_avatar = None
            if target_discord_id and interaction.guild:
                member = interaction.guild.get_member(target_discord_id)
                if member and member.display_avatar:
                    fallback_avatar = member.display_avatar.url
            if not fallback_avatar and target_discord_id == interaction.user.id and interaction.user.display_avatar:
                fallback_avatar = interaction.user.display_avatar.url

            embed = create_profile_embed(
                profile_data,
                avg_rating=avg_rating,
                rated_count=rated_count,
                recent_played=recent_played,
                latest_review=latest_review,
                fallback_avatar_url=fallback_avatar,
                freshness_note=freshness_note,
                collection_counts=collection_counts,
            )

            extra_buttons = []
            if latest_review and getattr(latest_review, "review_id", None):
                review_url = build_backloggd_review_url(profile_data.username, latest_review.review_id)
                extra_buttons.append(("Read Latest Review", review_url))

            async def _reply_ephemeral(btn_interaction: discord.Interaction, message: str) -> None:
                is_done_fn = getattr(getattr(btn_interaction, "response", None), "is_done", None)
                if callable(is_done_fn) and is_done_fn():
                    await send_private_followup(btn_interaction, message, log=logger)
                else:
                    await btn_interaction.response.send_message(message, ephemeral=True)

            async def on_compare(btn_interaction: discord.Interaction) -> None:
                get_cog: Optional[Callable[..., Any]] = getattr(self.bot, "get_cog", None)
                social_cog: Any = get_cog("Social") if callable(get_cog) else None
                if social_cog is None or not hasattr(social_cog, "_execute_compare"):
                    await _reply_ephemeral(btn_interaction, "❌ Taste comparison is currently unavailable.")
                    return

                linked_caller = await self.bot.db.get_linked_user(btn_interaction.user.id)
                if not linked_caller:
                    await _reply_ephemeral(
                        btn_interaction,
                        "❌ You haven't linked your Backloggd account yet! Use `/link <username>` first.",
                    )
                    return

                caller_key = str(linked_caller["backloggd_username"]).strip().lower()
                if caller_key == canonical_key:
                    await _reply_ephemeral(
                        btn_interaction,
                        "ℹ️ Both inputs resolve to the same Backloggd profile, so there is no separate profile to compare.",
                    )
                    return

                cooldowns = getattr(self.bot, "cooldowns", None)
                check_cd: Optional[Callable[..., Any]] = getattr(
                    cooldowns, "check_component_cooldown", None
                )
                if callable(check_cd):
                    retry_after = await check_cd(
                        btn_interaction,
                        "compare",
                        db=getattr(self.bot, "db", None),
                    )
                    if retry_after is not None:
                        await _reply_ephemeral(
                            btn_interaction,
                            f"⏳ Slow down! Please try comparing again in **{retry_after:.1f}s**.",
                        )
                        return

                await social_cog._execute_compare(btn_interaction, canonical_username, None)

            async def on_roulette(btn_interaction: discord.Interaction) -> None:
                get_cog: Optional[Callable[..., Any]] = getattr(self.bot, "get_cog", None)
                discovery_cog: Any = get_cog("Discovery") if callable(get_cog) else None
                if discovery_cog is None or not hasattr(discovery_cog, "_execute_roulette_for_profile"):
                    await _reply_ephemeral(btn_interaction, "❌ Backlog roulette is currently unavailable.")
                    return

                await discovery_cog._execute_roulette_for_profile(
                    btn_interaction,
                    profile_key=canonical_key,
                    username=canonical_username,
                    pool="Both",
                    discord_id=target_discord_id,
                )

            view = ProfileCardView(
                profile_url=build_backloggd_user_url(profile_data.username),
                extra_links=extra_buttons if extra_buttons else None,
                on_compare=on_compare,
                on_roulette=on_roulette if subscribed else None,
            )

            view.message = await send_public_followup(interaction, embed=embed, view=view, wait=True)

        except Exception as e:
            logger.error("Error looking up profile for '%s': %s", target_username, e, exc_info=True)
            await send_private_followup(
                interaction,
                f"❌ An error occurred while retrieving profile for **{safe_target}**.",
                log=logger,
            )

async def setup(bot):
    await bot.add_cog(UsersCog(bot))
