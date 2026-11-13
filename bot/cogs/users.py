"""UsersCog: Handles /profile command displaying stats, metrics, and favorite games."""

import asyncio
from datetime import datetime, timezone
import discord
from discord import app_commands
from discord.ext import commands
import logging
from typing import Optional
from backloggd.exceptions import ResourceNotFoundError, UserNotFoundError
from bot.input_validation import (
    InvalidUsernameError,
    escape_display_text,
    normalize_backloggd_username,
    parse_discord_member_id,
)
from bot.ui.embeds import create_profile_embed
from bot.ui.views import LinkButtonView
from bot.ui.helpers import build_backloggd_review_url, build_backloggd_user_url
from bot.services.freshness import ensure_profiles_fresh, format_profile_freshness_line

logger = logging.getLogger("backloggd.cogs.users")

class UsersCog(commands.Cog, name="Users"):
    def __init__(self, bot, *, clock=None):
        self.bot = bot
        self.clock = clock or (lambda: datetime.now(timezone.utc))

    @app_commands.command(name="profile", description="View a Backloggd user's profile, library stats, and favorite games")
    @app_commands.describe(user="Optional: Mention a server member or provide a Backloggd username (defaults to you)")
    async def profile(self, interaction: discord.Interaction, user: Optional[str] = None):
        await interaction.response.defer()

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
                    await interaction.followup.send(
                        "❌ That user has not linked their Backloggd account yet.",
                        ephemeral=True,
                        allowed_mentions=discord.AllowedMentions.none(),
                    )
                    return
            else:
                try:
                    target_username = normalize_backloggd_username(user)
                except InvalidUsernameError as err:
                    await interaction.followup.send(
                        f"❌ {err}",
                        ephemeral=True,
                        allowed_mentions=discord.AllowedMentions.none(),
                    )
                    return
        else:
            target_discord_id = interaction.user.id
            linked = await self.bot.db.get_linked_user(target_discord_id)
            if linked:
                target_username = linked["backloggd_username"]
            else:
                await interaction.followup.send(
                    "❌ You haven't linked your Backloggd account yet! Use `/link <username>` first.",
                    ephemeral=True,
                    allowed_mentions=discord.AllowedMentions.none(),
                )
                return

        safe_target = escape_display_text(target_username)
        try:
            # Fetch profile, recent journal, and latest reviews concurrently
            profile_task = self.bot.backloggd.get_user_profile(target_username)
            journal_task = self.bot.backloggd.get_user_journal(target_username, max_pages=1)
            reviews_task = self.bot.backloggd.get_user_reviews(target_username, max_pages=1)

            results = await asyncio.gather(profile_task, journal_task, reviews_task, return_exceptions=True)
            profile_error = results[0] if isinstance(results[0], Exception) else None
            profile_data = results[0] if profile_error is None else None
            journal_entries = results[1] if not isinstance(results[1], Exception) and isinstance(results[1], list) else []
            user_reviews = results[2] if not isinstance(results[2], Exception) and isinstance(results[2], list) else []

            if profile_error:
                if isinstance(profile_error, (UserNotFoundError, ResourceNotFoundError)):
                    await interaction.followup.send(
                        f"❌ Backloggd user **{safe_target}** could not be found.",
                        ephemeral=True,
                        allowed_mentions=discord.AllowedMentions.none(),
                    )
                    return
                logger.warning("Could not load Backloggd profile for %s: %s", target_username, profile_error)
                await interaction.followup.send(
                    f"❌ Backloggd could not load **{safe_target}**'s profile right now. Please try again shortly.",
                    ephemeral=True,
                    allowed_mentions=discord.AllowedMentions.none(),
                )
                return

            if not profile_data or not profile_data.username:
                await interaction.followup.send(
                    f"❌ Backloggd user **{safe_target}** could not be found.",
                    ephemeral=True,
                    allowed_mentions=discord.AllowedMentions.none(),
                )
                return

            # Read stats from the shared profile cache, without choosing a Discord link.
            avg_rating = None
            rated_count = None
            recent_played = journal_entries[:5] if journal_entries else []

            canonical_key = profile_data.username.strip().lower()
            statuses = await ensure_profiles_fresh(
                self.bot,
                [(canonical_key, profile_data.username)],
                now=self.clock(),
                require_subscribed=True,
            )
            status = statuses.get(canonical_key)
            sub_checker = getattr(self.bot.db, "is_profile_library_subscribed", None)
            subscribed = bool(await sub_checker(profile_data.username)) if sub_checker is not None else bool(target_discord_id)

            stats = await self.bot.db.get_profile_stats(target_username)
            if stats and stats.get("rated_count", 0) > 0:
                avg_rating = stats.get("average_rating")
                rated_count = stats.get("rated_count")

            freshness_note = None
            has_cached_stats = bool(stats and (stats.get("rated_count", 0) > 0 or stats.get("total_games", 0) > 0))
            if status is not None and (has_cached_stats or subscribed or status.synced_at is not None):
                freshness_note = format_profile_freshness_line(status, label="Stats cache")
                if status.missing and subscribed and not has_cached_stats:
                    freshness_note = f"{freshness_note} — library data is being fetched"
            elif status is not None:
                freshness_note = "Overview fetched live from Backloggd (no cached library snapshot)"

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
            )

            extra_buttons = []
            if latest_review and getattr(latest_review, "review_id", None):
                review_url = build_backloggd_review_url(profile_data.username, latest_review.review_id)
                extra_buttons.append(("Read Latest Review", review_url))

            view = LinkButtonView(
                label="View Profile on Backloggd",
                url=build_backloggd_user_url(profile_data.username),
                extra_buttons=extra_buttons if extra_buttons else None
            )

            await interaction.followup.send(embed=embed, view=view)

        except Exception as e:
            logger.error("Error looking up profile for '%s': %s", target_username, e, exc_info=True)
            await interaction.followup.send(
                f"❌ An error occurred while retrieving profile for **{safe_target}**.",
                ephemeral=True,
                allowed_mentions=discord.AllowedMentions.none(),
            )

async def setup(bot):
    await bot.add_cog(UsersCog(bot))
