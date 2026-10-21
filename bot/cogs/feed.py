"""FeedCog: Handles /setchannel, /unsetchannel, and the periodic playthrough journal feed loop."""

import discord
from discord import app_commands
from discord.ext import commands, tasks
import logging
import asyncio
from typing import Any, List, Optional
from bot.ui.feed_embed import create_journal_feed_embed
from bot.ui.views import LinkButtonView
from bot.ui.helpers import build_backloggd_game_url
from bot.activity_parser import parse_user_activity_page, ActivityEntry

logger = logging.getLogger("backloggd.cogs.feed")

class FeedCog(commands.Cog, name="Feed"):
    def __init__(self, bot):
        self.bot = bot

    async def cog_load(self):
        # Start loop when cog is loaded and bot event loop is active
        if not self.journal_feed_loop.is_running():
            self.journal_feed_loop.start()

    def cog_unload(self):
        if self.journal_feed_loop.is_running():
            self.journal_feed_loop.cancel()

    # =========================================================================
    # Channel Configuration Slash Commands (Admin Only)
    # =========================================================================


    async def _fetch_user_activity_or_journal(self, username: str) -> list[Any]:
        """Fetch latest game events from user's /activity/you/ stream, falling back to /journal/."""
        # 1. Try personal activity stream (/activity/you/)
        clean_user = username.strip()
        url = f"https://backloggd.com/u/{clean_user}/activity/you/"
        try:
            resp = await self.bot.backloggd._request_with_retry("GET", url, resource_type="user")
            self.bot.backloggd._check_response(resp, resource_type="user")
            activities = parse_user_activity_page(resp.text, clean_user)
            if activities:
                return activities
        except Exception as e:
            logger.debug("Activity stream fetch failed for %s (%s), falling back to journal", username, e)

        # 2. Fallback to /journal/
        try:
            journal = await self.bot.backloggd.get_user_journal(clean_user, max_pages=1)
            return journal or []
        except Exception as e:
            logger.warning("Could not fetch journal or activity for %s: %s", username, e)
            return []

    @app_commands.command(name="setchannel", description="Set the channel where automatic Backloggd playthrough logs are posted")
    @app_commands.describe(channel="The text channel for game log announcements")
    @app_commands.checks.has_permissions(manage_channels=True)
    async def setchannel(self, interaction: discord.Interaction, channel: discord.TextChannel):
        await interaction.response.defer(ephemeral=True)

        # Seed each linked member's current journal before enabling the feed,
        # so setup does not publish existing history as new activity.
        member_ids = [member.id for member in interaction.guild.members]
        linked_members = await self.bot.db.get_guild_linked_users(member_ids)
        for user_row in linked_members:
            discord_id = user_row["discord_id"]
            username = user_row["backloggd_username"]
            try:
                entries = await self._fetch_user_activity_or_journal(username)
                e_ids = [getattr(e, "activity_id", None) or getattr(e, "entry_id", None) for e in entries]
                await self.bot.db.seed_journal_entries_for_guild(
                    interaction.guild_id,
                    discord_id,
                    [eid for eid in e_ids if eid],
                )
            except Exception as e:
                logger.warning("Could not seed current activity for %s: %s", username, e)

        await self.bot.db.set_feed_channel(interaction.guild_id, channel.id)
        await interaction.followup.send(
            f"✅ Successfully set {channel.mention} as the automatic Backloggd activity feed channel!",
            ephemeral=True
        )

    @app_commands.command(name="unsetchannel", description="Disable the automatic Backloggd activity feed for this server")
    @app_commands.checks.has_permissions(manage_channels=True)
    async def unsetchannel(self, interaction: discord.Interaction):
        removed = await self.bot.db.unset_feed_channel(interaction.guild_id)
        if removed:
            await interaction.response.send_message(
                "✅ Automatic activity feed has been disabled for this server.",
                ephemeral=True
            )
        else:
            await interaction.response.send_message(
                "ℹ️ No activity feed channel was currently set for this server.",
                ephemeral=True
            )

    @setchannel.error
    @unsetchannel.error
    async def channel_command_error(self, interaction: discord.Interaction, error: app_commands.AppCommandError):
        if isinstance(error, app_commands.MissingPermissions):
            await interaction.response.send_message(
                "❌ You need the **Manage Channels** permission to configure the activity feed channel.",
                ephemeral=True
            )

    # =========================================================================
    # Background Polling Task Loop (Filmlinkd-style)
    # =========================================================================

    @tasks.loop(minutes=5.0)
    async def journal_feed_loop(self):
        """Periodically check linked users' journals and broadcast new logs."""
        try:
            if not self.bot.db or not self.bot.db.is_connected:
                return

            guild_settings = await self.bot.db.get_all_guild_settings()
            if not guild_settings:
                return

            # Per-tick journal cache to avoid duplicate HTTP requests across multiple guilds
            user_journal_cache: dict[str, list] = {}

            for setting in guild_settings:
                guild_id = setting["guild_id"]
                channel_id = setting["feed_channel_id"]

                guild = self.bot.get_guild(guild_id)
                if not guild:
                    continue

                channel = guild.get_channel(channel_id)
                if not channel or not isinstance(channel, discord.TextChannel):
                    continue

                # Check bot permissions in target channel
                permissions = channel.permissions_for(guild.me)
                if not permissions.send_messages or not permissions.embed_links:
                    logger.warning("Missing send_messages or embed_links permission in guild %d channel %d", guild_id, channel_id)
                    continue

                # Get linked members in this guild
                member_ids = [m.id for m in guild.members]
                linked_members = await self.bot.db.get_guild_linked_users(member_ids)

                for user_row in linked_members:
                    discord_id = user_row["discord_id"]
                    username = user_row["backloggd_username"]
                    avatar_url = user_row["avatar_url"]

                    try:
                        # Fetch latest activity or journal page (cached per loop iteration)
                        if username not in user_journal_cache:
                            user_journal_cache[username] = (
                                await self._fetch_user_activity_or_journal(username)
                            )
                        entries = user_journal_cache[username]
                        if not entries:
                            continue

                        for entry in entries:
                            entry_id = getattr(entry, "activity_id", None) or getattr(entry, "entry_id", None)
                            game_slug = getattr(entry, "game_slug", "")
                            if not entry_id or not game_slug:
                                continue

                            # Deduplication check
                            is_seen = await self.bot.db.is_journal_entry_delivered(
                                guild_id, entry_id
                            )
                            if not is_seen:
                                embed = create_journal_feed_embed(username, avatar_url, entry)
                                view = LinkButtonView(label="View on Backloggd", url=build_backloggd_game_url(game_slug))

                                await channel.send(embed=embed, view=view)
                                await self.bot.db.mark_journal_entry_delivered(
                                    guild_id, entry_id, discord_id
                                )
                                logger.info("Posted activity feed event '%s' by %s to guild %d", entry_id, username, guild_id)

                                # Brief pause between posts to respect rate limits
                                await asyncio.sleep(1.0)

                    except Exception as e:
                        logger.warning("Error polling activity for %s: %s", username, e)

                    # Pause between users to stay respectful to Backloggd servers
                    await asyncio.sleep(1.5)

        except Exception as e:
            logger.error("Error in journal_feed_loop: %s", e, exc_info=True)

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
