"""FeedCog: Handles /setchannel, /unsetchannel, and the periodic playthrough journal feed loop."""

import discord
from discord import app_commands
from discord.ext import commands, tasks
import logging
import asyncio
from bot.ui.feed_embed import create_journal_feed_embed
from bot.ui.views import LinkButtonView
from bot.ui.helpers import build_backloggd_game_url

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
                entries = await self.bot.backloggd.get_user_journal(username, max_pages=1)
                await self.bot.db.seed_journal_entries_for_guild(
                    interaction.guild_id,
                    discord_id,
                    [entry.entry_id for entry in entries if entry.entry_id],
                )
            except Exception as e:
                logger.warning("Could not seed current journal for %s: %s", username, e)
                await interaction.followup.send(
                    f"Could not read **{username}**'s current journal, so the activity feed was left unchanged. Try `/setchannel` again shortly.",
                    ephemeral=True,
                )
                return

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
                        # Fetch latest journal page (cached per loop iteration)
                        if username not in user_journal_cache:
                            user_journal_cache[username] = (
                                await self.bot.backloggd.get_user_journal(username, max_pages=1) or []
                            )
                        journal_entries = user_journal_cache[username]
                        if not journal_entries:
                            continue

                        for entry in journal_entries:
                            if not entry.entry_id:
                                continue

                            # Deduplication check
                            is_seen = await self.bot.db.is_journal_entry_delivered(
                                guild_id, entry.entry_id
                            )
                            if not is_seen:
                                embed = create_journal_feed_embed(username, avatar_url, entry)
                                view = LinkButtonView(label="View on Backloggd", url=build_backloggd_game_url(entry.game_slug))

                                await channel.send(embed=embed, view=view)
                                await self.bot.db.mark_journal_entry_delivered(
                                    guild_id, entry.entry_id, discord_id
                                )
                                logger.info("Posted journal log '%s' by %s to guild %d", entry.entry_id, username, guild_id)

                                # Brief pause between posts to respect rate limits
                                await asyncio.sleep(1.0)

                    except Exception as e:
                        logger.warning("Error polling journal for %s: %s", username, e)

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
