"""Custom Discord Bot Client with lifecycle hooks and Backloggd integration."""

import discord
from discord import app_commands
from discord.ext import commands
import logging
from pathlib import Path
from backloggd import BackloggdClient
from bot.command_errors import handle_app_command_error
from bot.db import Database
from bot.config import DATABASE_PATH, DEV_GUILD_ID
from bot.services.guild_membership import GuildMembershipService
from bot.services.library_sync import LibrarySyncService

logger = logging.getLogger("backloggd.bot")

class BackloggdBot(commands.Bot):
    def __init__(self):
        intents = discord.Intents.default()
        intents.members = True          # Required for server-specific member queries
        intents.message_content = False  # Slash commands only; message content is unused

        super().__init__(
            command_prefix="!",
            intents=intents,
            help_command=None,
            allowed_mentions=discord.AllowedMentions.none(),
        )

        self.db = Database(DATABASE_PATH)
        self.backloggd = BackloggdClient(
            request_delay=1.0,
            profile_ttl=3600,
            cache_path=Path(DATABASE_PATH).parent / 'sdk-cache' / 'backloggd_cache.db',
        )
        self.library_sync = LibrarySyncService(max_workers=2)
        self.guild_membership = GuildMembershipService()
        self.tree.error(self.on_app_command_error)

    async def on_app_command_error(
        self,
        interaction: discord.Interaction,
        error: app_commands.AppCommandError,
    ) -> None:
        await handle_app_command_error(interaction, error, log=logger)

    async def setup_hook(self) -> None:
        """Called automatically before bot logs into Discord."""
        # 1. Connect SQLite database
        await self.db.connect()

        # 2. Load Cogs
        initial_extensions = [
            "bot.cogs.account",
            "bot.cogs.games",
            "bot.cogs.users",
            "bot.cogs.social",
            "bot.cogs.discovery",
            "bot.cogs.stats",
            "bot.cogs.feed",
        ]
        for ext in initial_extensions:
            try:
                await self.load_extension(ext)
                logger.info("Loaded extension: %s", ext)
            except Exception as e:
                logger.error("Failed to load required extension %s: %s", ext, e, exc_info=True)
                raise RuntimeError(f"Required extension {ext} failed to load") from e

        # 3. Slash Command Sync
        try:
            if self.application_id:
                if DEV_GUILD_ID:
                    guild = discord.Object(id=DEV_GUILD_ID)
                    self.tree.copy_global_to(guild=guild)
                    synced = await self.tree.sync(guild=guild)
                    logger.info("Synced %d slash commands to Dev Guild ID: %d", len(synced), DEV_GUILD_ID)
                else:
                    synced = await self.tree.sync()
                    logger.info("Globally synced %d slash commands", len(synced))
            else:
                logger.info("Application ID not set yet; slash commands will sync on bot startup.")
        except Exception as e:
            logger.warning("Could not sync slash commands in setup_hook: %s", e)

    async def close(self) -> None:
        """Graceful shutdown hook."""
        logger.info("Shutting down BackloggdBot...")
        feed_cog = self.get_cog("Feed")
        if feed_cog and hasattr(feed_cog, "close"):
            await feed_cog.close()

        stats_cog = self.get_cog("Stats")
        if stats_cog and hasattr(stats_cog, "close"):
            await stats_cog.close()

        account_cog = self.get_cog("Account")
        if account_cog and hasattr(account_cog, "close"):
            await account_cog.close()

        social_cog = self.get_cog("Social")
        if social_cog and hasattr(social_cog, "close"):
            await social_cog.close()

        await self.guild_membership.close()
        await self.library_sync.close()
        await self.backloggd.close()
        await self.db.close()
        await super().close()

    async def on_ready(self) -> None:
        """Triggered when connection is established and cache is ready."""
        logger.info("Logged in as %s (ID: %d)", self.user.name, self.user.id)
        feed_cog = self.get_cog("Feed")
        if feed_cog:
            await feed_cog.reconcile_legacy_enrollments()
        activity = discord.Activity(
            type=discord.ActivityType.watching,
            name="games on Backloggd | /game"
        )
        await self.change_presence(activity=activity)
