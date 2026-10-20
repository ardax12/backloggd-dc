"""Custom Discord Bot Client with lifecycle hooks and Backloggd integration."""

import discord
from discord.ext import commands
import logging
from backloggd import BackloggdClient
from bot.db import Database
from bot.config import DATABASE_PATH, DEV_GUILD_ID

logger = logging.getLogger("backloggd.bot")

class BackloggdBot(commands.Bot):
    def __init__(self):
        intents = discord.Intents.default()
        intents.members = True          # Required for server-specific member queries
        intents.message_content = True  # Required for link unfurling and chat helpers

        super().__init__(
            command_prefix="!",
            intents=intents,
            help_command=None
        )

        self.db = Database(DATABASE_PATH)
        self.backloggd = BackloggdClient(request_delay=1.0)

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
                logger.error("Failed to load extension %s: %s", ext, e, exc_info=True)

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
        if feed_cog and hasattr(feed_cog, "journal_feed_loop") and feed_cog.journal_feed_loop.is_running():
            feed_cog.journal_feed_loop.cancel()

        await self.backloggd.close()
        await self.db.close()
        await super().close()

    async def on_ready(self) -> None:
        """Triggered when connection is established and cache is ready."""
        logger.info("Logged in as %s (ID: %d)", self.user.name, self.user.id)
        activity = discord.Activity(
            type=discord.ActivityType.watching,
            name="games on Backloggd | /game"
        )
        await self.change_presence(activity=activity)
