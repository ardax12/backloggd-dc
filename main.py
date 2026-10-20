"""Entry point script for the Backloggd Discord bot."""

import asyncio
import logging
import sys
from bot.client import BackloggdBot
from bot.config import DISCORD_TOKEN

# Configure structured logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("backloggd.main")

async def main():
    if not DISCORD_TOKEN or DISCORD_TOKEN == "your_bot_token_here":
        logger.error(
            "DISCORD_TOKEN is not configured! Please provide your bot token in the .env file."
        )
        sys.exit(1)

    bot = BackloggdBot()
    async with bot:
        await bot.start(DISCORD_TOKEN)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Bot execution terminated by user.")
