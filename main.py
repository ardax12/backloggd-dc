"""Entry point script for the Backloggd Discord bot."""

import asyncio
import logging
import os
import sys
from aiohttp import web
from bot.client import BackloggdBot
from bot.config import DISCORD_TOKEN

# Configure structured logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("backloggd.main")

async def handle_health_check(request: web.Request) -> web.Response:
    """Simple HTTP health check endpoint for cloud hosting platforms like Render."""
    return web.json_response({"status": "online", "service": "backloggd-discord-bot"})

async def start_health_server(port: int) -> web.AppRunner:
    """Start a lightweight HTTP server on the designated cloud port."""
    app = web.Application()
    app.router.add_get("/", handle_health_check)
    app.router.add_get("/health", handle_health_check)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", port)
    await site.start()
    logger.info("Health check server listening on http://0.0.0.0:%d", port)
    return runner

async def main():
    if not DISCORD_TOKEN or DISCORD_TOKEN == "your_bot_token_here":
        logger.error(
            "DISCORD_TOKEN is not configured! Please provide your bot token in the .env file."
        )
        sys.exit(1)

    # If hosted on Render/cloud with a PORT environment variable, run web health server
    runner = None
    port_env = os.getenv("PORT")
    if port_env and port_env.isdigit():
        runner = await start_health_server(int(port_env))

    bot = BackloggdBot()
    try:
        async with bot:
            await bot.start(DISCORD_TOKEN)
    finally:
        if runner:
            await runner.cleanup()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Bot execution terminated by user.")
