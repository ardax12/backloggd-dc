"""Entry point script for the Backloggd Discord bot."""

import asyncio
import logging
import os
import sys
from typing import Any, Optional
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

SERVICE_NAME = "backloggd-discord-bot"
BOT_APP_KEY: web.AppKey[Optional[Any]] = web.AppKey("bot")


def get_deployed_revision() -> Optional[str]:
    """Return the deployed commit revision from build/platform configuration if set."""
    for env_var in ("APP_REVISION", "RENDER_GIT_COMMIT"):
        value = os.getenv(env_var, "").strip()
        if value:
            return value
    return None


async def handle_liveness_check(request: web.Request) -> web.Response:
    """Lightweight HTTP liveness endpoint for hosting platforms."""
    return web.json_response({"status": "online", "service": SERVICE_NAME})


async def handle_health_check(request: web.Request) -> web.Response:
    """State-aware readiness endpoint derived from local bot and database state."""
    bot = request.app.get(BOT_APP_KEY)
    if bot is None:
        http_status, status_name, ready = 503, "starting", False
    else:
        http_status, status_name, ready = bot.get_readiness_status()

    payload: dict[str, Any] = {
        "status": status_name,
        "ready": ready,
        "service": SERVICE_NAME,
    }
    revision = get_deployed_revision()
    if revision is not None:
        payload["revision"] = revision
    return web.json_response(payload, status=http_status)


def create_health_app(bot: Optional[Any] = None) -> web.Application:
    """Build the aiohttp application exposing liveness (/) and readiness (/health)."""
    app = web.Application()
    app[BOT_APP_KEY] = bot
    app.router.add_get("/", handle_liveness_check)
    app.router.add_get("/health", handle_health_check)
    return app


async def start_health_server(port: int, *, bot: Optional[Any] = None) -> web.AppRunner:
    """Start a lightweight HTTP server on the designated cloud port."""
    app = create_health_app(bot=bot)
    runner = web.AppRunner(app)
    await runner.setup()
    try:
        site = web.TCPSite(runner, "0.0.0.0", port)
        await site.start()
    except BaseException:
        await runner.cleanup()
        raise
    logger.info("Health check server listening on http://0.0.0.0:%d", port)
    return runner


async def main():
    if not DISCORD_TOKEN or DISCORD_TOKEN == "your_bot_token_here":
        logger.error(
            "DISCORD_TOKEN is not configured! Please provide your bot token in the .env file."
        )
        sys.exit(1)

    session = os.getenv("BACKLOGGD_SESSION", "").strip()
    if not session or session == "your_backloggd_session_cookie_here":
        logger.error(
            "BACKLOGGD_SESSION is required. Set your Backloggd _backloggd_session "
            "cookie in .env or your deployment environment."
        )
        sys.exit(1)

    bot = BackloggdBot()
    runner = None
    try:
        async with bot:
            # Hosting platforms can request a health server through PORT.
            port_env = os.getenv("PORT")
            if port_env and port_env.isdigit():
                runner = await start_health_server(int(port_env), bot=bot)
            await bot.start(DISCORD_TOKEN)
    finally:
        if runner:
            await runner.cleanup()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Bot execution terminated by user.")
