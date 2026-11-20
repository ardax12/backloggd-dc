"""Entry point script for the Backloggd Discord bot."""

import asyncio
import json
import logging
import os
import signal
import sys
from datetime import datetime, timezone
from typing import Any, Optional

from aiohttp import web

from bot.client import BackloggdBot
from bot.config import DISCORD_TOKEN

_PLAIN_LOG_FORMAT = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"


class JsonLogFormatter(logging.Formatter):
    """Format log records as single-line JSON objects for structured log collectors."""

    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "timestamp": datetime.fromtimestamp(record.created, tz=timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        if record.exc_info:
            payload["exc_info"] = self.formatException(record.exc_info)
        elif record.exc_text:
            payload["exc_info"] = record.exc_text
        if record.stack_info:
            payload["stack_info"] = self.formatStack(record.stack_info)
        return json.dumps(payload, ensure_ascii=False)


def build_log_formatter(log_format: Optional[str] = None) -> logging.Formatter:
    """Return a JSON formatter when LOG_FORMAT=json, otherwise the default plain-text formatter."""
    selected = (os.getenv("LOG_FORMAT", "") if log_format is None else log_format).strip().lower()
    if selected == "json":
        return JsonLogFormatter()
    return logging.Formatter(_PLAIN_LOG_FORMAT)


def configure_logging(log_format: Optional[str] = None, *, force: bool = True) -> logging.Handler:
    """Configure stdout logging using LOG_FORMAT (plain text by default, or json)."""
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(build_log_formatter(log_format))
    logging.basicConfig(level=logging.INFO, handlers=[handler], force=force)
    return handler


configure_logging(force=False)
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
    session_health = getattr(bot, "session_health", None)
    if session_health is not None:
        get_metadata: Any = getattr(session_health, "get_health_metadata", None)
        if callable(get_metadata):
            metadata = get_metadata()
            if isinstance(metadata, dict):
                payload.update(metadata)
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
    loop = asyncio.get_running_loop()
    shutdown_tasks: set[asyncio.Task[Any]] = set()
    sigterm_registered = False

    def _on_shutdown_task_done(task: asyncio.Task[Any]) -> None:
        shutdown_tasks.discard(task)
        if not task.cancelled():
            exc = task.exception()
            if exc is not None:
                logger.error("Error during signal-triggered bot shutdown", exc_info=exc)

    def _handle_sigterm() -> None:
        if shutdown_tasks or getattr(bot, "_closing", False) is True:
            return
        logger.info("Received SIGTERM; initiating graceful shutdown.")
        task = loop.create_task(bot.close())
        shutdown_tasks.add(task)
        task.add_done_callback(_on_shutdown_task_done)

    sigterm = getattr(signal, "SIGTERM", None)
    if sigterm is not None:
        try:
            loop.add_signal_handler(sigterm, _handle_sigterm)
            sigterm_registered = True
        except NotImplementedError:
            pass

    try:
        async with bot:
            # Hosting platforms can request a health server through PORT.
            port_env = os.getenv("PORT")
            if port_env and port_env.isdigit():
                runner = await start_health_server(int(port_env), bot=bot)
            if getattr(bot, "_closing", False) is not True:
                await bot.start(DISCORD_TOKEN)
    finally:
        if sigterm_registered and sigterm is not None:
            try:
                loop.remove_signal_handler(sigterm)
            except (NotImplementedError, ValueError):
                pass
        if shutdown_tasks:
            await asyncio.gather(*shutdown_tasks, return_exceptions=True)
        if runner:
            await runner.cleanup()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Bot execution terminated by user.")
