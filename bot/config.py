"""Configuration constants for Backloggd Discord Bot."""

import os
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

# Discord Configuration
DISCORD_TOKEN = os.getenv("DISCORD_TOKEN", "")
DEV_GUILD_ID_RAW = os.getenv("DEV_GUILD_ID", None)
DEV_GUILD_ID = int(DEV_GUILD_ID_RAW) if DEV_GUILD_ID_RAW and DEV_GUILD_ID_RAW.isdigit() else None

# Database
BASE_DIR = Path(__file__).resolve().parent.parent
DATABASE_PATH = os.getenv("DATABASE_PATH", str(BASE_DIR / "data" / "backloggd.db"))
# Accept the previous names for existing deployments; explicit new values win.
LIBSQL_DATABASE_URL = os.getenv("LIBSQL_DATABASE_URL", os.getenv("TURSO_DATABASE_URL", "")).strip()
LIBSQL_AUTH_TOKEN = os.getenv("LIBSQL_AUTH_TOKEN", os.getenv("TURSO_AUTH_TOKEN", "")).strip()

# Operational defaults
def _parse_int_env(name: str, default: int, *, minimum: int = 0) -> int:
    raw = os.getenv(name, "").strip()
    if not raw:
        return default
    try:
        value = int(raw)
    except ValueError:
        return default
    return value if value >= minimum else default


def _parse_bool_env(name: str, default: bool = False) -> bool:
    raw = os.getenv(name, "").strip().lower()
    if not raw:
        return default
    return raw in {"1", "true", "yes", "on"}


MISSING_GUILD_PAUSE_HOURS = _parse_int_env("MISSING_GUILD_PAUSE_HOURS", 24, minimum=1)
HISTORY_RETENTION_DAYS = _parse_int_env("HISTORY_RETENTION_DAYS", 90, minimum=7)
FEED_ADAPTIVE_POLL_THRESHOLD = _parse_int_env("FEED_ADAPTIVE_POLL_THRESHOLD", 40, minimum=0)
ALERT_WEBHOOK_URL = os.getenv("ALERT_WEBHOOK_URL", "").strip()
ALERT_OWNER_DM = _parse_bool_env("ALERT_OWNER_DM", False)

# Embed Colors (Backloggd Palette)
COLOR_PRIMARY = 0x8E61F2    # Backloggd Violet
COLOR_GOLD = 0xF5A623       # Star Rating / Backlog Amber
COLOR_PLAYED = 0x10C47E     # Emerald Green
COLOR_PLAYING = 0x3B82F6    # Sky Blue
COLOR_WISHLIST = 0xEC4899   # Pink
COLOR_ERROR = 0xEF4444      # Red
COLOR_MUTED = 0x6B7280      # Slate / Gray
