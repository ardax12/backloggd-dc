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

# Embed Colors (Backloggd Palette)
COLOR_PRIMARY = 0x8E61F2    # Backloggd Violet
COLOR_GOLD = 0xF5A623       # Star Rating / Backlog Amber
COLOR_PLAYED = 0x10C47E     # Emerald Green
COLOR_PLAYING = 0x3B82F6    # Sky Blue
COLOR_WISHLIST = 0xEC4899   # Pink
COLOR_ERROR = 0xEF4444      # Red
COLOR_MUTED = 0x6B7280      # Slate / Gray
