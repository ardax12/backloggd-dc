"""Embed constructor for automatic activity feed events (Filmlinkd-style)."""

import discord
from typing import Any
from bot.config import COLOR_PRIMARY, COLOR_GOLD, COLOR_PLAYED, COLOR_WISHLIST
from bot.ui.helpers import (
    rating_to_stars,
    build_backloggd_game_url,
    build_backloggd_user_url,
    clean_image_url,
    DEFAULT_BACKLOGGD_AVATAR
)

def create_journal_feed_embed(
    username: str,
    avatar_url: str | None,
    entry: Any
) -> discord.Embed:
    """Construct embed for an automatic playthrough journal or activity log announcement."""
    user_url = build_backloggd_user_url(username)
    game_slug = getattr(entry, "game_slug", "")
    game_url = build_backloggd_game_url(game_slug)
    game_title = getattr(entry, "game_title", "Unknown Game")

    # Format dynamic action headline based on activity type
    action_type = getattr(entry, "action_type", "played").lower()
    if action_type == "completed":
        title = f"🏆 {username} completed {game_title}"
        color = COLOR_PLAYED
    elif action_type == "reviewed":
        title = f"📝 {username} reviewed {game_title}"
        color = COLOR_PRIMARY
    elif action_type == "backlog":
        title = f"⏳ {username} added {game_title} to their backlog"
        color = COLOR_GOLD
    elif action_type == "wishlist":
        title = f"💖 {username} wishlisted {game_title}"
        color = COLOR_WISHLIST
    elif action_type == "playing":
        title = f"▶️ {username} started playing {game_title}"
        color = COLOR_PRIMARY
    elif getattr(entry, "rating", None) is not None:
        title = f"⭐ {username} rated {game_title}"
        color = COLOR_GOLD
    else:
        title = f"🎮 {username} logged {game_title}"
        color = COLOR_PLAYED

    embed = discord.Embed(
        title=title,
        url=game_url,
        color=color
    )

    clean_avatar = clean_image_url(avatar_url) or DEFAULT_BACKLOGGD_AVATAR
    embed.set_author(name=username, icon_url=clean_avatar, url=user_url)

    cover = clean_image_url(getattr(entry, "cover_url", None))
    if cover:
        embed.set_thumbnail(url=cover)

    details = []
    if getattr(entry, "rating", None) is not None:
        stars = rating_to_stars(entry.rating)
        details.append(f"**Rating:** {stars} (`{entry.rating:.1f}` / 5.0)")

    if getattr(entry, "platform", None):
        details.append(f"**Platform:** {entry.platform}")

    if getattr(entry, "is_replay", False):
        details.append("**Replay:** 🔁 Yes")

    date_val = getattr(entry, "played_date", None) or getattr(entry, "date_str", None)
    if date_val:
        details.append(f"**Activity Time:** {date_val}")

    if details:
        embed.description = "\n".join(details)

    # Review notes or written thoughts if present
    notes = getattr(entry, "review_notes", None)
    if notes:
        if len(notes) > 400:
            notes = notes[:397] + "..."
        embed.add_field(name="💬 Review / Notes", value=f"> {notes}", inline=False)

    embed.set_footer(text="Backloggd Activity Stream • Connect yours with /link")
    return embed
