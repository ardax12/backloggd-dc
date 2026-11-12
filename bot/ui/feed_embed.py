"""Embed constructor for automatic activity feed events (Filmlinkd-style)."""

import discord
from datetime import datetime, timezone
from bot.feed_rules import calendar_date
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

    previous_rating = getattr(entry, "previous_rating", None)
    if action_type == "rated" and previous_rating is not None:
        title = f"{username} changed their rating for {game_title}"
    elif action_type == "replay":
        title = f"{username} logged a replay of {game_title}"
    played_date = getattr(entry, "played_date", None)
    played_day = calendar_date(played_date)
    action_day = calendar_date(getattr(entry, "occurred_at", None)) or datetime.now(timezone.utc).date().isoformat()
    is_backdated = played_day and played_day < action_day
    if is_backdated:
        title = f"{username} logged {'a replay' if getattr(entry, 'is_replay', False) else 'a playthrough'} of {game_title}"

    embed = discord.Embed(
        title=title[:256],
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
        if previous_rating is not None and action_type == "rated":
            details.append(f"**Rating:** {previous_rating:.1f} \u2192 {entry.rating:.1f} / 5.0")
        else:
            details.append(f"**Rating:** {stars} (`{entry.rating:.1f}` / 5.0)")

    if getattr(entry, "platform", None):
        details.append(f"**Platform:** {entry.platform}")

    if getattr(entry, "is_replay", False):
        details.append("**Replay:** 🔁 Yes")

    if getattr(entry, "session_labels", ()):
        details.append("**Session:** " + " / ".join(entry.session_labels))
    if played_date:
        details.append(f"**Played date:** {played_date}")
    activity_time = getattr(entry, "occurred_at", None) or getattr(entry, "date_str", None)
    if activity_time:
        details.append(f"**Activity time:** {activity_time}")

    if details:
        embed.description = "\n".join(details)

    # Review notes or written thoughts if present
    notes = getattr(entry, "review_notes", None)
    if notes:
        if len(notes) > 400:
            notes = notes[:397] + "..."
        notes = f"||{notes}||" if getattr(entry, "has_spoilers", False) else notes
        embed.add_field(name="💬 Review / Notes", value=f"> {notes}", inline=False)

    embed.set_footer(text="Backloggd Activity Stream • Connect yours with /link")
    return embed
