"""Embed constructor for automatic activity feed events (Filmlinkd-style)."""

from datetime import datetime, timezone
from typing import Any

import discord

from bot.config import COLOR_GOLD, COLOR_PLAYED, COLOR_PRIMARY, COLOR_WISHLIST
from bot.feed_rules import calendar_date
from bot.input_validation import escape_display_text
from bot.ui.helpers import (
    DEFAULT_BACKLOGGD_AVATAR,
    build_backloggd_game_url,
    build_backloggd_user_url,
    clamp_escaped_text,
    clean_image_url,
    rating_to_stars,
)


def create_journal_feed_embed(
    username: str,
    avatar_url: str | None,
    entry: Any,
    *,
    now: datetime | None = None,
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
    current_now = now or datetime.now(timezone.utc)
    if current_now.tzinfo is None:
        current_now = current_now.replace(tzinfo=timezone.utc)
    action_day = calendar_date(getattr(entry, "occurred_at", None)) or current_now.astimezone(timezone.utc).date().isoformat()
    is_backdated = played_day and played_day < action_day
    if is_backdated and action_type == "completed":
        title = f"{username} finished {game_title}"
    elif is_backdated and action_type == "playing":
        title = f"{username} started {game_title}"

    embed = discord.Embed(
        title=title[:256],
        url=game_url,
        color=color
    )

    clean_avatar = clean_image_url(avatar_url) or DEFAULT_BACKLOGGD_AVATAR
    embed.set_author(name=username[:256], icon_url=clean_avatar, url=user_url)

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
        details.append(f"**Platform:** {clamp_escaped_text(escape_display_text(entry.platform), 200)}")

    if getattr(entry, "is_replay", False):
        details.append("**Replay:** 🔁 Yes")

    session_labels = getattr(entry, "session_labels", ())
    if session_labels:
        safe_labels = [escape_display_text(label) for label in session_labels]
        details.append("**Session:** " + clamp_escaped_text(" / ".join(safe_labels), 300))
    if played_date:
        details.append(f"**Played date:** {clamp_escaped_text(escape_display_text(played_date), 120)}")
    activity_time = getattr(entry, "occurred_at", None) or getattr(entry, "date_str", None)
    if activity_time:
        details.append(f"**Activity time:** {clamp_escaped_text(escape_display_text(activity_time), 120)}")

    if details:
        embed.description = clamp_escaped_text("\n".join(details), 4096)

    # Review notes or written thoughts if present
    notes = getattr(entry, "review_notes", None)
    if notes:
        has_spoilers = bool(getattr(entry, "has_spoilers", False))
        normalized = str(notes).replace("\r\n", "\n").replace("\r", "\n")
        escaped_notes = escape_display_text(normalized)
        wrapped_notes = f"||{escaped_notes}||" if has_spoilers else escaped_notes
        quoted_notes = "\n".join(f"> {line}" for line in wrapped_notes.split("\n"))
        bounded_notes = clamp_escaped_text(
            quoted_notes,
            500,
            suffix="...||" if has_spoilers else "...",
        )
        embed.add_field(name="💬 Review / Notes", value=bounded_notes, inline=False)

    embed.set_footer(text="Backloggd Activity Stream • Connect yours with /link")
    return embed
