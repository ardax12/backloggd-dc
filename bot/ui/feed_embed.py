"""Embed constructor for automatic playthrough journal log feeds (Filmlinkd-style)."""

import discord
from typing import Any
from bot.config import COLOR_PRIMARY, COLOR_GOLD, COLOR_PLAYED
from bot.ui.helpers import rating_to_stars, build_backloggd_game_url, build_backloggd_user_url

def create_journal_feed_embed(
    username: str,
    avatar_url: str | None,
    entry: Any
) -> discord.Embed:
    """Construct embed for an automatic playthrough journal log announcement."""
    user_url = build_backloggd_user_url(username)
    game_url = build_backloggd_game_url(entry.game_slug)
    
    title = f"{username} logged {entry.game_title}"
    embed = discord.Embed(
        title=title,
        url=game_url,
        color=COLOR_PLAYED
    )

    if avatar_url:
        embed.set_author(name=username, icon_url=avatar_url, url=user_url)
    else:
        embed.set_author(name=username, url=user_url)

    if getattr(entry, "cover_url", None):
        embed.set_thumbnail(url=entry.cover_url)

    details = []
    if entry.rating is not None:
        stars = rating_to_stars(entry.rating)
        details.append(f"**Rating:** {stars} (`{entry.rating:.1f}` / 5.0)")
    else:
        details.append("**Rating:** *Unrated*")

    if getattr(entry, "platform", None):
        details.append(f"**Platform:** {entry.platform}")

    if getattr(entry, "is_replay", False):
        details.append("**Replay:** 🔁 Yes")

    if getattr(entry, "played_date", None):
        details.append(f"**Date:** {entry.played_date}")

    embed.description = "\n".join(details)

    # Review notes if user included written thoughts
    if getattr(entry, "review_notes", None):
        notes = entry.review_notes
        if len(notes) > 400:
            notes = notes[:397] + "..."
        embed.add_field(name="📝 Review / Notes", value=f"> {notes}", inline=False)

    embed.set_footer(text="Logged on Backloggd • Connect yours with /link")
    return embed
