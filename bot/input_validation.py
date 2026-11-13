"""Shared Discord mention/ID parsing and Backloggd username validation helpers."""

from __future__ import annotations

import re
import unicodedata
import urllib.parse
from typing import Any, Optional

import discord

# Conservative bot input-validation policy (not an official Backloggd limit).
MAX_USERNAME_LENGTH = 100

_DISCORD_ID_OR_MENTION_RE = re.compile(r"^(?:<@!?(\d{1,25})>|(\d{1,25}))$")
_FORBIDDEN_USERNAME_CHARS = set('/\\?#&=;:\'"`*`~|[]()<>@')


class InvalidUsernameError(ValueError):
    """Raised when user-supplied Backloggd username input fails validation."""


def parse_discord_member_id(raw: Optional[str]) -> Optional[int]:
    """Return a Discord user ID from a mention (<@id> / <@!id>) or raw snowflake, else None."""
    if not raw or not isinstance(raw, str):
        return None
    match = _DISCORD_ID_OR_MENTION_RE.fullmatch(raw.strip())
    if not match:
        return None
    raw_id = match.group(1) or match.group(2)
    user_id = int(raw_id)
    return user_id if user_id > 0 else None


def escape_display_text(value: Any) -> str:
    """Escape Discord mentions and markdown syntax in user-controlled strings."""
    return discord.utils.escape_markdown(discord.utils.escape_mentions(str(value)))


def normalize_backloggd_username(raw: str) -> str:
    """Validate and normalize a Backloggd username or exact Backloggd profile URL.

    Enforces a conservative bot policy length cap (`MAX_USERNAME_LENGTH = 100`),
    accepts optional single leading `@name` input or exact `backloggd.com/u/<name>/`
    profile URLs, and rejects blank, control-character, path/query/fragment,
    whitespace, and Discord-markdown/mention injection inputs.
    """
    if not isinstance(raw, str):
        raise InvalidUsernameError("Please enter a valid Backloggd username.")
    text = raw.strip()
    if not text:
        raise InvalidUsernameError("Please enter a Backloggd username.")
    if any(unicodedata.category(ch).startswith("C") for ch in text):
        raise InvalidUsernameError("Backloggd usernames cannot contain control characters or line breaks.")

    lowered = text.lower()
    if "://" in text or text.startswith("//") or lowered.startswith(("backloggd.com/", "www.backloggd.com/")):
        candidate = f"https:{text}" if text.startswith("//") else (text if "://" in text else f"https://{text}")
        parsed = urllib.parse.urlsplit(candidate)
        if parsed.scheme not in {"http", "https"} or parsed.username or parsed.password:
            raise InvalidUsernameError("Profile URLs must point to `https://www.backloggd.com/u/<username>/`.")
        if parsed.netloc.lower() not in {"backloggd.com", "www.backloggd.com"}:
            raise InvalidUsernameError("Profile URLs must use a `backloggd.com` host.")
        if parsed.query or parsed.fragment:
            raise InvalidUsernameError("Profile URLs cannot include query parameters or fragments.")
        segments = [seg for seg in parsed.path.split("/") if seg]
        if len(segments) != 2 or segments[0] != "u":
            raise InvalidUsernameError("Profile URLs must point directly to `/u/<username>/`.")
        text = urllib.parse.unquote(segments[1])
        if not text:
            raise InvalidUsernameError("Please enter a Backloggd username.")
    elif text.startswith("@"):
        text = text[1:]
        if not text:
            raise InvalidUsernameError("Please enter a Backloggd username.")

    if len(text) > MAX_USERNAME_LENGTH:
        raise InvalidUsernameError(
            f"Backloggd usernames must be {MAX_USERNAME_LENGTH} characters or fewer."
        )
    if any(ch.isspace() or unicodedata.category(ch).startswith("C") for ch in text):
        raise InvalidUsernameError("Backloggd usernames cannot contain spaces or control characters.")
    if text in {".", ".."} or ".." in text or any(ch in _FORBIDDEN_USERNAME_CHARS for ch in text):
        raise InvalidUsernameError(
            "Backloggd usernames cannot contain path separators, query characters, or Discord formatting."
        )

    return text
