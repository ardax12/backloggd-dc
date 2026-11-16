"""Formatting helpers for stars, status badges, numbers, and Backloggd URLs."""

from typing import Any, Optional
import urllib.parse

def rating_to_stars(rating: Optional[float], max_stars: int = 5) -> str:
    """
    Convert a numeric rating (e.g. 4.5) to a clean star string (e.g. '★★★★½').
    """
    if rating is None:
        return "☆☆☆☆☆"
    
    full_stars = int(rating)
    decimal = rating - full_stars
    has_half = decimal >= 0.25 and decimal < 0.75
    if decimal >= 0.75:
        full_stars += 1

    stars = "★" * full_stars
    if has_half:
        stars += "½"
    empty_stars = max_stars - len(stars)
    if empty_stars > 0:
        stars += "☆" * empty_stars
    return stars

def format_number(val: Optional[Any]) -> str:
    """Format large numbers or count strings into compact representations (e.g. 14200 -> 14.2k, '24K' -> 24K)."""
    if val is None:
        return "0"

    if isinstance(val, str):
        cleaned = val.strip()
        if not cleaned:
            return "0"
        # If it already has unit suffix like 24K, 1.2M, etc.
        if cleaned[-1].upper() in ("K", "M", "B"):
            return cleaned
        # Try parsing numeric string
        try:
            num = float(cleaned.replace(",", ""))
            if num.is_integer():
                num = int(num)
            val = num
        except ValueError:
            return cleaned

    if isinstance(val, (int, float)):
        if val >= 1_000_000:
            return f"{val / 1_000_000:.1f}M"
        if val >= 1_000:
            return f"{val / 1_000:.1f}k"
        return str(int(val)) if isinstance(val, float) and val.is_integer() else str(val)

    return str(val)

def format_status(status: str) -> str:
    """Convert status enum/string into capitalized display with emoji."""
    status_lower = str(status).lower().strip()
    if status_lower in ("unknown", "none", ""):
        return "🎮 Played"
    mapping = {
        "played": "🎮 Played",
        "playing": "▶️ Playing",
        "backlog": "⏳ Backlog",
        "wishlist": "💖 Wishlist",
        "abandoned": "🛑 Abandoned"
    }
    return mapping.get(status_lower, status.capitalize())

def build_backloggd_game_url(slug: str) -> str:
    """Return full URL to game page on Backloggd."""
    encoded = urllib.parse.quote(str(slug).strip(), safe="")
    return f"https://www.backloggd.com/games/{encoded}/"

def build_backloggd_user_url(username: str) -> str:
    """Return full URL to user profile on Backloggd."""
    encoded = urllib.parse.quote(str(username).strip().lstrip("@"), safe="")
    return f"https://www.backloggd.com/u/{encoded}/"

def build_backloggd_review_url(username: str, review_id: Any) -> str:
    """Return full URL to a user's review on Backloggd."""
    encoded_user = urllib.parse.quote(str(username).strip().lstrip("@"), safe="")
    encoded_id = urllib.parse.quote(str(review_id).strip(), safe="")
    return f"https://backloggd.com/u/{encoded_user}/review/{encoded_id}/"

DEFAULT_BACKLOGGD_AVATAR = "https://backloggd.b-cdn.net/no_avatar.jpg"

def clean_image_url(url: Optional[str]) -> Optional[str]:
    """Ensure image URLs are well-formed http/https URLs for Discord embeds."""
    if not url:
        return None
    cleaned = str(url).strip()
    if not cleaned:
        return None
    if any(ch.isspace() or ord(ch) < 32 or ord(ch) == 127 for ch in cleaned):
        return None
    if cleaned.startswith("//"):
        cleaned = f"https:{cleaned}"
    elif cleaned.startswith("/"):
        cleaned = f"https://backloggd.com{cleaned}"
    elif not cleaned.lower().startswith(("http://", "https://")):
        return None

    parsed = urllib.parse.urlsplit(cleaned)
    if parsed.scheme.lower() not in {"http", "https"}:
        return None
    if not parsed.netloc or not parsed.hostname:
        return None
    return cleaned


def truncate_button_label(text: Any, max_length: int = 80) -> str:
    """Bound plain-text Discord button labels to max_length characters including ellipsis."""
    raw = "" if text is None else str(text)
    if len(raw) <= max_length:
        return raw
    if max_length <= 3:
        return raw[: max(0, max_length)]
    return raw[: max_length - 3].rstrip() + "..."


def clamp_escaped_text(
    text: str,
    max_length: int,
    *,
    suffix: str = "...",
    preserve_word_boundary: bool = False,
) -> str:
    """Clamp already-escaped Markdown text without splitting a backslash escape."""
    if len(text) <= max_length:
        return text
    if max_length <= 0:
        return ""
    if len(suffix) >= max_length:
        return suffix[:max_length]

    cutoff = max_length - len(suffix)
    cut = text[:cutoff]
    if preserve_word_boundary and " " in cut:
        candidate = cut.rsplit(" ", 1)[0]
        if len(candidate) >= cutoff // 2:
            cut = candidate
    cut = cut.rstrip()
    trailing_backslashes = len(cut) - len(cut.rstrip("\\"))
    if trailing_backslashes % 2 == 1:
        cut = cut[:-1]
    return f"{cut}{suffix}"
