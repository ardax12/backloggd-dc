"""Formatting helpers for stars, status badges, numbers, and Backloggd URLs."""

from typing import Any, Optional

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
    status_lower = str(status).lower()
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
    return f"https://www.backloggd.com/games/{slug}/"

def build_backloggd_user_url(username: str) -> str:
    """Return full URL to user profile on Backloggd."""
    return f"https://www.backloggd.com/u/{username}/"

DEFAULT_BACKLOGGD_AVATAR = "https://backloggd.b-cdn.net/no_avatar.jpg"

def clean_image_url(url: Optional[str]) -> Optional[str]:
    """Ensure image URLs are well-formed absolute URLs for Discord embeds."""
    if not url:
        return None
    cleaned = str(url).strip()
    if not cleaned:
        return None
    if cleaned.startswith("//"):
        return f"https:{cleaned}"
    if cleaned.startswith("/"):
        return f"https://backloggd.com{cleaned}"
    if not cleaned.startswith("http://") and not cleaned.startswith("https://"):
        return f"https://{cleaned}"
    return cleaned
