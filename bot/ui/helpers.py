"""Formatting helpers for stars, status badges, numbers, and Backloggd URLs."""

from typing import Optional

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

def format_number(val: Optional[int]) -> str:
    """Format large numbers into compact strings (e.g. 14200 -> 14.2k)."""
    if val is None:
        return "0"
    if val >= 1_000_000:
        return f"{val / 1_000_000:.1f}M"
    if val >= 1_000:
        return f"{val / 1_000:.1f}k"
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
