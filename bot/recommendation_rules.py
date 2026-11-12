"""Small rules shared by recommendation and profile-comparison commands."""

from typing import Mapping, Optional


def numeric_ratings(ratings: Mapping[str, Optional[float]]) -> dict[str, float]:
    """Keep explicitly rated games for similarity calculations and thresholds."""
    return {
        slug: rating
        for slug, rating in ratings.items()
        if isinstance(rating, (int, float)) and not isinstance(rating, bool)
    }
