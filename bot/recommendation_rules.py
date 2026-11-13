"""Small rules shared by recommendation and profile-comparison commands."""

from typing import Any, Mapping, Optional

ELIGIBLE_UNPLAYED_STATUSES = frozenset({"backlog", "wishlist"})
KNOWN_PLAYED_OR_ACTIVE_STATUSES = frozenset({"played", "playing"})


def numeric_ratings(ratings: Mapping[str, Optional[float]]) -> dict[str, float]:
    """Keep explicitly rated games for similarity calculations and thresholds."""
    return {
        slug: rating
        for slug, rating in ratings.items()
        if isinstance(rating, (int, float)) and not isinstance(rating, bool)
    }


def normalize_library_status(status: Any) -> str:
    """Normalize a Backloggd status enum or string value."""
    if hasattr(status, "value"):
        status = status.value
    if status is None:
        return "unknown"
    text = str(status).strip().lower()
    return text or "unknown"


def is_excluded_from_recommendations(status: Any, rating: Optional[float]) -> bool:
    """Exclude played, playing, unknown/unmapped, and any explicitly rated entries."""
    if rating is not None:
        return True
    return normalize_library_status(status) not in ELIGIBLE_UNPLAYED_STATUSES


def excluded_recommendation_slugs(entries: Mapping[str, Any]) -> set[str]:
    """Compute excluded game slugs from {slug: {'status': ..., 'rating': ...}}."""
    excluded: set[str] = set()
    for slug, entry in entries.items():
        if isinstance(entry, Mapping):
            status = entry.get("status")
            rating = entry.get("rating")
        elif isinstance(entry, tuple) and len(entry) >= 2:
            status, rating = entry[0], entry[1]
        else:
            excluded.add(slug)
            continue
        if is_excluded_from_recommendations(status, rating):
            excluded.add(slug)
    return excluded


def has_uncertain_library_exclusion(
    candidate_ratings: Mapping[str, Optional[float]],
    target_entries: Optional[Mapping[str, Any]],
) -> bool:
    """Return true if any 4.0+ candidate matched an unrated unknown/unmapped status."""
    if not target_entries:
        return False
    for slug, rating in candidate_ratings.items():
        if not isinstance(rating, (int, float)) or isinstance(rating, bool) or rating < 4.0:
            continue
        entry = target_entries.get(slug)
        if not isinstance(entry, Mapping):
            continue
        if (
            entry.get("rating") is None
            and normalize_library_status(entry.get("status"))
            not in (ELIGIBLE_UNPLAYED_STATUSES | KNOWN_PLAYED_OR_ACTIVE_STATUSES)
        ):
            return True
    return False
