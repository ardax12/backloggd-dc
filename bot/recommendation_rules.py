"""Pure scoring, filtering, and ranking rules for recommendation and taste commands."""

from typing import Any, Callable, Iterable, Mapping, Optional

from backloggd.analytics import calculate_similarity_raw

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


def eligible_pool_slugs(
    entries: Optional[Mapping[str, Any]],
    allowed_statuses: frozenset[str],
) -> set[str]:
    """Return unrated game slugs whose normalized status is in allowed_statuses."""
    if not entries or not allowed_statuses:
        return set()
    valid_statuses = allowed_statuses & ELIGIBLE_UNPLAYED_STATUSES
    if not valid_statuses:
        return set()
    matched: set[str] = set()
    for slug, entry in entries.items():
        if isinstance(entry, Mapping):
            status = entry.get("status")
            rating = entry.get("rating")
        elif isinstance(entry, tuple) and len(entry) >= 2:
            status, rating = entry[0], entry[1]
        else:
            continue
        if rating is None and normalize_library_status(status) in valid_statuses:
            matched.add(slug)
    return matched


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


def resolve_display_title(
    slug: str,
    cached_meta: Optional[Mapping[str, Any]] = None,
    fallback_titles: Optional[Mapping[str, Optional[str]]] = None,
) -> str:
    """Resolve a human-readable game title from cached metadata, fallback titles, or slug."""
    meta = cached_meta.get(slug) if cached_meta else None
    raw_title = meta["title"] if meta else None
    if isinstance(raw_title, str) and raw_title.strip():
        return raw_title.strip()
    fallback_title = fallback_titles.get(slug) if fallback_titles else None
    if isinstance(fallback_title, str) and fallback_title.strip():
        return fallback_title.strip()
    return slug.replace("-", " ").title()


def compute_taste_compatibility(
    caller_ratings: Mapping[str, Optional[float]],
    friend_ratings: Mapping[str, Optional[float]],
    *,
    min_ratings: int = 5,
    similarity_fn: Callable[..., tuple[float, Any, Any]] = calculate_similarity_raw,
) -> Optional[int]:
    """Return rounded similarity percentage when both profiles meet the minimum rated count."""
    caller_numeric = numeric_ratings(caller_ratings)
    friend_numeric = numeric_ratings(friend_ratings)
    if len(caller_numeric) < min_ratings or len(friend_numeric) < min_ratings:
        return None
    sim_score, _, _ = similarity_fn(caller_numeric, friend_numeric)
    return int(round(sim_score))


def qualifying_high_rated_games(
    ratings: Mapping[str, Optional[float]],
    *,
    min_rating: float = 4.0,
) -> list[tuple[str, float]]:
    """Return (slug, rating) pairs rated at or above min_rating."""
    return [
        (slug, rating)
        for slug, rating in ratings.items()
        if rating is not None and rating >= min_rating
    ]


def select_friend_recommendations(
    qualifying_games: Iterable[tuple[str, float]],
    *,
    excluded_slugs: set[str],
    pool_slugs: Optional[set[str]] = None,
    limit: int = 5,
) -> list[tuple[str, float]]:
    """Filter qualifying games for a friend, rank by rating descending, and cap to limit."""
    eligible = [
        (slug, rating)
        for slug, rating in qualifying_games
        if slug not in excluded_slugs and (pool_slugs is None or slug in pool_slugs)
    ]
    eligible.sort(key=lambda item: item[1], reverse=True)
    return eligible[:limit]


def rank_taste_matches(
    my_numeric_ratings: Mapping[str, float],
    other_profiles: Iterable[Mapping[str, Any]],
    all_other_ratings: Mapping[str, Mapping[str, Optional[float]]],
    *,
    min_other_ratings: int = 5,
    min_common_ratings: int = 2,
    similarity_fn: Callable[..., tuple[float, Any, Any]] = calculate_similarity_raw,
) -> tuple[list[tuple[str, str, float, Mapping[str, Optional[float]]]], bool]:
    """Rank server candidate profiles by taste similarity descending."""
    taste_matches: list[tuple[str, str, float, Mapping[str, Optional[float]]]] = []
    enough_rated_profiles = False
    my_slugs = set(my_numeric_ratings)

    for other in other_profiles:
        other_profile_key = other["profile_key"]
        other_username = other["display_username"]
        other_ratings = all_other_ratings.get(other_profile_key, {})
        other_numeric_ratings = numeric_ratings(other_ratings)

        if len(other_numeric_ratings) < min_other_ratings:
            continue
        enough_rated_profiles = True

        if len(my_slugs & set(other_numeric_ratings)) < min_common_ratings:
            continue

        final_score, _, _ = similarity_fn(my_numeric_ratings, other_numeric_ratings)
        taste_matches.append((other_profile_key, other_username, final_score, other_ratings))

    taste_matches.sort(key=lambda item: item[2], reverse=True)
    return taste_matches, enough_rated_profiles


def select_diverse_recommendations(
    taste_matches: Iterable[tuple[str, str, float, Mapping[str, Optional[float]]]],
    *,
    excluded_slugs: set[str],
    pool_slugs: Optional[set[str]] = None,
    min_rating: float = 4.0,
    limit: int = 5,
) -> list[tuple[str, float, str, int]]:
    """Select up to limit recommendations with a 1-per-profile diversity pass then backfill."""
    matches = list(taste_matches)
    raw_candidates: list[tuple[str, float, str, int]] = []
    chosen_slugs: set[str] = set()

    def _eligible_for_profile(
        other_ratings: Mapping[str, Optional[float]],
    ) -> list[tuple[str, float]]:
        games = [
            (slug, rating)
            for slug, rating in other_ratings.items()
            if (
                rating is not None
                and rating >= min_rating
                and slug not in excluded_slugs
                and slug not in chosen_slugs
                and (pool_slugs is None or slug in pool_slugs)
            )
        ]
        games.sort(key=lambda item: item[1], reverse=True)
        return games

    # PASS 1: Strict diversity — at most 1 top recommendation per profile.
    for _, other_name, match_score, other_ratings in matches:
        match_pct = int(round(match_score))
        user_top_games = _eligible_for_profile(other_ratings)
        if user_top_games:
            best_slug, best_rating = user_top_games[0]
            raw_candidates.append((best_slug, best_rating, other_name, match_pct))
            chosen_slugs.add(best_slug)
            if len(raw_candidates) >= limit:
                break

    # PASS 2: Backfill remaining slots from highest taste matches in order.
    if len(raw_candidates) < limit:
        for _, other_name, match_score, other_ratings in matches:
            match_pct = int(round(match_score))
            for slug, rating in _eligible_for_profile(other_ratings):
                raw_candidates.append((slug, rating, other_name, match_pct))
                chosen_slugs.add(slug)
                if len(raw_candidates) >= limit:
                    break
            if len(raw_candidates) >= limit:
                break

    return raw_candidates


def compute_taste_twin_candidates(
    my_numeric_ratings: Mapping[str, float],
    other_profiles: Iterable[Mapping[str, Any]],
    all_other_ratings: Mapping[str, Mapping[str, Optional[float]]],
    *,
    min_rated_profiles_threshold: int = 5,
    min_common_ratings: int = 2,
    similarity_fn: Callable[..., tuple[float, Any, Any]] = calculate_similarity_raw,
) -> tuple[list[dict[str, Any]], set[str], bool]:
    """Score server profiles for /taste-twins and collect highlight slugs to resolve."""
    candidates: list[dict[str, Any]] = []
    slugs_to_lookup: set[str] = set()
    enough_rated_profiles = False
    my_slugs = set(my_numeric_ratings)

    for other in other_profiles:
        other_profile_key = other["profile_key"]
        other_username = other["display_username"]
        other_avatar = other["avatar_url"]
        other_ratings = numeric_ratings(all_other_ratings.get(other_profile_key, {}))
        if len(other_ratings) >= min_rated_profiles_threshold:
            enough_rated_profiles = True

        common_slugs = sorted(my_slugs & set(other_ratings))
        if len(common_slugs) < min_common_ratings:
            continue

        final_score, _, _ = similarity_fn(my_numeric_ratings, other_ratings)
        match_pct = int(round(final_score))

        shared_loves = [
            (slug, (my_numeric_ratings[slug] + other_ratings[slug]) / 2.0)
            for slug in common_slugs
            if my_numeric_ratings[slug] >= 3.5 and other_ratings[slug] >= 3.5
        ]
        shared_loves.sort(key=lambda item: item[1], reverse=True)
        top_fav_slug = shared_loves[0][0] if shared_loves else None

        disagreements = [
            (slug, abs(my_numeric_ratings[slug] - other_ratings[slug]))
            for slug in common_slugs
        ]
        disagreements.sort(key=lambda item: item[1], reverse=True)
        top_disagree_slug, max_delta = (
            (disagreements[0][0], disagreements[0][1]) if disagreements else (None, 0.0)
        )

        if top_fav_slug:
            slugs_to_lookup.add(top_fav_slug)
        if top_disagree_slug:
            slugs_to_lookup.add(top_disagree_slug)

        candidates.append({
            "profile_key": other_profile_key,
            "username": other_username,
            "avatar_url": other_avatar,
            "match_pct": match_pct,
            "shared_count": len(common_slugs),
            "top_fav_slug": top_fav_slug,
            "top_disagree_slug": top_disagree_slug,
            "delta": max_delta,
        })

    return candidates, slugs_to_lookup, enough_rated_profiles


def summarize_taste_twins(
    candidates: Iterable[dict[str, Any]],
    *,
    top_n: int = 3,
    nemesis_threshold: int = 65,
) -> tuple[list[dict[str, Any]], Optional[dict[str, Any]], float]:
    """Rank twin candidates and derive top twins, optional nemesis, and server average."""
    ranked = sorted(
        candidates,
        key=lambda item: (item["match_pct"], item["shared_count"]),
        reverse=True,
    )
    if not ranked:
        return [], None, 0.0
    twins = ranked[:top_n]
    nemesis = ranked[-1] if len(ranked) > 1 and ranked[-1]["match_pct"] < nemesis_threshold else None
    server_avg_match = sum(item["match_pct"] for item in ranked) / len(ranked)
    return twins, nemesis, server_avg_match


def build_comparison_highlights(
    ratings1: Mapping[str, float],
    ratings2: Mapping[str, float],
    cached_meta: Optional[Mapping[str, Any]] = None,
    *,
    fallback_titles: Optional[Mapping[str, Optional[str]]] = None,
    love_threshold: float = 4.0,
    disagree_threshold: float = 1.0,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Build sorted shared-love and disagreement highlights for two numeric rating dicts."""
    shared_loves: list[dict[str, Any]] = []
    disagreements: list[dict[str, Any]] = []

    for slug in sorted(set(ratings1) & set(ratings2)):
        r1 = ratings1.get(slug)
        r2 = ratings2.get(slug)
        if r1 is None or r2 is None:
            continue
        delta = abs(r1 - r2)
        title = resolve_display_title(slug, cached_meta, fallback_titles=fallback_titles)
        if r1 >= love_threshold and r2 >= love_threshold:
            shared_loves.append({"title": title, "r1": r1, "r2": r2, "avg": (r1 + r2) / 2})
        if delta >= disagree_threshold:
            disagreements.append({"title": title, "r1": r1, "r2": r2, "delta": delta})

    shared_loves.sort(key=lambda item: (-item["avg"], item["title"]))
    disagreements.sort(key=lambda item: (-item["delta"], item["title"]))
    return shared_loves, disagreements
