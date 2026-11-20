"""Server leaderboard and community statistics queries for Database."""

import json
from datetime import datetime, timezone
from typing import Any, Iterable, List

MAX_SQL_BIND_PARAMS = 800


def iter_sql_chunks(items: list[Any], *, fixed_params: int = 0) -> Iterable[list[Any]]:
    """Yield non-empty slices sized so fixed_params + len(chunk) <= MAX_SQL_BIND_PARAMS."""
    chunk_size = max(1, MAX_SQL_BIND_PARAMS - fixed_params)
    for start in range(0, len(items), chunk_size):
        yield items[start:start + chunk_size]


async def get_guild_stats_context(db: Any, member_ids: List[int]) -> dict[str, Any]:
    """Resolve linked-member count, distinct profiles, and sync metadata in one pass."""
    members = list(dict.fromkeys(int(value) for value in member_ids))
    if not members:
        return {
            "linked_members": 0,
            "profile_keys": [],
            "profiles": [],
        }
    linked_discord_ids: set[int] = set()
    profiles: dict[str, dict[str, Any]] = {}
    for chunk in iter_sql_chunks(members, fixed_params=2):
        placeholders = ",".join("?" for _ in chunk)
        query = f"""
            SELECT u.discord_id,
                   p.profile_key, p.display_username, p.avatar_url,
                   p.last_synced_at, p.last_sync_attempt_at, p.last_sync_error,
                   p.played_count, p.played_count_observed_at,
                   pending.fingerprint AS pending_refresh_token
            FROM linked_users AS u
            JOIN profiles AS p ON p.profile_key = u.profile_key
            LEFT JOIN profile_activity_observations AS pending
                   ON pending.profile_key = p.profile_key
                  AND pending.source = ?
                  AND pending.observation_key = ?
            WHERE u.discord_id IN ({placeholders})
            ORDER BY p.profile_key, u.discord_id
        """
        for row in await db._fetchall(query, [db._LIBRARY_SYNC_SOURCE, db._PENDING_REFRESH_KEY, *chunk]):
            linked_discord_ids.add(int(row["discord_id"]))
            key = row["profile_key"]
            if key not in profiles:
                profiles[key] = {
                    "profile_key": key,
                    "display_username": row["display_username"],
                    "avatar_url": row["avatar_url"],
                    "last_synced_at": row["last_synced_at"],
                    "last_sync_attempt_at": row["last_sync_attempt_at"],
                    "last_sync_error": row["last_sync_error"],
                    "played_count": row["played_count"],
                    "played_count_observed_at": row["played_count_observed_at"],
                    "pending_refresh_token": row["pending_refresh_token"],
                }
    sorted_keys = sorted(profiles)
    return {
        "linked_members": len(linked_discord_ids),
        "profile_keys": sorted_keys,
        "profiles": [profiles[key] for key in sorted_keys],
    }


async def get_server_top_rated_for_profiles(
    db: Any, profile_keys: List[str], limit: int = 10, min_ratings: int = 2
) -> List[dict[str, Any]]:
    """Query distinct canonical profile keys; return plain result dictionaries."""
    if not profile_keys or limit <= 0:
        return []
    aggregates: dict[str, dict[str, Any]] = {}
    for chunk in iter_sql_chunks(profile_keys):
        placeholders = ",".join("?" for _ in chunk)
        query = f"""
            SELECT g.slug, g.title, g.cover_url, g.release_year,
                   SUM(r.rating) AS rating_sum,
                   COUNT(r.rating) AS rating_count
            FROM profile_ratings r
            JOIN cached_games g ON r.game_slug = g.slug
            WHERE r.profile_key IN ({placeholders}) AND r.rating IS NOT NULL
            GROUP BY g.slug;
        """
        for row in await db._fetchall(query, chunk):
            entry = aggregates.setdefault(
                row["slug"],
                {
                    "slug": row["slug"],
                    "title": row["title"],
                    "cover_url": row["cover_url"],
                    "release_year": row["release_year"],
                    "rating_sum": 0.0,
                    "rating_count": 0,
                },
            )
            entry["rating_sum"] += float(row["rating_sum"] or 0.0)
            entry["rating_count"] += int(row["rating_count"] or 0)
    results = []
    for entry in aggregates.values():
        count = entry["rating_count"]
        if count >= min_ratings and count > 0:
            results.append({
                "slug": entry["slug"],
                "title": entry["title"],
                "cover_url": entry["cover_url"],
                "release_year": entry["release_year"],
                "avg_rating": entry["rating_sum"] / count,
                "rating_count": count,
            })
    results.sort(key=lambda row: (-row["avg_rating"], -row["rating_count"], row["slug"]))
    return results[:limit]


async def get_server_most_played_for_profiles(
    db: Any, profile_keys: List[str], limit: int = 10
) -> List[dict[str, Any]]:
    """Query distinct canonical profile keys; return plain result dictionaries."""
    if not profile_keys or limit <= 0:
        return []
    aggregates: dict[str, dict[str, Any]] = {}
    for chunk in iter_sql_chunks(profile_keys):
        placeholders = ",".join("?" for _ in chunk)
        query = f"""
            SELECT g.slug, g.title, g.cover_url, g.release_year,
                   COUNT(r.profile_key) AS play_count,
                   SUM(r.rating) AS rating_sum,
                   COUNT(r.rating) AS rating_count
            FROM profile_ratings r
            JOIN cached_games g ON r.game_slug = g.slug
            WHERE r.profile_key IN ({placeholders}) AND r.status = 'played'
            GROUP BY g.slug;
        """
        for row in await db._fetchall(query, chunk):
            entry = aggregates.setdefault(
                row["slug"],
                {
                    "slug": row["slug"],
                    "title": row["title"],
                    "cover_url": row["cover_url"],
                    "release_year": row["release_year"],
                    "play_count": 0,
                    "rating_sum": 0.0,
                    "rating_count": 0,
                },
            )
            entry["play_count"] += int(row["play_count"] or 0)
            if row["rating_sum"] is not None:
                entry["rating_sum"] += float(row["rating_sum"])
            entry["rating_count"] += int(row["rating_count"] or 0)
    results = []
    for entry in aggregates.values():
        rating_count = entry["rating_count"]
        results.append({
            "slug": entry["slug"],
            "title": entry["title"],
            "cover_url": entry["cover_url"],
            "release_year": entry["release_year"],
            "play_count": entry["play_count"],
            "avg_rating": (entry["rating_sum"] / rating_count) if rating_count > 0 else None,
        })
    results.sort(
        key=lambda row: (
            -row["play_count"],
            row["avg_rating"] is None,
            -(row["avg_rating"] if row["avg_rating"] is not None else 0.0),
            row["slug"],
        )
    )
    return results[:limit]


async def get_server_shame_list_for_profiles(
    db: Any, profile_keys: List[str], limit: int = 10
) -> List[dict[str, Any]]:
    """Query distinct canonical profile keys; return plain result dictionaries."""
    if not profile_keys or limit <= 0:
        return []
    aggregates: dict[str, dict[str, Any]] = {}
    for chunk in iter_sql_chunks(profile_keys):
        placeholders = ",".join("?" for _ in chunk)
        query = f"""
            SELECT g.slug, g.title, g.cover_url, g.release_year,
                   COUNT(r.profile_key) AS backlog_count
            FROM profile_ratings r
            JOIN cached_games g ON r.game_slug = g.slug
            WHERE r.profile_key IN ({placeholders}) AND r.status = 'backlog'
            GROUP BY g.slug;
        """
        for row in await db._fetchall(query, chunk):
            entry = aggregates.setdefault(
                row["slug"],
                {
                    "slug": row["slug"],
                    "title": row["title"],
                    "cover_url": row["cover_url"],
                    "release_year": row["release_year"],
                    "backlog_count": 0,
                },
            )
            entry["backlog_count"] += int(row["backlog_count"] or 0)
    results = list(aggregates.values())
    results.sort(key=lambda row: (-row["backlog_count"], row["slug"]))
    return results[:limit]


async def get_server_polarizing_for_profiles(
    db: Any, profile_keys: List[str], limit: int = 10, min_ratings: int = 2
) -> List[dict[str, Any]]:
    """Query distinct canonical profile keys; return plain result dictionaries."""
    if not profile_keys or limit <= 0:
        return []
    aggregates: dict[str, dict[str, Any]] = {}
    for chunk in iter_sql_chunks(profile_keys):
        placeholders = ",".join("?" for _ in chunk)
        query = f"""
            SELECT g.slug, g.title, g.cover_url, g.release_year,
                   MAX(r.rating) AS max_rating,
                   MIN(r.rating) AS min_rating,
                   SUM(r.rating) AS rating_sum,
                   COUNT(r.rating) AS rating_count
            FROM profile_ratings r
            JOIN cached_games g ON r.game_slug = g.slug
            WHERE r.profile_key IN ({placeholders}) AND r.rating IS NOT NULL
            GROUP BY g.slug;
        """
        for row in await db._fetchall(query, chunk):
            row_max = float(row["max_rating"])
            row_min = float(row["min_rating"])
            entry = aggregates.get(row["slug"])
            if entry is None:
                aggregates[row["slug"]] = {
                    "slug": row["slug"],
                    "title": row["title"],
                    "cover_url": row["cover_url"],
                    "release_year": row["release_year"],
                    "max_rating": row_max,
                    "min_rating": row_min,
                    "rating_sum": float(row["rating_sum"] or 0.0),
                    "rating_count": int(row["rating_count"] or 0),
                }
            else:
                if row_max > entry["max_rating"]:
                    entry["max_rating"] = row_max
                if row_min < entry["min_rating"]:
                    entry["min_rating"] = row_min
                entry["rating_sum"] += float(row["rating_sum"] or 0.0)
                entry["rating_count"] += int(row["rating_count"] or 0)
    results = []
    for entry in aggregates.values():
        count = entry["rating_count"]
        delta = entry["max_rating"] - entry["min_rating"]
        if count >= min_ratings and delta > 0:
            results.append({
                "slug": entry["slug"],
                "title": entry["title"],
                "cover_url": entry["cover_url"],
                "release_year": entry["release_year"],
                "rating_delta": delta,
                "avg_rating": entry["rating_sum"] / count,
                "rating_count": count,
            })
    results.sort(key=lambda row: (-row["rating_delta"], -row["rating_count"], row["slug"]))
    return results[:limit]


async def get_server_hidden_gems_for_profiles(
    db: Any, profile_keys: List[str], limit: int = 10
) -> List[dict[str, Any]]:
    """Query distinct canonical profile keys; return plain result dictionaries."""
    if not profile_keys or limit <= 0:
        return []
    aggregates: dict[str, dict[str, Any]] = {}
    for chunk in iter_sql_chunks(profile_keys):
        placeholders = ",".join("?" for _ in chunk)
        query = f"""
            SELECT g.slug, g.title, g.cover_url, g.release_year,
                   SUM(r.rating) AS rating_sum,
                   COUNT(r.rating) AS rating_count
            FROM profile_ratings r
            JOIN cached_games g ON r.game_slug = g.slug
            WHERE r.profile_key IN ({placeholders}) AND r.rating IS NOT NULL
            GROUP BY g.slug;
        """
        for row in await db._fetchall(query, chunk):
            entry = aggregates.setdefault(
                row["slug"],
                {
                    "slug": row["slug"],
                    "title": row["title"],
                    "cover_url": row["cover_url"],
                    "release_year": row["release_year"],
                    "rating_sum": 0.0,
                    "rating_count": 0,
                },
            )
            entry["rating_sum"] += float(row["rating_sum"] or 0.0)
            entry["rating_count"] += int(row["rating_count"] or 0)
    results = []
    for entry in aggregates.values():
        count = entry["rating_count"]
        if 0 < count <= 2:
            avg_rating = entry["rating_sum"] / count
            if avg_rating >= 4.5:
                results.append({
                    "slug": entry["slug"],
                    "title": entry["title"],
                    "cover_url": entry["cover_url"],
                    "release_year": entry["release_year"],
                    "avg_rating": avg_rating,
                    "rating_count": count,
                })
    results.sort(key=lambda row: (-row["avg_rating"], row["rating_count"], row["slug"]))
    return results[:limit]


async def get_server_stats_for_profiles(
    db: Any,
    profile_keys: List[str],
    *,
    linked_members: int,
    include_library_row_counts: bool = True,
) -> dict:
    """Query distinct canonical profile keys; return plain result dictionaries."""
    profile_count = len(profile_keys)
    if not profile_keys:
        result = {
            "linked_members": linked_members,
            "profile_count": 0,
            "server_avg_rating": None,
            "top_game": None,
        }
        if include_library_row_counts:
            result["total_logged"] = 0
            result["total_played"] = 0
        return result

    total_logged = 0
    total_played = 0
    rating_sum = 0.0
    rating_count = 0
    played_counts: dict[str, dict[str, Any]] = {}

    for chunk in iter_sql_chunks(profile_keys):
        placeholders = ",".join("?" for _ in chunk)
        if include_library_row_counts:
            summary_sql = f"""
            SELECT COUNT(*) AS total_logged,
                   SUM(CASE WHEN status = 'played' THEN 1 ELSE 0 END) AS total_played,
                   SUM(rating) AS rating_sum,
                   COUNT(rating) AS rating_count
            FROM profile_ratings
            WHERE profile_key IN ({placeholders});
            """
        else:
            summary_sql = f"""
            SELECT SUM(rating) AS rating_sum,
                   COUNT(rating) AS rating_count
            FROM profile_ratings
            WHERE profile_key IN ({placeholders});
            """
        row = await db._fetchone(summary_sql, chunk)
        if row:
            if include_library_row_counts:
                total_logged += int(row["total_logged"] or 0)
                total_played += int(row["total_played"] or 0)
            if row["rating_sum"] is not None:
                rating_sum += float(row["rating_sum"])
            rating_count += int(row["rating_count"] or 0)

        top_rows = await db._fetchall(
            f"""
            SELECT g.slug, g.title, COUNT(r.profile_key) AS count
            FROM profile_ratings r
            JOIN cached_games g ON r.game_slug = g.slug
            WHERE r.profile_key IN ({placeholders}) AND r.status = 'played'
            GROUP BY g.slug;
            """,
            chunk,
        )
        for top_row in top_rows:
            entry = played_counts.setdefault(
                top_row["slug"],
                {"slug": top_row["slug"], "title": top_row["title"], "count": 0},
            )
            entry["count"] += int(top_row["count"] or 0)

    avg_rating = round(rating_sum / rating_count, 2) if rating_count > 0 else None
    top_game = None
    if played_counts:
        best = min(played_counts.values(), key=lambda item: (-item["count"], item["slug"]))
        top_game = f"{best['title']} ({best['count']} profile{'s' if best['count'] != 1 else ''})"

    result = {
        "linked_members": linked_members,
        "profile_count": profile_count,
        "server_avg_rating": avg_rating,
        "top_game": top_game,
    }
    if include_library_row_counts:
        result["total_logged"] = total_logged
        result["total_played"] = total_played
    return result


async def get_server_currently_playing_for_profiles(
    db: Any,
    profile_keys: List[str],
    *,
    limit: int = 50,
) -> List[dict[str, Any]]:
    """Return games marked 'playing' by distinct canonical profile keys, grouped by game."""
    if not profile_keys or limit <= 0:
        return []
    deduped_keys = list(dict.fromkeys(str(key).strip().lower() for key in profile_keys if str(key).strip()))
    if not deduped_keys:
        return []

    aggregates: dict[str, dict[str, Any]] = {}
    for chunk in iter_sql_chunks(deduped_keys):
        placeholders = ",".join("?" for _ in chunk)
        query = f"""
            SELECT g.slug, g.title, g.cover_url, g.release_year,
                   r.profile_key, p.display_username
            FROM profile_ratings r
            JOIN cached_games g ON r.game_slug = g.slug
            JOIN profiles p ON p.profile_key = r.profile_key
            WHERE r.profile_key IN ({placeholders})
              AND LOWER(r.status) = 'playing';
        """
        for row in await db._fetchall(query, chunk):
            slug = row["slug"]
            entry = aggregates.setdefault(
                slug,
                {
                    "slug": slug,
                    "title": row["title"],
                    "cover_url": row["cover_url"],
                    "release_year": row["release_year"],
                    "players_by_key": {},
                },
            )
            profile_key = str(row["profile_key"]).strip().lower()
            username = str(row["display_username"] or row["profile_key"])
            if profile_key not in entry["players_by_key"]:
                entry["players_by_key"][profile_key] = username

    results: list[dict[str, Any]] = []
    for entry in aggregates.values():
        players = sorted(
            entry["players_by_key"].values(),
            key=lambda name: (name.lower(), name),
        )
        results.append({
            "slug": entry["slug"],
            "title": entry["title"],
            "cover_url": entry["cover_url"],
            "release_year": entry["release_year"],
            "players": players,
            "player_count": len(players),
        })

    results.sort(key=lambda row: (-row["player_count"], str(row["title"] or "").lower(), row["slug"]))
    return results[:limit]


def _format_since_sql_timestamp(value: datetime | str) -> str:
    if isinstance(value, datetime):
        dt = value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)
        return dt.strftime("%Y-%m-%d %H:%M:%S")
    raw = str(value).strip()
    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        dt = parsed.replace(tzinfo=timezone.utc) if parsed.tzinfo is None else parsed.astimezone(timezone.utc)
        return dt.strftime("%Y-%m-%d %H:%M:%S")
    except ValueError:
        return raw


async def get_guild_activity_recap(
    db: Any,
    guild_id: int,
    *,
    since_utc: datetime | str,
) -> dict[str, Any]:
    """Summarize delivered feed announcements for a guild since a UTC timestamp."""
    since_sql = _format_since_sql_timestamp(since_utc)
    rows = await db._fetchall(
        """SELECT m.profile_key,
                  COALESCE(p.display_username, af.display_username, m.profile_key) AS display_username,
                  m.event_id,
                  m.message_id,
                  m.payload,
                  m.recorded_at
           FROM profile_feed_messages AS m
           LEFT JOIN profiles AS p ON p.profile_key = m.profile_key
           LEFT JOIN guild_admin_follows AS af
                  ON af.guild_id = m.guild_id AND af.profile_key = m.profile_key
           WHERE m.guild_id = ?
             AND m.delivery_state = 'sent'
             AND m.recorded_at IS NOT NULL
             AND m.recorded_at >= ?
           ORDER BY m.recorded_at DESC, m.event_id ASC;""",
        (int(guild_id), since_sql),
    )

    deduped: list[dict[str, Any]] = []
    seen_keys: set[tuple[Any, ...]] = set()
    for row in rows:
        message_id = row["message_id"]
        profile_key = str(row["profile_key"] or "").strip().lower()
        dedupe_key: tuple[Any, ...] = (
            ("msg", int(message_id))
            if message_id is not None
            else ("ev", profile_key, str(row["event_id"] or ""))
        )
        if dedupe_key in seen_keys:
            continue
        seen_keys.add(dedupe_key)
        deduped.append({
            "profile_key": profile_key,
            "username": str(row["display_username"] or profile_key).strip() or profile_key,
            "payload": row["payload"],
            "recorded_at": row["recorded_at"],
        })

    finished_count = 0
    started_count = 0
    rated_count = 0
    reviewed_count = 0
    top_rated: list[dict[str, Any]] = []
    recently_finished: list[dict[str, Any]] = []
    seen_finished: set[tuple[str, str]] = set()
    player_activity: dict[str, dict[str, Any]] = {}

    for item in deduped:
        profile_key = item["profile_key"]
        username = item["username"]
        player_entry = player_activity.setdefault(
            profile_key,
            {"profile_key": profile_key, "username": username, "count": 0},
        )
        player_entry["count"] += 1

        try:
            data = json.loads(item["payload"]) if item["payload"] else {}
        except (TypeError, ValueError):
            data = {}
        if not isinstance(data, dict):
            data = {}

        action = str(data.get("action_type") or "").strip().lower()
        raw_labels = data.get("session_labels")
        labels = tuple(raw_labels) if isinstance(raw_labels, (list, tuple)) else ()
        rating_raw = data.get("rating")
        rating: float | None = None
        if isinstance(rating_raw, (int, float)) and not isinstance(rating_raw, bool):
            rating = float(rating_raw)
        review_notes = str(data.get("review_notes") or "").strip()
        game_title = str(data.get("game_title") or "Unknown Game").strip() or "Unknown Game"
        game_slug = str(data.get("game_slug") or "").strip()

        is_finished = action == "completed" or "Finished" in labels
        is_started = action == "playing" or "Started" in labels
        has_review = action == "reviewed" or bool(review_notes)

        if is_finished:
            finished_count += 1
            fin_key = (profile_key, game_slug or game_title.lower())
            if fin_key not in seen_finished:
                seen_finished.add(fin_key)
                recently_finished.append({
                    "title": game_title,
                    "slug": game_slug,
                    "rating": rating,
                    "username": username,
                    "recorded_at": item["recorded_at"],
                })
        if is_started:
            started_count += 1
        if rating is not None:
            rated_count += 1
            top_rated.append({
                "title": game_title,
                "slug": game_slug,
                "rating": rating,
                "username": username,
                "recorded_at": item["recorded_at"],
            })
        if has_review:
            reviewed_count += 1

    top_rated.sort(
        key=lambda entry: (
            -entry["rating"],
            entry["title"].lower(),
            entry["slug"],
            entry["username"].lower(),
        )
    )
    most_active_players = sorted(
        player_activity.values(),
        key=lambda entry: (-entry["count"], entry["username"].lower(), entry["profile_key"]),
    )

    return {
        "total_announcements": len(deduped),
        "finished_count": finished_count,
        "started_count": started_count,
        "rated_count": rated_count,
        "reviewed_count": reviewed_count,
        "top_rated": top_rated[:5],
        "recently_finished": recently_finished[:5],
        "most_active_players": most_active_players[:5],
    }
