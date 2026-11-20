"""Cached games, profile ratings, library sync, and batch-sizing helpers for Database."""

from typing import Any, Iterable, List, Optional

from bot.db_stats import MAX_SQL_BIND_PARAMS, iter_sql_chunks

MAX_SQL_BATCH_BYTES = 56 * 1024

ROULETTE_POOL_STATUSES = {
    "both": ("backlog", "wishlist"),
    "backlog": ("backlog",),
    "wishlist": ("wishlist",),
}


def estimate_row_payload_bytes(row: tuple[Any, ...]) -> int:
    """Conservatively estimate JSON/SQL serialized byte cost for one multi-row VALUES tuple."""
    total = 16 + len(row) * 4
    for value in row:
        if value is None:
            total += 6
        elif isinstance(value, bool):
            total += 6
        elif isinstance(value, (int, float)):
            total += 24
        else:
            total += len(str(value).encode("utf-8")) + 8
    return total


def iter_bounded_row_batches(
    rows: list[tuple[Any, ...]],
    *,
    params_per_row: int,
    fixed_params: int = 0,
    fixed_sql_bytes: int = 512,
    max_bytes: int = MAX_SQL_BATCH_BYTES,
) -> Iterable[list[tuple[Any, ...]]]:
    """Yield row batches respecting both bind-parameter budget and serialized statement size."""
    max_rows_by_params = max(1, (MAX_SQL_BIND_PARAMS - fixed_params) // max(1, params_per_row))
    batch: list[tuple[Any, ...]] = []
    current_bytes = fixed_sql_bytes
    for row in rows:
        row_bytes = estimate_row_payload_bytes(row)
        if batch and (len(batch) >= max_rows_by_params or current_bytes + row_bytes > max_bytes):
            yield batch
            batch = []
            current_bytes = fixed_sql_bytes
        batch.append(row)
        current_bytes += row_bytes
    if batch:
        yield batch


async def upsert_game(
    db: Any,
    slug: str,
    title: str,
    cover_url: Optional[str] = None,
    release_year: Optional[int] = None,
    community_rating: Optional[float] = None,
) -> None:
    """Insert or update metadata for a game in cache."""
    from bot.ui.helpers import clean_image_url

    clean_cover = clean_image_url(cover_url)
    query = """
        INSERT INTO cached_games (slug, title, cover_url, release_year, community_rating)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(slug) DO UPDATE SET
            title = excluded.title,
            cover_url = COALESCE(excluded.cover_url, cached_games.cover_url),
            release_year = COALESCE(excluded.release_year, cached_games.release_year),
            community_rating = COALESCE(excluded.community_rating, cached_games.community_rating);
    """
    await db._execute(query, (slug, title, clean_cover, release_year, community_rating))


async def upsert_user_rating(
    db: Any,
    discord_id: int,
    game_slug: str,
    rating: Optional[float],
    status: str,
    is_liked: bool = False,
) -> None:
    """Retained Discord-ID compatibility shim writing through linked_users to shared profile_ratings."""
    query = """
        INSERT INTO profile_ratings (profile_key, game_slug, rating, status, is_liked, updated_at)
        SELECT profile_key, ?, ?, ?, ?, CURRENT_TIMESTAMP
        FROM linked_users WHERE discord_id = ?
        ON CONFLICT(profile_key, game_slug) DO UPDATE SET
            rating = excluded.rating,
            status = excluded.status,
            is_liked = excluded.is_liked,
            updated_at = CURRENT_TIMESTAMP;
    """
    await db._execute(query, (game_slug, rating, status, is_liked, discord_id))


async def get_server_ratings_for_game(
    db: Any, guild_member_ids: List[int], game_slug: str
) -> List[Any]:
    """Fetch one rating/status row per Backloggd profile connected in this guild."""
    if not guild_member_ids:
        return []
    profiles = await db.get_guild_linked_profiles(guild_member_ids)
    profile_keys = [row["profile_key"] for row in profiles]
    if not profile_keys:
        return []
    rows: List[Any] = []
    for chunk in iter_sql_chunks(profile_keys, fixed_params=1):
        placeholders = ",".join("?" for _ in chunk)
        query = f"""
            SELECT r.profile_key, p.display_username AS backloggd_username,
                   p.avatar_url, r.rating, r.status, r.is_liked
            FROM profile_ratings r
            JOIN profiles p ON p.profile_key = r.profile_key
            WHERE r.profile_key IN ({placeholders}) AND r.game_slug = ?;
        """
        rows.extend(await db._fetchall(query, [*chunk, game_slug]))
    rows.sort(
        key=lambda r: (
            r["rating"] is None,
            -(r["rating"] if r["rating"] is not None else 0.0),
            r["backloggd_username"],
        )
    )
    return rows


async def get_user_backlog(db: Any, discord_id: int, pool: str = "both") -> List[Any]:
    """Retrieve unrated backlog and/or wishlist games for a user."""
    if not isinstance(pool, str):
        raise ValueError(f"Unsupported roulette pool: {pool!r}")
    statuses = ROULETTE_POOL_STATUSES.get(pool.strip().lower())
    if statuses is None:
        raise ValueError(f"Unsupported roulette pool: {pool!r}")
    placeholders = ", ".join("?" for _ in statuses)
    query = f"""
        SELECT g.slug, g.title, g.cover_url, g.release_year, g.community_rating, r.rating, r.is_liked
        FROM profile_ratings r
        JOIN cached_games g ON r.game_slug = g.slug
        WHERE r.profile_key = (
            SELECT profile_key FROM linked_users WHERE discord_id = ?
        )
          AND r.status IN ({placeholders})
          AND r.rating IS NULL;
    """
    return await db._fetchall(query, (discord_id, *statuses))


async def get_profile_backlog(db: Any, profile_key: str, pool: str = "both") -> List[Any]:
    """Retrieve unrated backlog and/or wishlist games for a canonical Backloggd profile key."""
    if not isinstance(pool, str):
        raise ValueError(f"Unsupported roulette pool: {pool!r}")
    statuses = ROULETTE_POOL_STATUSES.get(pool.strip().lower())
    if statuses is None:
        raise ValueError(f"Unsupported roulette pool: {pool!r}")
    normalized_key = str(profile_key).strip().lower()
    if not normalized_key:
        return []
    placeholders = ", ".join("?" for _ in statuses)
    query = f"""
        SELECT g.slug, g.title, g.cover_url, g.release_year, g.community_rating, r.rating, r.is_liked
        FROM profile_ratings r
        JOIN cached_games g ON r.game_slug = g.slug
        WHERE r.profile_key = ?
          AND r.status IN ({placeholders})
          AND r.rating IS NULL;
    """
    return await db._fetchall(query, (normalized_key, *statuses))


async def replace_user_library(
    db: Any,
    discord_id: int,
    expected_username: str,
    games: Iterable[Any],
    *,
    synced_at: Optional[Any] = None,
    clear_pending_token: Optional[str] = None,
) -> bool:
    """Retained Discord-ID fallback delegating to replace_profile_library(..., require_linked=True)."""
    return await db.replace_profile_library(
        expected_username,
        games,
        synced_at=synced_at,
        require_linked=True,
        clear_pending_token=clear_pending_token,
    )


async def replace_profile_library(
    db: Any,
    username: str,
    games: Iterable[Any],
    *,
    synced_at: Optional[Any] = None,
    require_linked: bool = True,
    clear_pending_token: Optional[str] = None,
) -> bool:
    """Atomically replace the shared profile library and record sync completion."""
    from bot.ui.helpers import clean_image_url

    profile_key = username.strip().lower()
    cached_game_rows: list[tuple[Any, ...]] = []
    rating_rows: list[tuple[Any, ...]] = []
    sqlite_rating_rows: list[tuple[Any, ...]] = []
    for game in games:
        slug = game.slug
        status_val = game.status.value if hasattr(game.status, "value") else str(game.status)
        rating_val = getattr(game, "rating", None)
        liked_val = bool(getattr(game, "is_liked", False))
        cached_game_rows.append((
            slug,
            game.title,
            clean_image_url(getattr(game, "cover_url", None)),
            getattr(game, "release_year", None),
            getattr(game, "community_rating", None),
        ))
        rating_rows.append((slug, rating_val, status_val, liked_val))
        sqlite_rating_rows.append((profile_key, slug, rating_val, status_val, liked_val))

    synced_sql = db._format_utc_sql_timestamp(synced_at)
    ownership_sql = "SELECT 1 FROM linked_users WHERE profile_key = ? LIMIT 1"

    if db.is_turso:
        statements = []
        if require_linked:
            statements.append((ownership_sql, (profile_key,)))
            guard = f"AND EXISTS ({ownership_sql})"
            profile_args = (profile_key, profile_key)
        else:
            guard = ""
            profile_args = (profile_key,)
        if synced_sql is not None:
            statements.append((
                f"""UPDATE profiles
                    SET last_synced_at = ?, last_sync_attempt_at = ?, last_sync_error = NULL
                    WHERE profile_key = ? {guard}""",
                (synced_sql, synced_sql, *profile_args),
            ))
            statements.append((
                f"""UPDATE linked_users SET last_synced_at = ?
                    WHERE profile_key = ? {guard}""",
                (synced_sql, *profile_args),
            ))
        else:
            statements.append((
                f"""UPDATE profiles
                    SET last_synced_at = CURRENT_TIMESTAMP, last_sync_attempt_at = CURRENT_TIMESTAMP, last_sync_error = NULL
                    WHERE profile_key = ? {guard}""",
                profile_args,
            ))
            statements.append((
                f"""UPDATE linked_users SET last_synced_at = CURRENT_TIMESTAMP
                    WHERE profile_key = ? {guard}""",
                profile_args,
            ))

        for batch in iter_bounded_row_batches(
            cached_game_rows,
            params_per_row=5,
            fixed_params=1 if require_linked else 0,
            fixed_sql_bytes=480,
        ):
            values_sql = ", ".join(["(?, ?, ?, ?, ?)"] * len(batch))
            flat_params = [value for row in batch for value in row]
            if require_linked:
                statements.append((f"""
                    INSERT INTO cached_games (slug, title, cover_url, release_year, community_rating)
                    SELECT v.column1, v.column2, v.column3, v.column4, v.column5
                    FROM (VALUES {values_sql}) AS v
                    WHERE EXISTS ({ownership_sql})
                    ON CONFLICT(slug) DO UPDATE SET
                        title = excluded.title,
                        cover_url = COALESCE(excluded.cover_url, cached_games.cover_url),
                        release_year = COALESCE(excluded.release_year, cached_games.release_year),
                        community_rating = COALESCE(excluded.community_rating, cached_games.community_rating)
                """, (*flat_params, profile_key)))
            else:
                statements.append((f"""
                    INSERT INTO cached_games (slug, title, cover_url, release_year, community_rating)
                    VALUES {values_sql}
                    ON CONFLICT(slug) DO UPDATE SET
                        title = excluded.title,
                        cover_url = COALESCE(excluded.cover_url, cached_games.cover_url),
                        release_year = COALESCE(excluded.release_year, cached_games.release_year),
                        community_rating = COALESCE(excluded.community_rating, cached_games.community_rating)
                """, tuple(flat_params)))

        statements.append((
            f"DELETE FROM profile_ratings WHERE profile_key = ? {guard}",
            profile_args,
        ))

        where_clause = f"WHERE EXISTS ({ownership_sql})" if require_linked else "WHERE 1 = 1"
        fixed_rating_params = 2 if require_linked else 1
        for batch in iter_bounded_row_batches(
            rating_rows,
            params_per_row=4,
            fixed_params=fixed_rating_params,
            fixed_sql_bytes=480,
        ):
            values_sql = ", ".join(["(?, ?, ?, ?)"] * len(batch))
            flat_params = [value for row in batch for value in row]
            insert_args = (profile_key, *flat_params)
            if require_linked:
                insert_args = (*insert_args, profile_key)
            statements.append((f"""
                INSERT INTO profile_ratings
                    (profile_key, game_slug, rating, status, is_liked, updated_at)
                SELECT ?, v.column1, v.column2, v.column3, v.column4, CURRENT_TIMESTAMP
                FROM (VALUES {values_sql}) AS v
                {where_clause}
                ON CONFLICT(profile_key, game_slug) DO UPDATE SET
                    rating = excluded.rating,
                    status = excluded.status,
                    is_liked = excluded.is_liked,
                    updated_at = CURRENT_TIMESTAMP
            """, insert_args))

        if clear_pending_token is not None:
            clear_args = (
                profile_key,
                db._LIBRARY_SYNC_SOURCE,
                db._PENDING_REFRESH_KEY,
                clear_pending_token,
            )
            if require_linked:
                clear_args = (*clear_args, profile_key)
            statements.append((
                f"""DELETE FROM profile_activity_observations
                    WHERE profile_key = ? AND source = ? AND observation_key = ?
                      AND fingerprint = ? {guard}""",
                clear_args,
            ))
        results = await db._execute_batch_results(statements)
        if require_linked:
            return bool(results and results[0].rows)
        return True

    async with db._transaction() as tx:
        if require_linked:
            link = await tx.fetchone(ownership_sql, (profile_key,))
            if not link:
                return False

        if synced_sql is not None:
            await tx.execute(
                """UPDATE profiles
                   SET last_synced_at = ?, last_sync_attempt_at = ?, last_sync_error = NULL
                   WHERE profile_key = ?""",
                (synced_sql, synced_sql, profile_key),
            )
            await tx.execute(
                "UPDATE linked_users SET last_synced_at = ? WHERE profile_key = ?",
                (synced_sql, profile_key),
            )
        else:
            await tx.execute(
                """UPDATE profiles
                   SET last_synced_at = CURRENT_TIMESTAMP,
                       last_sync_attempt_at = CURRENT_TIMESTAMP,
                       last_sync_error = NULL
                   WHERE profile_key = ?""",
                (profile_key,),
            )
            await tx.execute(
                "UPDATE linked_users SET last_synced_at = CURRENT_TIMESTAMP WHERE profile_key = ?",
                (profile_key,),
            )
        if cached_game_rows:
            await tx.executemany("""
                INSERT INTO cached_games (slug, title, cover_url, release_year, community_rating)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(slug) DO UPDATE SET
                    title = excluded.title,
                    cover_url = COALESCE(excluded.cover_url, cached_games.cover_url),
                    release_year = COALESCE(excluded.release_year, cached_games.release_year),
                    community_rating = COALESCE(excluded.community_rating, cached_games.community_rating)
            """, cached_game_rows)
        await tx.execute(
            "DELETE FROM profile_ratings WHERE profile_key = ?;", (profile_key,)
        )
        if sqlite_rating_rows:
            await tx.executemany("""
                INSERT INTO profile_ratings
                    (profile_key, game_slug, rating, status, is_liked, updated_at)
                VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                ON CONFLICT(profile_key, game_slug) DO UPDATE SET
                    rating = excluded.rating,
                    status = excluded.status,
                    is_liked = excluded.is_liked,
                    updated_at = CURRENT_TIMESTAMP
            """, sqlite_rating_rows)
        if clear_pending_token is not None:
            await tx.execute(
                """DELETE FROM profile_activity_observations
                   WHERE profile_key = ? AND source = ? AND observation_key = ?
                     AND fingerprint = ?""",
                (
                    profile_key,
                    db._LIBRARY_SYNC_SOURCE,
                    db._PENDING_REFRESH_KEY,
                    clear_pending_token,
                ),
            )
        return True


async def record_profile_sync_failure(
    db: Any,
    username: str,
    error: str,
    *,
    attempted_at: Optional[Any] = None,
) -> None:
    """Record a failed library refresh without erasing cached ratings or last_synced_at."""
    profile_key = username.strip().lower()
    attempted_sql = db._format_utc_sql_timestamp(attempted_at)
    error_text = str(error)[:500]
    if attempted_sql is not None:
        await db._execute(
            """UPDATE profiles
               SET last_sync_attempt_at = ?, last_sync_error = ?
               WHERE profile_key = ?""",
            (attempted_sql, error_text, profile_key),
        )
    else:
        await db._execute(
            """UPDATE profiles
               SET last_sync_attempt_at = CURRENT_TIMESTAMP, last_sync_error = ?
               WHERE profile_key = ?""",
            (error_text, profile_key),
        )


async def update_profile_played_count(
    db: Any,
    username: str,
    played_count: int,
    *,
    observed_at: Optional[Any] = None,
) -> bool:
    """Persist an authoritative Played-tab count for a known profile."""
    if not isinstance(played_count, int) or isinstance(played_count, bool) or played_count < 0:
        return False
    profile_key = str(username or "").strip().lower()
    if not profile_key:
        return False
    updated = await db.update_profiles_played_counts(
        [(profile_key, played_count)],
        observed_at=observed_at,
    )
    return updated > 0


async def update_profiles_played_counts(
    db: Any,
    counts: Iterable[tuple[str, int]],
    *,
    observed_at: Optional[Any] = None,
) -> int:
    """Persist authoritative Played-tab counts for known profiles atomically."""
    valid_updates: dict[str, int] = {}
    for raw_key, raw_count in counts:
        key = str(raw_key or "").strip().lower()
        if not key:
            continue
        if isinstance(raw_count, int) and not isinstance(raw_count, bool) and raw_count >= 0:
            valid_updates[key] = raw_count
    if not valid_updates:
        return 0

    observed_sql = db._now_utc_sql_timestamp(observed_at)
    rows = [(count, observed_sql, key) for key, count in valid_updates.items()]
    sql = """UPDATE profiles
             SET played_count = ?, played_count_observed_at = ?
             WHERE profile_key = ?"""
    if db.is_turso:
        total_updated = 0
        for batch in iter_bounded_row_batches(
            rows,
            params_per_row=3,
            fixed_params=0,
            fixed_sql_bytes=128,
        ):
            results = await db._execute_batch_results([(sql, row) for row in batch])
            total_updated += sum(getattr(rs, "rows_affected", 0) or 0 for rs in results)
        return total_updated

    async with db._transaction() as tx:
        total_updated = 0
        for row in rows:
            total_updated += await tx.execute(sql, row)
        return total_updated


async def get_multi_profile_ratings_dict(
    db: Any,
    profile_keys: List[str],
) -> dict[str, dict[str, Optional[float]]]:
    """Return one cached rating map per normalized Backloggd profile."""
    normalized_keys = list(dict.fromkeys(key.strip().lower() for key in profile_keys if key.strip()))
    result: dict[str, dict[str, Optional[float]]] = {key: {} for key in normalized_keys}
    for key_chunk in iter_sql_chunks(normalized_keys):
        placeholders = ",".join("?" for _ in key_chunk)
        rows = await db._fetchall(
            f"""SELECT profile_key, game_slug, rating
                FROM profile_ratings WHERE profile_key IN ({placeholders})""",
            key_chunk,
        )
        for row in rows:
            result[row["profile_key"]][row["game_slug"]] = row["rating"]
    return result


async def get_games_by_slugs(db: Any, slugs: List[str]) -> dict[str, Any]:
    """Fetch cached game records by slugs in bounded batch queries."""
    clean_slugs = list(dict.fromkeys(slug for slug in slugs if slug))
    if not clean_slugs:
        return {}
    result: dict[str, Any] = {}
    for chunk in iter_sql_chunks(clean_slugs):
        placeholders = ",".join("?" for _ in chunk)
        query = (
            "SELECT slug, title, cover_url, release_year, community_rating "
            f"FROM cached_games WHERE slug IN ({placeholders});"
        )
        for row in await db._fetchall(query, chunk):
            result[row["slug"]] = row
    return result
