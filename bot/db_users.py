"""User account linking and profile metadata queries for Database."""

from typing import Any, Iterable, List, Optional

from bot.db_stats import iter_sql_chunks


async def link_user(
    db: Any,
    discord_id: int,
    username: str,
    avatar_url: Optional[str] = None,
) -> bool:
    """Connect a Discord member to a normalized shared profile, maintaining feed continuity."""
    from bot.ui.helpers import clean_image_url

    clean_avatar = clean_image_url(avatar_url)
    display_username = username.strip()
    profile_key = display_username.lower()
    async with db._transaction() as tx:
        existing = await tx.fetchone(
            "SELECT profile_key FROM linked_users WHERE discord_id = ? LIMIT 1;",
            (discord_id,),
        )
        old_profile_key = existing["profile_key"] if existing else None
        if old_profile_key != profile_key:
            await db._prepare_relinked_user_preferences_tx(tx, discord_id, profile_key)
        await tx.execute(
            """
            INSERT INTO profiles (profile_key, display_username, avatar_url)
            VALUES (?, ?, ?)
            ON CONFLICT(profile_key) DO UPDATE SET
                display_username = excluded.display_username,
                avatar_url = COALESCE(excluded.avatar_url, profiles.avatar_url)
            """,
            (profile_key, display_username, clean_avatar),
        )
        await tx.execute(
            """
            INSERT INTO linked_users (discord_id, profile_key, backloggd_username, avatar_url, last_synced_at)
            VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT(discord_id) DO UPDATE SET
                profile_key = excluded.profile_key,
                backloggd_username = excluded.backloggd_username,
                avatar_url = COALESCE(excluded.avatar_url, linked_users.avatar_url),
                last_synced_at = CURRENT_TIMESTAMP;
            """,
            (discord_id, profile_key, display_username, clean_avatar),
        )
        if old_profile_key is not None and old_profile_key != profile_key:
            active_guilds = await tx.fetchall(
                "SELECT guild_id FROM guild_profile_feed_activity WHERE profile_key = ? AND active = 1",
                (old_profile_key,),
            )
            for active_row in active_guilds:
                await db._reconcile_removed_profile_source_tx(
                    tx,
                    active_row["guild_id"],
                    old_profile_key,
                    present_member_ids=None,
                    removed_discord_id=discord_id,
                )
    return True


async def link_user_if_unlinked(
    db: Any,
    discord_id: int,
    username: str,
    avatar_url: Optional[str] = None,
) -> bool:
    """Create a member link only if no link exists, without replacing a concurrent winner."""
    from bot.ui.helpers import clean_image_url

    clean_avatar = clean_image_url(avatar_url)
    display_username = username.strip()
    profile_key = display_username.lower()
    async with db._transaction() as tx:
        existing = await tx.fetchone(
            "SELECT 1 FROM linked_users WHERE discord_id = ? LIMIT 1;", (discord_id,)
        )
        if existing is not None:
            return False

        await db._prepare_relinked_user_preferences_tx(tx, discord_id, profile_key)
        await tx.execute(
            """
            INSERT INTO profiles (profile_key, display_username, avatar_url)
            VALUES (?, ?, ?)
            ON CONFLICT(profile_key) DO UPDATE SET
                display_username = excluded.display_username,
                avatar_url = COALESCE(excluded.avatar_url, profiles.avatar_url)
            """,
            (profile_key, display_username, clean_avatar),
        )
        inserted = await tx.execute(
            """
            INSERT INTO linked_users
                (discord_id, profile_key, backloggd_username, avatar_url, last_synced_at)
            VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT(discord_id) DO NOTHING;
            """,
            (discord_id, profile_key, display_username, clean_avatar),
        )
        return inserted > 0


async def unlink_user(db: Any, discord_id: int) -> bool:
    """Remove this member's connection and deactivate guild feeds with no remaining source."""
    async with db._transaction() as tx:
        row = await tx.fetchone(
            "SELECT profile_key FROM linked_users WHERE discord_id = ?;", (discord_id,)
        )
        if not row:
            return False
        profile_key = row["profile_key"]
        deleted = await tx.execute(
            "DELETE FROM linked_users WHERE discord_id = ?;", (discord_id,)
        )
        if deleted <= 0:
            return False
        active_guilds = await tx.fetchall(
            "SELECT guild_id FROM guild_profile_feed_activity WHERE profile_key = ? AND active = 1",
            (profile_key,),
        )
        for active_row in active_guilds:
            await db._reconcile_removed_profile_source_tx(
                tx,
                active_row["guild_id"],
                profile_key,
                present_member_ids=None,
                removed_discord_id=discord_id,
            )
        return True


async def get_linked_user(db: Any, discord_id: int) -> Optional[Any]:
    """Fetch linked user record by Discord ID."""
    return await db._fetchone(
        "SELECT discord_id, profile_key, backloggd_username, avatar_url, last_synced_at FROM linked_users WHERE discord_id = ?;",
        (discord_id,),
    )


async def get_profile_connections(
    db: Any,
    username: str,
    member_ids: List[int],
    exclude_discord_id: Optional[int] = None,
) -> List[Any]:
    """List only this profile's links among explicitly supplied guild members."""
    members = list(dict.fromkeys(int(value) for value in member_ids))
    if not members:
        return []
    excluded = exclude_discord_id if exclude_discord_id is not None else -1
    connections = []
    clean_username = username.strip()
    for member_chunk in iter_sql_chunks(members, fixed_params=2):
        placeholders = ",".join("?" for _ in member_chunk)
        query = f"""
            SELECT discord_id FROM linked_users
            WHERE profile_key = LOWER(TRIM(?))
              AND discord_id IN ({placeholders})
              AND discord_id <> ?
            ORDER BY discord_id
        """
        params = [clean_username, *member_chunk, excluded]
        connections.extend(await db._fetchall(query, params))
    connections.sort(key=lambda row: row["discord_id"])
    return connections


async def get_profile_sync_info(db: Any, username: str) -> Optional[dict[str, Any]]:
    """Return the profile's last complete sync time, sync health, cached count, and pending token."""
    row = await db._fetchone("""
        SELECT profiles.profile_key, profiles.display_username,
               profiles.last_synced_at, profiles.last_sync_attempt_at,
               profiles.last_sync_error,
               COUNT(profile_ratings.game_slug) AS game_count,
               pending.fingerprint AS pending_refresh_token
        FROM profiles
        LEFT JOIN profile_ratings ON profile_ratings.profile_key = profiles.profile_key
        LEFT JOIN profile_activity_observations AS pending
               ON pending.profile_key = profiles.profile_key
              AND pending.source = ?
              AND pending.observation_key = ?
        WHERE profiles.profile_key = LOWER(TRIM(?))
        GROUP BY profiles.profile_key, profiles.display_username,
                 profiles.last_synced_at, profiles.last_sync_attempt_at,
                 profiles.last_sync_error, pending.fingerprint
    """, (db._LIBRARY_SYNC_SOURCE, db._PENDING_REFRESH_KEY, username.strip()))
    return dict(row) if row is not None else None


async def get_profiles_sync_info(db: Any, profile_keys: Iterable[str]) -> dict[str, dict[str, Any]]:
    """Return sync freshness metadata keyed by normalized profile_key."""
    normalized = list(dict.fromkeys(key.strip().lower() for key in profile_keys if key and key.strip()))
    result: dict[str, dict[str, Any]] = {}
    for chunk in iter_sql_chunks(normalized, fixed_params=2):
        placeholders = ",".join("?" for _ in chunk)
        rows = await db._fetchall(f"""
            SELECT profiles.profile_key, profiles.display_username,
                   profiles.last_synced_at, profiles.last_sync_attempt_at,
                   profiles.last_sync_error,
                   COUNT(profile_ratings.game_slug) AS game_count,
                   pending.fingerprint AS pending_refresh_token
            FROM profiles
            LEFT JOIN profile_ratings ON profile_ratings.profile_key = profiles.profile_key
            LEFT JOIN profile_activity_observations AS pending
                   ON pending.profile_key = profiles.profile_key
                  AND pending.source = ?
                  AND pending.observation_key = ?
            WHERE profiles.profile_key IN ({placeholders})
            GROUP BY profiles.profile_key, profiles.display_username,
                     profiles.last_synced_at, profiles.last_sync_attempt_at,
                     profiles.last_sync_error, pending.fingerprint
        """, [db._LIBRARY_SYNC_SOURCE, db._PENDING_REFRESH_KEY, *chunk])
        for row in rows:
            result[row["profile_key"]] = dict(row)
    return result


async def get_profiles_refresh_pending_tokens(
    db: Any, profile_keys: Iterable[str],
) -> dict[str, Optional[str]]:
    """Return active pending refresh tokens keyed by normalized profile_key in bounded chunks."""
    normalized = list(dict.fromkeys(key.strip().lower() for key in profile_keys if key and key.strip()))
    result: dict[str, Optional[str]] = {key: None for key in normalized}
    for chunk in iter_sql_chunks(normalized, fixed_params=2):
        placeholders = ",".join("?" for _ in chunk)
        rows = await db._fetchall(
            f"""SELECT profile_key, fingerprint
                FROM profile_activity_observations
                WHERE source = ? AND observation_key = ?
                  AND profile_key IN ({placeholders})""",
            [db._LIBRARY_SYNC_SOURCE, db._PENDING_REFRESH_KEY, *chunk],
        )
        for row in rows:
            result[row["profile_key"]] = row["fingerprint"]
    return result


async def is_profile_library_subscribed(db: Any, username: str) -> bool:
    """Return true when at least one Discord link consumes this profile's library."""
    row = await db._fetchone(
        "SELECT 1 FROM linked_users WHERE profile_key = LOWER(TRIM(?)) LIMIT 1",
        (username.strip(),),
    )
    return row is not None


async def get_profiles_library_subscription_map(
    db: Any, profile_keys: Iterable[str],
) -> dict[str, bool]:
    """Return linked-member subscription evidence for normalized profiles in bounded chunks."""
    normalized = list(dict.fromkeys(
        key.strip().lower() for key in profile_keys if key and key.strip()
    ))
    result = {key: False for key in normalized}
    for chunk in iter_sql_chunks(normalized):
        placeholders = ",".join("?" for _ in chunk)
        rows = await db._fetchall(
            f"""SELECT DISTINCT profile_key FROM linked_users
                WHERE profile_key IN ({placeholders})""",
            chunk,
        )
        for row in rows:
            result[row["profile_key"]] = True
    return result


async def get_profile(db: Any, username: str) -> Optional[Any]:
    """Fetch canonical profile data without attributing it to one Discord member."""
    return await db._fetchone("""
        SELECT profile_key, display_username, avatar_url, last_synced_at,
               last_sync_attempt_at, last_sync_error
        FROM profiles WHERE profile_key = LOWER(TRIM(?))
    """, (username.strip(),))


async def get_profile_ratings_dict(db: Any, username: str) -> dict[str, Optional[float]]:
    """Return the shared profile's slug-to-rating mapping."""
    rows = await db._fetchall("""
        SELECT game_slug, rating FROM profile_ratings
        WHERE profile_key = LOWER(TRIM(?))
    """, (username.strip(),))
    return {row["game_slug"]: row["rating"] for row in rows}


async def get_profile_library_entries(db: Any, username: str) -> dict[str, dict[str, Any]]:
    """Return the shared profile's slug-to-{rating, status} mapping."""
    rows = await db._fetchall("""
        SELECT game_slug, rating, status FROM profile_ratings
        WHERE profile_key = LOWER(TRIM(?))
    """, (username.strip(),))
    return {
        row["game_slug"]: {"rating": row["rating"], "status": row["status"]}
        for row in rows
    }


async def get_cached_profile_rating_evidence(
    db: Any, profile_key: str, game_slugs: Iterable[str]
) -> dict:
    """Read just the named games and their library snapshot timestamp."""
    result = {}
    for chunk in iter_sql_chunks(list(dict.fromkeys(game_slugs)), fixed_params=1):
        placeholders = ",".join("?" for _ in chunk)
        rows = await db._fetchall(
            f"""SELECT r.game_slug, r.rating, p.last_synced_at
                FROM profile_ratings AS r JOIN profiles AS p USING (profile_key)
                WHERE r.profile_key = ? AND r.game_slug IN ({placeholders})""",
            [profile_key, *chunk],
        )
        result.update((row["game_slug"], (row["last_synced_at"], row["rating"])) for row in rows)
    return result


async def get_profile_stats(db: Any, username: str) -> dict:
    """Fetch aggregate rating stats once for a Backloggd profile."""
    row = await db._fetchone("""
        SELECT COUNT(*) AS total_games,
               COUNT(rating) AS rated_count,
               AVG(rating) AS average_rating
        FROM profile_ratings
        WHERE profile_key = LOWER(TRIM(?))
    """, (username.strip(),))
    return {
        "total_games": row["total_games"] if row else 0,
        "rated_count": row["rated_count"] if row else 0,
        "average_rating": (
            round(row["average_rating"], 2)
            if row and row["average_rating"] is not None
            else None
        ),
    }


async def get_guild_linked_users(db: Any, member_ids: List[int]) -> List[Any]:
    """Fetch all linked users present in a specific guild."""
    members = list(dict.fromkeys(int(value) for value in member_ids))
    if not members:
        return []
    rows: List[Any] = []
    for chunk in iter_sql_chunks(members):
        placeholders = ",".join("?" for _ in chunk)
        query = f"SELECT discord_id, profile_key, backloggd_username, avatar_url FROM linked_users WHERE discord_id IN ({placeholders});"
        rows.extend(await db._fetchall(query, chunk))
    rows.sort(key=lambda row: row["discord_id"])
    return rows


async def get_guild_linked_profiles(
    db: Any,
    member_ids: List[int],
    exclude_profile_key: Optional[str] = None,
) -> List[dict[str, Any]]:
    """Fetch distinct linked profiles present among the supplied guild members."""
    members = list(dict.fromkeys(int(value) for value in member_ids))
    if not members:
        return []
    excluded = exclude_profile_key.strip().lower() if exclude_profile_key and exclude_profile_key.strip() else None
    exclusion = " AND p.profile_key <> ?" if excluded else ""
    fixed_params = 1 if excluded else 0
    profiles: dict[str, dict[str, Any]] = {}
    for chunk in iter_sql_chunks(members, fixed_params=fixed_params):
        placeholders = ",".join("?" for _ in chunk)
        params: list[Any] = list(chunk)
        if excluded:
            params.append(excluded)
        query = f"""
            SELECT DISTINCT p.profile_key, p.display_username, p.avatar_url
            FROM linked_users AS u
            JOIN profiles AS p ON p.profile_key = u.profile_key
            WHERE u.discord_id IN ({placeholders}){exclusion}
            ORDER BY p.profile_key
        """
        for row in await db._fetchall(query, params):
            profiles[row["profile_key"]] = dict(row)
    return [profiles[key] for key in sorted(profiles)]


async def get_guild_linked_profiles_with_sync_info(
    db: Any,
    member_ids: List[int],
    exclude_profile_key: Optional[str] = None,
) -> List[dict[str, Any]]:
    """Fetch distinct linked guild profiles with library sync timestamps, errors, and pending tokens."""
    members = list(dict.fromkeys(int(value) for value in member_ids))
    if not members:
        return []
    excluded = exclude_profile_key.strip().lower() if exclude_profile_key and exclude_profile_key.strip() else None
    exclusion = " AND p.profile_key <> ?" if excluded else ""
    fixed_params = 3 if excluded else 2
    profiles: dict[str, dict[str, Any]] = {}
    for chunk in iter_sql_chunks(members, fixed_params=fixed_params):
        placeholders = ",".join("?" for _ in chunk)
        params: list[Any] = [db._LIBRARY_SYNC_SOURCE, db._PENDING_REFRESH_KEY, *chunk]
        if excluded:
            params.append(excluded)
        query = f"""
            SELECT DISTINCT p.profile_key, p.display_username, p.avatar_url,
                   p.last_synced_at, p.last_sync_attempt_at, p.last_sync_error,
                   pending.fingerprint AS pending_refresh_token
            FROM linked_users AS u
            JOIN profiles AS p ON p.profile_key = u.profile_key
            LEFT JOIN profile_activity_observations AS pending
                   ON pending.profile_key = p.profile_key
                  AND pending.source = ?
                  AND pending.observation_key = ?
            WHERE u.discord_id IN ({placeholders}){exclusion}
            ORDER BY p.profile_key
        """
        for row in await db._fetchall(query, params):
            profiles[row["profile_key"]] = dict(row)
    return [profiles[key] for key in sorted(profiles)]
