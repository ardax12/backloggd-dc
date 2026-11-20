"""Guild settings, cooldowns, feed filters, digests, preferences, and admin follows for Database."""

from typing import Any, Iterable, List, Optional

from bot.db_library import iter_bounded_row_batches
from bot.db_stats import iter_sql_chunks
from bot.feed_policy import (
    DEFAULT_GUILD_FEED_SETTINGS,
    FEED_FILTER_KEYS,
    GUILD_FEED_OPTION_KEYS,
)

GUILD_FEED_SETTING_KEYS = FEED_FILTER_KEYS + GUILD_FEED_OPTION_KEYS


async def set_guild_feed_preference(
    db: Any,
    guild_id: int,
    discord_id: int,
    enabled: bool,
    *,
    member_ids: Optional[Iterable[int]] = None,
) -> Optional[str]:
    """Set a member's server-local feed preference; return their profile key."""
    async with db._transaction() as tx:
        linked = await tx.fetchone(
            "SELECT profile_key FROM linked_users WHERE discord_id = ? LIMIT 1",
            (discord_id,),
        )
        if not linked:
            return None
        profile_key = linked["profile_key"]
        if enabled:
            await db._prepare_profile_source_addition_tx(
                tx, guild_id, profile_key, present_member_ids=member_ids,
            )
        await tx.execute(
            """INSERT INTO guild_feed_preferences (guild_id, discord_id, enabled, updated_at)
               VALUES (?, ?, ?, CURRENT_TIMESTAMP)
               ON CONFLICT(guild_id, discord_id) DO UPDATE SET
                   enabled = excluded.enabled, updated_at = CURRENT_TIMESTAMP""",
            (guild_id, discord_id, bool(enabled)),
        )
        return profile_key


async def get_guild_feed_preference(db: Any, guild_id: int, discord_id: int) -> bool:
    """Absent member preferences are disabled."""
    row = await db._fetchone(
        "SELECT enabled FROM guild_feed_preferences WHERE guild_id = ? AND discord_id = ?",
        (guild_id, discord_id),
    )
    return bool(row and row["enabled"])


async def get_guild_feed_profiles(
    db: Any, guild_id: int, member_ids: Iterable[int]
) -> List[Any]:
    """Retained member-only opt-in profile lookup isolating member preferences from admin follows."""
    members = list(dict.fromkeys(int(value) for value in member_ids))
    profiles = {}
    for chunk in iter_sql_chunks(members, fixed_params=1):
        placeholders = ",".join("?" for _ in chunk)
        rows = await db._fetchall(
            f"""SELECT DISTINCT p.profile_key, p.display_username, p.avatar_url
                FROM guild_feed_preferences AS pref
                JOIN linked_users AS u ON u.discord_id = pref.discord_id
                JOIN profiles AS p ON p.profile_key = u.profile_key
                WHERE pref.guild_id = ? AND pref.enabled = 1
                  AND pref.discord_id IN ({placeholders})
                ORDER BY p.profile_key""",
            [guild_id, *chunk],
        )
        profiles.update((row["profile_key"], row) for row in rows)
    return [profiles[key] for key in sorted(profiles)]


async def get_legacy_feed_guilds_pending(db: Any) -> List[int]:
    """Configured legacy guilds whose authoritative membership pass is pending."""
    rows = await db._fetchall(
        """SELECT legacy.guild_id
           FROM guild_feed_legacy_migrations AS legacy
           JOIN guild_settings AS settings ON settings.guild_id = legacy.guild_id
           WHERE legacy.completed = 0 ORDER BY legacy.guild_id"""
    )
    return [row["guild_id"] for row in rows]


async def complete_legacy_feed_migration(
    db: Any, guild_id: int, present_member_ids: Iterable[int]
) -> bool:
    """Enroll only migration-scope links present in a complete member snapshot."""
    members = list(dict.fromkeys(int(value) for value in present_member_ids))
    async with db._transaction() as tx:
        pending = await tx.fetchone(
            "SELECT completed FROM guild_feed_legacy_migrations WHERE guild_id = ?",
            (guild_id,),
        )
        if not pending or pending["completed"]:
            return False
        eligible_ids: list[int] = []
        for chunk in iter_sql_chunks(members, fixed_params=1):
            placeholders = ",".join("?" for _ in chunk)
            eligible = await tx.fetchall(
                f"""SELECT scope.discord_id
                    FROM guild_feed_legacy_scope AS scope
                    JOIN linked_users AS linked
                      ON linked.discord_id = scope.discord_id
                     AND linked.profile_key = scope.profile_key
                    WHERE scope.guild_id = ? AND scope.discord_id IN ({placeholders})""",
                [guild_id, *chunk],
            )
            eligible_ids.extend(int(row["discord_id"]) for row in eligible)
        pref_rows = [(guild_id, did) for did in dict.fromkeys(eligible_ids)]
        for batch in iter_bounded_row_batches(
            pref_rows,
            params_per_row=2,
            fixed_params=0,
            fixed_sql_bytes=128,
        ):
            values_sql = ", ".join(["(?, ?, 1)"] * len(batch))
            flat_params = [value for row in batch for value in row]
            await tx.execute(
                f"""INSERT OR IGNORE INTO guild_feed_preferences
                    (guild_id, discord_id, enabled) VALUES {values_sql}""",
                flat_params,
            )
        await tx.execute(
            """UPDATE guild_feed_legacy_migrations
               SET completed = 1, completed_at = CURRENT_TIMESTAMP
               WHERE guild_id = ? AND completed = 0""",
            (guild_id,),
        )
        return True


async def get_guild_profile_member_sources(
    db: Any, guild_id: int, profile_key: str, member_ids: Iterable[int]
) -> List[int]:
    """Return current member IDs that actively enroll the given profile."""
    members = list(dict.fromkeys(int(value) for value in member_ids))
    normalized_key = profile_key.strip().lower()
    matching = []
    for chunk in iter_sql_chunks(members, fixed_params=2):
        placeholders = ",".join("?" for _ in chunk)
        rows = await db._fetchall(
            f"""SELECT pref.discord_id FROM guild_feed_preferences AS pref
                JOIN linked_users AS linked ON linked.discord_id = pref.discord_id
                WHERE pref.guild_id = ? AND pref.enabled = 1
                  AND linked.profile_key = ?
                  AND pref.discord_id IN ({placeholders})
                ORDER BY pref.discord_id""",
            [guild_id, normalized_key, *chunk],
        )
        matching.extend(row["discord_id"] for row in rows)
    matching.sort()
    return matching


async def add_guild_admin_follow(
    db: Any,
    guild_id: int,
    username: str,
    avatar_url: Optional[str],
    added_by: int,
    *,
    member_ids: Optional[Iterable[int]] = None,
) -> bool:
    """Add an idempotent server follow to a validated public profile."""
    from bot.ui.helpers import clean_image_url

    display_username = username.strip()
    profile_key = display_username.lower()
    clean_avatar = clean_image_url(avatar_url)
    async with db._transaction() as tx:
        await db._prepare_profile_source_addition_tx(
            tx, guild_id, profile_key, present_member_ids=member_ids,
        )
        await tx.execute(
            """INSERT INTO profiles (profile_key, display_username, avatar_url)
               VALUES (?, ?, ?)
               ON CONFLICT(profile_key) DO UPDATE SET
                   display_username = excluded.display_username,
                   avatar_url = COALESCE(excluded.avatar_url, profiles.avatar_url)""",
            (profile_key, display_username, clean_avatar),
        )
        inserted = await tx.execute(
            """INSERT OR IGNORE INTO guild_admin_follows
               (guild_id, profile_key, display_username, avatar_url, added_by)
               VALUES (?, ?, ?, ?, ?)""",
            (guild_id, profile_key, display_username, clean_avatar, added_by),
        )
        return inserted > 0


async def remove_guild_admin_follow(db: Any, guild_id: int, username: str) -> bool:
    return await db._rowcount_execute(
        "DELETE FROM guild_admin_follows WHERE guild_id = ? AND profile_key = LOWER(TRIM(?))",
        (guild_id, username),
    ) > 0


async def has_guild_admin_follow(db: Any, guild_id: int, profile_key: str) -> bool:
    return await db._fetchone(
        "SELECT 1 FROM guild_admin_follows WHERE guild_id = ? AND profile_key = LOWER(TRIM(?))",
        (guild_id, profile_key),
    ) is not None


async def get_guild_admin_follows(db: Any, guild_id: int) -> List[Any]:
    return await db._fetchall(
        """SELECT profile_key, display_username, avatar_url, added_by, created_at
           FROM guild_admin_follows WHERE guild_id = ? ORDER BY profile_key""",
        (guild_id,),
    )


async def get_guild_feed_candidates(
    db: Any, guild_id: int, member_ids: Iterable[int]
) -> List[dict]:
    """Union current member enrollment and admin follows by public profile."""
    members = list(dict.fromkeys(int(value) for value in member_ids))
    candidates: dict[str, dict] = {}
    for chunk in iter_sql_chunks(members, fixed_params=1):
        placeholders = ",".join("?" for _ in chunk)
        rows = await db._fetchall(
            f"""SELECT p.profile_key, p.display_username, p.avatar_url, pref.discord_id
                FROM guild_feed_preferences AS pref
                JOIN linked_users AS linked ON linked.discord_id = pref.discord_id
                JOIN profiles AS p ON p.profile_key = linked.profile_key
                WHERE pref.guild_id = ? AND pref.enabled = 1
                  AND pref.discord_id IN ({placeholders})
                ORDER BY p.profile_key, pref.discord_id""",
            [guild_id, *chunk],
        )
        for row in rows:
            candidate = candidates.setdefault(row["profile_key"], {
                "profile_key": row["profile_key"],
                "display_username": row["display_username"],
                "avatar_url": row["avatar_url"],
                "member_ids": [],
                "admin": False,
            })
            candidate["member_ids"].append(row["discord_id"])
    for row in await db.get_guild_admin_follows(guild_id):
        candidate = candidates.setdefault(row["profile_key"], {
            "profile_key": row["profile_key"],
            "display_username": row["display_username"],
            "avatar_url": row["avatar_url"],
            "member_ids": [],
            "admin": False,
        })
        candidate["admin"] = True
        candidate["avatar_url"] = candidate["avatar_url"] or row["avatar_url"]
    for candidate in candidates.values():
        candidate["member_ids"].sort()
        member = bool(candidate["member_ids"])
        candidate["sources"] = "both" if member and candidate["admin"] else "member" if member else "admin"
    return [candidates[key] for key in sorted(candidates)]


async def set_feed_channel(db: Any, guild_id: int, channel_id: int) -> None:
    """Configure or update the feed channel for a guild."""
    query = """
        INSERT INTO guild_settings (guild_id, feed_channel_id)
        VALUES (?, ?)
        ON CONFLICT(guild_id) DO UPDATE SET feed_channel_id = excluded.feed_channel_id;
    """
    await db._execute(query, (guild_id, channel_id))


async def unset_feed_channel(db: Any, guild_id: int) -> bool:
    """Remove feed channel setting for a guild and pause subscriptions atomically."""
    pause_statements = [
        (
            """UPDATE guild_profile_feed_activity SET active = 0
               WHERE guild_id = ? AND active = 1""",
            (guild_id,),
        ),
        (
            "DELETE FROM profile_feed_sources WHERE guild_id = ?",
            (guild_id,),
        ),
        (
            "DELETE FROM guild_profile_feed_reconciliation WHERE guild_id = ?",
            (guild_id,),
        ),
        (
            "DELETE FROM guild_profile_feed_recipient_gaps WHERE guild_id = ?",
            (guild_id,),
        ),
        (
            """DELETE FROM profile_feed_decisions
               WHERE guild_id = ?
                 AND decision IN ('baseline', 'eligible')
                 AND event_id NOT IN (
                     SELECT event_id FROM profile_feed_messages
                     WHERE guild_id = ?
                       AND profile_key = profile_feed_decisions.profile_key
                       AND delivery_state IN ('sent', 'deleted', 'pending', 'unknown')
                 )""",
            (guild_id, guild_id),
        ),
    ]
    async with db._transaction() as tx:
        deleted = await tx.execute(
            "DELETE FROM guild_settings WHERE guild_id = ?;", (guild_id,)
        )
        if deleted > 0:
            for sql, args in pause_statements:
                await tx.execute(sql, args)
        return deleted > 0


async def get_feed_channel(db: Any, guild_id: int) -> Optional[int]:
    """Get designated feed channel ID for a guild."""
    row = await db._fetchone(
        "SELECT feed_channel_id FROM guild_settings WHERE guild_id = ?;", (guild_id,)
    )
    return row["feed_channel_id"] if row else None


async def get_all_guild_settings(db: Any) -> List[Any]:
    """Retrieve all guilds with a configured feed channel."""
    return await db._fetchall("SELECT guild_id, feed_channel_id FROM guild_settings;")


async def get_guild_command_cooldowns(db: Any, guild_id: int) -> dict[str, int]:
    """Return persisted per-command cooldown overrides (in seconds) for a guild."""
    rows = await db._fetchall(
        """SELECT command_name, cooldown_seconds
           FROM guild_command_cooldowns
           WHERE guild_id = ?;""",
        (guild_id,),
    )
    return {
        str(row["command_name"]): int(row["cooldown_seconds"])
        for row in rows
    }


async def set_guild_command_cooldown(
    db: Any, guild_id: int, command_name: str, seconds: int
) -> None:
    """Persist or update a per-guild cooldown override in seconds."""
    normalized_name = str(command_name).strip().lstrip("/").lower()
    if not normalized_name:
        raise ValueError("command_name must be a non-empty string")
    if isinstance(seconds, bool) or not isinstance(seconds, int) or seconds < 1:
        raise ValueError("seconds must be a positive integer")
    await db._execute(
        """INSERT INTO guild_command_cooldowns
               (guild_id, command_name, cooldown_seconds, updated_at)
           VALUES (?, ?, ?, CURRENT_TIMESTAMP)
           ON CONFLICT(guild_id, command_name) DO UPDATE SET
               cooldown_seconds = excluded.cooldown_seconds,
               updated_at = CURRENT_TIMESTAMP;""",
        (guild_id, normalized_name, seconds),
    )


async def reset_guild_command_cooldown(
    db: Any, guild_id: int, command_name: Optional[str] = None
) -> int:
    """Delete one or all cooldown overrides for a guild, returning deleted row count."""
    if command_name is None:
        return await db._rowcount_execute(
            "DELETE FROM guild_command_cooldowns WHERE guild_id = ?;",
            (guild_id,),
        )
    normalized_name = str(command_name).strip().lstrip("/").lower()
    if not normalized_name:
        raise ValueError("command_name must be a non-empty string")
    return await db._rowcount_execute(
        "DELETE FROM guild_command_cooldowns WHERE guild_id = ? AND command_name = ?;",
        (guild_id, normalized_name),
    )


async def get_guild_feed_filters(db: Any, guild_id: int) -> dict[str, bool]:
    """Return per-guild activity feed category filters and options."""
    row = await db._fetchone(
        """SELECT allow_started, allow_finished, allow_replays,
                  allow_ratings, allow_reviews, spoiler_reviews
           FROM guild_feed_filters
           WHERE guild_id = ?;""",
        (guild_id,),
    )
    if not row:
        return dict(DEFAULT_GUILD_FEED_SETTINGS)
    return {key: bool(row[key]) for key in GUILD_FEED_SETTING_KEYS}


async def get_all_guild_feed_filters(
    db: Any, guild_ids: Iterable[int]
) -> dict[int, dict[str, bool]]:
    """Bulk fetch feed category filters and options for multiple guilds within SQL bind budgets."""
    unique_ids = list(dict.fromkeys(int(gid) for gid in guild_ids))
    result: dict[int, dict[str, bool]] = {
        gid: dict(DEFAULT_GUILD_FEED_SETTINGS) for gid in unique_ids
    }
    if not unique_ids:
        return result
    for chunk in iter_sql_chunks(unique_ids, fixed_params=0):
        placeholders = ",".join("?" for _ in chunk)
        rows = await db._fetchall(
            f"""SELECT guild_id, allow_started, allow_finished, allow_replays,
                       allow_ratings, allow_reviews, spoiler_reviews
                FROM guild_feed_filters
                WHERE guild_id IN ({placeholders});""",
            chunk,
        )
        for row in rows:
            result[int(row["guild_id"])] = {
                key: bool(row[key]) for key in GUILD_FEED_SETTING_KEYS
            }
    return result


async def set_guild_feed_filters(
    db: Any,
    guild_id: int,
    *,
    allow_started: Optional[bool] = None,
    allow_finished: Optional[bool] = None,
    allow_replays: Optional[bool] = None,
    allow_ratings: Optional[bool] = None,
    allow_reviews: Optional[bool] = None,
    spoiler_reviews: Optional[bool] = None,
) -> dict[str, bool]:
    """Atomically update one or more per-guild feed category filters/options and return the active set."""
    overrides = {
        "allow_started": allow_started,
        "allow_finished": allow_finished,
        "allow_replays": allow_replays,
        "allow_ratings": allow_ratings,
        "allow_reviews": allow_reviews,
        "spoiler_reviews": spoiler_reviews,
    }
    for key, value in overrides.items():
        if value is not None and not isinstance(value, bool):
            raise ValueError(f"{key} must be a boolean or None")

    async with db._transaction() as tx:
        existing = await tx.fetchone(
            """SELECT allow_started, allow_finished, allow_replays,
                      allow_ratings, allow_reviews, spoiler_reviews
               FROM guild_feed_filters
               WHERE guild_id = ?;""",
            (guild_id,),
        )
        merged = (
            {key: bool(existing[key]) for key in GUILD_FEED_SETTING_KEYS}
            if existing
            else dict(DEFAULT_GUILD_FEED_SETTINGS)
        )
        for key, value in overrides.items():
            if value is not None:
                merged[key] = value
        await tx.execute(
            """INSERT INTO guild_feed_filters
                   (guild_id, allow_started, allow_finished, allow_replays,
                    allow_ratings, allow_reviews, spoiler_reviews, updated_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
               ON CONFLICT(guild_id) DO UPDATE SET
                   allow_started = excluded.allow_started,
                   allow_finished = excluded.allow_finished,
                   allow_replays = excluded.allow_replays,
                   allow_ratings = excluded.allow_ratings,
                   allow_reviews = excluded.allow_reviews,
                   spoiler_reviews = excluded.spoiler_reviews,
                   updated_at = CURRENT_TIMESTAMP;""",
            (
                guild_id,
                int(merged["allow_started"]),
                int(merged["allow_finished"]),
                int(merged["allow_replays"]),
                int(merged["allow_ratings"]),
                int(merged["allow_reviews"]),
                int(merged["spoiler_reviews"]),
            ),
        )
    return merged


async def set_guild_digest(
    db: Any,
    guild_id: int,
    channel_id: int,
    cadence: str,
    *,
    now: Optional[Any] = None,
) -> dict[str, Any]:
    """Enable or update a server's scheduled activity digest without triggering immediate delivery."""
    cadence_norm = str(cadence or "").strip().lower()
    if cadence_norm not in {"weekly", "monthly"}:
        raise ValueError(f"Unsupported digest cadence: {cadence!r}")
    seed_ts = db._now_utc_sql_timestamp(now)
    async with db._transaction() as tx:
        await tx.execute(
            """INSERT INTO guild_digest_settings
                   (guild_id, channel_id, cadence, last_sent_at, updated_at)
               VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
               ON CONFLICT(guild_id) DO UPDATE SET
                   channel_id = excluded.channel_id,
                   cadence = excluded.cadence,
                   last_sent_at = COALESCE(guild_digest_settings.last_sent_at, excluded.last_sent_at),
                   updated_at = CURRENT_TIMESTAMP;""",
            (int(guild_id), int(channel_id), cadence_norm, seed_ts),
        )
        row = await tx.fetchone(
            """SELECT guild_id, channel_id, cadence, last_sent_at, updated_at
               FROM guild_digest_settings
               WHERE guild_id = ?;""",
            (int(guild_id),),
        )
    return {
        "guild_id": int(row["guild_id"]),
        "channel_id": int(row["channel_id"]),
        "cadence": str(row["cadence"]),
        "last_sent_at": row["last_sent_at"],
        "updated_at": row["updated_at"],
    }


async def unset_guild_digest(db: Any, guild_id: int) -> bool:
    """Disable scheduled digests for a server. Return True if a row was deleted."""
    deleted = await db._rowcount_execute(
        "DELETE FROM guild_digest_settings WHERE guild_id = ?;",
        (int(guild_id),),
    )
    return deleted > 0


async def get_guild_digest(db: Any, guild_id: int) -> Optional[dict[str, Any]]:
    """Return a server's scheduled digest configuration, or None when disabled."""
    row = await db._fetchone(
        """SELECT guild_id, channel_id, cadence, last_sent_at, updated_at
           FROM guild_digest_settings
           WHERE guild_id = ?;""",
        (int(guild_id),),
    )
    if not row:
        return None
    return {
        "guild_id": int(row["guild_id"]),
        "channel_id": int(row["channel_id"]),
        "cadence": str(row["cadence"]),
        "last_sent_at": row["last_sent_at"],
        "updated_at": row["updated_at"],
    }


async def get_all_guild_digests(db: Any) -> List[dict[str, Any]]:
    """Return all configured server digest schedules."""
    rows = await db._fetchall(
        """SELECT guild_id, channel_id, cadence, last_sent_at, updated_at
           FROM guild_digest_settings
           ORDER BY guild_id ASC;"""
    )
    return [
        {
            "guild_id": int(row["guild_id"]),
            "channel_id": int(row["channel_id"]),
            "cadence": str(row["cadence"]),
            "last_sent_at": row["last_sent_at"],
            "updated_at": row["updated_at"],
        }
        for row in rows
    ]


async def mark_guild_digest_sent(
    db: Any,
    guild_id: int,
    *,
    sent_at: Optional[Any] = None,
) -> None:
    """Record the UTC timestamp of the most recent completed digest window."""
    ts = db._now_utc_sql_timestamp(sent_at)
    await db._execute(
        """UPDATE guild_digest_settings
           SET last_sent_at = ?, updated_at = CURRENT_TIMESTAMP
           WHERE guild_id = ?;""",
        (ts, int(guild_id)),
    )
