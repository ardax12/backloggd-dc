"""Versioned, atomic migrations for bot-owned database schemas."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
import re
from typing import Any


Execute = Callable[[str, tuple | list], Awaitable[Any]]
FetchOne = Callable[[str, tuple | list], Awaitable[Any]]


async def _migrate_profile_schema(execute: Execute, fetchone: FetchOne) -> None:
    """Move legacy per-member library rows to a normalized shared profile."""
    await execute("""
        CREATE TABLE IF NOT EXISTS profiles (
            profile_key TEXT PRIMARY KEY,
            display_username TEXT NOT NULL,
            avatar_url TEXT,
            last_synced_at TIMESTAMP
        )
    """, ())
    await execute("""
        CREATE TABLE IF NOT EXISTS profile_ratings (
            profile_key TEXT NOT NULL,
            game_slug TEXT NOT NULL,
            rating REAL,
            status TEXT NOT NULL,
            is_liked BOOLEAN DEFAULT 0,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (profile_key, game_slug),
            FOREIGN KEY (profile_key) REFERENCES profiles(profile_key) ON DELETE CASCADE,
            FOREIGN KEY (game_slug) REFERENCES cached_games(slug) ON DELETE CASCADE
        )
    """, ())

    rating_object = await fetchone(
        "SELECT type FROM sqlite_master WHERE name = 'user_ratings'", ()
    )
    if rating_object and rating_object["type"] == "table":
        await execute("DELETE FROM profile_ratings", ())
        await execute("DELETE FROM profiles", ())
        await execute("""
            INSERT INTO profiles (profile_key, display_username, avatar_url, last_synced_at)
            SELECT profile_key, backloggd_username, avatar_url, last_synced_at
            FROM (
                SELECT LOWER(TRIM(backloggd_username)) AS profile_key,
                       backloggd_username, avatar_url, last_synced_at, discord_id,
                       ROW_NUMBER() OVER (
                           PARTITION BY LOWER(TRIM(backloggd_username))
                           ORDER BY COALESCE(last_synced_at, '') DESC, discord_id ASC
                       ) AS profile_rank
                FROM linked_users
            )
            WHERE profile_rank = 1
        """, ())
        await execute("""
            WITH ranked_users AS (
                SELECT discord_id,
                       LOWER(TRIM(backloggd_username)) AS profile_key,
                       ROW_NUMBER() OVER (
                           PARTITION BY LOWER(TRIM(backloggd_username))
                           ORDER BY COALESCE(last_synced_at, '') DESC, discord_id ASC
                       ) AS profile_rank
                FROM linked_users
            )
            INSERT INTO profile_ratings
                (profile_key, game_slug, rating, status, is_liked, updated_at)
            SELECT ranked_users.profile_key, user_ratings.game_slug, user_ratings.rating,
                   user_ratings.status, user_ratings.is_liked, user_ratings.updated_at
            FROM ranked_users
            JOIN user_ratings ON user_ratings.discord_id = ranked_users.discord_id
            WHERE ranked_users.profile_rank = 1
        """, ())

    await execute("DROP TABLE IF EXISTS linked_users_new", ())
    await execute("""
        CREATE TABLE linked_users_new (
            discord_id INTEGER PRIMARY KEY,
            profile_key TEXT NOT NULL,
            backloggd_username TEXT NOT NULL,
            avatar_url TEXT,
            last_synced_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (profile_key) REFERENCES profiles(profile_key) ON DELETE CASCADE
        )
    """, ())
    await execute("""
        INSERT INTO linked_users_new
            (discord_id, profile_key, backloggd_username, avatar_url, last_synced_at)
        SELECT discord_id, LOWER(TRIM(backloggd_username)), backloggd_username,
               avatar_url, last_synced_at
        FROM linked_users
    """, ())

    if rating_object and rating_object["type"] == "table":
        await execute("DROP TABLE user_ratings", ())
    elif rating_object and rating_object["type"] == "view":
        await execute("DROP VIEW user_ratings", ())
    await execute("DROP TABLE linked_users", ())
    await execute("ALTER TABLE linked_users_new RENAME TO linked_users", ())
    await execute("DROP INDEX IF EXISTS idx_linked_users_username_ci", ())

    await execute("""
        CREATE VIEW user_ratings AS
        SELECT linked_users.discord_id, profile_ratings.game_slug, profile_ratings.rating,
               profile_ratings.status, profile_ratings.is_liked, profile_ratings.updated_at
        FROM linked_users
        JOIN profile_ratings ON profile_ratings.profile_key = linked_users.profile_key
    """, ())
    await execute("CREATE INDEX IF NOT EXISTS idx_linked_users_profile ON linked_users(profile_key)", ())
    await execute("CREATE INDEX IF NOT EXISTS idx_profile_ratings_game ON profile_ratings(game_slug)", ())
    await execute("CREATE INDEX IF NOT EXISTS idx_profile_ratings_status ON profile_ratings(status)", ())


async def _migrate_profile_feed_schema(execute: Execute, fetchone: FetchOne) -> None:
    """Copy provable legacy feed ownership to profiles and retain legacy rows for audit."""
    await execute("""
        CREATE TABLE IF NOT EXISTS profile_feed_sources (
            guild_id INTEGER NOT NULL,
            profile_key TEXT NOT NULL,
            source TEXT NOT NULL,
            PRIMARY KEY (guild_id, profile_key, source),
            FOREIGN KEY (profile_key) REFERENCES profiles(profile_key)
        )
    """, ())
    await execute("""
        CREATE TABLE IF NOT EXISTS profile_feed_messages (
            guild_id INTEGER NOT NULL,
            profile_key TEXT NOT NULL,
            event_id TEXT NOT NULL,
            payload TEXT NOT NULL,
            message_id INTEGER,
            channel_id INTEGER,
            delivery_state TEXT NOT NULL DEFAULT 'baseline',
            PRIMARY KEY (guild_id, profile_key, event_id),
            FOREIGN KEY (profile_key) REFERENCES profiles(profile_key)
        )
    """, ())
    await execute("""
        CREATE TABLE IF NOT EXISTS profile_seen_journal_deliveries (
            guild_id INTEGER NOT NULL,
            profile_key TEXT NOT NULL,
            entry_id TEXT NOT NULL,
            posted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (guild_id, profile_key, entry_id),
            FOREIGN KEY (profile_key) REFERENCES profiles(profile_key)
        )
    """, ())
    await execute("""
        CREATE TABLE IF NOT EXISTS profile_seen_journal_logs (
            profile_key TEXT NOT NULL,
            entry_id TEXT NOT NULL,
            posted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (profile_key, entry_id),
            FOREIGN KEY (profile_key) REFERENCES profiles(profile_key)
        )
    """, ())

    await execute("""
        INSERT OR IGNORE INTO profile_feed_sources (guild_id, profile_key, source)
        SELECT DISTINCT old.guild_id, linked.profile_key, old.source
        FROM feed_sources AS old
        JOIN linked_users AS linked ON linked.discord_id = old.discord_id
    """, ())
    await execute("""
        WITH ranked_messages AS (
            SELECT old.guild_id, linked.profile_key, old.event_id, old.payload,
                   old.message_id, old.channel_id, linked.discord_id,
                   ROW_NUMBER() OVER (
                       PARTITION BY old.guild_id, linked.profile_key, old.event_id
                       ORDER BY CASE WHEN old.message_id IS NULL THEN 1 ELSE 0 END,
                                old.message_id ASC, linked.discord_id ASC
                   ) AS profile_rank
            FROM feed_messages AS old
            JOIN linked_users AS linked ON linked.discord_id = old.discord_id
        )
        INSERT OR IGNORE INTO profile_feed_messages
            (guild_id, profile_key, event_id, payload, message_id, channel_id, delivery_state)
        SELECT guild_id, profile_key, event_id, payload, message_id, channel_id,
               CASE WHEN message_id IS NULL THEN 'baseline' ELSE 'sent' END
        FROM ranked_messages
        WHERE profile_rank = 1
    """, ())
    await execute("""
        INSERT OR IGNORE INTO profile_seen_journal_deliveries
            (guild_id, profile_key, entry_id, posted_at)
        SELECT DISTINCT old.guild_id, linked.profile_key, old.entry_id, old.posted_at
        FROM seen_journal_deliveries AS old
        JOIN linked_users AS linked ON linked.discord_id = old.discord_id
    """, ())
    await execute("""
        INSERT OR IGNORE INTO profile_seen_journal_logs (profile_key, entry_id, posted_at)
        SELECT DISTINCT linked.profile_key, old.entry_id, old.posted_at
        FROM seen_journal_logs AS old
        JOIN linked_users AS linked ON linked.discord_id = old.discord_id
    """, ())
    await execute("CREATE INDEX IF NOT EXISTS idx_profile_feed_messages_lookup ON profile_feed_messages(guild_id, profile_key)", ())
    await execute("CREATE INDEX IF NOT EXISTS idx_profile_feed_deliveries_lookup ON profile_seen_journal_deliveries(guild_id, profile_key)", ())


async def _migrate_guild_feed_preferences(execute: Execute, fetchone: FetchOne) -> None:
    """Add server-local member enrollment and preserve the one-time legacy scope."""
    await execute("""
        CREATE TABLE IF NOT EXISTS guild_feed_preferences (
            guild_id INTEGER NOT NULL,
            discord_id INTEGER NOT NULL,
            enabled BOOLEAN NOT NULL,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (guild_id, discord_id)
        )
    """, ())
    await execute("""
        CREATE TABLE IF NOT EXISTS guild_feed_legacy_scope (
            guild_id INTEGER NOT NULL,
            discord_id INTEGER NOT NULL,
            profile_key TEXT NOT NULL,
            PRIMARY KEY (guild_id, discord_id)
        )
    """, ())
    await execute("""
        CREATE TABLE IF NOT EXISTS guild_feed_legacy_migrations (
            guild_id INTEGER PRIMARY KEY,
            completed BOOLEAN NOT NULL DEFAULT 0,
            completed_at TIMESTAMP
        )
    """, ())
    await execute("""
        CREATE TABLE IF NOT EXISTS guild_profile_feed_activity (
            guild_id INTEGER NOT NULL,
            profile_key TEXT NOT NULL,
            active BOOLEAN NOT NULL DEFAULT 0,
            last_active_at TIMESTAMP,
            PRIMARY KEY (guild_id, profile_key)
        )
    """, ())

    await execute("""
        INSERT OR IGNORE INTO guild_feed_legacy_migrations (guild_id, completed)
        SELECT guild_id, 0 FROM guild_settings
    """, ())
    await execute("""
        INSERT OR IGNORE INTO guild_feed_legacy_scope (guild_id, discord_id, profile_key)
        SELECT settings.guild_id, linked.discord_id, linked.profile_key
        FROM guild_settings AS settings
        CROSS JOIN linked_users AS linked
    """, ())
    await execute("""
        INSERT OR IGNORE INTO guild_profile_feed_activity
            (guild_id, profile_key, active, last_active_at)
        SELECT DISTINCT sources.guild_id, sources.profile_key, 1, CURRENT_TIMESTAMP
        FROM profile_feed_sources AS sources
        JOIN guild_settings AS settings ON settings.guild_id = sources.guild_id
    """, ())
    await execute("CREATE INDEX IF NOT EXISTS idx_guild_feed_preferences_enabled ON guild_feed_preferences(guild_id, enabled, discord_id)", ())
    await execute("CREATE INDEX IF NOT EXISTS idx_guild_feed_legacy_scope_guild ON guild_feed_legacy_scope(guild_id, discord_id)", ())


async def _migrate_guild_admin_follows(execute: Execute, fetchone: FetchOne) -> None:
    """Store independent server subscriptions to public Backloggd profiles."""
    await execute("""
        CREATE TABLE IF NOT EXISTS guild_admin_follows (
            guild_id INTEGER NOT NULL,
            profile_key TEXT NOT NULL,
            display_username TEXT NOT NULL,
            avatar_url TEXT,
            added_by INTEGER NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (guild_id, profile_key)
        )
    """, ())
    await execute("CREATE INDEX IF NOT EXISTS idx_guild_admin_follows_profile ON guild_admin_follows(profile_key, guild_id)", ())


async def _migrate_feed_event_decisions(execute: Execute, fetchone: FetchOne) -> None:
    """Persist first-discovery age and uncertainty decisions per feed alias."""
    await execute("""
        CREATE TABLE IF NOT EXISTS profile_feed_decisions (
            guild_id INTEGER NOT NULL,
            profile_key TEXT NOT NULL,
            event_id TEXT NOT NULL,
            source TEXT NOT NULL,
            event_date TEXT,
            decision TEXT NOT NULL,
            reason TEXT NOT NULL,
            payload TEXT NOT NULL,
            observed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (guild_id, profile_key, event_id)
        )
    """, ())
    await execute("CREATE INDEX IF NOT EXISTS idx_profile_feed_decisions_reason ON profile_feed_decisions(guild_id, profile_key, decision)", ())


async def _migrate_profile_feed_source_health(execute: Execute, fetchone: FetchOne) -> None:
    """Track validated source attempts, coverage, and unresolved history gaps."""
    await execute("""
        CREATE TABLE IF NOT EXISTS profile_feed_source_health (
            profile_key TEXT NOT NULL,
            source TEXT NOT NULL,
            attempts INTEGER NOT NULL DEFAULT 0,
            successes INTEGER NOT NULL DEFAULT 0,
            last_attempt_at TIMESTAMP,
            last_success_at TIMESTAMP,
            coverage TEXT,
            pages_read INTEGER,
            last_continuous_boundary_event_id TEXT,
            last_continuous_boundary_at TIMESTAMP,
            unresolved_gap_since TIMESTAMP,
            gap_reason TEXT,
            last_error TEXT,
            PRIMARY KEY (profile_key, source)
        )
    """, ())


async def _migrate_profile_sync_and_observations(execute: Execute, fetchone: FetchOne) -> None:
    """Track separate library sync health, outage gap end times, and observation fingerprints."""
    profile_row = await fetchone(
        "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = 'profiles'", ()
    )
    profile_sql = (profile_row["sql"] or "") if profile_row else ""
    if "last_sync_attempt_at" not in profile_sql:
        await execute("ALTER TABLE profiles ADD COLUMN last_sync_attempt_at TIMESTAMP", ())
    if "last_sync_error" not in profile_sql:
        await execute("ALTER TABLE profiles ADD COLUMN last_sync_error TEXT", ())

    health_row = await fetchone(
        "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = 'profile_feed_source_health'", ()
    )
    health_sql = (health_row["sql"] or "") if health_row else ""
    if "unresolved_gap_until" not in health_sql:
        await execute("ALTER TABLE profile_feed_source_health ADD COLUMN unresolved_gap_until TIMESTAMP", ())

    await execute("""
        CREATE TABLE IF NOT EXISTS profile_activity_observations (
            profile_key TEXT NOT NULL,
            source TEXT NOT NULL,
            observation_key TEXT NOT NULL,
            fingerprint TEXT NOT NULL,
            observed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (profile_key, source, observation_key)
        )
    """, ())
    await execute("""
        CREATE TABLE IF NOT EXISTS profile_activity_observation_sources (
            profile_key TEXT NOT NULL,
            source TEXT NOT NULL,
            initialized_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (profile_key, source)
        )
    """, ())


async def _migrate_feed_source_reconciliation(execute: Execute, fetchone: FetchOne) -> None:
    """Persist uncertain source-removal reconciliation state across restarts."""
    await execute("""
        CREATE TABLE IF NOT EXISTS guild_profile_feed_reconciliation (
            guild_id INTEGER NOT NULL,
            profile_key TEXT NOT NULL,
            had_admin_source BOOLEAN NOT NULL DEFAULT 0,
            candidate_member_ids TEXT NOT NULL DEFAULT '[]',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (guild_id, profile_key)
        )
    """, ())


async def _migrate_guild_recipient_gaps(execute: Execute, fetchone: FetchOne) -> None:
    """Track per-guild feed recipient delivery gaps separately from profile source uncertainty."""
    await execute("""
        CREATE TABLE IF NOT EXISTS guild_profile_feed_recipient_gaps (
            guild_id INTEGER NOT NULL,
            profile_key TEXT NOT NULL,
            gap_since TIMESTAMP NOT NULL,
            reason TEXT NOT NULL,
            last_error TEXT,
            PRIMARY KEY (guild_id, profile_key)
        )
    """, ())
    await execute(
        """CREATE INDEX IF NOT EXISTS idx_guild_profile_recipient_gaps_profile
           ON guild_profile_feed_recipient_gaps (profile_key, guild_id)""",
        (),
    )


async def _migrate_profile_played_count_cache(execute: Execute, fetchone: FetchOne) -> None:
    """Add nullable persisted Played-tab total and observation timestamp to profiles."""
    profile_row = await fetchone(
        "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = 'profiles'", ()
    )
    profile_sql = (profile_row["sql"] or "") if profile_row else ""
    if not re.search(r"\bplayed_count\b", profile_sql):
        await execute("ALTER TABLE profiles ADD COLUMN played_count INTEGER", ())
    if not re.search(r"\bplayed_count_observed_at\b", profile_sql):
        await execute("ALTER TABLE profiles ADD COLUMN played_count_observed_at TIMESTAMP", ())


async def _migrate_profile_feed_history_indexes(execute: Execute, fetchone: FetchOne) -> None:
    """Add guarded scalar expression indexes and reverse alias table for archive lookups."""
    await execute(
        """CREATE INDEX IF NOT EXISTS idx_profile_feed_messages_activity_id
           ON profile_feed_messages (
               guild_id,
               profile_key,
               (CASE WHEN json_valid(payload)
                          AND json_type(payload) = 'object'
                          AND json_type(payload, '$.activity_id') = 'text'
                     THEN json_extract(payload, '$.activity_id') END)
           )""",
        (),
    )
    await execute(
        """CREATE INDEX IF NOT EXISTS idx_profile_feed_messages_game_slug
           ON profile_feed_messages (
               guild_id,
               profile_key,
               (CASE WHEN json_valid(payload)
                          AND json_type(payload) = 'object'
                          AND json_type(payload, '$.game_slug') = 'text'
                     THEN json_extract(payload, '$.game_slug') END)
           )""",
        (),
    )
    await execute(
        """CREATE INDEX IF NOT EXISTS idx_profile_feed_messages_reference
           ON profile_feed_messages (
               guild_id,
               profile_key,
               (CASE WHEN json_valid(payload)
                          AND json_type(payload) = 'object'
                          AND json_type(payload, '$.reference') = 'text'
                     THEN json_extract(payload, '$.reference') END)
           )""",
        (),
    )
    await execute(
        """CREATE TABLE IF NOT EXISTS profile_feed_message_aliases (
               guild_id INTEGER NOT NULL,
               profile_key TEXT NOT NULL,
               alias_id TEXT NOT NULL,
               event_id TEXT NOT NULL,
               PRIMARY KEY (guild_id, profile_key, alias_id, event_id)
           )""",
        (),
    )
    await execute(
        """CREATE INDEX IF NOT EXISTS idx_profile_feed_message_aliases_event
           ON profile_feed_message_aliases (guild_id, profile_key, event_id)""",
        (),
    )
    await execute(
        """CREATE TRIGGER IF NOT EXISTS trg_profile_feed_messages_aliases_ai
           AFTER INSERT ON profile_feed_messages
           WHEN json_valid(NEW.payload)
                AND json_type(NEW.payload) = 'object'
                AND json_type(NEW.payload, '$.event_ids') = 'array'
           BEGIN
               INSERT OR IGNORE INTO profile_feed_message_aliases
                   (guild_id, profile_key, alias_id, event_id)
               SELECT NEW.guild_id, NEW.profile_key, je.value, NEW.event_id
               FROM json_each(NEW.payload, '$.event_ids') AS je
               WHERE je.type = 'text' AND TRIM(je.value) != '';
           END""",
        (),
    )
    await execute(
        """CREATE TRIGGER IF NOT EXISTS trg_profile_feed_messages_aliases_au
           AFTER UPDATE OF payload ON profile_feed_messages
           BEGIN
               DELETE FROM profile_feed_message_aliases
               WHERE guild_id = OLD.guild_id
                 AND profile_key = OLD.profile_key
                 AND event_id = OLD.event_id;
               INSERT OR IGNORE INTO profile_feed_message_aliases
                   (guild_id, profile_key, alias_id, event_id)
               SELECT DISTINCT NEW.guild_id, NEW.profile_key, je.value, NEW.event_id
               FROM json_each(
                   CASE WHEN json_valid(NEW.payload)
                             AND json_type(NEW.payload) = 'object'
                             AND json_type(NEW.payload, '$.event_ids') = 'array'
                        THEN NEW.payload ELSE '{}' END,
                   '$.event_ids'
               ) AS je
               WHERE je.type = 'text' AND TRIM(je.value) != '';
           END""",
        (),
    )
    await execute(
        """CREATE TRIGGER IF NOT EXISTS trg_profile_feed_messages_aliases_ad
           AFTER DELETE ON profile_feed_messages
           BEGIN
               DELETE FROM profile_feed_message_aliases
               WHERE guild_id = OLD.guild_id
                 AND profile_key = OLD.profile_key
                 AND event_id = OLD.event_id;
           END""",
        (),
    )

    last_rowid = 0
    batch_size = 500
    while True:
        bound_row = await fetchone(
            """SELECT MAX(rowid) AS max_rowid
               FROM (
                   SELECT rowid FROM profile_feed_messages
                   WHERE rowid > ?
                   ORDER BY rowid ASC
                   LIMIT ?
               )""",
            (last_rowid, batch_size),
        )
        max_rowid = bound_row["max_rowid"] if bound_row else None
        if max_rowid is None:
            break
        await execute(
            """INSERT OR IGNORE INTO profile_feed_message_aliases
                   (guild_id, profile_key, alias_id, event_id)
               SELECT m.guild_id, m.profile_key, je.value, m.event_id
               FROM profile_feed_messages AS m
               JOIN json_each(
                   CASE WHEN json_valid(m.payload)
                             AND json_type(m.payload) = 'object'
                             AND json_type(m.payload, '$.event_ids') = 'array'
                        THEN m.payload ELSE '{}' END,
                   '$.event_ids'
               ) AS je
               WHERE m.rowid > ? AND m.rowid <= ?
                 AND je.type = 'text'
                 AND TRIM(je.value) != ''""",
            (last_rowid, int(max_rowid)),
        )
        last_rowid = int(max_rowid)


async def run_migrations(database) -> None:
    """Apply every pending migration atomically on SQLite or libSQL."""
    await database._execute("""
        CREATE TABLE IF NOT EXISTS schema_migrations (
            version INTEGER PRIMARY KEY,
            applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    async with database._transaction() as tx:
        row = await tx.fetchone("SELECT MAX(version) AS version FROM schema_migrations")
        version = row["version"] if row and row["version"] is not None else 0
        if version < 1:
            await _migrate_profile_schema(tx.execute, tx.fetchone)
            await tx.execute("INSERT INTO schema_migrations (version) VALUES (1)")
        if version < 2:
            await _migrate_profile_feed_schema(tx.execute, tx.fetchone)
            await tx.execute("INSERT INTO schema_migrations (version) VALUES (2)")
        if version < 3:
            await _migrate_guild_feed_preferences(tx.execute, tx.fetchone)
            await tx.execute("INSERT INTO schema_migrations (version) VALUES (3)")
        if version < 4:
            await _migrate_guild_admin_follows(tx.execute, tx.fetchone)
            await tx.execute("INSERT INTO schema_migrations (version) VALUES (4)")
        if version < 5:
            await _migrate_feed_event_decisions(tx.execute, tx.fetchone)
            await tx.execute("INSERT INTO schema_migrations (version) VALUES (5)")
        if version < 6:
            await _migrate_profile_feed_source_health(tx.execute, tx.fetchone)
            await tx.execute("INSERT INTO schema_migrations (version) VALUES (6)")
        if version < 7:
            await _migrate_profile_sync_and_observations(tx.execute, tx.fetchone)
            await tx.execute("INSERT INTO schema_migrations (version) VALUES (7)")
        if version < 8:
            await _migrate_feed_source_reconciliation(tx.execute, tx.fetchone)
            await tx.execute("INSERT INTO schema_migrations (version) VALUES (8)")
        if version < 9:
            await _migrate_guild_recipient_gaps(tx.execute, tx.fetchone)
            await tx.execute("INSERT INTO schema_migrations (version) VALUES (9)")
        if version < 10:
            await _migrate_profile_played_count_cache(tx.execute, tx.fetchone)
            await tx.execute("INSERT INTO schema_migrations (version) VALUES (10)")
        if version < 11:
            await _migrate_profile_feed_history_indexes(tx.execute, tx.fetchone)
            await tx.execute("INSERT INTO schema_migrations (version) VALUES (11)")
