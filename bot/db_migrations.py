"""Versioned, atomic migrations for bot-owned database schemas."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
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


async def run_migrations(database) -> None:
    """Apply every pending migration atomically on SQLite or libSQL."""
    await database._execute("""
        CREATE TABLE IF NOT EXISTS schema_migrations (
            version INTEGER PRIMARY KEY,
            applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    async with database._operation_lock:
        if database.is_turso:
            transaction = database._client.transaction()

            async def execute(query: str, params: tuple | list = ()):
                return await transaction.execute(query, list(params))

            async def fetchone(query: str, params: tuple | list = ()):
                result = await transaction.execute(query, list(params))
                return result.rows[0] if result.rows else None

            try:
                row = await fetchone("SELECT MAX(version) AS version FROM schema_migrations")
                version = row["version"] if row and row["version"] is not None else 0
                if version < 1:
                    await _migrate_profile_schema(execute, fetchone)
                    await execute("INSERT INTO schema_migrations (version) VALUES (1)")
                if version < 2:
                    await _migrate_profile_feed_schema(execute, fetchone)
                    await execute("INSERT INTO schema_migrations (version) VALUES (2)")
                await transaction.commit()
            except BaseException:
                await transaction.rollback()
                raise
            return

        connection = database._conn
        await connection.execute("BEGIN IMMEDIATE")

        async def execute(query: str, params: tuple | list = ()):
            return await connection.execute(query, params)

        async def fetchone(query: str, params: tuple | list = ()):
            async with connection.execute(query, params) as cursor:
                return await cursor.fetchone()

        try:
            row = await fetchone("SELECT MAX(version) AS version FROM schema_migrations")
            version = row["version"] if row and row["version"] is not None else 0
            if version < 1:
                await _migrate_profile_schema(execute, fetchone)
                await execute("INSERT INTO schema_migrations (version) VALUES (1)")
            if version < 2:
                await _migrate_profile_feed_schema(execute, fetchone)
                await execute("INSERT INTO schema_migrations (version) VALUES (2)")
            await connection.commit()
        except BaseException:
            await connection.rollback()
            raise
