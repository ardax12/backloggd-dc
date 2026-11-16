import os
os.environ["PYTHON_DOTENV_DISABLED"] = "1"

import sqlite3
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from bot.db import Database
from bot.db_migrations import run_migrations


def create_legacy_database(path: Path, *, case_insensitive_index: bool = False) -> None:
    connection = sqlite3.connect(path)
    username_constraint = "" if case_insensitive_index else " UNIQUE"
    connection.executescript(f"""
        CREATE TABLE linked_users (
            discord_id INTEGER PRIMARY KEY,
            backloggd_username TEXT NOT NULL{username_constraint},
            avatar_url TEXT,
            last_synced_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE cached_games (
            slug TEXT PRIMARY KEY,
            title TEXT NOT NULL,
            cover_url TEXT,
            release_year INTEGER,
            community_rating REAL
        );
        CREATE TABLE user_ratings (
            discord_id INTEGER NOT NULL,
            game_slug TEXT NOT NULL,
            rating REAL,
            status TEXT NOT NULL,
            is_liked BOOLEAN DEFAULT 0,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (discord_id, game_slug),
            FOREIGN KEY (discord_id) REFERENCES linked_users (discord_id) ON DELETE CASCADE,
            FOREIGN KEY (game_slug) REFERENCES cached_games (slug) ON DELETE CASCADE
        );
        CREATE TABLE feed_sources (
            guild_id INTEGER NOT NULL,
            discord_id INTEGER NOT NULL,
            source TEXT NOT NULL,
            PRIMARY KEY (guild_id, discord_id, source)
        );
        CREATE TABLE feed_messages (
            guild_id INTEGER NOT NULL,
            discord_id INTEGER NOT NULL,
            event_id TEXT NOT NULL,
            payload TEXT NOT NULL,
            message_id INTEGER,
            channel_id INTEGER,
            PRIMARY KEY (guild_id, discord_id, event_id)
        );
        CREATE TABLE seen_journal_deliveries (
            guild_id INTEGER NOT NULL,
            entry_id TEXT NOT NULL,
            discord_id INTEGER NOT NULL,
            posted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (guild_id, entry_id)
        );
        CREATE TABLE seen_journal_logs (
            entry_id TEXT PRIMARY KEY,
            discord_id INTEGER NOT NULL,
            posted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE guild_settings (
            guild_id INTEGER PRIMARY KEY,
            feed_channel_id INTEGER NOT NULL
        );
    """)
    connection.execute("INSERT INTO guild_settings VALUES (1, 123)")
    if case_insensitive_index:
        connection.execute(
            "CREATE UNIQUE INDEX idx_linked_users_username_ci ON linked_users(LOWER(backloggd_username))"
        )
        connection.execute(
            "INSERT INTO linked_users VALUES (10, 'Millers', 'avatar-old', '2026-01-01 00:00:00')"
        )
    else:
        connection.executemany(
            "INSERT INTO linked_users VALUES (?, ?, ?, ?)",
            [
                (10, "Millers", "avatar-old", "2026-01-01 00:00:00"),
                (20, "millers", "avatar-new", "2026-02-01 00:00:00"),
            ],
        )
        connection.executemany(
            "INSERT INTO cached_games (slug, title) VALUES (?, ?)",
            [("old-game", "Old Game"), ("new-game", "New Game"), ("shared-game", "Shared Game")],
        )
        connection.executemany(
            "INSERT INTO user_ratings (discord_id, game_slug, rating, status) VALUES (?, ?, ?, 'played')",
            [
                (10, "old-game", 3.0),
                (10, "shared-game", 2.0),
                (20, "new-game", 4.0),
                (20, "shared-game", 4.5),
            ],
        )
    connection.commit()
    connection.close()


class ProfileMigrationTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.path = Path(self.temp_dir.name) / "legacy.db"

    async def asyncTearDown(self):
        self.temp_dir.cleanup()

    async def test_empty_database_gets_profile_schema_and_migration_version(self):
        db = Database(str(self.path), turso_url="")
        await db.connect()
        try:
            version = await db._fetchone("SELECT MAX(version) AS version FROM schema_migrations")
            self.assertEqual(version["version"], 8)
            self.assertEqual(await db._fetchall("SELECT * FROM guild_feed_preferences"), [])
            self.assertEqual(await db._fetchall("SELECT * FROM profiles"), [])
            self.assertEqual(await db._fetchall("SELECT * FROM profile_activity_observations"), [])
            self.assertEqual(await db._fetchall("SELECT * FROM profile_activity_observation_sources"), [])
            self.assertEqual(await db._fetchall("SELECT * FROM guild_profile_feed_reconciliation"), [])
            profile_cols = {row["name"] for row in await db._fetchall("PRAGMA table_info(profiles)")}
            self.assertTrue({"last_sync_attempt_at", "last_sync_error"}.issubset(profile_cols))
            health_cols = {row["name"] for row in await db._fetchall("PRAGMA table_info(profile_feed_source_health)")}
            self.assertIn("unresolved_gap_until", health_cols)
            self.assertIsNotNone(await db._fetchone(
                "SELECT type FROM sqlite_master WHERE name = 'user_ratings'"
            ))
        finally:
            await db.close()

    async def test_case_variants_share_one_winning_complete_profile_snapshot(self):
        create_legacy_database(self.path)
        db = Database(str(self.path), turso_url="")
        await db.connect()
        try:
            profile = await db._fetchone("SELECT * FROM profiles WHERE profile_key = 'millers'")
            self.assertEqual(profile["display_username"], "millers")
            self.assertEqual(profile["avatar_url"], "avatar-new")
            self.assertEqual(profile["last_synced_at"], "2026-02-01 00:00:00")

            profile_ratings = await db._fetchall(
                "SELECT game_slug, rating FROM profile_ratings WHERE profile_key = 'millers' ORDER BY game_slug"
            )
            self.assertEqual(
                [(row["game_slug"], row["rating"]) for row in profile_ratings],
                [("new-game", 4.0), ("shared-game", 4.5)],
            )
            compatibility_rows = await db._fetchall(
                "SELECT discord_id, game_slug FROM user_ratings WHERE game_slug = 'shared-game' ORDER BY discord_id"
            )
            self.assertEqual([row["discord_id"] for row in compatibility_rows], [10, 20])
            self.assertEqual(await db.get_profile_stats("millers"), {
                "total_games": 2, "rated_count": 2, "average_rating": 4.25
            })

            await db._execute(
                "INSERT INTO linked_users (discord_id, profile_key, backloggd_username) VALUES (30, 'millers', 'millers')"
            )
            await run_migrations(db)
            repeated = await db._fetchall(
                "SELECT game_slug, rating FROM profile_ratings WHERE profile_key = 'millers' ORDER BY game_slug"
            )
            self.assertEqual(len(repeated), 2)
            version = await db._fetchone("SELECT COUNT(*) AS count FROM schema_migrations")
            self.assertEqual(version["count"], 8)
        finally:
            await db.close()

    async def test_case_insensitive_unique_index_is_removed_by_rebuild(self):
        create_legacy_database(self.path, case_insensitive_index=True)
        db = Database(str(self.path), turso_url="")
        await db.connect()
        try:
            indexes = await db._fetchall("PRAGMA index_list(linked_users)")
            unique_username_indexes = []
            for index in indexes:
                columns = await db._fetchall(f"PRAGMA index_info('{index['name']}')")
                if index["unique"] and any(column["name"] == "backloggd_username" for column in columns):
                    unique_username_indexes.append(index["name"])
            self.assertEqual(unique_username_indexes, [])
            self.assertIsNotNone(await db._fetchone(
                "SELECT 1 FROM profiles WHERE profile_key = 'millers'"
            ))
        finally:
            await db.close()

    async def test_interrupted_migration_rolls_back_and_retries_cleanly(self):
        create_legacy_database(self.path)
        db = Database(str(self.path), turso_url="")

        async def fail_after_partial_ddl(execute, fetchone):
            await execute("CREATE TABLE migration_partial (value INTEGER)", ())
            raise RuntimeError("injected migration interruption")

        try:
            with patch("bot.db_migrations._migrate_profile_schema", side_effect=fail_after_partial_ddl):
                with self.assertRaisesRegex(RuntimeError, "injected migration interruption"):
                    await db.connect()
        finally:
            await db.close()

        check = sqlite3.connect(self.path)
        try:
            self.assertIsNone(check.execute(
                "SELECT 1 FROM sqlite_master WHERE name = 'migration_partial'"
            ).fetchone())
            self.assertEqual(check.execute("SELECT COUNT(*) FROM linked_users").fetchone()[0], 2)
            self.assertEqual(check.execute("SELECT COUNT(*) FROM schema_migrations").fetchone()[0], 0)
        finally:
            check.close()

        retry = Database(str(self.path), turso_url="")
        await retry.connect()
        try:
            self.assertEqual(await retry.get_profile_ratings_dict("millers"), {
                "new-game": 4.0, "shared-game": 4.5
            })
            version = await retry._fetchone("SELECT MAX(version) AS version FROM schema_migrations")
            self.assertEqual(version["version"], 8)
        finally:
            await retry.close()

    async def test_migration_uses_libsql_transaction_api(self):
        create_legacy_database(self.path)
        connection = sqlite3.connect(self.path)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")

        class Transaction:
            async def execute(self, query, params=None):
                cursor = connection.execute(query, params or [])
                return SimpleNamespace(rows=cursor.fetchall() if cursor.description else [])

            async def commit(self):
                connection.commit()

            async def rollback(self):
                connection.rollback()

        class Client:
            def transaction(self):
                return Transaction()

            async def execute(self, query, params=None):
                cursor = connection.execute(query, params or [])
                connection.commit()
                return SimpleNamespace(rows=cursor.fetchall() if cursor.description else [])

            async def close(self):
                connection.close()

        db = Database(str(self.path), turso_url="https://libsql-transaction-test")
        db._client = Client()
        try:
            await run_migrations(db)
            rows = connection.execute(
                "SELECT game_slug, rating FROM profile_ratings WHERE profile_key = 'millers' ORDER BY game_slug"
            ).fetchall()
            self.assertEqual([(row["game_slug"], row["rating"]) for row in rows], [
                ("new-game", 4.0), ("shared-game", 4.5)
            ])
            self.assertEqual(connection.execute("SELECT MAX(version) FROM schema_migrations").fetchone()[0], 8)
            self.assertEqual(connection.execute("SELECT COUNT(*) FROM profile_activity_observations").fetchone()[0], 0)
            self.assertEqual(connection.execute("SELECT COUNT(*) FROM guild_profile_feed_reconciliation").fetchone()[0], 0)
        finally:
            await db.close()
