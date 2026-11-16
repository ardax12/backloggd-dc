"""Regression tests for Task 9: gate obsolete startup DDL/backfill safely across fresh, legacy, v1..v8, and Turso."""

import os

os.environ["PYTHON_DOTENV_DISABLED"] = "1"
os.environ["TURSO_DATABASE_URL"] = ""
os.environ["TURSO_AUTH_TOKEN"] = ""
os.environ["DEV_GUILD_ID"] = ""

import sqlite3
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from bot.db import Database
from tests.test_db_migrations import create_legacy_database


class _RecordingTursoTx:
    def __init__(self, client: "_RecordingTursoClient"):
        self._client = client

    async def execute(self, query, params=None):
        self._client.tx_sql.append(query)
        cursor = self._client.conn.execute(query, params or [])
        rows = cursor.fetchall() if cursor.description else []
        return SimpleNamespace(rows=rows, rows_affected=cursor.rowcount)

    async def commit(self):
        self._client.conn.commit()

    async def rollback(self):
        self._client.conn.rollback()


class _RecordingTursoClient:
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.conn = sqlite3.connect(db_path)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA foreign_keys = ON")
        self.direct_sql: list[str] = []
        self.tx_sql: list[str] = []

    def transaction(self):
        return _RecordingTursoTx(self)

    async def execute(self, query, params=None):
        self.direct_sql.append(query)
        cursor = self.conn.execute(query, params or [])
        self.conn.commit()
        rows = cursor.fetchall() if cursor.description else []
        return SimpleNamespace(rows=rows, rows_affected=cursor.rowcount)

    async def close(self):
        self.conn.close()


def _create_partially_migrated_v1(path: Path) -> None:
    """Create a schema v1 fixture (profiles/profile_ratings + user_ratings VIEW, pre-v2)."""
    conn = sqlite3.connect(path)
    conn.executescript("""
        CREATE TABLE schema_migrations (version INTEGER PRIMARY KEY, applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP);
        INSERT INTO schema_migrations (version) VALUES (1);
        CREATE TABLE profiles (profile_key TEXT PRIMARY KEY, display_username TEXT NOT NULL, avatar_url TEXT, last_synced_at TIMESTAMP);
        CREATE TABLE cached_games (slug TEXT PRIMARY KEY, title TEXT NOT NULL, cover_url TEXT, release_year INTEGER, community_rating REAL);
        CREATE TABLE linked_users (
            discord_id INTEGER PRIMARY KEY,
            profile_key TEXT NOT NULL,
            backloggd_username TEXT NOT NULL,
            avatar_url TEXT,
            last_synced_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (profile_key) REFERENCES profiles(profile_key) ON DELETE CASCADE
        );
        CREATE TABLE profile_ratings (
            profile_key TEXT NOT NULL,
            game_slug TEXT NOT NULL,
            rating REAL,
            status TEXT NOT NULL,
            is_liked BOOLEAN DEFAULT 0,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (profile_key, game_slug),
            FOREIGN KEY (profile_key) REFERENCES profiles(profile_key) ON DELETE CASCADE,
            FOREIGN KEY (game_slug) REFERENCES cached_games(slug) ON DELETE CASCADE
        );
        CREATE VIEW user_ratings AS
        SELECT linked_users.discord_id, profile_ratings.game_slug, profile_ratings.rating,
               profile_ratings.status, profile_ratings.is_liked, profile_ratings.updated_at
        FROM linked_users
        JOIN profile_ratings ON profile_ratings.profile_key = linked_users.profile_key;
        CREATE TABLE guild_settings (guild_id INTEGER PRIMARY KEY, feed_channel_id INTEGER NOT NULL);
        CREATE TABLE seen_journal_logs (entry_id TEXT PRIMARY KEY, discord_id INTEGER NOT NULL, posted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP);

        INSERT INTO profiles VALUES ('millers', 'Millers', 'av1', '2026-01-01 00:00:00');
        INSERT INTO cached_games (slug, title) VALUES ('hades', 'Hades');
        INSERT INTO linked_users (discord_id, profile_key, backloggd_username, avatar_url) VALUES (10, 'millers', 'Millers', 'av1');
        INSERT INTO profile_ratings (profile_key, game_slug, rating, status) VALUES ('millers', 'hades', 5.0, 'played');
        INSERT INTO guild_settings VALUES (1, 555);
        INSERT INTO seen_journal_logs VALUES ('j-old', 10, '2026-01-01 00:00:00');
    """)
    conn.commit()
    conn.close()


class StartupMigrationGatingTests(unittest.IsolatedAsyncioTestCase):
    async def test_repeat_v8_restart_skips_obsolete_ddl_and_cross_join_on_sqlite_and_turso(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            sqlite_path = Path(tmpdir) / "v8_sqlite.db"
            db = Database(str(sqlite_path), turso_url="")
            await db.connect()
            # Seed representative v8 state across all preserved tables
            await db.link_user(10, "Millers")
            await db._execute(
                "INSERT INTO cached_games (slug, title) VALUES (?, ?)", ("hades", "Hades")
            )
            await db._execute(
                "INSERT INTO profile_ratings (profile_key, game_slug, rating, status) VALUES (?, ?, ?, ?)",
                ("millers", "hades", 4.5, "played"),
            )
            await db.set_feed_channel(1, 555)
            await db.set_guild_feed_preference(1, 10, True)
            await db.add_guild_admin_follow(2, "AdminTarget", None, added_by=99)
            await db.seed_feed_source(1, "millers", "activity-display-v1", ["ev-base"])
            await db.record_feed_event_decision(
                1, "millers", "ev-old", source="activity", event_date="2026-01-01",
                decision="suppressed_old", reason="old", payload='{"id":"ev-old"}',
            )
            await db.store_feed_message(
                1, "millers", ["ev-sent"], '{"id":"ev-sent"}',
                message_id=9001, channel_id=555, delivery_state="sent",
            )
            # Simulate an already-cleaned v8 DB without unused legacy tables
            for legacy_table in ("feed_sources", "feed_messages", "seen_journal_logs", "seen_journal_deliveries"):
                await db._execute(f"DROP TABLE IF EXISTS {legacy_table}")
            await db.close()

            # Reconnect on v8 SQLite with SQL instrumentation
            db2 = Database(str(sqlite_path), turso_url="")
            executed_sql: list[str] = []
            orig_execute = db2._execute

            async def spy_execute(query, params=()):
                executed_sql.append(query)
                return await orig_execute(query, params)

            db2._execute = spy_execute
            await db2.connect()
            try:
                joined = "\n".join(executed_sql)
                self.assertNotIn("CROSS JOIN", joined)
                for legacy_table in ("feed_sources", "feed_messages", "seen_journal_logs", "seen_journal_deliveries"):
                    self.assertNotIn(legacy_table, joined)
                    self.assertIsNone(
                        await db2._fetchone("SELECT 1 FROM sqlite_master WHERE name = ?", (legacy_table,))
                    )
                self.assertNotIn("CREATE TABLE IF NOT EXISTS user_ratings", joined)
                # Required PRAGMAs and foreign key enforcement still active
                fk_row = await db2._fetchone("PRAGMA foreign_keys")
                self.assertEqual(fk_row[0], 1)
                # Row-level preservation
                self.assertEqual((await db2.get_linked_user(10))["profile_key"], "millers")
                self.assertEqual((await db2.get_profile_ratings_dict("millers")), {"hades": 4.5})
                self.assertEqual(await db2.get_feed_channel(1), 555)
                self.assertTrue(await db2.get_guild_feed_preference(1, 10))
                self.assertTrue(await db2.has_guild_admin_follow(2, "admintarget"))
                self.assertTrue(await db2.has_feed_source(1, "millers", "activity-display-v1"))
                self.assertEqual((await db2.get_feed_event_decision(1, "millers", "ev-old"))["decision"], "suppressed_old")
                msgs = {r["event_id"]: r for r in await db2.get_feed_messages(1, "millers")}
                self.assertEqual(msgs["ev-sent"]["message_id"], 9001)
                self.assertEqual(msgs["ev-sent"]["delivery_state"], "sent")
            finally:
                await db2.close()

            # Repeat v8 connect via Turso client branch
            turso_client = _RecordingTursoClient(str(sqlite_path))
            turso_db = Database(str(sqlite_path), turso_url="https://v8-turso.example")
            with patch("bot.db.libsql_client.create_client", return_value=turso_client):
                await turso_db.connect()
            try:
                all_turso_sql = "\n".join(turso_client.direct_sql + turso_client.tx_sql)
                self.assertNotIn("CROSS JOIN", all_turso_sql)
                for legacy_table in ("feed_sources", "feed_messages", "seen_journal_logs", "seen_journal_deliveries"):
                    self.assertNotIn(legacy_table, all_turso_sql)
                self.assertNotIn("CREATE TABLE IF NOT EXISTS user_ratings", all_turso_sql)
                self.assertEqual((await turso_db.get_linked_user(10))["profile_key"], "millers")
                self.assertTrue(await turso_db.has_feed_source(1, "millers", "activity-display-v1"))
            finally:
                await turso_db.close()

    async def test_unversioned_legacy_and_v1_v2_v7_upgrade_paths_preserve_rows(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            # 1. Unversioned legacy DB with seen_journal_logs + user_ratings table -> full upgrade preserves backfill
            legacy_path = Path(tmpdir) / "unversioned.db"
            create_legacy_database(legacy_path)
            raw = sqlite3.connect(legacy_path)
            raw.execute("INSERT INTO seen_journal_logs VALUES ('legacy-entry-1', 10, '2026-01-05 00:00:00')")
            raw.commit()
            raw.close()

            db_leg = Database(str(legacy_path), turso_url="")
            await db_leg.connect()
            try:
                self.assertEqual((await db_leg._fetchone("SELECT MAX(version) AS v FROM schema_migrations"))["v"], 8)
                self.assertEqual(await db_leg.get_profile_ratings_dict("millers"), {"new-game": 4.0, "shared-game": 4.5})
                self.assertTrue(await db_leg.is_journal_entry_delivered(1, "millers", "legacy-entry-1"))
            finally:
                await db_leg.close()

            # 2. v1 DB -> preserves user_ratings VIEW, backfills seen_journal_logs -> v2 -> v8
            v1_path = Path(tmpdir) / "v1.db"
            _create_partially_migrated_v1(v1_path)
            db_v1 = Database(str(v1_path), turso_url="")
            await db_v1.connect()
            try:
                self.assertEqual((await db_v1._fetchone("SELECT MAX(version) AS v FROM schema_migrations"))["v"], 8)
                self.assertEqual(
                    (await db_v1._fetchone("SELECT type FROM sqlite_master WHERE name = 'user_ratings'"))["type"],
                    "view",
                )
                self.assertEqual(await db_v1.get_profile_ratings_dict("millers"), {"hades": 5.0})
                self.assertTrue(await db_v1.is_journal_entry_delivered(1, "millers", "j-old"))
            finally:
                await db_v1.close()

            # 3. v2 DB with legacy tables dropped -> upgrades v3..v8 without recreating legacy tables or running CROSS JOIN
            v2_path = Path(tmpdir) / "v2.db"
            seed = Database(str(v2_path), turso_url="")
            await seed.connect()
            await seed.link_user(10, "Millers")
            await seed.set_feed_channel(1, 555)
            await seed.seed_feed_source(1, "millers", "activity-display-v1", ["ev-1"])
            await seed._execute("DELETE FROM schema_migrations WHERE version > 2")
            for tbl in ("guild_feed_preferences", "guild_feed_legacy_scope", "guild_feed_legacy_migrations",
                        "guild_profile_feed_activity", "guild_admin_follows", "profile_feed_decisions",
                        "profile_feed_source_health", "profile_activity_observations",
                        "profile_activity_observation_sources", "guild_profile_feed_reconciliation",
                        "feed_sources", "feed_messages", "seen_journal_logs", "seen_journal_deliveries"):
                await seed._execute(f"DROP TABLE IF EXISTS {tbl}")
            await seed.close()

            db_v2 = Database(str(v2_path), turso_url="")
            v2_sql: list[str] = []
            orig_exec = db_v2._execute

            async def spy_v2(q, p=()):
                v2_sql.append(q)
                return await orig_exec(q, p)

            db_v2._execute = spy_v2
            await db_v2.connect()
            try:
                self.assertEqual((await db_v2._fetchone("SELECT MAX(version) AS v FROM schema_migrations"))["v"], 8)
                self.assertNotIn("CROSS JOIN", "\n".join(v2_sql))
                self.assertIsNone(await db_v2._fetchone("SELECT 1 FROM sqlite_master WHERE name = 'seen_journal_logs'"))
                self.assertTrue(await db_v2.has_feed_source(1, "millers", "activity-display-v1"))
            finally:
                await db_v2.close()

            # 4. v7 DB -> upgrades to v8 cleanly skipping v1..v7 & legacy DDL
            v7_path = Path(tmpdir) / "v7.db"
            seed7 = Database(str(v7_path), turso_url="")
            await seed7.connect()
            await seed7.link_user(10, "Millers")
            await seed7._execute("DELETE FROM schema_migrations WHERE version > 7")
            await seed7._execute("DROP TABLE IF EXISTS guild_profile_feed_reconciliation")
            for legacy_table in ("feed_sources", "feed_messages", "seen_journal_logs", "seen_journal_deliveries"):
                await seed7._execute(f"DROP TABLE IF EXISTS {legacy_table}")
            await seed7.close()

            db_v7 = Database(str(v7_path), turso_url="")
            v7_sql: list[str] = []
            orig_exec7 = db_v7._execute

            async def spy_v7(q, p=()):
                v7_sql.append(q)
                return await orig_exec7(q, p)

            db_v7._execute = spy_v7
            await db_v7.connect()
            try:
                self.assertEqual((await db_v7._fetchone("SELECT MAX(version) AS v FROM schema_migrations"))["v"], 8)
                self.assertNotIn("CROSS JOIN", "\n".join(v7_sql))
                self.assertIsNotNone(await db_v7._fetchone("SELECT 1 FROM sqlite_master WHERE name = 'guild_profile_feed_reconciliation'"))
                self.assertIsNone(await db_v7._fetchone("SELECT 1 FROM sqlite_master WHERE name = 'feed_sources'"))
            finally:
                await db_v7.close()

            # 5. Mixed partially initialized DB (empty schema_migrations table) + failed v2 rollback/retry
            partial_path = Path(tmpdir) / "partial_empty_migrations.db"
            create_legacy_database(partial_path)
            raw_p = sqlite3.connect(partial_path)
            raw_p.execute("CREATE TABLE schema_migrations (version INTEGER PRIMARY KEY, applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)")
            raw_p.execute("INSERT INTO seen_journal_logs VALUES ('partial-entry', 10, '2026-01-06 00:00:00')")
            raw_p.commit()
            raw_p.close()

            async def fail_v2(execute, fetchone):
                await execute("CREATE TABLE migration_partial_v2 (x INTEGER)", ())
                raise RuntimeError("injected v2 failure")

            db_fail = Database(str(partial_path), turso_url="")
            try:
                with patch("bot.db_migrations._migrate_profile_feed_schema", side_effect=fail_v2):
                    with self.assertRaisesRegex(RuntimeError, "injected v2 failure"):
                        await db_fail.connect()
            finally:
                await db_fail.close()

            db_recover = Database(str(partial_path), turso_url="")
            await db_recover.connect()
            try:
                self.assertEqual((await db_recover._fetchone("SELECT MAX(version) AS v FROM schema_migrations"))["v"], 8)
                self.assertIsNone(await db_recover._fetchone("SELECT 1 FROM sqlite_master WHERE name = 'migration_partial_v2'"))
                self.assertTrue(await db_recover.is_journal_entry_delivered(1, "millers", "partial-entry"))
            finally:
                await db_recover.close()


if __name__ == "__main__":
    unittest.main()
