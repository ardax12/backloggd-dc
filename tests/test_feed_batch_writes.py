"""Regression tests for Task 5 bounded atomic baseline, legacy migration, and feed write batching."""

import asyncio
import os
import sqlite3
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace

os.environ["PYTHON_DOTENV_DISABLED"] = "1"
os.environ["TURSO_DATABASE_URL"] = ""
os.environ["TURSO_AUTH_TOKEN"] = ""
os.environ["DEV_GUILD_ID"] = ""

from bot.db import Database, _MAX_SQL_BATCH_BYTES, _MAX_SQL_BIND_PARAMS
from bot.db_migrations import run_migrations


class CountingTursoClient:
    """Turso client double tracking execute/batch calls, bind params, and statement payload sizes."""

    def __init__(self, sqlite_path: str, *, max_params: int = _MAX_SQL_BIND_PARAMS):
        self.conn = sqlite3.connect(sqlite_path)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA foreign_keys = ON")
        self.max_params = max_params
        self.max_params_seen = 0
        self.max_statement_bytes_seen = 0
        self.tx_execute_calls = 0
        self.direct_execute_calls = 0
        self.batch_calls = 0
        self.batch_statement_count = 0
        self.commits = 0
        self.rollbacks = 0
        self.closed = False
        self.on_tx_execute = None

    def _record_and_check(self, query: str, params) -> list:
        values = list(params or ())
        if len(values) > self.max_params_seen:
            self.max_params_seen = len(values)
        if len(values) > self.max_params:
            raise sqlite3.OperationalError(
                f"SQL bind parameter budget exceeded: {len(values)} > {self.max_params}"
            )
        est_bytes = len(query.encode("utf-8")) + sum(
            len(str(v).encode("utf-8")) if v is not None else 4 for v in values
        )
        if est_bytes > self.max_statement_bytes_seen:
            self.max_statement_bytes_seen = est_bytes
        return values

    def reset_counters(self) -> None:
        self.max_params_seen = 0
        self.max_statement_bytes_seen = 0
        self.tx_execute_calls = 0
        self.direct_execute_calls = 0
        self.batch_calls = 0
        self.batch_statement_count = 0

    def transaction(self):
        client = self

        class _Tx:
            async def execute(self, query, params=None):
                client.tx_execute_calls += 1
                values = client._record_and_check(query, params)
                if client.on_tx_execute is not None:
                    await client.on_tx_execute(client.tx_execute_calls, query, values)
                cursor = client.conn.execute(query, values)
                rows = cursor.fetchall() if cursor.description else []
                return SimpleNamespace(rows=rows, rows_affected=cursor.rowcount)

            async def commit(self):
                client.commits += 1
                client.conn.commit()

            async def rollback(self):
                client.rollbacks += 1
                client.conn.rollback()

        return _Tx()

    async def execute(self, query, params=None):
        self.direct_execute_calls += 1
        values = self._record_and_check(query, params)
        cursor = self.conn.execute(query, values)
        rows = cursor.fetchall() if cursor.description else []
        self.conn.commit()
        return SimpleNamespace(rows=rows, rows_affected=cursor.rowcount)

    async def batch(self, statements):
        self.batch_calls += 1
        self.batch_statement_count += len(statements)
        results = []
        self.conn.execute("BEGIN IMMEDIATE")
        try:
            for query, params in statements:
                values = self._record_and_check(query, params)
                cursor = self.conn.execute(query, values)
                rows = cursor.fetchall() if cursor.description else []
                results.append(SimpleNamespace(rows=rows, rows_affected=cursor.rowcount))
            self.conn.commit()
            self.commits += 1
            return results
        except BaseException:
            self.conn.rollback()
            self.rollbacks += 1
            raise

    async def close(self):
        self.closed = True
        self.conn.close()


class FeedBatchWritesTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmpdir.cleanup)

        self.sqlite_path = Path(self.tmpdir.name) / "sqlite.db"
        self.db = Database(str(self.sqlite_path), turso_url="")
        self.addAsyncCleanup(self.db.close)
        await self.db.connect()
        await self.db._conn._execute(
            self.db._conn._conn.setlimit, sqlite3.SQLITE_LIMIT_VARIABLE_NUMBER, 999
        )

        self.turso_path = Path(self.tmpdir.name) / "turso.db"
        seed = Database(str(self.turso_path), turso_url="")
        await seed.connect()
        await seed.close()
        self.turso_client = CountingTursoClient(str(self.turso_path))
        self.turso_db = Database(str(self.turso_path), turso_url="https://batch-test.turso.io")
        self.turso_db._client = self.turso_client
        self.addAsyncCleanup(self.turso_db.close)

    async def _backends(self):
        return (("sqlite", self.db), ("turso", self.turso_db))

    async def test_seed_feed_source_batches_empty_dup_blank_large_quotes_unicode_and_idempotency(self):
        for label, db in await self._backends():
            with self.subTest(backend=label):
                await db.link_user(10, "Millers")
                await db.link_user(20, "PlayerB")

                # Empty and blank-only baseline still initializes the source with 0 seen rows.
                self.assertTrue(await db.seed_feed_source(1, "millers", "empty-src", ["", "   ", ""]))
                self.assertTrue(await db.has_feed_source(1, "millers", "empty-src"))
                empty_count = (
                    await db._fetchone(
                        "SELECT COUNT(*) AS c FROM profile_seen_journal_deliveries WHERE guild_id = 1 AND profile_key = 'millers'"
                    )
                )["c"]
                self.assertEqual(empty_count, 0)

                # 1,500+ entry IDs with duplicates, blanks, quotes, SQL-like text, and Unicode.
                special_ids = [
                    "O'Reilly's \"quoted\" entry",
                    "entry; DROP TABLE profiles; --",
                    "日本語🎮-entry-α",
                ]
                entry_ids = (
                    ["", "   ", "ev-0000", "ev-0000"]
                    + special_ids
                    + [f"ev-{i:04d}" for i in range(1550)]
                )

                sqlite_exec_calls = 0
                if label == "sqlite":
                    real_conn = db._conn
                    orig_execute = real_conn.execute

                    def tracked_execute(sql, params=()):
                        nonlocal sqlite_exec_calls
                        if "profile_seen_journal_deliveries" in sql:
                            sqlite_exec_calls += 1
                        return orig_execute(sql, params)

                    real_conn.execute = tracked_execute
                    try:
                        seeded = await db.seed_feed_source(1, "MiLLeRs", "activity-display-v1", entry_ids)
                    finally:
                        real_conn.execute = orig_execute
                    self.assertLessEqual(sqlite_exec_calls, 10)
                else:
                    self.turso_client.reset_counters()
                    seeded = await db.seed_feed_source(1, "MiLLeRs", "activity-display-v1", entry_ids)
                    # 1 check + <= 6 multi-row inserts + 1 source marker = <= 10 tx executes (vs 1555!)
                    self.assertLessEqual(self.turso_client.tx_execute_calls, 10)
                    self.assertLessEqual(self.turso_client.max_params_seen, _MAX_SQL_BIND_PARAMS)

                self.assertTrue(seeded)
                self.assertTrue(await db.has_feed_source(1, "millers", "activity-display-v1"))
                for sid in special_ids + ["ev-0000", "ev-1549"]:
                    self.assertTrue(await db.is_journal_entry_delivered(1, "millers", sid))
                self.assertFalse(await db.is_journal_entry_delivered(1, "millers", "   "))

                # Subsequent seeding returns False and does not replay or insert new history.
                self.assertFalse(await db.seed_feed_source(1, "millers", "activity-display-v1", ["late-ev"]))
                self.assertFalse(await db.is_journal_entry_delivered(1, "millers", "late-ev"))

                # Large strings (> 56 KB total) split by payload byte budget as well as bind limit.
                long_ids = [f"long-{i:02d}-" + ("あ" * 1000) for i in range(30)]
                if label == "turso":
                    self.turso_client.reset_counters()
                self.assertTrue(await db.seed_feed_source(1, "playerb", "journal-display-v1", long_ids))
                if label == "turso":
                    # 30 * ~3KB = ~90KB -> must split into >= 2 chunks under ~64KB per statement
                    self.assertGreaterEqual(self.turso_client.tx_execute_calls, 4)
                    self.assertLessEqual(
                        self.turso_client.max_statement_bytes_seen, _MAX_SQL_BATCH_BYTES + 4096
                    )
                self.assertTrue(await db.is_journal_entry_delivered(1, "playerb", long_ids[-1]))

    async def test_seed_feed_source_middle_chunk_failure_and_cancellation_rollback_cleanly(self):
        await self.turso_db.link_user(10, "Millers")
        self.turso_client.reset_counters()
        ids = [f"rb-{i:04d}" for i in range(600)]  # > 1 chunk (params_per_row=3 -> max 266 rows/chunk)

        async def fail_on_third_tx_exec(call_no, query, values):
            if call_no == 3:
                raise RuntimeError("injected middle chunk failure")

        self.turso_client.on_tx_execute = fail_on_third_tx_exec
        try:
            with self.assertRaisesRegex(RuntimeError, "injected middle chunk failure"):
                await self.turso_db.seed_feed_source(1, "millers", "activity-display-v1", ids)
        finally:
            self.turso_client.on_tx_execute = None

        self.assertFalse(self.turso_db._operation_lock.locked())
        self.assertFalse(await self.turso_db.has_feed_source(1, "millers", "activity-display-v1"))
        count = (
            await self.turso_db._fetchone(
                "SELECT COUNT(*) AS c FROM profile_seen_journal_deliveries WHERE guild_id = 1"
            )
        )["c"]
        self.assertEqual(count, 0)

        # Cancellation in middle chunk also rolls back completely.
        self.turso_client.reset_counters()
        hit_middle = asyncio.Event()
        release_middle = asyncio.Event()

        async def block_on_third_tx_exec(call_no, query, values):
            if call_no == 3:
                hit_middle.set()
                await release_middle.wait()

        self.turso_client.on_tx_execute = block_on_third_tx_exec
        task = asyncio.create_task(
            self.turso_db.seed_feed_source(1, "millers", "activity-display-v1", ids)
        )
        try:
            await asyncio.wait_for(hit_middle.wait(), timeout=2.0)
            task.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await task
        finally:
            release_middle.set()
            if not task.done():
                task.cancel()
                try:
                    await task
                except BaseException:
                    pass
            self.turso_client.on_tx_execute = None

        self.assertFalse(self.turso_db._operation_lock.locked())
        self.assertFalse(await self.turso_db.has_feed_source(1, "millers", "activity-display-v1"))
        count_after_cancel = (
            await self.turso_db._fetchone(
                "SELECT COUNT(*) AS c FROM profile_seen_journal_deliveries WHERE guild_id = 1"
            )
        )["c"]
        self.assertEqual(count_after_cancel, 0)

        # Retry succeeds cleanly once.
        self.assertTrue(await self.turso_db.seed_feed_source(1, "millers", "activity-display-v1", ids))
        self.assertFalse(await self.turso_db.seed_feed_source(1, "millers", "activity-display-v1", ids))
        count_after_retry = (
            await self.turso_db._fetchone(
                "SELECT COUNT(*) AS c FROM profile_seen_journal_deliveries WHERE guild_id = 1"
            )
        )["c"]
        self.assertEqual(count_after_retry, 600)

    async def test_complete_legacy_feed_migration_batches_preserves_scope_optouts_and_rolls_back(self):
        for label, db in await self._backends():
            with self.subTest(backend=label):
                # Seed 1,250 linked users with case variants before v3 legacy scope snapshot.
                profile_rows = []
                linked_rows = []
                for idx in range(1, 1251):
                    display = f"Member_{idx:04d}" if idx % 2 == 0 else f"member_{idx:04d}"
                    key = display.lower()
                    profile_rows.append((key, display, None))
                    linked_rows.append((idx, key, display, None))
                if db.is_turso:
                    conn = db._client.conn
                    conn.executemany(
                        "INSERT OR IGNORE INTO profiles (profile_key, display_username, avatar_url) VALUES (?, ?, ?)",
                        profile_rows,
                    )
                    conn.executemany(
                        "INSERT OR IGNORE INTO linked_users (discord_id, profile_key, backloggd_username, avatar_url) VALUES (?, ?, ?, ?)",
                        linked_rows,
                    )
                    conn.commit()
                else:
                    await db._conn.executemany(
                        "INSERT OR IGNORE INTO profiles (profile_key, display_username, avatar_url) VALUES (?, ?, ?)",
                        profile_rows,
                    )
                    await db._conn.executemany(
                        "INSERT OR IGNORE INTO linked_users (discord_id, profile_key, backloggd_username, avatar_url) VALUES (?, ?, ?, ?)",
                        linked_rows,
                    )
                    await db._conn.commit()

                await db.set_feed_channel(1, 100)
                await db._execute("DELETE FROM schema_migrations WHERE version >= 3")
                await run_migrations(db)

                # Post-snapshot changes: member 2001 joins later; member 1 unlinks/relinks different profile;
                # member 2 explicitly opts out (must stay disabled); member 1250 departed snapshot.
                await db.link_user(2001, "Latecomer")
                await db.unlink_user(1)
                await db.link_user(1, "SwitchedProfile")
                await db.set_guild_feed_preference(1, 2, False)

                present = list(range(1, 1250)) + [2001, 99999]

                if label == "turso":
                    # Inject middle-chunk failure first -> full rollback.
                    async def fail_mid(call_no, query, values):
                        if "INSERT OR IGNORE INTO guild_feed_preferences" in query:
                            raise RuntimeError("legacy write abort")

                    self.turso_client.on_tx_execute = fail_mid
                    try:
                        with self.assertRaisesRegex(RuntimeError, "legacy write abort"):
                            await db.complete_legacy_feed_migration(1, present)
                    finally:
                        self.turso_client.on_tx_execute = None
                    self.assertEqual(await db.get_legacy_feed_guilds_pending(), [1])
                    self.assertFalse(await db.get_guild_feed_preference(1, 3))

                    self.turso_client.reset_counters()
                    ok = await db.complete_legacy_feed_migration(1, present)
                    # ~2 scope SELECT chunks + <= 5 pref INSERT chunks + 1 completion UPDATE = <= 12 tx executes (vs 1250+!)
                    self.assertLessEqual(self.turso_client.tx_execute_calls, 12)
                    self.assertLessEqual(self.turso_client.max_params_seen, _MAX_SQL_BIND_PARAMS)
                else:
                    sqlite_pref_inserts = 0
                    orig_exec = db._conn.execute

                    def count_pref_inserts(sql, params=()):
                        nonlocal sqlite_pref_inserts
                        if "INSERT OR IGNORE INTO guild_feed_preferences" in sql:
                            sqlite_pref_inserts += 1
                        return orig_exec(sql, params)

                    db._conn.execute = count_pref_inserts
                    try:
                        ok = await db.complete_legacy_feed_migration(1, present)
                    finally:
                        db._conn.execute = orig_exec
                    self.assertLessEqual(sqlite_pref_inserts, 6)

                self.assertTrue(ok)
                self.assertFalse(await db.complete_legacy_feed_migration(1, present))
                self.assertEqual(await db.get_legacy_feed_guilds_pending(), [])
                self.assertFalse(await db.get_guild_feed_preference(1, 1))
                self.assertFalse(await db.get_guild_feed_preference(1, 2))
                self.assertTrue(await db.get_guild_feed_preference(1, 3))
                self.assertTrue(await db.get_guild_feed_preference(1, 1249))
                self.assertFalse(await db.get_guild_feed_preference(1, 1250))
                self.assertFalse(await db.get_guild_feed_preference(1, 2001))

    async def test_hot_alias_and_observation_write_paths_scale_by_chunks_and_preserve_parity(self):
        for label, db in await self._backends():
            with self.subTest(backend=label):
                await db.link_user(10, "Millers")
                aliases = [f"alias-{i:04d}" for i in range(1100)]

                if label == "turso":
                    self.turso_client.reset_counters()
                started = await db.begin_feed_delivery(1, "millers", aliases, '{"k":"v"}', 50)
                self.assertTrue(started)
                if label == "turso":
                    # 2 SELECT chunks + <= 8 multi-row INSERT chunks = <= 12 tx executes (vs 1102!)
                    self.assertLessEqual(self.turso_client.tx_execute_calls, 12)
                    self.assertLessEqual(self.turso_client.max_params_seen, _MAX_SQL_BIND_PARAMS)

                self.assertFalse(await db.begin_feed_delivery(1, "millers", ["alias-0500"], '{"k":"v"}', 50))

                if label == "turso":
                    self.turso_client.reset_counters()
                await db.store_feed_message(1, "millers", aliases, '{"v":2}', message_id=900, channel_id=50)
                if label == "turso":
                    # <= 16 statements in 1 batch (vs 2200 statements!)
                    self.assertLessEqual(self.turso_client.batch_statement_count, 20)
                    self.assertLessEqual(self.turso_client.max_params_seen, _MAX_SQL_BIND_PARAMS)

                seed_aliases = [f"seed-{i:04d}" for i in range(1100)]
                if label == "turso":
                    self.turso_client.reset_counters()
                await db.seed_feed_message(1, "millers", seed_aliases, '{"v":0}')
                if label == "turso":
                    self.assertLessEqual(self.turso_client.batch_statement_count, 20)

                obs_new = [(f"obs-{i:04d}", f"fp-{i:04d}") for i in range(1100)]
                if label == "turso":
                    self.turso_client.reset_counters()
                res1 = await db.record_profile_activity_observations("millers", "activity", obs_new)
                self.assertEqual(res1, {"initialized": True, "new": 1100, "updated": 0, "changed": 1100})
                if label == "turso":
                    # 1 init check + 1 init insert + 2 SELECT chunks + <= 6 INSERT chunks = <= 12 (vs 1104!)
                    self.assertLessEqual(self.turso_client.tx_execute_calls, 12)
                    self.assertLessEqual(self.turso_client.max_params_seen, _MAX_SQL_BIND_PARAMS)

                obs_upd = [(f"obs-{i:04d}", f"fp2-{i:04d}") for i in range(1100)]
                if label == "turso":
                    self.turso_client.reset_counters()
                res2 = await db.record_profile_activity_observations("millers", "activity", obs_upd)
                self.assertEqual(res2, {"initialized": False, "new": 0, "updated": 1100, "changed": 1100})
                if label == "turso":
                    self.assertLessEqual(self.turso_client.tx_execute_calls, 12)


if __name__ == "__main__":
    unittest.main()
