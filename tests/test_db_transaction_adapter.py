"""Regression tests for Task 13 DB transaction adapter, lock re-entrancy guard, and rollback/cancellation."""

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

from bot.db import Database


class _TrackingTursoClient:
    """Minimal Turso client double tracking transaction commits, rollbacks, and row semantics."""

    def __init__(self, sqlite_path: str):
        self.conn = sqlite3.connect(sqlite_path)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA foreign_keys = ON")
        self.commits = 0
        self.rollbacks = 0
        self.closed = False

    def transaction(self):
        client = self

        class _Tx:
            async def execute(self, query, params=None):
                cursor = client.conn.execute(query, list(params or ()))
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
        cursor = self.conn.execute(query, list(params or ()))
        rows = cursor.fetchall() if cursor.description else []
        self.conn.commit()
        return SimpleNamespace(rows=rows, rows_affected=cursor.rowcount)

    async def batch(self, statements):
        results = []
        self.conn.execute("BEGIN IMMEDIATE")
        try:
            for query, params in statements:
                cursor = self.conn.execute(query, list(params or ()))
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


class DbTransactionAdapterTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmpdir.cleanup)
        self.sqlite_path = Path(self.tmpdir.name) / "adapter_sqlite.db"
        self.db = Database(str(self.sqlite_path), turso_url="")
        self.addAsyncCleanup(self.db.close)
        await self.db.connect()

        self.turso_path = Path(self.tmpdir.name) / "adapter_turso.db"
        seed = Database(str(self.turso_path), turso_url="")
        await seed.connect()
        await seed.close()
        self.turso_client = _TrackingTursoClient(str(self.turso_path))
        self.turso_db = Database(str(self.turso_path), turso_url="https://adapter-test.turso.io")
        self.turso_db._client = self.turso_client
        self.addAsyncCleanup(self.turso_db.close)

    async def test_reentrant_operation_lock_raises_instead_of_deadlocking(self):
        async with self.db._transaction():
            with self.assertRaisesRegex(RuntimeError, "operation_lock|re-entrant|reentrant"):
                await self.db._fetchone("SELECT 1")

        # Lock is released cleanly after exiting or raising.
        self.assertFalse(self.db._operation_lock.locked())
        self.assertIsNotNone(await self.db._fetchone("SELECT 1"))

    async def test_transaction_adapter_commits_and_rolls_back_on_error_or_cancellation_both_backends(self):
        for label, db in (("sqlite", self.db), ("turso", self.turso_db)):
            with self.subTest(backend=label):
                async with db._transaction() as tx:
                    rowcount = await tx.execute(
                        "INSERT INTO profiles (profile_key, display_username) VALUES (?, ?)",
                        ("committed", "Committed"),
                    )
                    self.assertEqual(rowcount, 1)
                    fetched = await tx.fetchone(
                        "SELECT profile_key, display_username FROM profiles WHERE profile_key = ?",
                        ("committed",),
                    )
                    self.assertEqual(fetched["display_username"], "Committed")

                with self.assertRaisesRegex(RuntimeError, "abort tx"):
                    async with db._transaction() as tx:
                        await tx.execute(
                            "INSERT INTO profiles (profile_key, display_username) VALUES (?, ?)",
                            ("rolledback", "RolledBack"),
                        )
                        raise RuntimeError("abort tx")

                self.assertIsNone(await db.get_profile("rolledback"))
                self.assertFalse(db._operation_lock.locked())

                started = asyncio.Event()

                async def cancelled_work():
                    async with db._transaction() as tx:
                        await tx.execute(
                            "INSERT INTO profiles (profile_key, display_username) VALUES (?, ?)",
                            ("cancelled", "Cancelled"),
                        )
                        started.set()
                        await asyncio.sleep(10)

                task = asyncio.create_task(cancelled_work())
                await asyncio.wait_for(started.wait(), timeout=1.0)
                task.cancel()
                with self.assertRaises(asyncio.CancelledError):
                    await task

                self.assertIsNone(await db.get_profile("cancelled"))
                self.assertFalse(db._operation_lock.locked())
                # Follow-up write succeeds after cancellation.
                await db.link_user(99, "AfterCancel")
                self.assertEqual((await db.get_linked_user(99))["profile_key"], "aftercancel")

        self.assertGreaterEqual(self.turso_client.commits, 1)
        self.assertEqual(self.turso_client.rollbacks, 2)

    async def test_converted_transaction_methods_preserve_sqlite_and_turso_parity(self):
        for label, db in (("sqlite", self.db), ("turso", self.turso_db)):
            with self.subTest(backend=label):
                await db.link_user(10, "Millers")
                await db.link_user(20, "millers")
                await db.set_feed_channel(1, 500)
                await db.set_guild_feed_preference(1, 10, True)
                await db.reconcile_guild_feed_activity(1, ["millers"])

                self.assertTrue(await db.seed_feed_source(1, "millers", "activity", ["ev-1"]))
                self.assertFalse(await db.seed_feed_source(1, "millers", "activity", ["ev-2"]))

                self.assertTrue(await db.begin_feed_delivery(1, "millers", ["ev-2", "ev-2-alias"], "{}", 500))
                self.assertFalse(await db.begin_feed_delivery(1, "millers", ["ev-2-alias"], "{}", 500))
                await db.store_feed_message(1, "millers", ["ev-2", "ev-2-alias"], '{"v":1}', message_id=777, channel_id=500)

                obs = await db.record_profile_activity_observations("millers", "activity", [("k1", "fp1")])
                self.assertEqual(obs, {"initialized": True, "new": 1, "updated": 0, "changed": 1})

                # Unlinking 10 keeps feed active because 20 is still linked, Wait: 20 is not enabled yet -> pauses guild 1!
                self.assertTrue(await db.unlink_user(10))
                self.assertFalse(await db.has_feed_source(1, "millers", "activity"))
                self.assertTrue(await db.unset_feed_channel(1))
                self.assertFalse(await db.unset_feed_channel(1))

    async def test_sqlite_batch_commit_and_rollback_double_failure_preserves_primary_error(self):
        primary_exc = RuntimeError("PRIMARY commit failure")
        secondary_exc = RuntimeError("SECONDARY rollback failure")
        real_conn = self.db._conn

        class _FailingCommitRollbackConn:
            def __init__(self, inner):
                self._inner = inner

            def execute(self, *args, **kwargs):
                return self._inner.execute(*args, **kwargs)

            async def commit(self):
                raise primary_exc

            async def rollback(self):
                await self._inner.rollback()
                raise secondary_exc

        self.db._conn = _FailingCommitRollbackConn(real_conn)
        try:
            with self.assertLogs("backloggd.db", level="WARNING") as logs:
                with self.assertRaises(RuntimeError) as raised:
                    await self.db._execute_batch([
                        ("INSERT INTO profiles (profile_key, display_username) VALUES (?, ?)", ("p1", "P1")),
                    ])
        finally:
            self.db._conn = real_conn

        self.assertIs(raised.exception, primary_exc)
        self.assertIn("Could not roll back failed SQLite transaction", "\n".join(logs.output))
        self.assertFalse(self.db._operation_lock.locked())
        self.assertIsNone(await self.db.get_profile("p1"))

        # Subsequent write succeeds cleanly.
        await self.db.link_user(501, "AfterDoubleFail")
        self.assertEqual((await self.db.get_linked_user(501))["profile_key"], "afterdoublefail")

    async def test_sqlite_batch_body_failure_real_deferred_fk_commit_and_cancellation(self):
        # 1. Body failure in second statement rolls back first statement.
        with self.assertRaises(Exception):
            await self.db._execute_batch([
                ("INSERT INTO profiles (profile_key, display_username) VALUES (?, ?)", ("partial", "Partial")),
                ("INSERT INTO non_existent_table (x) VALUES (?)", ("boom",)),
            ])
        self.assertIsNone(await self.db.get_profile("partial"))
        self.assertFalse(self.db._operation_lock.locked())

        # 2. Real SQLite deferred-FK violation fails at COMMIT, not statement body.
        await self.db._execute(
            "CREATE TABLE batch_fk_parent (id INTEGER PRIMARY KEY)"
        )
        await self.db._execute(
            "CREATE TABLE batch_fk_child (id INTEGER PRIMARY KEY, parent_id INTEGER NOT NULL REFERENCES batch_fk_parent(id) DEFERRABLE INITIALLY DEFERRED)"
        )
        with self.assertRaises(sqlite3.IntegrityError):
            await self.db._execute_batch([
                ("INSERT INTO profiles (profile_key, display_username) VALUES (?, ?)", ("fk_fail", "FkFail")),
                ("INSERT INTO batch_fk_child (id, parent_id) VALUES (?, ?)", (1, 999999)),
            ])
        self.assertIsNone(await self.db.get_profile("fk_fail"))
        self.assertFalse(self.db._operation_lock.locked())

        # 3. Cancellation during batch rolls back and releases lock.
        started = asyncio.Event()
        real_conn = self.db._conn

        class _SlowSecondExecuteConn:
            def __init__(self, inner):
                self._inner = inner
                self._count = 0

            def execute(self, sql, params=()):
                self._count += 1
                if "INSERT INTO profiles" in sql:
                    return self._inner.execute(sql, params)
                if "SELECT 'wait'" in sql:
                    started.set()
                    conn = self._inner

                    class _WaitCtx:
                        async def __aenter__(self):
                            await asyncio.sleep(10)
                            return await conn.execute(sql, params)

                        async def __aexit__(self, *a):
                            return False

                    return _WaitCtx()
                return self._inner.execute(sql, params)

            def commit(self):
                return self._inner.commit()

            def rollback(self):
                return self._inner.rollback()

        self.db._conn = _SlowSecondExecuteConn(real_conn)
        try:
            task = asyncio.create_task(
                self.db._execute_batch([
                    ("INSERT INTO profiles (profile_key, display_username) VALUES (?, ?)", ("cancel_batch", "CancelBatch")),
                    ("SELECT 'wait'", ()),
                ])
            )
            await asyncio.wait_for(started.wait(), timeout=1.0)
            task.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await task
        finally:
            self.db._conn = real_conn

        self.assertIsNone(await self.db.get_profile("cancel_batch"))
        self.assertFalse(self.db._operation_lock.locked())
        await self.db.link_user(502, "AfterBatchCancel")
        self.assertEqual((await self.db.get_linked_user(502))["profile_key"], "afterbatchcancel")

    async def test_batch_empty_success_and_turso_parity(self):
        # Empty batch is a no-op on both backends
        self.assertEqual(await self.db._execute_batch_results([]), [])
        self.assertEqual(await self.turso_db._execute_batch_results([]), [])
        self.assertFalse(self.db._operation_lock.locked())
        self.assertFalse(self.turso_db._operation_lock.locked())

        # Successful SQLite batch preserves statement order & params and returns []
        sqlite_res = await self.db._execute_batch_results([
            ("INSERT INTO profiles (profile_key, display_username) VALUES (?, ?)", ("b1", "B1")),
            ("INSERT INTO profiles (profile_key, display_username) VALUES (?, ?)", ("b2", "B2")),
        ])
        self.assertEqual(sqlite_res, [])
        self.assertEqual((await self.db.get_profile("b1"))["display_username"], "B1")
        self.assertEqual((await self.db.get_profile("b2"))["display_username"], "B2")

        # Turso batch executes atomically without nested transaction() calls
        tx_called = False
        orig_tx = self.turso_client.transaction

        def _forbid_tx():
            nonlocal tx_called
            tx_called = True
            return orig_tx()

        self.turso_client.transaction = _forbid_tx
        try:
            turso_res = await self.turso_db._execute_batch_results([
                ("INSERT INTO profiles (profile_key, display_username) VALUES (?, ?)", ("tb1", "TB1")),
                ("SELECT display_username FROM profiles WHERE profile_key = ?", ("tb1",)),
            ])
        finally:
            self.turso_client.transaction = orig_tx

        self.assertFalse(tx_called)
        self.assertEqual(len(turso_res), 2)
        self.assertEqual(turso_res[1].rows[0]["display_username"], "TB1")
        self.assertFalse(self.turso_db._operation_lock.locked())


if __name__ == "__main__":
    unittest.main()
