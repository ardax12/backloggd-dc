"""Regression tests for Task 13 DB transaction adapter, lock re-entrancy guard, and rollback/cancellation."""

import asyncio
import os
import sqlite3
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

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

    async def test_source_removal_reconciliation_rollback_and_turso_parity(self):
        from unittest.mock import patch

        for label, db in (("sqlite", self.db), ("turso", self.turso_db)):
            with self.subTest(backend=label):
                await db.link_user(10, "Millers")
                await db.link_user(11, "Millers")
                await db.set_feed_channel(1, 500)
                await db.set_guild_feed_preference(1, 10, True)
                await db.set_guild_feed_preference(1, 11, True)
                await db.reconcile_guild_feed_activity(1, ["millers"])
                await db.seed_feed_source(1, "millers", "activity-display-v1", ["old"])
                await db.seed_feed_message(1, "millers", ["old"], '{"id":"old"}')

                # 1. Opt-off member 10 with unknown membership -> retains uncertainty because member 11 exists
                res1 = await db.disable_guild_feed_preference_and_reconcile(1, 10, member_ids=None)
                self.assertEqual(res1["profile_key"], "millers")
                self.assertFalse(res1["deactivated"])
                self.assertTrue(res1["pending_reconciliation"])
                self.assertTrue(await db.has_feed_source(1, "millers", "activity-display-v1"))

                # 2. Simulate transaction rollback during member 11 opt-off -> member 11 pref stays enabled & pending row stays intact!
                orig_deactivate = db._deactivate_guild_profile_tx
                with patch.object(db, "_deactivate_guild_profile_tx", side_effect=RuntimeError("mid-tx boom")):
                    with self.assertRaisesRegex(RuntimeError, "mid-tx boom"):
                        await db.disable_guild_feed_preference_and_reconcile(1, 11, member_ids=None)
                self.assertTrue(await db.get_guild_feed_preference(1, 11))
                self.assertTrue(await db.has_feed_source(1, "millers", "activity-display-v1"))
                self.assertFalse(db._operation_lock.locked())

                # 3. Clean opt-off of member 11 -> sole remaining holder gone -> deactivated atomically while archive kept!
                res2 = await db.disable_guild_feed_preference_and_reconcile(1, 11, member_ids=None)
                self.assertTrue(res2["deactivated"])
                self.assertFalse(await db.has_feed_source(1, "millers", "activity-display-v1"))
                self.assertFalse((await db.get_guild_profile_feed_status(1, "millers"))["active"])
                self.assertIsNotNone(await db._fetchone(
                    "SELECT 1 FROM profile_feed_messages WHERE guild_id = 1 AND profile_key = 'millers' AND event_id = 'old'"
                ))

                # 4. Admin follow removal under unknown membership (no stored members) -> deactivated atomically
                await db.add_guild_admin_follow(1, "SoloAdmin", None, 99)
                await db.reconcile_guild_feed_activity(1, ["soloadmin"], present_member_ids=[])
                await db.seed_feed_source(1, "soloadmin", "activity-display-v1", ["a1"])
                admin_res = await db.remove_guild_admin_follow_and_reconcile(1, "SoloAdmin", member_ids=None)
                self.assertTrue(admin_res["removed"])
                self.assertTrue(admin_res["deactivated"])
                self.assertFalse(await db.has_feed_source(1, "soloadmin", "activity-display-v1"))

    async def test_membership_backed_source_additions_are_atomic_and_keep_handoff_parity(self):
        t0 = "2026-11-13 08:00:00"
        for label, db in (("sqlite", self.db), ("turso", self.turso_db)):
            with self.subTest(backend=label):
                initial_rollbacks = self.turso_client.rollbacks if label == "turso" else None
                for discord_id in (10, 11, 12):
                    await db.link_user(discord_id, "Millers")
                await db.set_guild_feed_preference(1, 10, True)
                await db.set_guild_feed_preference(1, 11, True)
                await db.reconcile_guild_feed_activity(
                    1, ["millers"], now=t0, present_member_ids=[10, 11],
                )
                await db.seed_feed_source(1, "millers", "activity-display-v1", ["old"])
                await db.record_feed_event_decision(
                    1, "millers", "validated-work", source="activity", event_date="2026-11-13",
                    decision="eligible", reason="recent", payload="{}", observed_at=t0,
                )
                await db.unlink_user(10)

                # A failure after pending resolution must roll the source and resolution back together.
                resolve = db._resolve_pending_reconciliation_tx

                async def fail_after_resolve(*args, **kwargs):
                    await resolve(*args, **kwargs)
                    raise RuntimeError("abort handoff")

                with patch.object(db, "_resolve_pending_reconciliation_tx", new=fail_after_resolve):
                    with self.assertRaisesRegex(RuntimeError, "abort handoff"):
                        await db.set_guild_feed_preference(
                            1, 12, True, member_ids=[10, 11, 12],
                        )
                self.assertIsNotNone(await db._fetchone(
                    "SELECT 1 FROM guild_profile_feed_reconciliation WHERE guild_id = 1 AND profile_key = 'millers'"
                ))
                self.assertFalse(await db.get_guild_feed_preference(1, 12))
                self.assertFalse(db._operation_lock.locked())

                # Cancellation at the same point also restores the pending intent and frees the lock.
                reached_resolution = asyncio.Event()
                wait_for_cancel = asyncio.Event()

                async def pause_after_resolve(*args, **kwargs):
                    await resolve(*args, **kwargs)
                    reached_resolution.set()
                    await wait_for_cancel.wait()

                with patch.object(db, "_resolve_pending_reconciliation_tx", new=pause_after_resolve):
                    task = asyncio.create_task(db.set_guild_feed_preference(
                        1, 12, True, member_ids=[10, 11, 12],
                    ))
                    try:
                        await asyncio.wait_for(reached_resolution.wait(), timeout=2.0)
                        task.cancel()
                        with self.assertRaises(asyncio.CancelledError):
                            await task
                    finally:
                        wait_for_cancel.set()
                        if not task.done():
                            task.cancel()
                            await asyncio.gather(task, return_exceptions=True)
                self.assertIsNotNone(await db._fetchone(
                    "SELECT 1 FROM guild_profile_feed_reconciliation WHERE guild_id = 1 AND profile_key = 'millers'"
                ))
                self.assertFalse(await db.get_guild_feed_preference(1, 12))
                self.assertFalse(db._operation_lock.locked())
                if label == "turso":
                    self.assertEqual(self.turso_client.rollbacks - initial_rollbacks, 2)

                # Retry with authoritative membership proves member 11 bridged the source addition.
                self.assertEqual(
                    await db.set_guild_feed_preference(1, 12, True, member_ids=[10, 11, 12]),
                    "millers",
                )
                self.assertIsNone(await db._fetchone(
                    "SELECT 1 FROM guild_profile_feed_reconciliation WHERE guild_id = 1 AND profile_key = 'millers'"
                ))
                await db.unlink_user(11)
                self.assertTrue((await db.get_guild_profile_feed_status(1, "millers"))["active"])
                self.assertTrue(await db.has_feed_source(1, "millers", "activity-display-v1"))
                self.assertEqual(
                    (await db.get_feed_event_decision(1, "millers", "validated-work"))["observed_at"],
                    t0,
                )

                # An admin follow can resolve the next handoff with the same atomic evidence.
                resolve = db._resolve_pending_reconciliation_tx

                async def fail_admin_after_resolve(*args, **kwargs):
                    await resolve(*args, **kwargs)
                    raise RuntimeError("abort admin handoff")

                with patch.object(db, "_resolve_pending_reconciliation_tx", new=fail_admin_after_resolve):
                    with self.assertRaisesRegex(RuntimeError, "abort admin handoff"):
                        await db.add_guild_admin_follow(
                            1, "MILLERS", None, 99, member_ids=[12],
                        )
                self.assertIsNotNone(await db._fetchone(
                    "SELECT 1 FROM guild_profile_feed_reconciliation WHERE guild_id = 1 AND profile_key = 'millers'"
                ))
                self.assertFalse(await db.has_guild_admin_follow(1, "millers"))
                await db.add_guild_admin_follow(
                    1, "MILLERS", None, 99, member_ids=[12],
                )
                await db.unlink_user(12)
                self.assertTrue((await db.get_guild_profile_feed_status(1, "millers"))["active"])
                self.assertTrue(await db.has_guild_admin_follow(1, "millers"))

                # A new admin source cannot bridge an unobserved gap after every original member left.
                for discord_id in (30, 31, 32):
                    await db.link_user(discord_id, "Departed")
                await db.set_guild_feed_preference(1, 30, True)
                await db.set_guild_feed_preference(1, 31, True)
                await db.reconcile_guild_feed_activity(1, ["departed"], now=t0)
                await db.seed_feed_source(1, "departed", "activity-display-v1", ["old"])
                await db.record_feed_event_decision(
                    1, "departed", "uncertain-work", source="activity", event_date="2026-11-13",
                    decision="eligible", reason="recent", payload="{}", observed_at=t0,
                )
                await db.unlink_user(30)
                await db.unlink_user(31)
                self.assertFalse((await db.get_guild_profile_feed_status(1, "departed"))["active"])
                await db.add_guild_admin_follow(
                    1, "Departed", None, 99, member_ids=[32, 99],
                )
                self.assertFalse((await db.get_guild_profile_feed_status(1, "departed"))["active"])
                self.assertFalse(await db.has_feed_source(1, "departed", "activity-display-v1"))
                self.assertIsNone(await db.get_feed_event_decision(1, "departed", "uncertain-work"))

    async def test_concurrent_source_addition_and_removal_are_serialized_on_both_backends(self):
        for label, db in (("sqlite", self.db), ("turso", self.turso_db)):
            with self.subTest(backend=label):
                for discord_id in (40, 41, 42):
                    await db.link_user(discord_id, "Concurrent")
                await db.set_guild_feed_preference(2, 40, True)
                await db.set_guild_feed_preference(2, 41, True)
                await db.reconcile_guild_feed_activity(2, ["concurrent"], present_member_ids=[40, 41])
                await db.seed_feed_source(2, "concurrent", "activity-display-v1", ["old"])
                await db.record_feed_event_decision(
                    2, "concurrent", "validated-work", source="activity", event_date="2026-11-13",
                    decision="eligible", reason="recent", payload="{}", observed_at="2026-11-13 08:00:00",
                )
                await db.unlink_user(40)

                resolution_started = asyncio.Event()
                continue_addition = asyncio.Event()
                resolve = db._resolve_pending_reconciliation_tx

                async def pause_after_resolution(*args, **kwargs):
                    result = await resolve(*args, **kwargs)
                    resolution_started.set()
                    await continue_addition.wait()
                    return result

                with patch.object(db, "_resolve_pending_reconciliation_tx", new=pause_after_resolution):
                    addition = asyncio.create_task(db.set_guild_feed_preference(
                        2, 42, True, member_ids=[40, 41, 42],
                    ))
                    removal_started = asyncio.Event()

                    async def remove_existing_source():
                        removal_started.set()
                        return await db.unlink_user(41)

                    removal = None
                    try:
                        await asyncio.wait_for(resolution_started.wait(), timeout=2.0)
                        removal = asyncio.create_task(remove_existing_source())
                        await asyncio.wait_for(removal_started.wait(), timeout=2.0)
                        continue_addition.set()
                        await asyncio.gather(addition, removal)
                    finally:
                        continue_addition.set()
                        for task in (addition, removal):
                            if task is not None and not task.done():
                                task.cancel()
                                await asyncio.gather(task, return_exceptions=True)

                status = await db.get_guild_profile_feed_status(2, "concurrent")
                self.assertTrue(status["active"])
                self.assertTrue(await db.has_feed_source(2, "concurrent", "activity-display-v1"))
                self.assertEqual(
                    (await db.get_feed_event_decision(2, "concurrent", "validated-work"))["decision"],
                    "eligible",
                )
                self.assertFalse(db._operation_lock.locked())

    async def test_link_user_strips_whitespace_and_preserves_overwrite_on_both_backends(self):
        for label, db in (("sqlite", self.db), ("turso", self.turso_db)):
            with self.subTest(backend=label):
                self.assertTrue(await db.link_user(42, "  Millers  "))
                linked = await db.get_linked_user(42)
                self.assertIsNotNone(linked)
                self.assertEqual(linked["profile_key"], "millers")
                self.assertEqual(linked["backloggd_username"], "Millers")
                profile = await db.get_profile("millers")
                self.assertEqual(profile["display_username"], "Millers")

                # Existing link overwrite contract remains intact for link_user
                self.assertTrue(await db.link_user(42, "  Other_Name  "))
                relinked = await db.get_linked_user(42)
                self.assertEqual(relinked["profile_key"], "other_name")
                self.assertEqual(relinked["backloggd_username"], "Other_Name")
                self.assertFalse(await db.link_user_if_unlinked(42, "Blocked_Name"))
                still_linked = await db.get_linked_user(42)
                self.assertEqual(still_linked["backloggd_username"], "Other_Name")


if __name__ == "__main__":
    unittest.main()
