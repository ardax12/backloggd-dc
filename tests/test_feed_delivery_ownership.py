"""Task 2 regressions for delivery ownership, cancellation finalization, and same-process recovery (F5)."""

import os

os.environ["PYTHON_DOTENV_DISABLED"] = "1"
os.environ["TURSO_DATABASE_URL"] = ""
os.environ["TURSO_AUTH_TOKEN"] = ""
os.environ["DEV_GUILD_ID"] = ""

import asyncio
import sqlite3
import tempfile
import unittest
from contextlib import asynccontextmanager
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import discord
from backloggd import ActivityEntry

from bot.cogs.feed import FeedCog
from bot.db import Database
from bot.feed_models import FeedEvent
from tests.test_feed_pre_addition_continuity import _TrackingTursoClient


def _feed_event(
    event_id: str,
    *,
    slug: str = "hades",
    title: str = "Hades",
    occurred_at: str = "2026-11-13T10:00:00Z",
    aliases: tuple[str, ...] | None = None,
) -> FeedEvent:
    ev = FeedEvent.from_activity(
        ActivityEntry(
            activity_id=event_id,
            game_title=title,
            game_slug=slug,
            cover_url=None,
            action_type="completed",
            action_text="completed",
            rating=4.5,
            review_notes=None,
            date_str="10:00",
            occurred_at=occurred_at,
        )
    )
    if aliases is not None:
        return replace(ev, event_ids=aliases)
    return ev


def _make_channel(channel_id: int = 100):
    channel = MagicMock(spec=discord.TextChannel)
    channel.id = channel_id
    channel.permissions_for.return_value = SimpleNamespace(
        view_channel=True, send_messages=True, embed_links=True
    )
    counter = {"n": 0}

    async def _send(**kwargs):
        counter["n"] += 1
        return SimpleNamespace(id=7000 + counter["n"], kwargs=kwargs)

    channel.send = AsyncMock(side_effect=_send)
    channel.get_partial_message = lambda mid: SimpleNamespace(id=mid, edit=AsyncMock())
    return channel


class FeedDeliveryOwnershipTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmpdir.cleanup)
        self.db_path = Path(self.tmpdir.name) / "delivery_ownership.db"
        self.db = Database(str(self.db_path), turso_url="")
        self.addAsyncCleanup(self.db.close)
        await self.db.connect()
        self.now = datetime(2026, 11, 13, 12, 0, tzinfo=timezone.utc)
        orig_sleep = asyncio.sleep

        async def _fast_sleep(*_a, **_kw):
            await orig_sleep(0)

        self.sleep_patch = patch(
            "bot.cogs.feed.asyncio.sleep",
            new=AsyncMock(side_effect=_fast_sleep),
        )
        self.sleep_patch.start()
        self.addCleanup(self.sleep_patch.stop)

    async def _seed_initialized_profile(self, db: Database, *, guild_id: int = 1):
        t0 = datetime(2026, 11, 13, 8, 0, tzinfo=timezone.utc)
        await db.link_user(10, "Millers")
        await db.set_feed_channel(guild_id, 100 + guild_id)
        await db.set_guild_feed_preference(guild_id, 10, True, member_ids=[10])
        await db.reconcile_guild_feed_activity(guild_id, ["millers"], now=t0, present_member_ids=[10])
        await db.seed_feed_source(guild_id, "millers", "activity-display-v1", [])
        await db.seed_feed_source(guild_id, "millers", "journal-display-v1", [])

    async def test_cancel_mid_send_clears_ownership_persists_unknown_and_blocks_duplicate_on_cog_reload(self):
        await self._seed_initialized_profile(self.db)
        channel = _make_channel(101)
        send_started = asyncio.Event()
        allow_unwind = asyncio.Event()

        async def blocked_send(**kwargs):
            send_started.set()
            try:
                await allow_unwind.wait()
            except asyncio.CancelledError:
                await allow_unwind.wait()
                raise

        channel.send.side_effect = blocked_send
        cog = FeedCog(SimpleNamespace(db=self.db), clock=lambda: self.now)
        ev1 = _feed_event("ev-cancel-1")
        ev2 = _feed_event("ev-after-2", slug="celeste", title="Celeste", occurred_at="2026-11-13T10:05:00Z")

        task = asyncio.create_task(
            cog._publish_sources(1, "millers", "Millers", None, channel, {"activity": [ev1], "journal": []})
        )
        try:
            await asyncio.wait_for(send_started.wait(), timeout=2.0)
            self.assertEqual(self.db._active_pending_deliveries, {(1, "millers", "ev-cancel-1")})
            concurrent = {r["event_id"]: r for r in await self.db.get_feed_messages(1, "millers")}
            self.assertEqual(concurrent["ev-cancel-1"]["delivery_state"], "pending")

            task.cancel()
            allow_unwind.set()
            with self.assertRaises(asyncio.CancelledError):
                await task
        finally:
            allow_unwind.set()
            await cog.close()

        # Same open DB: active ownership is released and durable row is 'unknown' (not left 'pending').
        self.assertEqual(self.db._active_pending_deliveries, set())
        rows = {r["event_id"]: r for r in await self.db.get_feed_messages(1, "millers")}
        self.assertEqual(rows["ev-cancel-1"]["delivery_state"], "unknown")

        # Reload FeedCog without reconnecting DB: ev-cancel-1 must NOT duplicate; ev-after-2 sends once.
        reloaded_channel = _make_channel(101)
        reloaded_cog = FeedCog(SimpleNamespace(db=self.db), clock=lambda: self.now)
        ok = await reloaded_cog._publish_sources(
            1, "millers", "Millers", None, reloaded_channel, {"activity": [ev1, ev2], "journal": []}
        )
        self.assertTrue(ok)
        reloaded_channel.send.assert_awaited_once()
        self.assertIn("Celeste", reloaded_channel.send.call_args.kwargs["embed"].title)

    async def test_send_succeeds_then_store_blocked_on_lock_and_cancelled_retains_message_id_or_unknown(self):
        await self._seed_initialized_profile(self.db)
        channel = _make_channel(101)
        store_started = asyncio.Event()
        release_store = asyncio.Event()
        orig_store = self.db.store_feed_message

        async def blocked_store(*args, **kwargs):
            store_started.set()
            await release_store.wait()
            return await orig_store(*args, **kwargs)

        cog = FeedCog(SimpleNamespace(db=self.db), clock=lambda: self.now)
        ev1 = _feed_event("ev-post-send")

        with patch.object(self.db, "store_feed_message", side_effect=blocked_store):
            publish_task = asyncio.create_task(
                cog._publish_sources(1, "millers", "Millers", None, channel, {"activity": [ev1], "journal": []})
            )
            await asyncio.wait_for(store_started.wait(), timeout=2.0)
            # Cancel publish_task while cleanup/store is blocked; unblocking store lets shielded cleanup finish.
            publish_task.cancel()
            release_store.set()
            with self.assertRaises(asyncio.CancelledError):
                await publish_task

        await cog.close()
        self.assertEqual(self.db._active_pending_deliveries, set())
        rows = {r["event_id"]: r for r in await self.db.get_feed_messages(1, "millers")}
        self.assertEqual(rows["ev-post-send"]["delivery_state"], "sent")
        self.assertEqual(rows["ev-post-send"]["message_id"], 7001)
        self.assertEqual(rows["ev-post-send"]["channel_id"], 101)

        # Next poll does not resend ev-post-send
        await cog._publish_sources(1, "millers", "Millers", None, channel, {"activity": [ev1], "journal": []})
        channel.send.assert_awaited_once()

        # Also verify fallback when store fails after send succeeds AND cleanup waits under secondary cancel.
        ev2 = _feed_event("ev-post-fail", slug="celeste", title="Celeste", occurred_at="2026-11-13T10:10:00Z")
        with patch.object(self.db, "store_feed_message", side_effect=RuntimeError("db write failed")):
            with self.assertRaises(RuntimeError):
                await cog._publish_sources(
                    1, "millers", "Millers", None, channel, {"activity": [ev1, ev2], "journal": []}
                )
        self.assertEqual(self.db._active_pending_deliveries, set())
        rows_after = {r["event_id"]: r for r in await self.db.get_feed_messages(1, "millers")}
        self.assertEqual(rows_after["ev-post-fail"]["delivery_state"], "unknown")

    async def test_begin_feed_delivery_commit_failure_and_cancellation_never_leak_ownership(self):
        turso_path = Path(self.tmpdir.name) / "commit_turso.db"
        seed = Database(str(turso_path), turso_url="")
        await seed.connect()
        await seed.close()
        turso_client = _TrackingTursoClient(str(turso_path))
        turso_db = Database(str(turso_path), turso_url="https://commit-test.turso.io")
        turso_db._client = turso_client
        self.addAsyncCleanup(turso_db.close)

        for label, db in (("sqlite", self.db), ("turso", turso_db)):
            with self.subTest(backend=label):
                await self._seed_initialized_profile(db)
                orig_tx = db._transaction

                # Case 1: Rollback before commit (failure raised at exit of transaction body -> never committed)
                @asynccontextmanager
                async def fail_before_commit():
                    async with orig_tx() as tx:
                        yield tx
                        raise sqlite3.OperationalError("commit boom")

                with patch.object(db, "_transaction", side_effect=fail_before_commit):
                    with self.assertRaises(sqlite3.OperationalError):
                        await db.begin_feed_delivery(1, "millers", ["ev-precommit-err"], "{}", 101)
                self.assertEqual(db._active_pending_deliveries, set())
                self.assertFalse(db._operation_lock.locked())
                self.assertEqual(await db.get_feed_messages(1, "millers"), [])

                # Case 2: Cancel right after inner commit while yielding/exiting
                @asynccontextmanager
                async def cancel_at_commit():
                    async with orig_tx() as tx:
                        yield tx
                        raise asyncio.CancelledError("cancelled at commit")

                with patch.object(db, "_transaction", side_effect=cancel_at_commit):
                    with self.assertRaises(asyncio.CancelledError):
                        await db.begin_feed_delivery(1, "millers", ["ev-precommit-cancel"], "{}", 101)
                self.assertEqual(db._active_pending_deliveries, set())
                self.assertFalse(db._operation_lock.locked())
                self.assertEqual(await db.get_feed_messages(1, "millers"), [])

                # Definitive retryable outcome works after failed begin_feed_delivery
                self.assertTrue(await db.begin_feed_delivery(1, "millers", ["ev-precommit-cancel"], "{}", 101))
                await db.release_feed_delivery(1, "millers", ["ev-precommit-cancel"])
                self.assertEqual(db._active_pending_deliveries, set())

    async def test_concurrent_live_send_stays_pending_while_abandoned_same_process_intent_recovers(self):
        await self._seed_initialized_profile(self.db, guild_id=1)
        await self._seed_initialized_profile(self.db, guild_id=2)
        # Warm connection-wide check so _stale_pending_recovery_checked starts True.
        self.assertEqual(await self.db.get_feed_messages(1, "millers"), [])

        # Abandoned same-process intent: started cleanly, then owner released without finalizing DB state
        # (simulates failed cleanup or crashed task without DB reconnect).
        self.assertTrue(
            await self.db.begin_feed_delivery(
                1, "millers", ["ev-abandoned"], '{"id":"ev-abandoned"}', 101, owner_token="dead-tok"
            )
        )
        self.db.release_feed_delivery_ownership(1, "millers", ["ev-abandoned"], "dead-tok")

        # Live in-flight send in Guild 2 for ev-live blocked in channel.send
        ch2 = _make_channel(102)
        live_started = asyncio.Event()
        allow_live_finish = asyncio.Event()

        async def live_send(**kwargs):
            live_started.set()
            await allow_live_finish.wait()
            raise discord.HTTPException(SimpleNamespace(status=400, reason="Bad Request"), "rejected")

        ch2.send.side_effect = live_send
        cog2 = FeedCog(SimpleNamespace(db=self.db), clock=lambda: self.now)
        ev_live = _feed_event("ev-live")
        live_task = asyncio.create_task(
            cog2._publish_sources(2, "millers", "Millers", None, ch2, {"activity": [ev_live], "journal": []})
        )
        try:
            await asyncio.wait_for(live_started.wait(), timeout=2.0)

            # Manual recovery AND reader-triggered recovery recover ev-abandoned without clobbering ev-live!
            g1_rows = {r["event_id"]: r for r in await self.db.get_feed_messages(1, "millers")}
            g2_rows = {r["event_id"]: r for r in await self.db.get_feed_messages(2, "millers")}
            self.assertEqual(g1_rows["ev-abandoned"]["delivery_state"], "unknown")
            self.assertEqual(g2_rows["ev-live"]["delivery_state"], "pending")

            # Also verify manual recover_stale_feed_deliveries works on an orphaned intent even if previously checked
            self.assertTrue(
                await self.db.begin_feed_delivery(
                    1, "millers", ["ev-manual"], '{"id":"ev-manual"}', 101, owner_token="tok2"
                )
            )
            self.db.release_feed_delivery_ownership(1, "millers", ["ev-manual"], "tok2")
            recovered_count = await self.db.recover_stale_feed_deliveries()
            self.assertEqual(recovered_count, 1)
            self.assertEqual(
                {r["event_id"]: r["delivery_state"] for r in await self.db.get_feed_messages(2, "millers")}["ev-live"],
                "pending",
            )

            allow_live_finish.set()
            self.assertFalse(await live_task)
        finally:
            allow_live_finish.set()
            await cog2.close()

        # ev-live received 400 -> released cleanly and retries; ev-abandoned stays protected 'unknown'.
        self.assertEqual(await self.db.get_feed_messages(2, "millers"), [])
        self.assertEqual(
            {r["event_id"]: r["delivery_state"] for r in await self.db.get_feed_messages(1, "millers")}["ev-manual"],
            "unknown",
        )

    async def test_multi_alias_guild_isolation_and_sqlite_turso_cancellation_parity(self):
        turso_path = Path(self.tmpdir.name) / "alias_turso.db"
        seed = Database(str(turso_path), turso_url="")
        await seed.connect()
        await seed.close()
        turso_client = _TrackingTursoClient(str(turso_path))
        turso_db = Database(str(turso_path), turso_url="https://alias-parity.turso.io")
        turso_db._client = turso_client
        self.addAsyncCleanup(turso_db.close)

        for label, db in (("sqlite", self.db), ("turso", turso_db)):
            with self.subTest(backend=label):
                await self._seed_initialized_profile(db, guild_id=1)
                await self._seed_initialized_profile(db, guild_id=2)
                ch1 = _make_channel(101)
                ch2 = _make_channel(102)
                send_started = asyncio.Event()
                allow_unwind = asyncio.Event()

                async def g1_send(**kwargs):
                    send_started.set()
                    await allow_unwind.wait()

                ch1.send.side_effect = g1_send
                cog = FeedCog(SimpleNamespace(db=db), clock=lambda: self.now)
                ev_alias = _feed_event("act-1", aliases=("act-1", "journal_j1"))

                task = asyncio.create_task(
                    cog._publish_sources(1, "millers", "Millers", None, ch1, {"activity": [ev_alias], "journal": []})
                )
                try:
                    await asyncio.wait_for(send_started.wait(), timeout=2.0)
                    task.cancel()
                    allow_unwind.set()
                    with self.assertRaises(asyncio.CancelledError):
                        await task
                finally:
                    allow_unwind.set()
                    await cog.close()

                g1_msgs = {r["event_id"]: r for r in await db.get_feed_messages(1, "millers")}
                self.assertEqual(g1_msgs["act-1"]["delivery_state"], "unknown")
                self.assertEqual(g1_msgs["journal_j1"]["delivery_state"], "unknown")

                # Guild 2 unaffected and delivers once
                ok_g2 = await cog._publish_sources(
                    2, "millers", "Millers", None, ch2, {"activity": [ev_alias], "journal": []}
                )
                self.assertTrue(ok_g2)
                ch2.send.assert_awaited_once()
                self.assertEqual(db._active_pending_deliveries, set())

    async def test_cancel_during_finalization_and_while_cleanup_waits(self):
        await self._seed_initialized_profile(self.db, guild_id=1)
        channel = _make_channel(101)
        channel.send.side_effect = discord.HTTPException(
            SimpleNamespace(status=400, reason="Bad Request"), "bad embed"
        )

        release_entered = asyncio.Event()
        allow_release = asyncio.Event()
        orig_release = self.db.release_feed_delivery

        async def blocked_release(*args, **kwargs):
            release_entered.set()
            await allow_release.wait()
            return await orig_release(*args, **kwargs)

        cog = FeedCog(SimpleNamespace(db=self.db), clock=lambda: self.now)
        ev_400 = _feed_event("ev-400-finalizing")

        with patch.object(self.db, "release_feed_delivery", side_effect=blocked_release):
            publish_task = asyncio.create_task(
                cog._publish_sources(1, "millers", "Millers", None, channel, {"activity": [ev_400], "journal": []})
            )
            await asyncio.wait_for(release_entered.wait(), timeout=2.0)
            # First cancel interrupts awaited release -> launches tracked shielded cleanup task
            publish_task.cancel()
            await asyncio.sleep(0)
            self.assertEqual(len(cog._delivery_cleanup_tasks), 1)
            # Second cancel hits publish_task while it is awaiting asyncio.shield(cleanup_task)
            publish_task.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await publish_task
            self.assertEqual(len(cog._delivery_cleanup_tasks), 1)
            self.assertEqual(self.db._active_pending_deliveries, {(1, "millers", "ev-400-finalizing")})

            # Cancel cog.close() while cleanup task is still waiting -> drain must survive!
            close_task = asyncio.create_task(cog.close())
            await asyncio.sleep(0)
            close_task.cancel()
            await asyncio.sleep(0)
            self.assertEqual(len(cog._delivery_cleanup_tasks), 1)

            allow_release.set()
            with self.assertRaises(asyncio.CancelledError):
                await close_task

        self.assertEqual(len(cog._delivery_cleanup_tasks), 0)
        self.assertEqual(self.db._active_pending_deliveries, set())
        self.assertFalse(self.db._operation_lock.locked())
        # Definitive 400 unsent outcome was released cleanly, so a retry succeeds once.
        self.assertEqual(await self.db.get_feed_messages(1, "millers"), [])
        channel2 = _make_channel(101)
        cog2 = FeedCog(SimpleNamespace(db=self.db), clock=lambda: self.now)
        ok = await cog2._publish_sources(1, "millers", "Millers", None, channel2, {"activity": [ev_400], "journal": []})
        self.assertTrue(ok)
        channel2.send.assert_awaited_once()
        await cog2.close()


if __name__ == "__main__":
    unittest.main()
