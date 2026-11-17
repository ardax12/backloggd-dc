"""Task 1 regressions for pre-addition source continuity checks (F1)."""

import os

os.environ["PYTHON_DOTENV_DISABLED"] = "1"
os.environ["TURSO_DATABASE_URL"] = ""
os.environ["TURSO_AUTH_TOKEN"] = ""
os.environ["DEV_GUILD_ID"] = ""

import asyncio
import json
import sqlite3
import tempfile
import unittest
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import discord
from backloggd import ActivityEntry

from bot.cogs.account import AccountCog
from bot.cogs.feed import FeedCog
from bot.db import Database
from bot.feed_models import FeedEvent
from bot.services.library_sync import LibrarySyncService


class _TrackingTursoClient:
    """Minimal Turso client double supporting transactions and batches."""

    def __init__(self, sqlite_path: str):
        self.conn = sqlite3.connect(sqlite_path)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA foreign_keys = ON")
        self.closed = False

    def transaction(self):
        client = self

        class _Tx:
            async def execute(self, query, params=None):
                cursor = client.conn.execute(query, list(params or ()))
                rows = cursor.fetchall() if cursor.description else []
                return SimpleNamespace(rows=rows, rows_affected=cursor.rowcount)

            async def commit(self):
                client.conn.commit()

            async def rollback(self):
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
            return results
        except BaseException:
            self.conn.rollback()
            raise

    async def close(self):
        self.closed = True
        self.conn.close()


def _activity(event_id: str, slug: str, title: str, occurred_at: str) -> ActivityEntry:
    return ActivityEntry(
        activity_id=event_id,
        game_title=title,
        game_slug=slug,
        cover_url=None,
        action_type="completed",
        action_text="completed",
        rating=4.5,
        review_notes=None,
        date_str="11:30",
        occurred_at=occurred_at,
    )


def _make_channel(channel_id: int = 100):
    channel = MagicMock(spec=discord.TextChannel)
    channel.id = channel_id
    channel.mention = f"<#{channel_id}>"
    channel.permissions_for.return_value = SimpleNamespace(
        view_channel=True, send_messages=True, embed_links=True
    )
    counter = {"n": 0}

    async def _send(**kwargs):
        counter["n"] += 1
        return SimpleNamespace(id=5000 + counter["n"], kwargs=kwargs)

    channel.send = AsyncMock(side_effect=_send)
    channel.get_partial_message = lambda mid: SimpleNamespace(id=mid, edit=AsyncMock())
    return channel


def _make_guild(guild_id: int, member_ids, channel=None, *, chunked: bool = True):
    return SimpleNamespace(
        id=guild_id,
        name=f"Guild {guild_id}",
        me=object(),
        chunked=chunked,
        member_count=len(member_ids) if chunked else None,
        members=[SimpleNamespace(id=mid, display_name=f"User{mid}") for mid in member_ids],
        chunk=AsyncMock(return_value=[]),
        get_channel=lambda cid: channel if channel is not None and cid == channel.id else None,
    )


def _interaction(*, user_id: int, guild=None):
    return SimpleNamespace(
        user=SimpleNamespace(id=user_id, display_name=f"User{user_id}"),
        guild=guild,
        guild_id=getattr(guild, "id", None),
        permissions=SimpleNamespace(manage_channels=True),
        response=SimpleNamespace(defer=AsyncMock(), send_message=AsyncMock()),
        followup=SimpleNamespace(send=AsyncMock(return_value=SimpleNamespace(edit=AsyncMock()))),
    )


class FeedPreAdditionContinuityTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmpdir.cleanup)
        self.db_path = Path(self.tmpdir.name) / "continuity.db"
        self.db = Database(str(self.db_path), turso_url="")
        await self.db.connect()

    async def asyncTearDown(self):
        await self.db.close()

    async def _seed_active_sole_member_m1(self, db: Database, *, t0: datetime, guild_id: int = 1):
        await db.set_feed_channel(guild_id, 100)
        await db.link_user(10, "Millers")
        await db.set_guild_feed_preference(guild_id, 10, True, member_ids=[10])
        await db.reconcile_guild_feed_activity(guild_id, ["millers"], now=t0, present_member_ids=[10])
        await db.seed_feed_source(guild_id, "millers", "activity-display-v1", ["old-1"])
        await db.seed_feed_source(guild_id, "millers", "journal-display-v1", [])

        old_ev = _activity("old-1", "old-game", "Old Game", "2026-11-13T07:00:00Z")
        del_ev = _activity("del-1", "del-game", "Deleted Game", "2026-11-13T07:05:00Z")
        unk_ev = _activity("unk-1", "unk-game", "Unknown Game", "2026-11-13T07:10:00Z")
        old_payload = json.dumps(asdict(FeedEvent.from_activity(old_ev)), sort_keys=True)
        del_payload = json.dumps(asdict(FeedEvent.from_activity(del_ev)), sort_keys=True)
        unk_payload = json.dumps(asdict(FeedEvent.from_activity(unk_ev)), sort_keys=True)
        await db.store_feed_message(guild_id, "millers", ("old-1",), old_payload, 500, 100, delivery_state="sent")
        await db.store_feed_message(guild_id, "millers", ("del-1",), del_payload, 501, 100, delivery_state="deleted")
        await db.store_feed_message(guild_id, "millers", ("unk-1",), unk_payload, None, 100, delivery_state="unknown")
        await db.record_feed_event_decision(
            guild_id, "millers", "unsent-1",
            source="activity", event_date="2026-11-13", decision="eligible",
            reason="recent", payload=old_payload, observed_at=t0,
        )
        return old_ev

    async def _assert_archives_intact(self, db: Database, *, guild_id: int = 1):
        rows = await db._fetchall(
            "SELECT event_id, delivery_state FROM profile_feed_messages WHERE guild_id = ? AND profile_key = 'millers'",
            (guild_id,),
        )
        states = {row["event_id"]: row["delivery_state"] for row in rows}
        self.assertEqual(states.get("old-1"), "sent")
        self.assertEqual(states.get("del-1"), "deleted")
        self.assertEqual(states.get("unk-1"), "unknown")

    async def test_sole_member_leaves_silently_then_new_member_feed_on_resets_and_skips_gap(self):
        t0 = datetime(2026, 11, 13, 8, 0, tzinfo=timezone.utc)
        old_ev = await self._seed_active_sole_member_m1(self.db, t0=t0)
        await self.db.link_user(20, "Millers")

        # M1 (10) silently leaves Discord (not in members=[20]); no unlink, no poll yet -> no pending row.
        ch = _make_channel(100)
        guild = _make_guild(1, [20], channel=ch)
        cog = FeedCog(SimpleNamespace(db=self.db, get_guild=lambda gid: guild if gid == 1 else None))
        await FeedCog.feed_on.callback(cog, _interaction(user_id=20, guild=guild))

        self.assertFalse(await self.db.has_feed_source(1, "millers", "activity-display-v1"))
        self.assertFalse(await self.db.has_feed_source(1, "millers", "journal-display-v1"))
        self.assertIsNone(await self.db.get_feed_event_decision(1, "millers", "unsent-1"))
        await self._assert_archives_intact(self.db)

        # Restart before next poll
        await self.db.close()
        self.db = Database(str(self.db_path), turso_url="")
        await self.db.connect()

        gap_ev = _activity("gap-1", "hades", "Hades", "2026-11-13T10:00:00Z")
        new_ev = _activity("after-1", "celeste", "Celeste", "2026-11-13T12:05:00Z")
        entries = [gap_ev, old_ev]

        api = SimpleNamespace(
            get_user_activity_snapshot=AsyncMock(
                side_effect=lambda *_a, **_kw: SimpleNamespace(entries=list(entries), coverage="exhausted", pages_read=1)
            ),
            get_user_journal=AsyncMock(return_value=[]),
        )
        now = datetime(2026, 11, 13, 12, 0, tzinfo=timezone.utc)
        loop_cog = FeedCog(
            SimpleNamespace(db=self.db, backloggd=api, get_guild=lambda gid: guild if gid == 1 else None),
            clock=lambda: now,
        )
        with patch("bot.cogs.feed.asyncio.sleep", new=AsyncMock()):
            await loop_cog.journal_feed_loop.coro(loop_cog)

        ch.send.assert_not_awaited()
        gap_dec = await self.db.get_feed_event_decision(1, "millers", "gap-1")
        self.assertEqual(gap_dec["decision"], "baseline")

        # Next event after resume announces once
        entries = [new_ev, gap_ev, old_ev]
        now = datetime(2026, 11, 13, 12, 10, tzinfo=timezone.utc)
        with patch("bot.cogs.feed.asyncio.sleep", new=AsyncMock()):
            await loop_cog.journal_feed_loop.coro(loop_cog)

        ch.send.assert_awaited_once()
        self.assertIn("Celeste", ch.send.call_args.kwargs["embed"].title)

    async def test_sole_member_leaves_silently_then_admin_add_and_guild_link_reset_and_publish_no_history(self):
        for mode in ("feed_add", "guild_link"):
            with self.subTest(mode=mode), tempfile.TemporaryDirectory() as folder:
                db = Database(str(Path(folder) / f"{mode}.db"), turso_url="")
                await db.connect()
                try:
                    t0 = datetime(2026, 11, 13, 8, 0, tzinfo=timezone.utc)
                    old_ev = await self._seed_active_sole_member_m1(db, t0=t0)
                    ch = _make_channel(100)
                    guild = _make_guild(1, [20, 99], channel=ch)

                    gap_ev = _activity("gap-1", "hades", "Hades", "2026-11-13T10:00:00Z")
                    api = SimpleNamespace(
                        get_user_profile=AsyncMock(return_value=SimpleNamespace(username="Millers", avatar_url=None)),
                        get_user_library=AsyncMock(return_value=[]),
                        get_user_activity_snapshot=AsyncMock(
                            return_value=SimpleNamespace(entries=[gap_ev, old_ev], coverage="exhausted", pages_read=1)
                        ),
                        get_user_journal=AsyncMock(return_value=[]),
                    )
                    sync_svc = LibrarySyncService()
                    bot = SimpleNamespace(
                        db=db,
                        backloggd=api,
                        library_sync=sync_svc,
                        get_guild=lambda gid: guild if gid == 1 else None,
                    )
                    try:
                        now = datetime(2026, 11, 13, 12, 0, tzinfo=timezone.utc)
                        feed_cog = FeedCog(bot, clock=lambda: now)
                        if mode == "feed_add":
                            await FeedCog.feed_add.callback(feed_cog, _interaction(user_id=99, guild=guild), "Millers")
                        else:
                            account_cog = AccountCog(bot)
                            await AccountCog.link.callback(account_cog, _interaction(user_id=20, guild=guild), "Millers")
                            await account_cog.close()

                        self.assertFalse(await db.has_feed_source(1, "millers", "activity-display-v1"))
                        self.assertFalse(await db.has_feed_source(1, "millers", "journal-display-v1"))
                        self.assertIsNone(await db.get_feed_event_decision(1, "millers", "unsent-1"))
                        await self._assert_archives_intact(db)

                        with patch("bot.cogs.feed.asyncio.sleep", new=AsyncMock()):
                            await feed_cog.journal_feed_loop.coro(feed_cog)

                        ch.send.assert_not_awaited()
                        gap_dec = await db.get_feed_event_decision(1, "millers", "gap-1")
                        self.assertEqual(gap_dec["decision"], "baseline")
                    finally:
                        await sync_svc.close()
                finally:
                    await db.close()

    async def test_unknown_membership_during_addition_then_prior_unlink_and_recovery(self):
        t0 = datetime(2026, 11, 13, 8, 0, tzinfo=timezone.utc)
        old_ev = await self._seed_active_sole_member_m1(self.db, t0=t0)
        await self.db.link_user(20, "Millers")

        # M2 enables feed and Admin also adds while membership snapshot is unavailable (None).
        ch = _make_channel(100)
        broken_guild = _make_guild(1, [], channel=ch, chunked=False)
        api = SimpleNamespace(
            get_user_profile=AsyncMock(return_value=SimpleNamespace(username="Millers", avatar_url=None)),
        )
        cog = FeedCog(
            SimpleNamespace(db=self.db, backloggd=api, get_guild=lambda gid: broken_guild if gid == 1 else None),
            clock=lambda: datetime(2026, 11, 13, 9, 0, tzinfo=timezone.utc),
        )
        await FeedCog.feed_on.callback(cog, _interaction(user_id=20, guild=broken_guild))
        await FeedCog.feed_add.callback(cog, _interaction(user_id=99, guild=broken_guild), "Millers")

        pending = await self.db._fetchone(
            "SELECT had_admin_source, candidate_member_ids FROM guild_profile_feed_reconciliation WHERE guild_id = 1 AND profile_key = 'millers'"
        )
        self.assertIsNotNone(pending)
        self.assertEqual(pending["had_admin_source"], 0)
        self.assertEqual(json.loads(pending["candidate_member_ids"]), [10])

        # Restart DB, then original M1 unlinks; neither M2 nor new admin is in candidate_member_ids.
        await self.db.close()
        self.db = Database(str(self.db_path), turso_url="")
        await self.db.connect()
        await self.db.unlink_user(10)

        gap_ev = _activity("gap-1", "hades", "Hades", "2026-11-13T10:00:00Z")
        ok_guild = _make_guild(1, [20, 99], channel=ch, chunked=True)
        api.get_user_activity_snapshot = AsyncMock(
            return_value=SimpleNamespace(entries=[gap_ev, old_ev], coverage="exhausted", pages_read=1)
        )
        api.get_user_journal = AsyncMock(return_value=[])
        loop_cog = FeedCog(
            SimpleNamespace(db=self.db, backloggd=api, get_guild=lambda gid: ok_guild if gid == 1 else None),
            clock=lambda: datetime(2026, 11, 13, 12, 0, tzinfo=timezone.utc),
        )
        with patch("bot.cogs.feed.asyncio.sleep", new=AsyncMock()):
            await loop_cog.journal_feed_loop.coro(loop_cog)

        ch.send.assert_not_awaited()
        self.assertEqual((await self.db.get_feed_event_decision(1, "millers", "gap-1"))["decision"], "baseline")
        await self._assert_archives_intact(self.db)

    async def test_true_overlap_and_duplicate_add_preserve_baseline_and_pending_work(self):
        t0 = datetime(2026, 11, 13, 8, 0, tzinfo=timezone.utc)
        await self._seed_active_sole_member_m1(self.db, t0=t0)
        await self.db.link_user(20, "Millers")

        # M1 (10) IS present when M2 enables, when Admin adds, and when repeated idempotently.
        await self.db.set_guild_feed_preference(1, 20, True, member_ids=[10, 20])
        await self.db.set_guild_feed_preference(1, 20, True, member_ids=[10, 20])
        await self.db.add_guild_admin_follow(1, "Millers", None, 99, member_ids=[10, 20])
        await self.db.add_guild_admin_follow(1, "Millers", None, 99, member_ids=[10, 20])

        self.assertTrue((await self.db.get_guild_profile_feed_status(1, "millers"))["active"])
        self.assertTrue(await self.db.has_feed_source(1, "millers", "activity-display-v1"))
        self.assertEqual(
            (await self.db.get_feed_event_decision(1, "millers", "unsent-1"))["decision"],
            "eligible",
        )

    async def test_persisted_preferences_on_relink_and_internal_link_user_preserve_continuity(self):
        t0 = datetime(2026, 11, 13, 8, 0, tzinfo=timezone.utc)
        # User 10 enabled Alpha in Guild 1, then unlinked -> pref(1, 10, enabled=1) stays in DB.
        await self.db.set_feed_channel(1, 100)
        await self.db.set_feed_channel(2, 200)
        await self.db.link_user(10, "Alpha")
        await self.db.set_guild_feed_preference(1, 10, True, member_ids=[10])
        await self.db.unlink_user(10)

        # User 20 enabled Beta in Guild 1, active + baselined.
        await self.db.link_user(20, "Beta")
        await self.db.set_guild_feed_preference(1, 20, True, member_ids=[10, 20])
        await self.db.reconcile_guild_feed_activity(1, ["beta"], now=t0, present_member_ids=[10, 20])
        await self.db.seed_feed_source(1, "beta", "activity-display-v1", ["beta-old"])
        await self.db.seed_feed_source(1, "beta", "journal-display-v1", [])

        # User 20 silently leaves Guild 1; User 10 links Beta in DM (guild=None)!
        self.assertTrue(await self.db.link_user_if_unlinked(10, "Beta"))
        # Guild 2 must NOT be globally enabled when linking in DM.
        self.assertFalse(await self.db.get_guild_feed_preference(2, 10))
        # Guild 1 must hold pending continuity ONLY on prior [20], NOT counting newly linked 10!
        pending = await self.db._fetchone(
            "SELECT candidate_member_ids FROM guild_profile_feed_reconciliation WHERE guild_id = 1 AND profile_key = 'beta'"
        )
        self.assertIsNotNone(pending)
        self.assertEqual(json.loads(pending["candidate_member_ids"]), [20])

        # When Guild 1 resolves with authoritative members [10] (20 absent), Beta resets and skips gap history!
        gap_ev = _activity("beta-gap", "hades", "Hades", "2026-11-13T10:00:00Z")
        ch = _make_channel(100)
        guild1 = _make_guild(1, [10], channel=ch)
        api = SimpleNamespace(
            get_user_activity_snapshot=AsyncMock(
                return_value=SimpleNamespace(entries=[gap_ev], coverage="exhausted", pages_read=1)
            ),
            get_user_journal=AsyncMock(return_value=[]),
        )
        cog = FeedCog(
            SimpleNamespace(db=self.db, backloggd=api, get_guild=lambda gid: guild1 if gid == 1 else None),
            clock=lambda: datetime(2026, 11, 13, 12, 0, tzinfo=timezone.utc),
        )
        with patch("bot.cogs.feed.asyncio.sleep", new=AsyncMock()):
            await cog.journal_feed_loop.coro(cog)
        ch.send.assert_not_awaited()
        self.assertEqual((await self.db.get_feed_event_decision(1, "beta", "beta-gap"))["decision"], "baseline")

        # Also verify internal link_user overwrite maintains both old and new profile continuity state.
        await self.db.link_user(30, "Gamma")
        await self.db.set_guild_feed_preference(1, 30, True, member_ids=[10, 30])
        await self.db.reconcile_guild_feed_activity(1, ["beta", "gamma"], now=t0, present_member_ids=[10, 30])
        await self.db.seed_feed_source(1, "gamma", "activity-display-v1", ["g-old"])
        await self.db.set_guild_feed_preference(1, 10, True, member_ids=[10, 30])
        # Overwrite user 10 from Beta -> Gamma while 10 was sole Beta source and 30 left Gamma silently:
        await self.db.link_user(10, "Gamma")
        beta_pending = await self.db._fetchone(
            "SELECT candidate_member_ids FROM guild_profile_feed_reconciliation WHERE guild_id = 1 AND profile_key = 'beta'"
        )
        gamma_pending = await self.db._fetchone(
            "SELECT candidate_member_ids FROM guild_profile_feed_reconciliation WHERE guild_id = 1 AND profile_key = 'gamma'"
        )
        self.assertIsNotNone(beta_pending)
        self.assertEqual(json.loads(beta_pending["candidate_member_ids"]), [20])
        self.assertIsNotNone(gamma_pending)
        self.assertEqual(json.loads(gamma_pending["candidate_member_ids"]), [30])

    async def test_multi_guild_multi_profile_and_admin_member_independence(self):
        t0 = datetime(2026, 11, 13, 8, 0, tzinfo=timezone.utc)
        await self._seed_active_sole_member_m1(self.db, t0=t0, guild_id=1)
        # Unrelated healthy profile in Guild 1 + Millers with admin follow in Guild 2
        await self.db.link_user(30, "Other")
        await self.db.set_guild_feed_preference(1, 30, True, member_ids=[10, 30])
        await self.db.reconcile_guild_feed_activity(1, ["millers", "other"], now=t0, present_member_ids=[10, 30])
        await self.db.seed_feed_source(1, "other", "activity-display-v1", ["other-old"])

        await self.db.set_feed_channel(2, 200)
        await self.db.add_guild_admin_follow(2, "Millers", None, 99, member_ids=[])
        await self.db.reconcile_guild_feed_activity(2, ["millers"], now=t0, present_member_ids=[])
        await self.db.seed_feed_source(2, "millers", "activity-display-v1", ["g2-old"])

        # M2 replaces silently departed M1 in Guild 1 for Millers only:
        await self.db.link_user(20, "Millers")
        await self.db.set_guild_feed_preference(1, 20, True, member_ids=[20, 30])

        self.assertFalse(await self.db.has_feed_source(1, "millers", "activity-display-v1"))
        self.assertTrue(await self.db.has_feed_source(1, "other", "activity-display-v1"))
        self.assertTrue(await self.db.has_feed_source(2, "millers", "activity-display-v1"))
        await self._assert_archives_intact(self.db, guild_id=1)

    async def test_rollback_cancellation_concurrency_and_sqlite_turso_parity(self):
        turso_path = Path(self.tmpdir.name) / "continuity_turso.db"
        seed = Database(str(turso_path), turso_url="")
        await seed.connect()
        await seed.close()
        turso_client = _TrackingTursoClient(str(turso_path))
        turso_db = Database(str(turso_path), turso_url="https://continuity.turso.io")
        turso_db._client = turso_client
        self.addAsyncCleanup(turso_db.close)

        t0 = datetime(2026, 11, 13, 8, 0, tzinfo=timezone.utc)
        for label, db in (("sqlite", self.db), ("turso", turso_db)):
            with self.subTest(backend=label):
                await self._seed_active_sole_member_m1(db, t0=t0)
                await db.link_user(20, "Millers")

                # Inject cancellation right after _prepare_profile_source_addition_tx inside set_guild_feed_preference
                original_prepare = db._prepare_profile_source_addition_tx

                async def cancelling_prepare(*args, **kwargs):
                    res = await original_prepare(*args, **kwargs)
                    raise asyncio.CancelledError("cancelled mid-tx")

                with patch.object(db, "_prepare_profile_source_addition_tx", side_effect=cancelling_prepare):
                    with self.assertRaises(asyncio.CancelledError):
                        await db.set_guild_feed_preference(1, 20, True, member_ids=[20])

                # Everything rolled back cleanly and lock is free
                self.assertFalse(db._operation_lock.locked())
                self.assertTrue(await db.has_feed_source(1, "millers", "activity-display-v1"))
                self.assertFalse(await db.get_guild_feed_preference(1, 20))

                # Concurrent addition (member 20 with unknown snapshot) vs removal (member 10 unlink) with Events
                prep_started = asyncio.Event()
                continue_add = asyncio.Event()
                unlink_started = asyncio.Event()
                orig_prepare_real = db._prepare_profile_source_addition_tx
                orig_unlink = db.unlink_user

                async def gated_prepare(tx, guild_id, profile_key, *, present_member_ids=None):
                    res = await orig_prepare_real(tx, guild_id, profile_key, present_member_ids=present_member_ids)
                    prep_started.set()
                    await continue_add.wait()
                    return res

                async def gated_unlink(discord_id: int):
                    unlink_started.set()
                    return await orig_unlink(discord_id)

                with patch.object(db, "_prepare_profile_source_addition_tx", side_effect=gated_prepare):
                    add_task = asyncio.create_task(
                        db.set_guild_feed_preference(1, 20, True, member_ids=None)
                    )
                    await asyncio.wait_for(prep_started.wait(), timeout=2.0)
                    rem_task = asyncio.create_task(gated_unlink(10))
                    await asyncio.wait_for(unlink_started.wait(), timeout=2.0)
                    continue_add.set()
                    await asyncio.gather(add_task, rem_task)

                pending = await db._fetchone(
                    "SELECT candidate_member_ids FROM guild_profile_feed_reconciliation WHERE guild_id = 1 AND profile_key = 'millers'"
                )
                self.assertIsNotNone(pending)
                self.assertEqual(json.loads(pending["candidate_member_ids"]), [])
                self.assertFalse(db._operation_lock.locked())


if __name__ == "__main__":
    unittest.main()
