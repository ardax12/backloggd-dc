"""Regression tests for Task 8 bounded recurring poll work, bulk lookups, and safe pending recovery."""

import asyncio
import json
import os
import sqlite3
import tempfile
import unittest
from dataclasses import asdict, replace
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

os.environ["PYTHON_DOTENV_DISABLED"] = "1"
os.environ["TURSO_DATABASE_URL"] = ""
os.environ["TURSO_AUTH_TOKEN"] = ""
os.environ["DEV_GUILD_ID"] = ""

import discord

from bot.cogs.feed import FeedCog
from bot.db import Database, _MAX_SQL_BIND_PARAMS
from bot.feed_models import FeedEvent
from tests.test_feed_batch_writes import CountingTursoClient


def _make_channel(channel_id: int, guild_id: int | None = None):
    channel = AsyncMock(spec=discord.TextChannel)
    channel.id = channel_id
    if guild_id is not None:
        channel.guild = SimpleNamespace(id=guild_id)
    perms = SimpleNamespace(view_channel=True, send_messages=True, embed_links=True)
    channel.permissions_for = lambda member: perms
    next_msg_id = {"value": 9000}

    async def _send(**kwargs):
        next_msg_id["value"] += 1
        return SimpleNamespace(id=next_msg_id["value"])

    channel.send = AsyncMock(side_effect=_send)
    channel._edits = {}

    def _get_partial(message_id):
        if message_id not in channel._edits:
            channel._edits[message_id] = AsyncMock()
        return SimpleNamespace(edit=channel._edits[message_id])

    channel.get_partial_message = _get_partial
    return channel


def _make_guild(guild_id: int, member_ids: list[int], channel):
    members = [SimpleNamespace(id=mid) for mid in member_ids]
    me = SimpleNamespace(id=999)
    channels = {channel.id: channel} if channel is not None else {}
    return SimpleNamespace(
        id=guild_id,
        me=me,
        member_count=len(members),
        chunked=True,
        members=members,
        get_member=lambda mid: next((m for m in members if m.id == mid), None),
        get_channel=lambda cid: channels.get(cid),
    )


def _activity_event(
    idx: int,
    *,
    occurred_at: str = "2026-11-01T10:00:00Z",
    played_date: str | None = None,
    rating: float | None = None,
) -> FeedEvent:
    action = "rated" if rating is not None else "completed"
    if played_date is None:
        played_date = "November, 2026 16" if occurred_at.startswith("2026-11-16") else "November, 2026 01"
    return FeedEvent(
        activity_id=f"act_{idx}",
        game_title=f"Game {idx}",
        game_slug=f"game-{idx}",
        cover_url=None,
        action_type=action,
        action_text=f"{action} Game {idx}",
        rating=rating,
        review_notes=None,
        date_str=occurred_at[:10],
        played_date=played_date,
        occurred_at=occurred_at,
        source="activity",
        event_ids=(f"act_{idx}",),
        reference=f"ref_{idx}",
    )


def _journal_entry(idx: int, *, played_date: str = "November, 2026 01") -> SimpleNamespace:
    return SimpleNamespace(
        entry_id=f"j_{idx}",
        game_title=f"Journal Game {idx}",
        game_slug=f"j-game-{idx}",
        cover_url=None,
        played_date=played_date,
        platform="PC",
        status="Played",
        rating=None,
        review_notes=None,
        hours=None,
        is_replay=False,
        session_labels=("Finished",),
    )


class FeedPollScalingTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmpdir.cleanup)
        self.now = datetime(2026, 11, 16, 12, 0, tzinfo=timezone.utc)

        self.sqlite_path = Path(self.tmpdir.name) / "poll_sqlite.db"
        self.db = Database(str(self.sqlite_path), turso_url="")
        self.addAsyncCleanup(self.db.close)
        await self.db.connect()
        await self.db._conn._execute(
            self.db._conn._conn.setlimit, sqlite3.SQLITE_LIMIT_VARIABLE_NUMBER, 999
        )

        self.turso_path = Path(self.tmpdir.name) / "poll_turso.db"
        seed = Database(str(self.turso_path), turso_url="")
        await seed.connect()
        await seed.close()
        self.turso_client = CountingTursoClient(str(self.turso_path))
        self.turso_db = Database(str(self.turso_path), turso_url="https://poll-scale.turso.io")
        self.turso_db._client = self.turso_client
        self.addAsyncCleanup(self.turso_db.close)

    async def _open_isolated_backend(self, label: str, tag: str):
        path = Path(self.tmpdir.name) / f"{label}_{tag}.db"
        if label == "sqlite":
            db = Database(str(path), turso_url="")
            await db.connect()
            await db._conn._execute(
                db._conn._conn.setlimit, sqlite3.SQLITE_LIMIT_VARIABLE_NUMBER, 999
            )
            return db, None
        seed = Database(str(path), turso_url="")
        await seed.connect()
        await seed.close()
        client = CountingTursoClient(str(path))
        db = Database(str(path), turso_url=f"https://{tag}.turso.io")
        db._client = client
        return db, client

    def _instrument_backend(self, backend_label: str, db: Database, turso_client=None):
        counts = {"ops": 0, "updates_on_messages": 0, "seed_tx": 0}
        orig_execute = db._execute
        orig_fetchone = db._fetchone
        orig_fetchall = db._fetchall
        orig_rowcount = db._rowcount_execute
        orig_batch = db._execute_batch_results

        def _track_sql(sql: str):
            norm = " ".join(sql.upper().split())
            if "UPDATE PROFILE_FEED_MESSAGES SET DELIVERY_STATE = 'UNKNOWN'" in norm and "EVENT_ID IN" not in norm:
                counts["updates_on_messages"] += 1

        active_turso = turso_client or self.turso_client
        if backend_label == "turso":
            active_turso.reset_counters()

        async def wrapped_execute(query, params=()):
            counts["ops"] += 1
            _track_sql(query)
            return await orig_execute(query, params)

        async def wrapped_fetchone(query, params=()):
            counts["ops"] += 1
            return await orig_fetchone(query, params)

        async def wrapped_fetchall(query, params=()):
            counts["ops"] += 1
            return await orig_fetchall(query, params)

        async def wrapped_rowcount(query, params=()):
            counts["ops"] += 1
            _track_sql(query)
            return await orig_rowcount(query, params)

        async def wrapped_batch(statements):
            counts["ops"] += len(statements)
            for q, _ in statements:
                _track_sql(q)
            return await orig_batch(statements)

        orig_seed = db.seed_feed_source_with_messages

        async def wrapped_seed(*args, **kwargs):
            counts["seed_tx"] += 1
            return await orig_seed(*args, **kwargs)

        db._execute = wrapped_execute
        db._fetchone = wrapped_fetchone
        db._fetchall = wrapped_fetchall
        db._rowcount_execute = wrapped_rowcount
        db._execute_batch_results = wrapped_batch
        db.seed_feed_source_with_messages = wrapped_seed

        def restore():
            db._execute = orig_execute
            db._fetchone = orig_fetchone
            db._fetchall = orig_fetchall
            db._rowcount_execute = orig_rowcount
            db._execute_batch_results = orig_batch
            db.seed_feed_source_with_messages = orig_seed
            if backend_label == "turso":
                counts["turso_remote_ops"] = (
                    active_turso.direct_execute_calls
                    + active_turso.tx_execute_calls
                    + active_turso.batch_statement_count
                )
            return counts

        return restore

    async def test_identical_polls_over_0_20_1500_baseline_events_skip_writes_and_bound_work(self):
        for label in ("sqlite", "turso"):
            for size in (0, 20, 1500):
                with self.subTest(backend=label, size=size):
                    db, turso_client = await self._open_isolated_backend(label, f"poll_{size}")
                    try:
                        gid = size + 10
                        uid = size + 100
                        uname = f"PollUser{size}"
                        await db.link_user(uid, uname)
                        await db.set_feed_channel(gid, gid * 10)
                        await db.set_guild_feed_preference(gid, uid, True)

                        act_count = size // 2
                        jrn_count = size - act_count
                        activities = [_activity_event(i) for i in range(act_count)]
                        journals = [_journal_entry(i) for i in range(jrn_count)]
                        api = SimpleNamespace(
                            get_user_activity_snapshot=AsyncMock(
                                return_value=SimpleNamespace(
                                    entries=activities,
                                    coverage="exhausted",
                                    pages_read=1,
                                )
                            ),
                            get_user_journal=AsyncMock(return_value=journals),
                        )
                        channel = _make_channel(gid * 10, gid)
                        guild = _make_guild(gid, [uid], channel)
                        lib_sync = SimpleNamespace(
                            is_refreshing=MagicMock(return_value=False),
                            request=MagicMock(return_value=None),
                        )
                        bot = SimpleNamespace(
                            db=db,
                            backloggd=api,
                            library_sync=lib_sync,
                            get_guild=lambda req_gid, g=guild, target=gid: g if req_gid == target else None,
                        )
                        cog = FeedCog(bot, clock=lambda: self.now)

                        with patch("bot.cogs.feed.asyncio.sleep", new=AsyncMock()):
                            await cog.journal_feed_loop.coro(cog)

                        channel.send.assert_not_awaited()
                        self.assertTrue(await db.has_feed_source(gid, uname.lower(), "activity-display-v1"))
                        self.assertTrue(await db.has_feed_source(gid, uname.lower(), "journal-display-v1"))
                        # Simulate Poll 1's triggered initial library sync finishing cleanly before steady-state Poll 2.
                        await db.clear_profile_refresh_pending(uname.lower())

                        # Poll 2: identical history must perform zero baseline writes, zero unconditional pending UPDATE,
                        # zero Discord sends/edits, zero unnecessary sync refreshes, and O(1) DB reads & JSON dumps.
                        lib_sync.request.reset_mock()
                        restore_db = self._instrument_backend(label, db, turso_client)
                        json_dumps_calls = 0
                        orig_dumps = json.dumps

                        def tracked_dumps(*args, **kwargs):
                            nonlocal json_dumps_calls
                            json_dumps_calls += 1
                            return orig_dumps(*args, **kwargs)

                        try:
                            with patch("bot.cogs.feed.json.dumps", side_effect=tracked_dumps), \
                                 patch("bot.cogs.feed.asyncio.sleep", new=AsyncMock()):
                                await cog.journal_feed_loop.coro(cog)
                        finally:
                            counts = restore_db()

                        print(f"[POLL2 {label} size={size}] counts={counts} json_dumps={json_dumps_calls}")
                        channel.send.assert_not_awaited()
                        lib_sync.request.assert_not_called()
                        self.assertEqual(counts["seed_tx"], 0, "Initialized sources must skip baseline seed_tx")
                        self.assertEqual(counts["updates_on_messages"], 0, "Poll reads must not run unconditional pending UPDATE")
                        chunk_limit = _MAX_SQL_BIND_PARAMS - 2
                        expected_chunks = ((size + chunk_limit - 1) // chunk_limit) if size else 0
                        self.assertLessEqual(
                            counts["ops"],
                            15 + expected_chunks,
                            f"Expected O(1) + chunked decision reads for size={size}, got {counts}",
                        )
                        if label == "turso":
                            self.assertLessEqual(
                                counts["turso_remote_ops"],
                                20 + expected_chunks * 2,
                                f"Expected bounded remote ops for size={size}, got {counts['turso_remote_ops']}",
                            )
                        self.assertLessEqual(json_dumps_calls, size)
                    finally:
                        await db.close()


    async def test_edited_event_and_alias_learning_among_1500_history_still_update_and_bound_work(self):
        await self.db.link_user(10, "Millers")
        await self.db.set_feed_channel(1, 100)
        await self.db.set_guild_feed_preference(1, 10, True)

        # Activate guild at 11:00
        cog_now = datetime(2026, 11, 16, 11, 0, tzinfo=timezone.utc)
        baseline_acts = [_activity_event(i, occurred_at="2026-11-01T10:00:00Z") for i in range(1499)]
        # Make one baseline event have a stable reference so it can learn a new alias on Poll 2
        baseline_acts[500] = replace(baseline_acts[500], reference="stable-ref-500")
        sent_event = FeedEvent(
            activity_id="act_sent_1",
            game_title="Hades",
            game_slug="hades",
            cover_url=None,
            action_type="rated",
            action_text="rated Hades",
            rating=4.0,
            review_notes=None,
            date_str="2026-11-16",
            played_date="November, 2026 16",
            occurred_at="2026-11-16T11:30:00Z",
            source="activity",
            event_ids=("act_sent_1",),
            reference="ref_sent_1",
        )

        # Poll 1: baseline 1499 history
        api = SimpleNamespace(
            get_user_activity_snapshot=AsyncMock(
                return_value=SimpleNamespace(entries=baseline_acts, coverage="exhausted", pages_read=1)
            ),
            get_user_journal=AsyncMock(return_value=[]),
        )
        channel = _make_channel(100, 1)
        guild = _make_guild(1, [10], channel)
        bot = SimpleNamespace(db=self.db, backloggd=api, get_guild=lambda gid: guild if gid == 1 else None)
        cog = FeedCog(bot, clock=lambda: cog_now)
        with patch("bot.cogs.feed.asyncio.sleep", new=AsyncMock()):
            await cog.journal_feed_loop.coro(cog)
        channel.send.assert_not_awaited()

        # Poll 2: send 1 new eligible event act_sent_1
        cog_now = datetime(2026, 11, 16, 11, 35, tzinfo=timezone.utc)
        api.get_user_activity_snapshot.return_value = SimpleNamespace(
            entries=[sent_event, *baseline_acts], coverage="exhausted", pages_read=1
        )
        with patch("bot.cogs.feed.asyncio.sleep", new=AsyncMock()):
            await cog.journal_feed_loop.coro(cog)
        channel.send.assert_awaited_once()
        sent_row = {r["event_id"]: r for r in await self.db.get_feed_messages(1, "millers")}["act_sent_1"]
        msg_id = sent_row["message_id"]

        # Poll 3: edit act_sent_1 (rating 4.0 -> 5.0), learn new alias for act_500, add 1 new recent event & 1 old (>7d) event
        cog_now = datetime(2026, 11, 16, 12, 0, tzinfo=timezone.utc)
        edited_sent = replace(sent_event, rating=5.0, review_notes="Masterpiece")
        alias_for_500 = replace(baseline_acts[500], activity_id="act_500_alias", event_ids=("act_500_alias",), rating=4.5)
        new_recent = _activity_event(9001, occurred_at="2026-11-16T11:55:00Z")
        old_event = _activity_event(
            9002, occurred_at="2026-11-16T11:50:00Z", played_date="October, 2026 01"
        )
        poll3_acts = [new_recent, edited_sent, old_event, alias_for_500, *baseline_acts[:500], *baseline_acts[501:]]
        api.get_user_activity_snapshot.return_value = SimpleNamespace(
            entries=poll3_acts, coverage="exhausted", pages_read=1
        )

        restore_db = self._instrument_backend("sqlite", self.db)
        try:
            with patch("bot.cogs.feed.asyncio.sleep", new=AsyncMock()):
                await cog.journal_feed_loop.coro(cog)
        finally:
            counts = restore_db()

        print(f"[POLL3 sqlite edited+alias+1500] counts={counts}")
        # sent_event edited once; new_recent sent once; old_event suppressed; alias_for_500 baseline-retained not posted
        channel._edits[msg_id].assert_awaited_once()
        self.assertEqual(channel.send.await_count, 2)  # initial + new_recent only
        msgs = {r["event_id"]: r for r in await self.db.get_feed_messages(1, "millers")}
        self.assertIn("act_500_alias", msgs)
        self.assertIsNone(msgs["act_500_alias"]["message_id"])
        self.assertEqual(json.loads(msgs["act_sent_1"]["payload"])["previous_rating"], 4.0)
        old_dec = await self.db.get_feed_event_decision(1, "millers", "act_9002")
        self.assertEqual(old_dec["decision"], "suppressed_old")
        self.assertEqual(counts["seed_tx"], 0)
        self.assertEqual(counts["updates_on_messages"], 0)
        self.assertLessEqual(counts["ops"], 28)

    async def test_bulk_journal_delivered_and_decision_lookups_avoid_per_event_queries(self):
        from bot.feed_rules import journal_events
        journals = [_journal_entry(i, played_date="November, 2026 15") for i in range(900)]
        events = journal_events(journals, "Millers")
        seen_rows = [(1, "millers", e.activity_id) for e in events]

        for label, db in (("sqlite", self.db), ("turso", self.turso_db)):
            with self.subTest(backend=label):
                await db.link_user(10, "Millers")
                await db.seed_feed_source(1, "millers", "activity-display-v1", [])
                await db.seed_feed_source(1, "millers", "journal-display-v1", [])

                # Seed 900 journal IDs in profile_seen_journal_deliveries WITHOUT rows in profile_feed_messages
                conn = db._client.conn if db.is_turso else db._conn
                if db.is_turso:
                    conn.executemany(
                        "INSERT INTO profile_seen_journal_deliveries (guild_id, profile_key, entry_id) VALUES (?, ?, ?)",
                        seen_rows,
                    )
                    conn.commit()
                else:
                    await conn.executemany(
                        "INSERT INTO profile_seen_journal_deliveries (guild_id, profile_key, entry_id) VALUES (?, ?, ?)",
                        seen_rows,
                    )
                    await conn.commit()

                channel = _make_channel(100, 1)
                cog = FeedCog(SimpleNamespace(db=db), clock=lambda: self.now)

                single_delivered_calls = 0
                single_decision_calls = 0
                orig_deliv = db.is_journal_entry_delivered
                orig_dec = db.get_feed_event_decision

                async def count_deliv(*a, **kw):
                    nonlocal single_delivered_calls
                    single_delivered_calls += 1
                    return await orig_deliv(*a, **kw)

                async def count_dec(*a, **kw):
                    nonlocal single_decision_calls
                    single_decision_calls += 1
                    return await orig_dec(*a, **kw)

                db.is_journal_entry_delivered = count_deliv
                db.get_feed_event_decision = count_dec
                if label == "turso":
                    self.turso_client.reset_counters()
                try:
                    with patch("bot.cogs.feed.asyncio.sleep", new=AsyncMock()):
                        await cog._publish_sources(1, "millers", "Millers", None, channel, {"activity": [], "journal": events})
                finally:
                    db.is_journal_entry_delivered = orig_deliv
                    db.get_feed_event_decision = orig_dec

                self.assertEqual(single_delivered_calls, 0, "Expected bulk delivered lookup instead of 900 single queries")
                self.assertEqual(single_decision_calls, 0, "Expected bulk decision lookup instead of 900 single queries")
                channel.send.assert_not_awaited()
                if label == "turso":
                    self.assertLessEqual(self.turso_client.max_params_seen, _MAX_SQL_BIND_PARAMS)

    async def test_multi_guild_shared_profile_reuses_health_and_checks_legacy_once_per_poll(self):
        await self.db.link_user(10, "Millers")
        await self.db.set_feed_channel(1, 101)
        await self.db.set_feed_channel(2, 201)
        await self.db.set_feed_channel(3, 301)
        for gid in (1, 2, 3):
            await self.db.set_guild_feed_preference(gid, 10, True)

        ch1 = _make_channel(101, 1)
        ch2 = _make_channel(201, 2)
        g1 = _make_guild(1, [10], ch1)
        g2 = _make_guild(2, [10], ch2)
        g3_unavail = _make_guild(3, [10], None)  # channel 301 unavailable

        # Baseline guilds 1, 2, 3 at 11:00 so subsequent 11:55/11:58 events occur after activation_at
        poll_now = datetime(2026, 11, 16, 11, 0, tzinfo=timezone.utc)
        ev_old = _activity_event(1, occurred_at="2026-11-01T10:00:00Z")
        api = SimpleNamespace(
            get_user_activity_snapshot=AsyncMock(
                return_value=SimpleNamespace(entries=[ev_old], coverage="exhausted", pages_read=1)
            ),
            get_user_journal=AsyncMock(return_value=[]),
        )
        guilds = {1: g1, 2: g2, 3: g3_unavail}
        bot = SimpleNamespace(db=self.db, backloggd=api, get_guild=lambda gid: guilds.get(gid))
        cog = FeedCog(bot, clock=lambda: poll_now)

        # Warm up activity row for g3 by temporarily making g3 available, then unavailable
        ch3 = _make_channel(301, 3)
        guilds[3] = _make_guild(3, [10], ch3)
        with patch("bot.cogs.feed.asyncio.sleep", new=AsyncMock()):
            await cog.journal_feed_loop.coro(cog)
        guilds[3] = g3_unavail

        # Next poll: share 1 profile across 3 guilds (1 unavailable)
        poll_now = self.now
        ev_new = _activity_event(2, occurred_at="2026-11-16T11:55:00Z")
        api.get_user_activity_snapshot.reset_mock()
        api.get_user_journal.reset_mock()
        api.get_user_activity_snapshot.return_value = SimpleNamespace(
            entries=[ev_new, ev_old], coverage="boundary_reached", pages_read=1
        )

        legacy_calls = 0
        health_calls = 0
        orig_legacy = self.db.get_legacy_feed_guilds_pending
        orig_health = self.db.get_profile_feed_source

        async def count_legacy():
            nonlocal legacy_calls
            legacy_calls += 1
            return await orig_legacy()

        async def count_health(*a, **kw):
            nonlocal health_calls
            health_calls += 1
            return await orig_health(*a, **kw)

        self.db.get_legacy_feed_guilds_pending = count_legacy
        self.db.get_profile_feed_source = count_health
        try:
            with patch("bot.cogs.feed.asyncio.sleep", new=AsyncMock()):
                await cog.journal_feed_loop.coro(cog)
        finally:
            self.db.get_legacy_feed_guilds_pending = orig_legacy
            self.db.get_profile_feed_source = orig_health

        api.get_user_activity_snapshot.assert_awaited_once()
        api.get_user_journal.assert_awaited_once()
        self.assertEqual(legacy_calls, 1, "Legacy pending-guild lookup must happen once per poll pass")
        self.assertLessEqual(health_calls, 2, "Source health must not be re-read for every target guild")
        ch1.send.assert_awaited_once()
        ch2.send.assert_awaited_once()
        health = await self.db.get_profile_feed_source("millers", "activity")
        self.assertEqual(health["last_continuous_boundary_event_id"], "act_1")
        self.assertEqual(health["gap_reason"], "unavailable_feed_target")

        # Cross-poll preference change must NOT be skipped by stale cache: opt out guild 2 cleanly.
        await self.db.set_guild_feed_preference(2, 10, False)
        ev_new2 = _activity_event(3, occurred_at="2026-11-16T11:58:00Z")
        api.get_user_activity_snapshot.return_value = SimpleNamespace(
            entries=[ev_new2, ev_new, ev_old], coverage="boundary_reached", pages_read=1
        )
        with patch("bot.cogs.feed.asyncio.sleep", new=AsyncMock()):
            await cog.journal_feed_loop.coro(cog)
        self.assertEqual(ch1.send.await_count, 2)
        self.assertEqual(ch2.send.await_count, 1)

    async def test_live_pending_intent_survives_concurrent_reader_and_recovers_only_after_restart(self):
        await self.db.link_user(10, "Millers")
        await self.db.seed_feed_source(1, "millers", "activity-display-v1", [])
        await self.db.seed_feed_source(1, "millers", "journal-display-v1", [])

        ev = _activity_event(77, occurred_at="2026-11-16T11:55:00Z")
        send_started = asyncio.Event()
        allow_send_finish = asyncio.Event()

        async def slow_send(**kwargs):
            send_started.set()
            await allow_send_finish.wait()
            raise discord.HTTPException(SimpleNamespace(status=400, reason="Bad Request"), "rejected")

        channel = _make_channel(100, 1)
        channel.send = AsyncMock(side_effect=slow_send)
        cog = FeedCog(SimpleNamespace(db=self.db), clock=lambda: self.now)

        with patch("bot.cogs.feed.asyncio.sleep", new=AsyncMock()):
            publish_task = asyncio.create_task(
                cog._publish_sources(1, "millers", "Millers", None, channel, {"activity": [ev], "journal": []})
            )
            try:
                await asyncio.wait_for(send_started.wait(), timeout=5.0)
            except BaseException:
                allow_send_finish.set()
                publish_task.cancel()
                await asyncio.gather(publish_task, return_exceptions=True)
                raise

            # Concurrent reader requests messages while send is live in-flight!
            concurrent_rows = {r["event_id"]: r for r in await self.db.get_feed_messages(1, "millers")}
            self.assertEqual(concurrent_rows["act_77"]["delivery_state"], "pending")

            allow_send_finish.set()
            published_ok = await publish_task
            self.assertFalse(published_ok)

        # Because concurrent read did NOT clobber 'pending' -> 'unknown', 400 release_feed_delivery
        # cleanly removed the pending row so it remains retryable!
        after_reject = await self.db.get_feed_messages(1, "millers")
        self.assertEqual(after_reject, [])

        # Contrast with process restart: stale pending row from previous run recovers to 'unknown' once.
        self.assertTrue(await self.db.begin_feed_delivery(1, "millers", ["act_crashed"], "{}", 100))
        await self.db.close()
        await self.db.connect()
        recovered = {r["event_id"]: r for r in await self.db.get_feed_messages(1, "millers")}
        self.assertEqual(recovered["act_crashed"]["delivery_state"], "unknown")
        self.assertFalse(await self.db.begin_feed_delivery(1, "millers", ["act_crashed"], "{}", 100))


if __name__ == "__main__":
    unittest.main()
