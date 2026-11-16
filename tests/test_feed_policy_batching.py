"""Regression tests for Task 6 bulk policy decision reads (and Task 7 batch writes later)."""

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
from unittest.mock import AsyncMock

os.environ["PYTHON_DOTENV_DISABLED"] = "1"
os.environ["TURSO_DATABASE_URL"] = ""
os.environ["TURSO_AUTH_TOKEN"] = ""
os.environ["DEV_GUILD_ID"] = ""

from bot.cogs.feed import FeedCog
from bot.db import Database, _MAX_SQL_BATCH_BYTES, _MAX_SQL_BIND_PARAMS
from bot.feed_models import FeedEvent
from tests.test_feed_batch_writes import CountingTursoClient


def _make_event(
    event_id: str,
    *,
    action: str = "completed",
    slug: str = "hades",
    title: str = "Hades",
    played_date: str | None = "2026-11-12",
    occurred_at: str | None = "2026-11-12T10:00:00Z",
    rating: float | None = None,
    review_notes: str | None = None,
    source: str = "activity",
    event_ids: tuple[str, ...] | None = None,
) -> FeedEvent:
    return FeedEvent(
        activity_id=event_id,
        game_title=title,
        game_slug=slug,
        cover_url=None,
        action_type=action,
        action_text=f"{action} {title}",
        rating=rating,
        review_notes=review_notes,
        date_str="2026-11-12",
        played_date=played_date,
        occurred_at=occurred_at,
        source=source,
        event_ids=event_ids or (event_id,),
    )


class FeedPolicyReadBatchingTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmpdir.cleanup)

        self.sqlite_path = Path(self.tmpdir.name) / "policy_sqlite.db"
        self.db = Database(str(self.sqlite_path), turso_url="")
        self.addAsyncCleanup(self.db.close)
        await self.db.connect()
        await self.db._conn._execute(
            self.db._conn._conn.setlimit, sqlite3.SQLITE_LIMIT_VARIABLE_NUMBER, 999
        )

        self.turso_path = Path(self.tmpdir.name) / "policy_turso.db"
        seed = Database(str(self.turso_path), turso_url="")
        await seed.connect()
        await seed.close()
        self.turso_client = CountingTursoClient(str(self.turso_path))
        self.turso_db = Database(str(self.turso_path), turso_url="https://policy-test.turso.io")
        self.turso_db._client = self.turso_client
        self.addAsyncCleanup(self.turso_db.close)

    async def test_bulk_get_feed_event_decisions_parity_empty_dups_chunking_and_isolation(self):
        for label, db in (("sqlite", self.db), ("turso", self.turso_db)):
            with self.subTest(backend=label):
                await db.link_user(10, "Millers")
                await db.link_user(20, "Other")

                # Empty input makes 0 backend reads
                if label == "turso":
                    self.turso_client.reset_counters()
                self.assertEqual(await db.get_feed_event_decisions(1, "millers", []), {})
                self.assertEqual(await db.get_feed_event_decisions(1, "millers", ["", None]), {})
                if label == "turso":
                    self.assertEqual(self.turso_client.direct_execute_calls, 0)

                # Populate 850 decisions for (1, "millers") plus cross-guild and cross-profile traps
                dec_rows = []
                decisions_cycle = (
                    "eligible",
                    "suppressed_old",
                    "held_future",
                    "held_uncertain",
                    "held_ambiguous",
                    "baseline",
                )
                for i in range(850):
                    dec_rows.append((
                        1,
                        "millers",
                        f"ev-{i:04d}",
                        "journal" if i % 3 == 0 else "activity",
                        f"2026-11-{(i % 28) + 1:02d}",
                        decisions_cycle[i % len(decisions_cycle)],
                        f"reason-{i}",
                        json.dumps({"i": i}),
                        "2026-11-12 09:00:00",
                    ))
                # Traps with matching event_ids in another guild / another profile
                dec_rows.append((2, "millers", "ev-0000", "activity", "2026-11-01", "suppressed_old", "other_guild", "{}", "2026-11-12 09:00:00"))
                dec_rows.append((1, "other", "ev-0000", "activity", "2026-11-01", "suppressed_old", "other_profile", "{}", "2026-11-12 09:00:00"))

                if db.is_turso:
                    db._client.conn.executemany(
                        """INSERT INTO profile_feed_decisions
                           (guild_id, profile_key, event_id, source, event_date, decision, reason, payload, observed_at)
                           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                        dec_rows,
                    )
                    db._client.conn.commit()
                else:
                    await db._conn.executemany(
                        """INSERT INTO profile_feed_decisions
                           (guild_id, profile_key, event_id, source, event_date, decision, reason, payload, observed_at)
                           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                        dec_rows,
                    )
                    await db._conn.commit()

                # Duplicates & empty strings collapse cleanly
                if label == "turso":
                    self.turso_client.reset_counters()
                dup_map = await db.get_feed_event_decisions(1, "MiLLeRs", ["ev-0000", "ev-0000", "", "absent-id"])
                self.assertEqual(set(dup_map.keys()), {"ev-0000"})
                if label == "turso":
                    self.assertEqual(self.turso_client.direct_execute_calls, 1)

                # 850 unique aliases + absent -> 2 chunked queries (850 > 800-2=798)
                query_keys = [f"ev-{i:04d}" for i in range(850)] + ["missing-1"]
                if label == "turso":
                    self.turso_client.reset_counters()
                bulk_map = await db.get_feed_event_decisions(1, "millers", query_keys)
                if label == "turso":
                    self.assertEqual(self.turso_client.direct_execute_calls, 2)
                    self.assertLessEqual(self.turso_client.max_params_seen, _MAX_SQL_BIND_PARAMS)

                self.assertEqual(len(bulk_map), 850)
                for idx in (0, 1, 400, 797, 798, 849):
                    eid = f"ev-{idx:04d}"
                    single = await db.get_feed_event_decision(1, "millers", eid)
                    got = bulk_map[eid]
                    for field in ("source", "event_date", "decision", "reason", "payload", "observed_at"):
                        self.assertEqual(got[field], single[field])
                    self.assertEqual(got["reason"], f"reason-{idx}")

    async def test_apply_discovery_policy_reads_scale_by_chunks_and_handle_overlapping_aliases(self):
        now = datetime(2026, 11, 12, 12, 0, tzinfo=timezone.utc)
        await self.db.link_user(10, "Millers")
        cog = FeedCog(SimpleNamespace(db=self.db), clock=lambda: now)

        # Empty poll makes 0 DB reads
        read_queries = 0
        orig_fetchone = self.db._fetchone
        orig_fetchall = self.db._fetchall

        async def count_fetchone(sql, params=()):
            nonlocal read_queries
            if "profile_feed_decisions" in sql:
                read_queries += 1
            return await orig_fetchone(sql, params)

        async def count_fetchall(sql, params=()):
            nonlocal read_queries
            if "profile_feed_decisions" in sql:
                read_queries += 1
            return await orig_fetchall(sql, params)

        self.db._fetchone = count_fetchone
        self.db._fetchall = count_fetchall
        try:
            self.assertEqual(await cog._apply_discovery_policy(1, "millers", [], {}, now), [])
            self.assertEqual(read_queries, 0)

            # Seed 820 stored decisions for (1, "millers") with some conflicting alias priorities
            seed_rows = []
            for i in range(818):
                seed_rows.append((
                    1, "millers", f"stored-{i:04d}", "activity", "2026-11-12",
                    "eligible" if i % 2 == 0 else "suppressed_old",
                    "seed", json.dumps(asdict(_make_event(f"stored-{i:04d}"))), "2026-11-12 09:00:00",
                ))
            # Conflicting priority aliases: alias "prio-elig" is eligible, "prio-supp" is suppressed_old -> must pick suppressed_old
            seed_rows.append((1, "millers", "prio-elig", "activity", "2026-11-12", "eligible", "r1", json.dumps(asdict(_make_event("prio-elig"))), "2026-11-12 09:00:00"))
            seed_rows.append((1, "millers", "prio-supp", "journal", "2026-11-01", "suppressed_old", "r2", json.dumps(asdict(_make_event("prio-supp"))), "2026-11-12 09:00:00"))
            await self.db._conn.executemany(
                """INSERT INTO profile_feed_decisions
                   (guild_id, profile_key, event_id, source, event_date, decision, reason, payload, observed_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                seed_rows,
            )
            await self.db._conn.commit()

            # Build 830 events:
            # - 818 stored events
            # - 1 conflicting priority event ("prio-elig", "prio-supp")
            # - 1 saved terminal event ("already-sent") -> skipped before DB decision read
            # - 5 policy variety new events (old, 7-day, future, uncertain, old explicit rating)
            # - Overlapping same-poll events:
            #     overlap-1 creates ("overlap-1", "shared-old") as suppressed_old
            #     overlap-2 later in same poll reuses "shared-old" -> must inherit suppressed_old from request-local map!
            #     overlap-3 creates ("overlap-3", "shared-tighten") as eligible (played today)
            #     overlap-4 later in same poll reuses "shared-tighten" with played_date 8 days old -> must tighten to suppressed_old!
            events = [
                _make_event(f"stored-{i:04d}") for i in range(818)
            ] + [
                _make_event("prio-elig", event_ids=("prio-elig", "prio-supp")),
                _make_event("already-sent"),
                _make_event("new-old", played_date="2026-11-04", occurred_at="2026-11-04T10:00:00Z"),
                _make_event("new-7d", played_date="2026-11-05", occurred_at="2026-11-05T10:00:00Z"),
                _make_event("new-future", played_date="2026-11-13", occurred_at="2026-11-13T10:00:00Z"),
                _make_event("new-uncertain", played_date=None, occurred_at=None),
                _make_event("new-old-rating", action="rated", rating=4.5, played_date="2020-01-01", occurred_at="2020-01-01T00:00:00Z"),
                _make_event("overlap-1", played_date="2026-11-01", occurred_at="2026-11-01T10:00:00Z", event_ids=("overlap-1", "shared-old")),
                _make_event("overlap-2", played_date="2026-11-12", occurred_at="2026-11-12T10:00:00Z", event_ids=("overlap-2", "shared-old")),
                _make_event("overlap-3", played_date="2026-11-12", occurred_at="2026-11-12T10:00:00Z", event_ids=("overlap-3", "shared-tighten")),
                _make_event("overlap-4", played_date="2026-11-01", occurred_at="2026-11-01T10:00:00Z", event_ids=("overlap-4", "shared-tighten")),
            ]
            saved = {
                "already-sent": {"message_id": 777, "delivery_state": "sent", "payload": "{}"},
            }

            read_queries = 0
            # Note: In Task 6 (before Task 7), record_feed_event_decision still calls _fetchone internally once per insert;
            # here we count explicit get_feed_event_decision(s) SELECT queries in _apply_discovery_policy specifically,
            # OR wrap _fetchall/_fetchone excluding record_feed_event_decision's read-back.
            # Actually let's count get_feed_event_decision vs get_feed_event_decisions calls AND SELECT queries!
            orig_get_single = self.db.get_feed_event_decision
            single_calls = 0
            bulk_calls = 0

            async def track_single(*args, **kwargs):
                nonlocal single_calls
                single_calls += 1
                return await orig_get_single(*args, **kwargs)

            self.db.get_feed_event_decision = track_single
            if hasattr(self.db, "get_feed_event_decisions"):
                orig_get_bulk = self.db.get_feed_event_decisions

                async def track_bulk(*args, **kwargs):
                    nonlocal bulk_calls
                    bulk_calls += 1
                    return await orig_get_bulk(*args, **kwargs)

                self.db.get_feed_event_decisions = track_bulk

            try:
                accepted = await cog._apply_discovery_policy(1, "millers", events, saved, now)
            finally:
                self.db.get_feed_event_decision = orig_get_single

            # Before Task 6: single_calls is 820+ (and read_queries > 820).
            # After Task 6: _apply_discovery_policy performs 1 bulk lookup (2 chunked SELECTs) and 0 per-alias reads!
            self.assertEqual(bulk_calls, 1)
            self.assertLessEqual(read_queries, 30)  # zero per-stored-alias reads; at most new-insert readbacks in Task 6

            accepted_ids = [e.activity_id for e in accepted]
            self.assertIn("already-sent", accepted_ids)
            self.assertIn("new-7d", accepted_ids)
            self.assertIn("new-old-rating", accepted_ids)
            # Final alias-group suppression also protects the earlier same-poll representation.
            self.assertNotIn("overlap-3", accepted_ids)
            self.assertNotIn("prio-elig", accepted_ids)
            self.assertNotIn("new-old", accepted_ids)
            self.assertNotIn("new-future", accepted_ids)
            self.assertNotIn("new-uncertain", accepted_ids)
            self.assertNotIn("overlap-1", accepted_ids)
            self.assertNotIn("overlap-2", accepted_ids)
            self.assertNotIn("overlap-4", accepted_ids)

            # Verify overlapping alias state in DB
            self.assertEqual((await orig_get_single(1, "millers", "overlap-2"))["decision"], "suppressed_old")
            self.assertEqual((await orig_get_single(1, "millers", "shared-tighten"))["decision"], "suppressed_old")
            self.assertEqual((await orig_get_single(1, "millers", "overlap-4"))["decision"], "suppressed_old")

            # Verify no cross-poll stale cache: external DB change to held_ambiguous -> resolved is seen on next poll
            await self.db.record_feed_event_decision(
                1, "millers", "ext-ambig", source="activity", event_date="2026-11-12",
                decision="held_ambiguous", reason="ambig", payload=json.dumps(asdict(_make_event("ext-ambig"))),
            )
            ev_ext = _make_event("ext-ambig")
            first_pass = await cog._apply_discovery_policy(1, "millers", [ev_ext], {}, now)
            self.assertEqual(first_pass, [])
            await self.db.resolve_feed_event_decision(
                1, "millers", "ext-ambig", json.dumps(asdict(ev_ext)), "2026-11-12"
            )
            second_pass = await cog._apply_discovery_policy(1, "millers", [ev_ext], {}, now)
            self.assertEqual([e.activity_id for e in second_pass], ["ext-ambig"])
        finally:
            self.db._fetchone = orig_fetchone
            self.db._fetchall = orig_fetchall


class FeedPolicyWriteBatchingTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmpdir.cleanup)

        self.sqlite_path = Path(self.tmpdir.name) / "write_sqlite.db"
        self.db = Database(str(self.sqlite_path), turso_url="")
        self.addAsyncCleanup(self.db.close)
        await self.db.connect()
        await self.db._conn._execute(
            self.db._conn._conn.setlimit, sqlite3.SQLITE_LIMIT_VARIABLE_NUMBER, 999
        )

        self.turso_path = Path(self.tmpdir.name) / "write_turso.db"
        seed = Database(str(self.turso_path), turso_url="")
        await seed.connect()
        await seed.close()
        self.turso_client = CountingTursoClient(str(self.turso_path))
        self.turso_db = Database(str(self.turso_path), turso_url="https://policy-write.turso.io")
        self.turso_db._client = self.turso_client
        self.addAsyncCleanup(self.turso_db.close)

    async def test_persist_feed_event_decisions_mixed_inserts_revisions_budgets_and_turso_parity(self):
        t_orig = datetime(2026, 11, 10, 8, 0, tzinfo=timezone.utc)
        t_new = datetime(2026, 11, 12, 12, 0, tzinfo=timezone.utc)
        for label, db in (("sqlite", self.db), ("turso", self.turso_db)):
            with self.subTest(backend=label):
                await db.link_user(10, "Millers")
                # Pre-seed 50 eligible decisions and 4 protected message rows ('sent', 'deleted', 'pending', 'unknown')
                # plus 1 already stricter stored row ('suppressed_old').
                seed_inserts = [
                    {
                        "event_id": f"rev-{i:03d}",
                        "source": "activity",
                        "event_date": "2026-11-10",
                        "decision": "eligible",
                        "reason": "initial",
                        "payload": json.dumps({"v": 1, "i": i}),
                        "observed_at": t_orig,
                    }
                    for i in range(50)
                ] + [
                    {
                        "event_id": "strict-existing",
                        "source": "activity",
                        "event_date": "2026-11-01",
                        "decision": "suppressed_old",
                        "reason": "already_old",
                        "payload": '{"v":1}',
                        "observed_at": t_orig,
                    }
                ]
                await db.persist_feed_event_decisions(1, "millers", inserts=seed_inserts)

                for idx, state in enumerate(("sent", "deleted", "pending", "unknown")):
                    eid = f"rev-{idx:03d}"
                    await db.store_feed_message(
                        1, "millers", [eid], '{"v":1}',
                        message_id=100 + idx if state == "sent" else None,
                        channel_id=50 if state == "sent" else None,
                        delivery_state=state,
                    )

                # Mixed batch: 1,500 new aliases + long payload inserts + 50 revisions (incl. 4 protected)
                # + attempt to overwrite strict-existing
                big_note = "N" * 4000
                inserts = [
                    {
                        "event_id": f"new-{i:04d}",
                        "source": "journal" if i % 2 == 0 else "activity",
                        "event_date": "2026-11-12",
                        "decision": "eligible",
                        "reason": "new",
                        "payload": json.dumps({"note": big_note if i < 30 else "ok", "i": i}),
                        "observed_at": t_new,
                    }
                    for i in range(1500)
                ]
                revisions = [
                    {
                        "event_id": f"rev-{i:03d}",
                        "decision": "suppressed_old",
                        "reason": "late_old_date",
                        "event_date": "2026-11-01",
                        "payload": json.dumps({"v": 2, "i": i}),
                    }
                    for i in range(50)
                ] + [
                    {
                        "event_id": "strict-existing",
                        "decision": "eligible",
                        "reason": "should_not_loosen",
                        "event_date": "2026-11-12",
                        "payload": '{"v":2}',
                    }
                ]

                if label == "turso":
                    self.turso_client.reset_counters()
                auth = await db.persist_feed_event_decisions(
                    1, "MiLLeRs", inserts=inserts, revisions=revisions
                )
                if label == "turso":
                    # Bounded remote tx executes (vs 3,000+ unbatched calls)
                    self.assertLessEqual(self.turso_client.tx_execute_calls, 40)
                    self.assertLessEqual(self.turso_client.max_params_seen, _MAX_SQL_BIND_PARAMS)
                    self.assertLessEqual(
                        self.turso_client.max_statement_bytes_seen, _MAX_SQL_BATCH_BYTES + 4096
                    )

                self.assertEqual(len(auth), 1551)
                self.assertEqual(auth["new-1499"]["decision"], "eligible")
                self.assertEqual(auth["strict-existing"]["decision"], "suppressed_old")
                # Protected messages rev-000..rev-003 stayed 'eligible'
                for idx in range(4):
                    self.assertEqual(auth[f"rev-{idx:03d}"]["decision"], "eligible")
                # Unprotected rev-004..rev-049 revised to suppressed_old, preserving original observed_at & source
                rev10 = await db.get_feed_event_decision(1, "millers", "rev-010")
                self.assertEqual(rev10["decision"], "suppressed_old")
                self.assertEqual(rev10["observed_at"], "2026-11-10 08:00:00")
                self.assertEqual(rev10["source"], "activity")

    async def test_apply_discovery_policy_batches_writes_scales_with_1500_events_and_reconciles_conflicts(self):
        t0 = datetime(2026, 11, 12, 12, 0, tzinfo=timezone.utc)
        await self.turso_db.link_user(10, "Millers")
        cog = FeedCog(SimpleNamespace(db=self.turso_db), clock=lambda: t0)

        # 1. Scaling check: 1,500 new events in one _apply_discovery_policy invocation
        bulk_events = [
            _make_event(
                f"bulk-{i:04d}",
                played_date="2026-11-12",
                occurred_at="2026-11-12T10:00:00Z",
                event_ids=(f"bulk-{i:04d}",),
            )
            for i in range(1500)
        ]
        self.turso_client.reset_counters()
        accepted_bulk = await cog._apply_discovery_policy(1, "millers", bulk_events, {}, t0)
        self.assertEqual(len(accepted_bulk), 1500)
        total_remote_calls = (
            self.turso_client.direct_execute_calls
            + self.turso_client.tx_execute_calls
            + self.turso_client.batch_calls
        )
        # Before Task 7: 3,000+ direct executes; after Task 7: <= 30 total calls!
        self.assertLessEqual(total_remote_calls, 30)
        self.assertLessEqual(self.turso_client.max_params_seen, _MAX_SQL_BIND_PARAMS)

        # 2. Concurrent conflict contract: between bulk prefetch and persist commit,
        # another writer inserts a stricter decision ('suppressed_old') for 'race-ev'
        # and tightens 'race-rev' from 'eligible' to 'held_uncertain'.
        await self.turso_db.record_feed_event_decision(
            1, "millers", "race-rev", source="activity", event_date="2026-11-12",
            decision="eligible", reason="r0", payload=json.dumps(asdict(_make_event("race-rev"))),
            observed_at=t0,
        )
        orig_persist = self.turso_db.persist_feed_event_decisions

        async def inject_concurrent_writes(guild_id, profile_key, *, inserts=(), revisions=(), alias_groups=()):
            # Simulate another worker committing before our transaction begins
            await self.turso_db.record_feed_event_decision(
                guild_id, profile_key, "race-ev", source="activity", event_date="2026-11-01",
                decision="suppressed_old", reason="concurrent_old",
                payload=json.dumps(asdict(_make_event("race-ev"))),
                observed_at=datetime(2026, 11, 11, 0, 0, tzinfo=timezone.utc),
            )
            await self.turso_db.revise_unsent_eligible_feed_decision(
                guild_id, profile_key, "race-rev",
                decision="held_uncertain", reason="concurrent_uncertain",
                event_date=None, payload=json.dumps(asdict(_make_event("race-rev", played_date="2026-11-10"))),
            )
            return await orig_persist(guild_id, profile_key, inserts=inserts, revisions=revisions, alias_groups=alias_groups)

        self.turso_db.persist_feed_event_decisions = inject_concurrent_writes
        try:
            race_events = [
                _make_event("race-ev", played_date="2026-11-12"),
                _make_event("race-rev", played_date="2026-11-10"),
            ]
            accepted_race = await cog._apply_discovery_policy(1, "millers", race_events, {}, t0)
        finally:
            self.turso_db.persist_feed_event_decisions = orig_persist

        # Neither event should be accepted because concurrent decisions were stricter!
        self.assertEqual(accepted_race, [])
        self.assertEqual((await self.turso_db.get_feed_event_decision(1, "millers", "race-ev"))["decision"], "suppressed_old")
        self.assertEqual((await self.turso_db.get_feed_event_decision(1, "millers", "race-rev"))["decision"], "held_uncertain")

    async def test_persist_feed_event_decisions_mid_batch_failure_and_cancellation_rollback_cleanly(self):
        await self.turso_db.link_user(10, "Millers")
        t0 = datetime(2026, 11, 12, 12, 0, tzinfo=timezone.utc)
        inserts = [
            {
                "event_id": f"atomic-{i:04d}",
                "source": "activity",
                "event_date": "2026-11-12",
                "decision": "eligible",
                "reason": "new",
                "payload": '{"v":1}',
                "observed_at": t0,
            }
            for i in range(400)  # > 4 multi-row insert chunks (88 rows/chunk)
        ]

        # 1. Mid-batch failure rolls back all chunks
        async def fail_mid(call_no, query, values):
            if "INSERT" in query and call_no >= 4:
                raise RuntimeError("mid-batch decision failure")

        self.turso_client.reset_counters()
        self.turso_client.on_tx_execute = fail_mid
        try:
            with self.assertRaisesRegex(RuntimeError, "mid-batch decision failure"):
                await self.turso_db.persist_feed_event_decisions(1, "millers", inserts=inserts)
        finally:
            self.turso_client.on_tx_execute = None

        self.assertFalse(self.turso_db._operation_lock.locked())
        count = (
            await self.turso_db._fetchone(
                "SELECT COUNT(*) AS c FROM profile_feed_decisions WHERE guild_id = 1"
            )
        )["c"]
        self.assertEqual(count, 0)

        # 2. Mid-batch cancellation rolls back all chunks cleanly
        self.turso_client.reset_counters()
        hit_mid = asyncio.Event()
        release_mid = asyncio.Event()

        async def block_mid(call_no, query, values):
            if "INSERT" in query and call_no >= 4:
                hit_mid.set()
                await release_mid.wait()

        self.turso_client.on_tx_execute = block_mid
        task = asyncio.create_task(
            self.turso_db.persist_feed_event_decisions(1, "millers", inserts=inserts)
        )
        try:
            await asyncio.wait_for(hit_mid.wait(), timeout=2.0)
            task.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await task
        finally:
            release_mid.set()
            if not task.done():
                task.cancel()
                try:
                    await task
                except BaseException:
                    pass
            self.turso_client.on_tx_execute = None

        self.assertFalse(self.turso_db._operation_lock.locked())
        count_cancel = (
            await self.turso_db._fetchone(
                "SELECT COUNT(*) AS c FROM profile_feed_decisions WHERE guild_id = 1"
            )
        )["c"]
        self.assertEqual(count_cancel, 0)

        # 3. Retry succeeds atomically
        res = await self.turso_db.persist_feed_event_decisions(1, "millers", inserts=inserts)
        self.assertEqual(len(res), 400)

    async def test_concurrent_suppression_propagates_to_alias_and_survives_alias_only_retry(self):
        for db in (self.db, self.turso_db):
            with self.subTest(turso=db.is_turso):
                await db.link_user(10, "Millers")
                now = datetime(2026, 11, 12, 12, tzinfo=timezone.utc)
                original = db.persist_feed_event_decisions
                async def racing(guild_id, profile_key, **kwargs):
                    await db.record_feed_event_decision(
                        guild_id, profile_key, "a", source="journal", event_date="2026-10-01",
                        decision="suppressed_old", reason="concurrent_old",
                        payload=json.dumps(asdict(_make_event("a"))), observed_at="2026-11-10 00:00:00",
                    )
                    return await original(guild_id, profile_key, **kwargs)
                db.persist_feed_event_decisions = racing
                cog = FeedCog(SimpleNamespace(db=db), clock=lambda: now)
                try:
                    accepted = await cog._apply_discovery_policy(
                        1, "millers", [_make_event("a", event_ids=("a", "b"))], {}, now
                    )
                finally:
                    db.persist_feed_event_decisions = original
                self.assertEqual(accepted, [])
                rows = await db.get_feed_event_decisions(1, "millers", ["a", "b"])
                self.assertEqual(rows["b"]["decision"], "suppressed_old")
                self.assertEqual(rows["b"]["observed_at"], rows["a"]["observed_at"])
                self.assertEqual(rows["b"]["source"], "activity")
                self.assertEqual(await cog._apply_discovery_policy(
                    1, "millers", [_make_event("b")], {}, now
                ), [])


if __name__ == "__main__":
    unittest.main()
