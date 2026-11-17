"""Regression coverage for bounded feed identity decisions and observation times."""

import json
import os
import tempfile
import unittest
from contextlib import asynccontextmanager
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
import asyncio

os.environ["PYTHON_DOTENV_DISABLED"] = "1"
os.environ["TURSO_DATABASE_URL"] = ""
os.environ["TURSO_AUTH_TOKEN"] = ""
os.environ["DEV_GUILD_ID"] = ""

from bot.cogs.feed import FeedCog
from bot.db import Database, _MAX_SQL_BATCH_BYTES, _MAX_SQL_BIND_PARAMS
from bot.feed_models import FeedEvent
from tests.test_feed_batch_writes import CountingTursoClient


def _event(event_id, *, reference=None, event_ids=None):
    return FeedEvent(
        activity_id=event_id,
        game_title="Hades II",
        game_slug="hades-ii",
        cover_url=None,
        action_type="playing",
        action_text="started playing Hades II",
        rating=None,
        review_notes=None,
        date_str="2026-11-12",
        played_date="2026-11-12",
        occurred_at="2026-11-12T10:00:00Z",
        reference=reference,
        source="activity",
        event_ids=tuple(event_ids or (event_id,)),
    )


class FeedIdentityBatchingTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmpdir.cleanup)
        sqlite_path = Path(self.tmpdir.name) / "identity.sqlite"
        self.sqlite_db = Database(str(sqlite_path), turso_url="")
        await self.sqlite_db.connect()
        self.addAsyncCleanup(self.sqlite_db.close)

        turso_path = Path(self.tmpdir.name) / "identity-turso.sqlite"
        seed = Database(str(turso_path), turso_url="")
        await seed.connect()
        await seed.close()
        self.turso_client = CountingTursoClient(str(turso_path))
        self.turso_db = Database(str(turso_path), turso_url="https://identity-test.turso.io")
        self.turso_db._client = self.turso_client
        self.addAsyncCleanup(self.turso_db.close)

        self.now = datetime(2026, 11, 12, 12, 34, 56, tzinfo=timezone.utc)

    def _cog(self, db):
        return FeedCog(SimpleNamespace(db=db), clock=lambda: self.now)

    async def test_ambiguity_records_the_injected_first_observation_time(self):
        db = self.sqlite_db
        previous = _event("old-session")
        incoming = _event("new-session")

        reconciled = await self._cog(db)._reconcile_changed_event_identities(
            1, "millers", "activity", [incoming], {previous.activity_id: previous},
        )

        self.assertEqual(reconciled, [])
        row = await db.get_feed_event_decision(1, "millers", "new-session")
        self.assertEqual(row["decision"], "held_ambiguous")
        self.assertEqual(row["observed_at"], "2026-11-12 12:34:56")

    async def test_ambiguity_retry_keeps_its_first_observation_time_on_both_adapters(self):
        previous = _event("old-session-retry")
        incoming = _event("new-session-retry")
        later = datetime(2026, 11, 13, 9, 0, tzinfo=timezone.utc)
        for label, db in (("sqlite", self.sqlite_db), ("turso", self.turso_db)):
            with self.subTest(backend=label):
                cog = self._cog(db)
                await cog._reconcile_changed_event_identities(
                    1, "millers", "activity", [incoming], {previous.activity_id: previous},
                )
                await FeedCog(SimpleNamespace(db=db), clock=lambda: later)._reconcile_changed_event_identities(
                    1, "millers", "activity", [incoming], {previous.activity_id: previous},
                )
                row = await db.get_feed_event_decision(1, "millers", incoming.activity_id)
                self.assertEqual(row["observed_at"], "2026-11-12 12:34:56")

    async def test_1500_ambiguous_sessions_use_bounded_identity_writes(self):
        previous = _event("previous-session")
        events = [_event(f"shifted-{index}") for index in range(1500)]
        self.turso_client.reset_counters()
        commits_before = self.turso_client.commits

        reconciled = await self._cog(self.turso_db)._reconcile_changed_event_identities(
            1, "millers", "activity", events, {previous.activity_id: previous},
        )

        self.assertEqual(reconciled, [])
        self.assertEqual(self.turso_client.direct_execute_calls, 0)
        self.assertEqual(self.turso_client.tx_execute_calls, 22)
        self.assertEqual(self.turso_client.commits - commits_before, 1)
        self.assertLessEqual(self.turso_client.max_params_seen, _MAX_SQL_BIND_PARAMS)
        self.assertLessEqual(self.turso_client.max_statement_bytes_seen, _MAX_SQL_BATCH_BYTES)
        decisions = await self.turso_db.get_feed_event_decisions(
            1, "millers", ["shifted-0", "shifted-1499"],
        )
        first, last = decisions["shifted-0"], decisions["shifted-1499"]
        self.assertEqual(first["decision"], "held_ambiguous")
        self.assertEqual(last["decision"], "held_ambiguous")
        self.assertEqual(first["observed_at"], "2026-11-12 12:34:56")

    async def test_stable_reference_resolution_is_batched_and_keeps_protected_rows_held(self):
        previous = [
            _event("old-a", reference="/u/millers/session/a"),
            _event("old-b", reference="/u/millers/session/b"),
            _event("old-protected-sent", reference="/u/millers/session/sent"),
            _event("old-protected-deleted", reference="/u/millers/session/deleted"),
            _event("old-protected-pending", reference="/u/millers/session/pending"),
            _event("old-protected-unknown", reference="/u/millers/session/unknown"),
            _event("old-suppressed", reference="/u/millers/session/suppressed"),
        ]
        incoming = [
            _event("new-a", reference=previous[0].reference),
            _event("new-b", reference=previous[1].reference),
            _event("new-protected-sent", reference=previous[2].reference),
            _event("new-protected-deleted", reference=previous[3].reference),
            _event("new-protected-pending", reference=previous[4].reference),
            _event("new-protected-unknown", reference=previous[5].reference),
            _event("new-suppressed", reference=previous[6].reference),
        ]
        original_time = datetime(2026, 11, 10, 8, 0, tzinfo=timezone.utc)
        for label, db in (("sqlite", self.sqlite_db), ("turso", self.turso_db)):
            with self.subTest(backend=label):
                await db.persist_feed_event_decisions(
                    1,
                    "millers",
                    inserts=[
                        {
                            "event_id": event.activity_id,
                            "source": "activity",
                            "event_date": "2026-11-12",
                            "decision": "suppressed_old" if event.activity_id == "new-suppressed" else "held_ambiguous",
                            "reason": "prior_ambiguity",
                            "payload": json.dumps(asdict(event), sort_keys=True),
                            "observed_at": original_time,
                        }
                        for event in incoming
                    ],
                )
                await db.link_user(10, "Millers")
                for state in ("sent", "deleted", "pending", "unknown"):
                    event_id = f"new-protected-{state}"
                    await db.store_feed_message(
                        1,
                        "millers",
                        (event_id,),
                        json.dumps(asdict(next(event for event in incoming if event.activity_id == event_id)), sort_keys=True),
                        message_id=555 if state == "sent" else None,
                        delivery_state=state,
                    )
                if label == "turso":
                    self.turso_client.reset_counters()

                reconciled = await self._cog(db)._reconcile_changed_event_identities(
                    1,
                    "millers",
                    "activity",
                    incoming,
                    {event.activity_id: event for event in previous},
                )

                self.assertEqual(
                    [event.activity_id for event in reconciled],
                    [event.activity_id for event in incoming],
                )
                if label == "turso":
                    self.assertEqual(self.turso_client.direct_execute_calls, 0)
                rows = await db.get_feed_event_decisions(
                    1, "millers", [event.activity_id for event in incoming],
                )
                self.assertEqual(rows["new-a"]["decision"], "eligible")
                self.assertEqual(rows["new-b"]["decision"], "eligible")
                for state in ("sent", "deleted", "pending", "unknown"):
                    self.assertEqual(rows[f"new-protected-{state}"]["decision"], "held_ambiguous")
                self.assertEqual(rows["new-suppressed"]["decision"], "suppressed_old")
                self.assertEqual(rows["new-a"]["observed_at"], "2026-11-10 08:00:00")

    async def test_ordered_identity_operations_preserve_first_observation_and_strict_guards(self):
        first_time = datetime(2026, 11, 10, 8, 0, tzinfo=timezone.utc)
        later_time = datetime(2026, 11, 11, 8, 0, tzinfo=timezone.utc)
        event = _event("same-request")
        payload = json.dumps(asdict(event), sort_keys=True)
        await self.sqlite_db.persist_feed_identity_decisions(
            1,
            "millers",
            operations=[
                {
                    "kind": "ambiguity", "event_id": "same-request", "source": "activity",
                    "event_date": "2026-11-10", "reason": "first observation", "payload": payload,
                    "observed_at": first_time,
                },
                {
                    "kind": "resolve", "event_id": "same-request", "event_date": "2026-11-12",
                    "payload": json.dumps(asdict(_event("same-request", reference="/session/stable")), sort_keys=True),
                },
                {
                    "kind": "ambiguity", "event_id": "same-request", "source": "activity",
                    "event_date": "2026-11-11", "reason": "retry", "payload": payload,
                    "observed_at": later_time,
                },
            ],
        )

        row = await self.sqlite_db.get_feed_event_decision(1, "millers", "same-request")

        self.assertEqual(row["decision"], "eligible")
        self.assertEqual(row["reason"], "resolved_by_stable_reference")
        self.assertEqual(row["event_date"], "2026-11-12")
        self.assertEqual(row["observed_at"], "2026-11-10 08:00:00")

    async def test_middle_chunk_failure_and_cancellation_roll_back_and_retry_on_both_adapters(self):
        previous = _event("old-session-atomic")

        for label, db in (("sqlite", self.sqlite_db), ("turso", self.turso_db)):
            for cancellation in (False, True):
                with self.subTest(backend=label, cancellation=cancellation):
                    events = [
                        _event(f"atomic-{label}-{int(cancellation)}-{index}")
                        for index in range(200)
                    ]
                    original_transaction = db._transaction
                    second_chunk_started = asyncio.Event()
                    release_second_chunk = asyncio.Event()
                    call_count = 0

                    @asynccontextmanager
                    async def injected_transaction():
                        async with original_transaction() as tx:
                            original_execute = tx.execute

                            async def injected_execute(query, params=()):
                                nonlocal call_count
                                call_count += 1
                                if call_count == 3:
                                    if cancellation:
                                        second_chunk_started.set()
                                        await release_second_chunk.wait()
                                    else:
                                        raise RuntimeError("injected middle-chunk failure")
                                return await original_execute(query, params)

                            tx.execute = injected_execute
                            yield tx

                    db._transaction = injected_transaction
                    try:
                        task = asyncio.create_task(
                            self._cog(db)._reconcile_changed_event_identities(
                                1, "millers", "activity", events,
                                {previous.activity_id: previous},
                            )
                        )
                        if cancellation:
                            started_waiter = asyncio.create_task(second_chunk_started.wait())
                            done, _ = await asyncio.wait(
                                {task, started_waiter},
                                return_when=asyncio.FIRST_COMPLETED,
                            )
                            if task in done:
                                started_waiter.cancel()
                                await asyncio.gather(started_waiter, return_exceptions=True)
                                await task
                                self.fail(
                                    f"identity writes did not reach the gated middle chunk ({call_count=})"
                                )
                            await started_waiter
                            task.cancel()
                            with self.assertRaises(asyncio.CancelledError):
                                await task
                        else:
                            with self.assertRaisesRegex(RuntimeError, "middle-chunk failure"):
                                await task
                    finally:
                        db._transaction = original_transaction

                    self.assertEqual(
                        await db.get_feed_event_decisions(
                            1, "millers", [event.activity_id for event in events],
                        ),
                        {},
                    )
                    await self._cog(db)._reconcile_changed_event_identities(
                        1, "millers", "activity", events,
                        {previous.activity_id: previous},
                    )
                    rows = await db.get_feed_event_decisions(
                        1, "millers", [events[0].activity_id, events[-1].activity_id],
                    )
                    self.assertEqual(set(rows), {events[0].activity_id, events[-1].activity_id})
                    self.assertTrue(all(row["decision"] == "held_ambiguous" for row in rows.values()))
