import os
os.environ["PYTHON_DOTENV_DISABLED"]="1"
import unittest
from types import SimpleNamespace
from datetime import datetime,timezone
from tests import test_feed_batch_writes as fixtures
from tests.test_feed_policy_batching import _make_event
from bot.cogs.feed import FeedCog

class BaselineBulkTests(unittest.IsolatedAsyncioTestCase):
    asyncSetUp = fixtures.FeedBatchWritesTests.asyncSetUp
    _backends = fixtures.FeedBatchWritesTests._backends
    async def test_full_baseline_has_no_per_event_batches(self):
        for label,db in await self._backends():
            await db.link_user(10,"Millers")
            for size in (0,20,1500):
                with self.subTest(backend=label,size=size):
                    calls=[]
                    if label == "turso":
                        self.turso_client.reset_counters()
                    original=db._execute_batch_results
                    async def counted(statements):
                        calls.append(len(statements))
                        return await original(statements)
                    db._execute_batch_results=counted
                    cog=FeedCog(SimpleNamespace(db=db),clock=lambda:datetime(2026,11,12,12,tzinfo=timezone.utc))
                    events=[_make_event("journal_"+str(i),source="journal") for i in range(size)]
                    try:
                        await cog._publish_sources(size+1,"millers","Millers",None,None,{"journal":events},deliver=False)
                    finally:
                        db._execute_batch_results=original
                    self.assertLessEqual(len(calls),2)
                    if label == "turso":
                        remote = self.turso_client.direct_execute_calls + self.turso_client.tx_execute_calls + self.turso_client.batch_calls
                        self.assertLessEqual(remote, 40 + size // 10)
                    rows=await db.get_feed_messages(size+1,"millers")
                    self.assertEqual(len(rows),size)

    async def test_message_failure_rolls_back_source_and_retry(self):
        for label,db in await self._backends():
            await db.link_user(10,"Millers")
            original=db._transaction
            from contextlib import asynccontextmanager
            @asynccontextmanager
            async def failing():
                async with original() as tx:
                    execute=tx.execute
                    async def fail(sql,args=()):
                        if "INSERT OR IGNORE INTO profile_feed_messages" in sql:
                            raise RuntimeError("message chunk failure")
                        return await execute(sql,args)
                    tx.execute=fail
                    yield tx
            db._transaction=failing
            try:
                with self.assertRaisesRegex(RuntimeError,"message chunk"):
                    await db.seed_feed_source_with_messages(8,"millers","journal-display-v1",[("journal_1","{}")])
            finally:
                db._transaction=original
            self.assertFalse(await db.has_feed_source(8,"millers","journal-display-v1"))
            self.assertTrue(await db.seed_feed_source_with_messages(8,"millers","journal-display-v1",[("journal_1","{}")]))

    async def test_cancelled_message_chunk_rolls_back_and_archives_are_preserved(self):
        import asyncio
        from contextlib import asynccontextmanager
        for label, db in await self._backends():
            await db.link_user(10, "Millers")
            await db.store_feed_message(7, "millers", ["protected"], "archive", message_id=55, channel_id=1)
            original = db._transaction
            @asynccontextmanager
            async def cancelled_transaction():
                async with original() as tx:
                    execute = tx.execute
                    async def fail(sql, args=()):
                        if "INSERT OR IGNORE INTO profile_feed_messages" in sql:
                            raise asyncio.CancelledError()
                        return await execute(sql, args)
                    tx.execute = fail
                    yield tx
            db._transaction = cancelled_transaction
            try:
                with self.assertRaises(asyncio.CancelledError):
                    await db.seed_feed_source_with_messages(7, "millers", "journal-display-v1", [("fresh", "{}")])
            finally:
                db._transaction = original
            self.assertFalse(await db.has_feed_source(7, "millers", "journal-display-v1"))
            self.assertFalse(await db.is_journal_entry_delivered(7, "millers", "fresh"))
            self.assertTrue(await db.seed_feed_source_with_messages(
                7, "millers", "journal-display-v1", [("protected", "new"), ("fresh", "{}")]
            ))
            saved = {r["event_id"]: r for r in await db.get_feed_messages(7, "millers")}
            self.assertEqual(saved["protected"]["payload"], "archive")
            self.assertEqual(saved["protected"]["message_id"], 55)
            self.assertEqual(saved["protected"]["delivery_state"], "sent")
            self.assertEqual(saved["fresh"]["delivery_state"], "baseline")

    async def test_baseline_decision_failure_and_cancellation_roll_back_entire_baseline(self):
        import asyncio
        from contextlib import asynccontextmanager
        from unittest.mock import AsyncMock, MagicMock
        import discord

        for label, db in await self._backends():
            for exc_kind, guild_id in (("error", 101), ("cancel", 102)):
                with self.subTest(backend=label, exc_kind=exc_kind):
                    await db.link_user(10, "Millers")
                    events = [_make_event(f"act-{i}", source="activity") for i in range(25)]
                    now = datetime(2026, 11, 12, 12, 0, tzinfo=timezone.utc)
                    cog = FeedCog(SimpleNamespace(db=db), clock=lambda: now)

                    original_tx = db._transaction

                    @asynccontextmanager
                    async def fail_on_decisions():
                        async with original_tx() as tx:
                            execute = tx.execute

                            async def wrapped(sql, args=()):
                                if "INSERT OR IGNORE INTO profile_feed_decisions" in sql:
                                    if exc_kind == "cancel":
                                        raise asyncio.CancelledError()
                                    raise RuntimeError("decision chunk failure")
                                return await execute(sql, args)

                            tx.execute = wrapped
                            yield tx

                    db._transaction = fail_on_decisions
                    try:
                        expected_exc = asyncio.CancelledError if exc_kind == "cancel" else RuntimeError
                        with self.assertRaises(expected_exc):
                            await cog._publish_sources(
                                guild_id, "millers", "Millers", None, None,
                                {"activity": events}, deliver=False,
                            )
                    finally:
                        db._transaction = original_tx

                    # Neither source marker, messages, seen IDs, nor decisions may survive rollback.
                    self.assertFalse(db._operation_lock.locked())
                    self.assertFalse(await db.has_feed_source(guild_id, "millers", "activity-display-v1"))
                    self.assertEqual(await db.get_feed_messages(guild_id, "millers"), [])
                    self.assertEqual(await db.get_feed_decision_counts(guild_id, "millers"), {})
                    seen_row = await db._fetchone(
                        "SELECT COUNT(*) AS c FROM profile_seen_journal_deliveries WHERE guild_id = ? AND profile_key = 'millers'",
                        (guild_id,),
                    )
                    self.assertEqual(seen_row["c"], 0)

                    # Retry (simulating restart with fresh Cog) completes baseline without announcing history.
                    channel = MagicMock(spec=discord.TextChannel)
                    channel.id = 500
                    channel.guild = SimpleNamespace(id=guild_id)
                    channel.send = AsyncMock()
                    fresh_cog = FeedCog(SimpleNamespace(db=db), clock=lambda: now)
                    self.assertTrue(
                        await fresh_cog._publish_sources(
                            guild_id, "millers", "Millers", None, channel,
                            {"activity": events}, deliver=True,
                        )
                    )
                    channel.send.assert_not_awaited()
                    self.assertTrue(await db.has_feed_source(guild_id, "millers", "activity-display-v1"))
                    status = await db.get_guild_profile_feed_status(guild_id, "millers")
                    self.assertEqual(status["suppression_counts"].get("baseline"), 25)
                    self.assertEqual(status["delivery_counts"].get("baseline"), 25)

    async def test_atomic_baseline_preserves_stricter_decisions_protected_states_long_payloads_and_sizes(self):
        from dataclasses import replace
        from bot.db import _MAX_SQL_BATCH_BYTES, _MAX_SQL_BIND_PARAMS

        now = datetime(2026, 11, 12, 12, 30, 0, tzinfo=timezone.utc)
        for label, db in await self._backends():
            await db.link_user(10, "Millers")
            for size in (0, 20, 1500):
                with self.subTest(backend=label, size=size):
                    guild_id = 2000 + size
                    if label == "turso":
                        self.turso_client.reset_counters()
                    events = [
                        _make_event(f"act-{i}", source="activity") for i in range(size)
                    ]
                    cog = FeedCog(SimpleNamespace(db=db), clock=lambda: now)
                    tx_count = 0
                    orig_tx = db._transaction

                    def counted_tx():
                        nonlocal tx_count
                        tx_count += 1
                        return orig_tx()

                    db._transaction = counted_tx
                    try:
                        await cog._publish_sources(
                            guild_id, "millers", "Millers", None, None,
                            {"activity": events}, deliver=False,
                        )
                    finally:
                        db._transaction = orig_tx

                    # Baseline write for the source uses a single transaction (no separate decision write tx).
                    self.assertEqual(tx_count, 1)
                    if label == "turso":
                        self.assertLessEqual(self.turso_client.max_params_seen, _MAX_SQL_BIND_PARAMS)
                        self.assertLessEqual(
                            self.turso_client.max_statement_bytes_seen, _MAX_SQL_BATCH_BYTES + 4096
                        )
                    self.assertTrue(await db.has_feed_source(guild_id, "millers", "activity-display-v1"))
                    counts = await db.get_feed_decision_counts(guild_id, "millers")
                    self.assertEqual(counts.get("baseline", 0), size)

            # Long payloads + preexisting stricter decisions + protected delivery states.
            with self.subTest(backend=label, case="protected_and_long_payloads"):
                guild_id = 3001
                await db.record_feed_event_decision(
                    guild_id, "millers", "strict-old", source="activity",
                    event_date="2026-10-01", decision="suppressed_old",
                    reason="session_is_older_than_seven_utc_calendar_days", payload="{}",
                )
                await db.store_feed_message(guild_id, "millers", ["ev-sent"], "sent-payload", message_id=901, channel_id=55, delivery_state="sent")
                await db.store_feed_message(guild_id, "millers", ["ev-deleted"], "del-payload", message_id=902, channel_id=55, delivery_state="deleted")
                await db.begin_feed_delivery(guild_id, "millers", ["ev-pending"], "pending-payload", 55)
                await db.begin_feed_delivery(guild_id, "millers", ["ev-unknown"], "unknown-payload", 55)
                await db.mark_feed_delivery_unknown(guild_id, "millers", ["ev-unknown"])

                long_events = [
                    replace(_make_event(f"long-{i}", source="activity"), review_notes="あ" * 1200)
                    for i in range(30)
                ] + [
                    _make_event("strict-old", source="activity"),
                    _make_event("ev-sent", source="activity"),
                    _make_event("ev-deleted", source="activity"),
                    _make_event("ev-pending", source="activity"),
                    _make_event("ev-unknown", source="activity"),
                ]
                if label == "turso":
                    self.turso_client.reset_counters()
                baseline_payloads = {
                    e.activity_id: "{}" for e in long_events
                }
                baseline_decisions = [
                    {
                        "event_id": e.activity_id,
                        "source": e.source,
                        "event_date": "2026-11-12",
                        "decision": "baseline",
                        "reason": "initial_or_resume_source_baseline",
                        "payload": f'{{"notes": "{(e.review_notes or "")}"}}',
                        "observed_at": now,
                    }
                    for e in long_events
                ]
                seeded = await db.seed_feed_source_with_messages(
                    guild_id, "millers", "activity-display-v1", baseline_payloads.items(),
                    baseline_decisions=baseline_decisions,
                )
                self.assertTrue(seeded)
                if label == "turso":
                    self.assertLessEqual(self.turso_client.max_params_seen, _MAX_SQL_BIND_PARAMS)
                    self.assertLessEqual(
                        self.turso_client.max_statement_bytes_seen, _MAX_SQL_BATCH_BYTES + 4096
                    )

                # Preexisting stricter decision & observed_at & source preserved.
                strict = await db.get_feed_event_decision(guild_id, "millers", "strict-old")
                self.assertEqual(strict["decision"], "suppressed_old")
                fresh_dec = await db.get_feed_event_decision(guild_id, "millers", "long-0")
                self.assertEqual(fresh_dec["decision"], "baseline")
                self.assertEqual(fresh_dec["source"], "activity")
                self.assertEqual(fresh_dec["observed_at"], "2026-11-12 12:30:00")

                # Protected message states survive untouched (query directly before get_feed_messages mutates pending).
                msg_rows = await db._fetchall(
                    "SELECT event_id, payload, message_id, delivery_state FROM profile_feed_messages WHERE guild_id = ? AND profile_key = ?",
                    (guild_id, "millers"),
                )
                by_id = {row["event_id"]: row for row in msg_rows}
                self.assertEqual(by_id["ev-sent"]["delivery_state"], "sent")
                self.assertEqual(by_id["ev-sent"]["message_id"], 901)
                self.assertEqual(by_id["ev-deleted"]["delivery_state"], "deleted")
                self.assertEqual(by_id["ev-pending"]["delivery_state"], "pending")
                self.assertEqual(by_id["ev-unknown"]["delivery_state"], "unknown")
                self.assertEqual(by_id["long-0"]["delivery_state"], "baseline")
