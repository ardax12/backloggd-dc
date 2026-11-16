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
