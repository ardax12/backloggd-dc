"""Feed polling shutdown ownership and shared-resource ordering regressions."""

import asyncio
import os
import tempfile
import unittest
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

os.environ["PYTHON_DOTENV_DISABLED"] = "1"
os.environ["TURSO_DATABASE_URL"] = ""
os.environ["TURSO_AUTH_TOKEN"] = ""
os.environ["DEV_GUILD_ID"] = ""

from bot.client import BackloggdBot
from bot.cogs.feed import FeedCog
from bot.db import Database
from bot.feed_models import FeedEvent


class FeedLifecycleTests(unittest.IsolatedAsyncioTestCase):
    async def _poll_fixture(self, db, *, fetch_sources):
        guild = SimpleNamespace(id=1)
        bot = SimpleNamespace(
            db=db,
            get_guild=lambda guild_id: guild if guild_id == 1 else None,
            wait_until_ready=AsyncMock(),
        )
        feed = FeedCog(
            bot,
            clock=lambda: datetime(2026, 11, 13, 12, 0, tzinfo=timezone.utc),
        )
        feed._inspect_target_channel = lambda *args, **kwargs: (
            SimpleNamespace(id=100), None, None,
        )
        feed._authoritative_member_ids = AsyncMock(return_value=[10])
        feed._fetch_feed_sources = fetch_sources
        feed._observe_profile_activity = AsyncMock()
        return bot, feed

    async def test_bot_close_drains_feed_poll_before_sdk_and_database_close(self):
        with tempfile.TemporaryDirectory() as folder, patch(
            "bot.client.DATABASE_PATH", str(Path(folder) / "bot.db"),
        ):
            bot = BackloggdBot()
        events = []
        fetch_started = asyncio.Event()
        fetch_cancelled = asyncio.Event()
        allow_fetch_unwind = asyncio.Event()

        async def db_close():
            events.append("db_close")

        async def sdk_close():
            events.append("sdk_close")

        async def blocked_fetch(username, **kwargs):
            events.append("fetch_started")
            fetch_started.set()
            try:
                await allow_fetch_unwind.wait()
            except asyncio.CancelledError:
                fetch_cancelled.set()
                await allow_fetch_unwind.wait()
                events.append("fetch_drained")
                raise

        bot.db = SimpleNamespace(
            is_connected=True,
            get_all_guild_settings=AsyncMock(return_value=[{"guild_id": 1, "feed_channel_id": 100}]),
            get_legacy_feed_guilds_pending=AsyncMock(return_value=[]),
            get_guild_feed_candidates=AsyncMock(return_value=[{
                "profile_key": "millers", "display_username": "Millers", "avatar_url": None,
            }]),
            reconcile_guild_feed_activity=AsyncMock(return_value={"activated": [], "deactivated": []}),
            close=AsyncMock(side_effect=db_close),
        )
        bot.backloggd.close = AsyncMock(side_effect=sdk_close)
        bot.library_sync.close = AsyncMock()
        bot.guild_membership.close = AsyncMock()
        bot.wait_until_ready = AsyncMock()

        feed = FeedCog(bot)
        feed._inspect_target_channel = lambda *args, **kwargs: (SimpleNamespace(id=100), None, None)
        feed._authoritative_member_ids = AsyncMock(return_value=[10])
        feed._fetch_feed_sources = blocked_fetch
        await bot.add_cog(feed)
        try:
            await asyncio.wait_for(fetch_started.wait(), timeout=2.0)
            loop_task = feed.journal_feed_loop.get_task()
            close_task = asyncio.create_task(bot.close())
            await asyncio.wait_for(fetch_cancelled.wait(), timeout=2.0)

            self.assertFalse(close_task.done())
            self.assertNotIn("sdk_close", events)
            self.assertNotIn("db_close", events)
            self.assertIsNotNone(loop_task)
            self.assertFalse(loop_task.done())

            allow_fetch_unwind.set()
            await asyncio.wait_for(close_task, timeout=2.0)
            self.assertLess(events.index("fetch_drained"), events.index("sdk_close"))
            self.assertLess(events.index("sdk_close"), events.index("db_close"))
            self.assertTrue(loop_task.done())

            # Repeated close/unload is safe after the loop has drained.
            await feed.close()
            await feed.cog_unload()
        finally:
            allow_fetch_unwind.set()
            if not bot.is_closed():
                await bot.close()
            task = feed.journal_feed_loop.get_task()
            if task is not None and not task.done():
                feed.journal_feed_loop.cancel()
                await asyncio.gather(task, return_exceptions=True)

    async def test_close_cancels_and_rolls_back_an_inflight_feed_transaction(self):
        with tempfile.TemporaryDirectory() as folder:
            db = Database(str(Path(folder) / "feed-transaction.db"), turso_url="")
            await db.connect()
            await db.link_user(10, "Millers")
            await db.set_guild_feed_preference(1, 10, True)
            await db.set_feed_channel(1, 100)
            await db.reconcile_guild_feed_activity(1, ["millers"], present_member_ids=[10])

            entered_transaction = asyncio.Event()
            transaction_cancelled = asyncio.Event()
            continue_transaction = asyncio.Event()
            open_transaction = db._transaction

            @asynccontextmanager
            async def pause_transaction():
                async with open_transaction() as tx:
                    entered_transaction.set()
                    try:
                        await continue_transaction.wait()
                    except asyncio.CancelledError:
                        transaction_cancelled.set()
                        raise
                    yield tx

            db._transaction = pause_transaction
            _, feed = await self._poll_fixture(
                db, fetch_sources=AsyncMock(return_value={"activity": [], "journal": []}),
            )
            feed.journal_feed_loop.start()
            try:
                await asyncio.wait_for(entered_transaction.wait(), timeout=2.0)
                poll_task = feed.journal_feed_loop.get_task()
                await feed.close()
                self.assertTrue(transaction_cancelled.is_set())
                self.assertTrue(poll_task.done())
                self.assertFalse(db._operation_lock.locked())
                self.assertTrue(db.is_connected)
            finally:
                continue_transaction.set()
                await feed.close()
                db._transaction = open_transaction
                await db.close()

    async def test_close_drains_send_and_restart_never_retries_uncertain_pending_delivery(self):
        with tempfile.TemporaryDirectory() as folder:
            db_path = Path(folder) / "feed-send.db"
            db = Database(str(db_path), turso_url="")
            await db.connect()
            await db.link_user(10, "Millers")
            await db.set_guild_feed_preference(1, 10, True)
            await db.set_feed_channel(1, 100)
            await db.reconcile_guild_feed_activity(
                1,
                ["millers"],
                now=datetime(2026, 11, 13, 8, 0, tzinfo=timezone.utc),
                present_member_ids=[10],
            )
            await db.seed_feed_source(1, "millers", "activity-display-v1", [])
            await db.seed_feed_source(1, "millers", "journal-display-v1", [])

            send_started = asyncio.Event()
            send_cancelled = asyncio.Event()
            allow_send_unwind = asyncio.Event()

            async def blocked_send(**kwargs):
                send_started.set()
                try:
                    await allow_send_unwind.wait()
                except asyncio.CancelledError:
                    send_cancelled.set()
                    await allow_send_unwind.wait()
                    raise
                return SimpleNamespace(id=501)

            channel = SimpleNamespace(id=100, send=AsyncMock(side_effect=blocked_send))
            event = FeedEvent(
                activity_id="during-close",
                game_title="Hades",
                game_slug="hades",
                cover_url=None,
                action_type="completed",
                action_text="completed Hades",
                rating=None,
                review_notes=None,
                date_str="2026-11-13",
                occurred_at="2026-11-13T10:00:00Z",
                played_date="2026-11-13",
                source="activity",
                event_ids=("during-close",),
            )
            _, feed = await self._poll_fixture(
                db,
                fetch_sources=AsyncMock(return_value={"activity": [event], "journal": []}),
            )
            feed._inspect_target_channel = lambda *args, **kwargs: (channel, None, None)
            feed.journal_feed_loop.start()
            try:
                await asyncio.wait_for(send_started.wait(), timeout=2.0)
                close_task = asyncio.create_task(feed.close())
                await asyncio.wait_for(send_cancelled.wait(), timeout=2.0)
                self.assertFalse(close_task.done())

                allow_send_unwind.set()
                await asyncio.wait_for(close_task, timeout=2.0)
                self.assertTrue(feed.journal_feed_loop.get_task().done())
                channel.send.assert_awaited_once()
                pending = {row["event_id"]: row for row in await db.get_feed_messages(1, "millers")}
                self.assertEqual(pending["during-close"]["delivery_state"], "unknown")

                # Restart leaves the uncertain intent protected from a duplicate send.
                await db.close()
                restarted_db = Database(str(db_path), turso_url="")
                await restarted_db.connect()
                retry_channel = SimpleNamespace(
                    id=100, send=AsyncMock(return_value=SimpleNamespace(id=502)),
                )
                retry_feed = FeedCog(
                    SimpleNamespace(db=restarted_db),
                    clock=lambda: datetime(2026, 11, 13, 12, 0, tzinfo=timezone.utc),
                )
                await retry_feed._publish_sources(
                    1, "millers", "Millers", None, retry_channel,
                    {"activity": [], "journal": []},
                )
                retry_channel.send.assert_not_awaited()
                await restarted_db.close()
            finally:
                allow_send_unwind.set()
                await feed.close()
                if db.is_connected:
                    await db.close()

    async def test_close_propagates_cancellation_after_draining_and_handles_stopped_loops(self):
        stopped = FeedCog(SimpleNamespace(db=SimpleNamespace(is_connected=False)))
        await stopped.close()
        await stopped.cog_unload()
        self.assertIsNone(stopped.journal_feed_loop.get_task())

        started = asyncio.Event()
        cancelled = asyncio.Event()
        release = asyncio.Event()
        fake_db = SimpleNamespace(
            is_connected=True,
            get_all_guild_settings=AsyncMock(return_value=[{"guild_id": 1, "feed_channel_id": 100}]),
            get_legacy_feed_guilds_pending=AsyncMock(return_value=[]),
            get_guild_feed_candidates=AsyncMock(return_value=[{
                "profile_key": "millers", "display_username": "Millers", "avatar_url": None,
            }]),
            reconcile_guild_feed_activity=AsyncMock(return_value={"activated": [], "deactivated": []}),
        )
        bot = SimpleNamespace(
            db=fake_db,
            get_guild=lambda guild_id: SimpleNamespace(id=guild_id),
            wait_until_ready=AsyncMock(),
        )
        feed = FeedCog(bot)
        feed._inspect_target_channel = lambda *args, **kwargs: (SimpleNamespace(id=100), None, None)
        feed._authoritative_member_ids = AsyncMock(return_value=[10])

        async def blocked_fetch(username, **kwargs):
            started.set()
            try:
                await release.wait()
            except asyncio.CancelledError:
                cancelled.set()
                await release.wait()
                raise

        feed._fetch_feed_sources = blocked_fetch
        feed.journal_feed_loop.start()
        close_task = None
        try:
            await asyncio.wait_for(started.wait(), timeout=2.0)
            close_task = asyncio.create_task(feed.close())
            await asyncio.wait_for(cancelled.wait(), timeout=2.0)
            close_task.cancel()
            release.set()
            with self.assertRaises(asyncio.CancelledError):
                await close_task
            self.assertTrue(feed.journal_feed_loop.get_task().done())
            self.assertTrue(feed._closing)
        finally:
            release.set()
            if close_task is not None and not close_task.done():
                close_task.cancel()
                await asyncio.gather(close_task, return_exceptions=True)
            await feed.close()

    async def test_close_retrieves_a_loop_that_failed_while_starting(self):
        async def failed_ready():
            raise ValueError("ready hook failed")

        feed = FeedCog(SimpleNamespace(
            db=SimpleNamespace(is_connected=False),
            wait_until_ready=failed_ready,
        ))
        feed.journal_feed_loop.start()
        task = feed.journal_feed_loop.get_task()
        results = await asyncio.gather(task, return_exceptions=True)

        self.assertIsInstance(results[0], ValueError)
        await feed.close()
        self.assertTrue(task.done())


if __name__ == "__main__":
    unittest.main()
