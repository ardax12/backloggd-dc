"""Regression tests for Task 6: SocialCog live-fetch drain on unload and shutdown."""

import os
os.environ["PYTHON_DOTENV_DISABLED"] = "1"
os.environ["TURSO_DATABASE_URL"] = ""
os.environ["TURSO_AUTH_TOKEN"] = ""
os.environ["DEV_GUILD_ID"] = ""

import asyncio
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from backloggd import GameEntry, GameStatus, UserLibrary
from bot.client import BackloggdBot
from bot.cogs.social import SocialCog
from bot.db import Database


class SocialLifecycleTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmpdir.cleanup)
        self.db = Database(str(Path(self.tmpdir.name) / "social_lifecycle.db"), turso_url="")
        await self.db.connect()
        self.addAsyncCleanup(self.db.close)

    async def test_cog_unload_drains_active_and_queued_fetches_and_blocks_new_fetches(self):
        started = []
        entered_two = asyncio.Event()
        release = asyncio.Event()
        cancelled = []

        async def fetch_library(username: str):
            started.append(username)
            if len(started) >= 2:
                entered_two.set()
            try:
                await release.wait()
                return UserLibrary(
                    username=username,
                    games={"hades": GameEntry(slug="hades", title="Hades", rating=4.5, status=GameStatus.PLAYED)},
                )
            except asyncio.CancelledError:
                cancelled.append(username)
                raise

        api = SimpleNamespace(get_user_library=AsyncMock(side_effect=fetch_library))
        cog = SocialCog(
            SimpleNamespace(db=self.db, backloggd=api),
            live_fetch_concurrency=2,
            live_fetch_timeout=5.0,
        )

        # 2 active fetches + 1 queued behind the semaphore.
        w1 = asyncio.create_task(cog._get_or_fetch_ratings("u1"))
        w2 = asyncio.create_task(cog._get_or_fetch_ratings("u2"))
        await asyncio.wait_for(entered_two.wait(), timeout=1.0)
        w3 = asyncio.create_task(cog._get_or_fetch_ratings("u3"))
        await asyncio.sleep(0.02)

        await cog.cog_unload()

        self.assertEqual(set(cancelled), {"u1", "u2"})
        self.assertEqual(cog._live_library_tasks, {})
        self.assertEqual(cog._live_library_waiters, {})
        results = await asyncio.gather(w1, w2, w3, return_exceptions=True)
        for item in results:
            self.assertIsInstance(item, asyncio.CancelledError)

        # Repeated cleanup is idempotent and no new fetch starts after closing begins.
        await cog.close()
        before_count = api.get_user_library.await_count
        with self.assertRaises(asyncio.CancelledError):
            await cog._get_or_fetch_ratings("u4")
        self.assertEqual(api.get_user_library.await_count, before_count)

    async def test_two_waiters_one_cancel_preserves_fetch_while_cog_close_cancels_shared_fetch(self):
        entered = asyncio.Event()
        release = asyncio.Event()
        cancelled = 0

        async def slow_fetch(username: str):
            nonlocal cancelled
            entered.set()
            try:
                await release.wait()
            except asyncio.CancelledError:
                cancelled += 1
                raise
            return UserLibrary(
                username=username,
                games={"hades": GameEntry(slug="hades", title="Hades", rating=5.0, status=GameStatus.PLAYED)},
            )

        api = SimpleNamespace(get_user_library=AsyncMock(side_effect=slow_fetch))
        cog = SocialCog(
            SimpleNamespace(db=self.db, backloggd=api),
            live_fetch_timeout=5.0,
        )

        # Part A: cancelling one waiter keeps shared fetch alive for the second waiter.
        w1 = asyncio.create_task(cog._get_or_fetch_ratings("SharedUser"))
        await asyncio.wait_for(entered.wait(), timeout=1.0)
        w2 = asyncio.create_task(cog._get_or_fetch_ratings("shareduser"))
        await asyncio.sleep(0.02)
        w1.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await w1
        self.assertEqual(cancelled, 0)
        release.set()
        self.assertEqual(await w2, {"hades": 5.0})
        self.assertEqual(cog._live_library_tasks, {})
        self.assertEqual(cog._live_library_waiters, {})

        # Part B: cog close cancels the shared fetch for both waiters.
        entered.clear()
        release.clear()
        w3 = asyncio.create_task(cog._get_or_fetch_ratings("SharedUser2"))
        await asyncio.wait_for(entered.wait(), timeout=1.0)
        w4 = asyncio.create_task(cog._get_or_fetch_ratings("shareduser2"))
        await asyncio.sleep(0.02)

        await cog.close()
        self.assertEqual(cancelled, 1)
        res = await asyncio.gather(w3, w4, return_exceptions=True)
        self.assertTrue(all(isinstance(item, asyncio.CancelledError) for item in res))
        self.assertEqual(cog._live_library_tasks, {})
        self.assertEqual(cog._live_library_waiters, {})

    async def test_bot_close_drains_social_cog_before_closing_sdk_and_db_and_cog_removal_unloads(self):
        events: list[str] = []
        entered = asyncio.Event()
        block = asyncio.Event()

        with patch("bot.client.DATABASE_PATH", str(Path(self.tmpdir.name) / "bot.db")):
            bot = BackloggdBot()
        try:
            async def blocked_fetch(username: str):
                events.append(f"fetch_start:{username}")
                entered.set()
                try:
                    await block.wait()
                except asyncio.CancelledError:
                    await asyncio.sleep(0.01)
                    events.append(f"fetch_drained:{username}")
                    raise
                return UserLibrary(username=username, games={})

            orig_sdk_close = bot.backloggd.close
            orig_db_close = bot.db.close

            async def wrapped_sdk_close():
                events.append("sdk_close")
                return await orig_sdk_close()

            async def wrapped_db_close():
                events.append("db_close")
                return await orig_db_close()

            bot.backloggd.get_user_library = AsyncMock(side_effect=blocked_fetch)
            bot.backloggd.close = wrapped_sdk_close
            bot.db.close = wrapped_db_close
            await bot.db.connect()
            await bot.add_cog(SocialCog(bot, live_fetch_timeout=5.0))

            # Actual cog removal calls async cog_unload and drains tracked fetches.
            waiter1 = asyncio.create_task(bot.get_cog("Social")._get_or_fetch_ratings("RemovedUser"))
            await asyncio.wait_for(entered.wait(), timeout=1.0)
            await bot.remove_cog("Social")
            self.assertIn("fetch_drained:RemovedUser", events)
            with self.assertRaises(asyncio.CancelledError):
                await waiter1

            # Re-add SocialCog and verify full bot.close() ordering.
            events.clear()
            entered.clear()
            await bot.add_cog(SocialCog(bot, live_fetch_timeout=5.0))
            waiter2 = asyncio.create_task(bot.get_cog("Social")._get_or_fetch_ratings("ShutdownUser"))
            await asyncio.wait_for(entered.wait(), timeout=1.0)

            await bot.close()
            self.assertIn("fetch_drained:ShutdownUser", events)
            self.assertIn("sdk_close", events)
            self.assertIn("db_close", events)
            self.assertLess(events.index("fetch_drained:ShutdownUser"), events.index("sdk_close"))
            self.assertLess(events.index("sdk_close"), events.index("db_close"))
            with self.assertRaises(asyncio.CancelledError):
                await waiter2
        finally:
            if not bot.is_closed():
                await bot.close()


if __name__ == "__main__":
    unittest.main()
