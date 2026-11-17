"""Stats worker ownership and shutdown ordering regressions."""

import asyncio
import os
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

os.environ["PYTHON_DOTENV_DISABLED"] = "1"
os.environ["TURSO_DATABASE_URL"] = ""
os.environ["TURSO_AUTH_TOKEN"] = ""
os.environ["DEV_GUILD_ID"] = ""

from bot.client import BackloggdBot
from bot.cogs.stats import StatsCog


class StatsLifecycleTests(unittest.IsolatedAsyncioTestCase):
    async def test_bot_close_drains_active_and_queued_count_workers_before_sdk_close(self):
        with tempfile.TemporaryDirectory() as folder, patch(
            "bot.client.DATABASE_PATH", str(Path(folder) / "bot.db"),
        ):
            bot = BackloggdBot()

        events = []
        fetch_started = asyncio.Event()
        fetch_cancelled = asyncio.Event()
        allow_fetch_unwind = asyncio.Event()
        operation_started = [asyncio.Event(), asyncio.Event()]
        calls = []

        async def blocked_profile(username):
            calls.append(username)
            events.append(f"fetch:{username}")
            fetch_started.set()
            try:
                await allow_fetch_unwind.wait()
            except asyncio.CancelledError:
                fetch_cancelled.set()
                await allow_fetch_unwind.wait()
                events.append("fetch_drained")
                raise
            return SimpleNamespace(played_count=12)

        async def sdk_close():
            events.append("sdk_close")

        async def db_close():
            events.append("db_close")

        bot.db = SimpleNamespace(close=AsyncMock(side_effect=db_close))
        bot.backloggd.get_user_profile = AsyncMock(side_effect=blocked_profile)
        bot.backloggd.close = AsyncMock(side_effect=sdk_close)
        bot.library_sync.close = AsyncMock()
        bot.guild_membership.close = AsyncMock()
        stats = StatsCog(bot, profile_count_concurrency=2)
        # One worker can fetch; the other workers are deterministically queued
        # behind the same global profile-count semaphore.
        stats._profile_count_slots = asyncio.Semaphore(1)
        await bot.add_cog(stats)

        profiles = [
            {"display_username": username}
            for username in ("Active", "Queued-1", "Queued-2", "Queued-3")
        ]

        async def count_profiles(started):
            started.set()
            return await stats._played_tab_counts([1], profiles=profiles)

        count_tasks = [
            asyncio.create_task(count_profiles(started))
            for started in operation_started
        ]
        close_task = None
        try:
            await asyncio.wait_for(fetch_started.wait(), timeout=2.0)
            for started in operation_started:
                await asyncio.wait_for(started.wait(), timeout=2.0)
            # Two count requests own four workers, with one active SDK fetch
            # and the rest queued behind the shared single-slot semaphore.

            close_task = asyncio.create_task(bot.close())
            await asyncio.wait_for(fetch_cancelled.wait(), timeout=2.0)
            self.assertFalse(close_task.done())
            self.assertNotIn("sdk_close", events)
            self.assertNotIn("db_close", events)
            self.assertTrue(any(not task.done() for task in count_tasks))

            allow_fetch_unwind.set()
            await asyncio.wait_for(close_task, timeout=2.0)
            for task in count_tasks:
                with self.assertRaises(asyncio.CancelledError):
                    await task

            self.assertEqual(events[-3:], ["fetch_drained", "sdk_close", "db_close"])
            self.assertEqual(calls, ["Active"])
            self.assertFalse(stats._profile_count_workers)
            self.assertFalse(stats._active_count_operations)
        finally:
            allow_fetch_unwind.set()
            for task in count_tasks:
                if not task.done():
                    task.cancel()
            await asyncio.gather(*count_tasks, return_exceptions=True)
            if close_task is not None and not close_task.done():
                close_task.cancel()
                await asyncio.gather(close_task, return_exceptions=True)
            if not bot.is_closed():
                await bot.close()

    async def test_cancelling_close_waits_for_workers_then_propagates(self):
        fetch_started = asyncio.Event()
        fetch_cancelled = asyncio.Event()
        allow_unwind = asyncio.Event()

        async def blocked_profile(username):
            fetch_started.set()
            try:
                await allow_unwind.wait()
            except asyncio.CancelledError:
                fetch_cancelled.set()
                await allow_unwind.wait()
                raise

        bot = SimpleNamespace(
            db=SimpleNamespace(get_guild_linked_profiles=AsyncMock(return_value=[])),
            backloggd=SimpleNamespace(get_user_profile=blocked_profile),
        )
        stats = StatsCog(bot, profile_count_concurrency=1)
        count_task = asyncio.create_task(stats._played_tab_counts(
            [1], profiles=[{"display_username": "Millers"}],
        ))
        close_task = None
        try:
            await asyncio.wait_for(fetch_started.wait(), timeout=2.0)
            close_task = asyncio.create_task(stats.close())
            await asyncio.wait_for(fetch_cancelled.wait(), timeout=2.0)
            close_task.cancel()
            allow_unwind.set()
            with self.assertRaises(asyncio.CancelledError):
                await close_task
            with self.assertRaises(asyncio.CancelledError):
                await count_task
            self.assertTrue(stats._closing)
            self.assertFalse(stats._profile_count_workers)
            self.assertFalse(stats._active_count_operations)
        finally:
            allow_unwind.set()
            if close_task is not None and not close_task.done():
                close_task.cancel()
                await asyncio.gather(close_task, return_exceptions=True)
            if not count_task.done():
                count_task.cancel()
                await asyncio.gather(count_task, return_exceptions=True)

    async def test_close_is_idempotent_and_rejects_new_count_workers(self):
        bot = SimpleNamespace(
            backloggd=SimpleNamespace(get_user_profile=AsyncMock()),
            db=SimpleNamespace(get_guild_linked_profiles=AsyncMock(return_value=[])),
        )
        stats = StatsCog(bot)
        await stats.close()
        await stats.cog_unload()
        await stats.close()

        with self.assertRaises(asyncio.CancelledError):
            await stats._played_tab_counts(
                [1], profiles=[{"display_username": "Millers"}],
            )
        bot.backloggd.get_user_profile.assert_not_awaited()
        self.assertFalse(stats._profile_count_workers)


if __name__ == "__main__":
    unittest.main()
