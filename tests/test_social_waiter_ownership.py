"""Task 3 regressions for per-generation coalesced live-fetch waiter ownership (F6)."""

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
from bot.cogs.social import SocialCog
from bot.db import Database


def _lib(username: str, score: float) -> UserLibrary:
    return UserLibrary(
        username=username,
        games={"hades": GameEntry(slug="hades", title="Hades", rating=score, status=GameStatus.PLAYED)},
    )


class SocialWaiterOwnershipTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmpdir.cleanup)
        self.db = Database(str(Path(self.tmpdir.name) / "social_ownership.db"), turso_url="")
        await self.db.connect()
        self.addAsyncCleanup(self.db.close)

    async def test_ordering1_task1_finishes_task2_registers_and_sole_waiter_cancels_before_old_waiters_unwind(self):
        """When old waiter unwind is deferred past task2 registration+cancel, task2 must still cancel."""
        fetch_num = 0
        fetch1_started = asyncio.Event()
        fetch1_release = asyncio.Event()
        fetch2_started = asyncio.Event()
        fetch2_cancelled = asyncio.Event()
        fetch2_release = asyncio.Event()
        task1_popped = asyncio.Event()
        allow_w1_unwind = asyncio.Event()

        async def get_library(username: str):
            nonlocal fetch_num
            fetch_num += 1
            if fetch_num == 1:
                fetch1_started.set()
                await fetch1_release.wait()
                return _lib(username, 4.0)
            fetch2_started.set()
            try:
                await fetch2_release.wait()
                return _lib(username, 5.0)
            except asyncio.CancelledError:
                fetch2_cancelled.set()
                raise

        lookup_num = 0

        async def get_sync_info(_username: str):
            nonlocal lookup_num
            lookup_num += 1
            if lookup_num == 2:
                await task1_popped.wait()
            return None

        db = SimpleNamespace(
            get_profile_sync_info=AsyncMock(side_effect=get_sync_info),
            get_profile_ratings_dict=AsyncMock(return_value={}),
        )
        api = SimpleNamespace(get_user_library=AsyncMock(side_effect=get_library))
        cog = SocialCog(SimpleNamespace(db=db, backloggd=api), live_fetch_timeout=10.0)
        self.addAsyncCleanup(cog.close)

        class TaskRegistrySpy(dict):
            def pop(self, key, *args):
                existed = key in self
                result = super().pop(key, *args)
                if existed and key == "alice":
                    task1_popped.set()
                return result

        cog._live_library_tasks = TaskRegistrySpy()

        orig_wait_for = asyncio.wait_for
        wait_num = 0

        async def gated_wait_for(awaitable, timeout):
            nonlocal wait_num
            is_cog_call = timeout == 10.0
            my_num = 0
            if is_cog_call:
                wait_num += 1
                my_num = wait_num
            res = await orig_wait_for(awaitable, timeout)
            if my_num == 1:
                # Hold old waiter w1 in unwinding before its finally runs (models multi-hop unwind).
                await allow_w1_unwind.wait()
            return res

        try:
            with patch("bot.cogs.social.asyncio.wait_for", side_effect=gated_wait_for):
                w1 = asyncio.create_task(cog._get_or_fetch_library_snapshot("Alice"))
                await orig_wait_for(fetch1_started.wait(), timeout=2.0)

                w2 = asyncio.create_task(cog._get_or_fetch_library_snapshot("  ALICE "))
                fetch1_release.set()

                await orig_wait_for(fetch2_started.wait(), timeout=2.0)
                # task2 registered while w1 has not exited its try/finally yet.
                w2.cancel()
                with self.assertRaises(asyncio.CancelledError):
                    await w2

                # task2's sole waiter cancelled -> task2 itself MUST cancel immediately, freeing its slot.
                await orig_wait_for(fetch2_cancelled.wait(), timeout=0.5)
                await asyncio.sleep(0)
                self.assertEqual(dict(cog._live_library_tasks), {})
                self.assertEqual(dict(cog._live_library_waiters), {})

                allow_w1_unwind.set()
                snap1 = await w1
                self.assertEqual(snap1.ratings, {"hades": 4.0})
        finally:
            fetch1_release.set()
            fetch2_release.set()
            allow_w1_unwind.set()

        await asyncio.sleep(0)
        self.assertEqual(dict(cog._live_library_tasks), {})
        self.assertEqual(dict(cog._live_library_waiters), {})
        self.assertEqual(cog._tracked_live_tasks, set())

    async def test_ordering2_old_finalizer_runs_while_task2_has_valid_waiter_without_wait_for_patch(self):
        """Real event-loop ordering: task1 cleanup wakes w2 ahead of w1 finally; count must stay 1 for task2."""
        fetch_num = 0
        fetch1_started = asyncio.Event()
        fetch1_release = asyncio.Event()
        fetch2_started = asyncio.Event()
        fetch2_cancelled = asyncio.Event()
        fetch2_release = asyncio.Event()
        task1_popped = asyncio.Event()
        observed_waiter_counts: list[int] = []

        async def get_library(username: str):
            nonlocal fetch_num
            fetch_num += 1
            if fetch_num == 1:
                fetch1_started.set()
                await fetch1_release.wait()
                return _lib(username, 3.5)
            fetch2_started.set()
            try:
                await fetch2_release.wait()
                return _lib(username, 4.5)
            except asyncio.CancelledError:
                fetch2_cancelled.set()
                raise

        lookup_num = 0

        async def get_sync_info(_username: str):
            nonlocal lookup_num
            lookup_num += 1
            if lookup_num == 2:
                await task1_popped.wait()
            return None

        db = SimpleNamespace(
            get_profile_sync_info=AsyncMock(side_effect=get_sync_info),
            get_profile_ratings_dict=AsyncMock(return_value={}),
        )
        api = SimpleNamespace(get_user_library=AsyncMock(side_effect=get_library))
        cog = SocialCog(SimpleNamespace(db=db, backloggd=api), live_fetch_timeout=10.0)
        self.addAsyncCleanup(cog.close)

        class TaskRegistrySpy(dict):
            def pop(self, key, *args):
                existed = key in self
                result = super().pop(key, *args)
                if existed and key == "alice":
                    task1_popped.set()
                return result

        class WaiterSpy(dict):
            def __setitem__(self, key, value):
                observed_waiter_counts.append(value)
                super().__setitem__(key, value)

        cog._live_library_tasks = TaskRegistrySpy()
        cog._live_library_waiters = WaiterSpy()

        w1 = asyncio.create_task(cog._get_or_fetch_library_snapshot("Alice"))
        await asyncio.wait_for(fetch1_started.wait(), timeout=2.0)

        w2 = asyncio.create_task(cog._get_or_fetch_library_snapshot("alice"))
        await asyncio.sleep(0)
        fetch1_release.set()

        await asyncio.wait_for(fetch2_started.wait(), timeout=2.0)
        snap1 = await w1
        self.assertEqual(snap1.ratings, {"hades": 3.5})
        # w2 joined task2 -> generation count must NEVER jump to 2, and w1's finally must NOT kill task2!
        self.assertEqual(observed_waiter_counts, [1, 1])
        self.assertFalse(fetch2_cancelled.is_set())
        self.assertEqual(cog._live_library_waiters.get("alice"), 1)

        fetch2_release.set()
        snap2 = await w2
        self.assertEqual(snap2.ratings, {"hades": 4.5})
        self.assertEqual(dict(cog._live_library_tasks), {})
        self.assertEqual(dict(cog._live_library_waiters), {})
        self.assertEqual(cog._tracked_live_tasks, set())

    async def test_same_generation_two_waiters_timeout_failed_fetch_case_variants_and_close(self):
        # 1. Two waiters (case variants), one times out/cancels, late arrival joins same generation, fetch fails.
        started = asyncio.Event()
        release = asyncio.Event()

        async def failing_fetch(_username: str):
            started.set()
            await release.wait()
            raise RuntimeError("boom")

        db = SimpleNamespace(
            get_profile_sync_info=AsyncMock(return_value=None),
            get_profile_ratings_dict=AsyncMock(return_value={}),
        )
        api = SimpleNamespace(get_user_library=AsyncMock(side_effect=failing_fetch))
        cog = SocialCog(SimpleNamespace(db=db, backloggd=api), live_fetch_timeout=10.0)
        self.addAsyncCleanup(cog.close)

        w1 = asyncio.create_task(cog._get_or_fetch_ratings("CaseUser"))
        await asyncio.wait_for(started.wait(), timeout=2.0)
        w2 = asyncio.create_task(cog._get_or_fetch_ratings("  caseuser  "))
        await asyncio.sleep(0)
        self.assertEqual(cog._live_library_waiters.get("caseuser"), 2)

        w1.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await w1
        self.assertEqual(cog._live_library_waiters.get("caseuser"), 1)

        # Late arrival coalesces onto the same fetch rather than starting a second request.
        w3 = asyncio.create_task(cog._get_or_fetch_ratings("CASEUSER"))
        await asyncio.sleep(0)
        self.assertEqual(cog._live_library_waiters.get("caseuser"), 2)
        self.assertEqual(api.get_user_library.await_count, 1)

        release.set()
        res = await asyncio.gather(w2, w3, return_exceptions=True)
        self.assertTrue(all(isinstance(r, RuntimeError) for r in res))
        self.assertEqual(dict(cog._live_library_tasks), {})
        self.assertEqual(dict(cog._live_library_waiters), {})

        # 2. Timeout cancels sole waiter and frees slot for subsequent retry.
        release.clear()
        cog_to = SocialCog(SimpleNamespace(db=db, backloggd=api), live_fetch_timeout=0.05)
        with self.assertRaises(asyncio.TimeoutError):
            await cog_to._get_or_fetch_ratings("TimeoutUser")
        await cog_to.close()
        self.assertEqual(dict(cog_to._live_library_tasks), {})
        self.assertEqual(dict(cog_to._live_library_waiters), {})

    async def test_close_drains_multiple_overlapping_generations(self):
        fetch1_started = asyncio.Event()
        fetch1_release = asyncio.Event()
        fetch2_started = asyncio.Event()
        fetch2_unwind = asyncio.Event()
        task1_popped = asyncio.Event()
        call_n = 0

        async def get_library(username: str):
            nonlocal call_n
            call_n += 1
            if call_n == 1:
                fetch1_started.set()
                await fetch1_release.wait()
                return _lib(username, 3.0)
            fetch2_started.set()
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                await fetch2_unwind.wait()
                raise

        lookup_n = 0

        async def get_sync_info(_username: str):
            nonlocal lookup_n
            lookup_n += 1
            if lookup_n == 2:
                await task1_popped.wait()
            return None

        db = SimpleNamespace(
            get_profile_sync_info=AsyncMock(side_effect=get_sync_info),
            get_profile_ratings_dict=AsyncMock(return_value={}),
        )
        api = SimpleNamespace(get_user_library=AsyncMock(side_effect=get_library))
        cog = SocialCog(SimpleNamespace(db=db, backloggd=api), live_fetch_timeout=10.0)

        class TaskRegistrySpy(dict):
            def pop(self, key, *args):
                existed = key in self
                result = super().pop(key, *args)
                if existed and key == "multi":
                    task1_popped.set()
                return result

        cog._live_library_tasks = TaskRegistrySpy()

        w1 = asyncio.create_task(cog._get_or_fetch_ratings("multi"))
        await asyncio.wait_for(fetch1_started.wait(), timeout=2.0)
        w2 = asyncio.create_task(cog._get_or_fetch_ratings("MULTI"))
        await asyncio.sleep(0)
        fetch1_release.set()
        await asyncio.wait_for(fetch2_started.wait(), timeout=2.0)

        close_task = asyncio.create_task(cog.close())
        await asyncio.sleep(0)
        self.assertFalse(close_task.done())
        fetch2_unwind.set()
        await asyncio.wait_for(close_task, timeout=2.0)

        self.assertEqual(await w1, {"hades": 3.0})
        with self.assertRaises(asyncio.CancelledError):
            await w2
        self.assertEqual(dict(cog._live_library_tasks), {})
        self.assertEqual(dict(cog._live_library_waiters), {})
        self.assertEqual(cog._tracked_live_tasks, set())


if __name__ == "__main__":
    unittest.main()
