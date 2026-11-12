import os
os.environ["PYTHON_DOTENV_DISABLED"] = "1"

import asyncio
import unittest

from bot.services.library_sync import LibrarySyncService, SyncOutcome


class LibrarySyncServiceTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.now = 0.0
        self.service = LibrarySyncService(clock=lambda: self.now)

    async def asyncTearDown(self):
        await self.service.close()

    async def test_concurrent_profile_requests_share_one_refresh_task(self):
        started = asyncio.Event()
        release = asyncio.Event()
        calls = 0

        async def refresh():
            nonlocal calls
            calls += 1
            started.set()
            await release.wait()
            return 12

        first = self.service.request("millers", refresh, manual=True)
        await started.wait()
        second = self.service.request("millers", refresh, manual=True)
        self.assertIs(first, second)

        release.set()
        outcome = await first
        self.assertEqual(calls, 1)
        self.assertEqual(outcome, SyncOutcome(status="success", game_count=12))

    async def test_automatic_requests_obey_debounce_and_failure_backoff(self):
        calls = 0

        async def refresh():
            nonlocal calls
            calls += 1
            return 5 if calls == 1 else None

        success = await self.service.request("millers", refresh, manual=True)
        skipped = await self.service.request("millers", refresh)
        self.assertEqual(success.status, "success")
        self.assertEqual(skipped, SyncOutcome(status="skipped", game_count=5, reason="debounce"))
        self.assertEqual(calls, 1)

        self.now += 300
        failure = await self.service.request("millers", refresh)
        backed_off = await self.service.request("millers", refresh)
        self.assertEqual(failure.status, "failed")
        self.assertEqual(backed_off.status, "skipped")
        self.assertEqual(calls, 2)

        self.now += 300
        retried = await self.service.request("millers", refresh)
        self.assertEqual(retried.status, "failed")
        self.assertEqual(calls, 3)

    async def test_fresh_persisted_snapshot_skips_automatic_link_sync(self):
        calls = 0

        async def refresh():
            nonlocal calls
            calls += 1
            return 5

        self.service.seed_success("millers", age_seconds=60, game_count=5)
        outcome = await self.service.request(
            "millers", refresh, freshness_window_seconds=3600
        )

        self.assertEqual(outcome, SyncOutcome(status="skipped", game_count=5, reason="fresh"))
        self.assertEqual(calls, 0)

    async def test_manual_request_bypasses_backoff_and_joins_running_request(self):
        async def fail():
            return None

        await self.service.request("millers", fail, manual=True)
        started = asyncio.Event()
        release = asyncio.Event()

        async def refresh():
            started.set()
            await release.wait()
            return 9

        first = self.service.request("millers", refresh, manual=True)
        await started.wait()
        joined = self.service.request("millers", refresh, manual=True)
        self.assertIs(first, joined)
        release.set()
        self.assertEqual((await first).game_count, 9)

    async def test_only_two_profiles_refresh_at_once(self):
        active = 0
        maximum_active = 0
        release = asyncio.Event()
        two_started = asyncio.Event()
        third_started = asyncio.Event()
        lock = asyncio.Lock()

        async def refresh(index):
            nonlocal active, maximum_active
            async with lock:
                active += 1
                maximum_active = max(maximum_active, active)
                if active == 2:
                    two_started.set()
                if active == 3:
                    third_started.set()
            await release.wait()
            async with lock:
                active -= 1
            return index

        tasks = [
            self.service.request(str(index), lambda i=index: refresh(i), manual=True)
            for index in range(3)
        ]
        await asyncio.wait_for(two_started.wait(), timeout=1)
        self.assertFalse(third_started.is_set())
        self.assertEqual(maximum_active, 2)
        release.set()
        outcomes = await asyncio.gather(*tasks)
        self.assertEqual([outcome.game_count for outcome in outcomes], [0, 1, 2])

    async def test_activity_during_refresh_keeps_only_one_latest_follow_up(self):
        started = asyncio.Event()
        release = asyncio.Event()
        calls = []

        async def first():
            calls.append("first")
            started.set()
            await release.wait()
            return 1

        async def superseded_follow_up():
            calls.append("superseded")
            return 2

        async def latest_follow_up():
            calls.append("latest")
            return 3

        task = self.service.request("millers", first, manual=True)
        await started.wait()
        self.service.request("millers", superseded_follow_up, dirty=True)
        self.service.request("millers", latest_follow_up, dirty=True)
        release.set()

        outcome = await task
        self.assertEqual(calls, ["first", "latest"])
        self.assertEqual(outcome.game_count, 3)


if __name__ == "__main__":
    unittest.main()
