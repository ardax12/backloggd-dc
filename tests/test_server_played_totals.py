import os
os.environ["PYTHON_DOTENV_DISABLED"] = "1"

import asyncio
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

from bot.cogs.stats import StatsCog
from bot.db import Database


class ServerPlayedTotalTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.db = Database(str(Path(self.directory.name) / "stats.db"), turso_url="")
        await self.db.connect()
        self.api = SimpleNamespace(get_user_profile=AsyncMock())
        self.cog = StatsCog(SimpleNamespace(db=self.db, backloggd=self.api))

    async def asyncTearDown(self):
        await self.db.close()
        self.directory.cleanup()

    async def stats(self, member_ids):
        interaction = SimpleNamespace(
            guild=SimpleNamespace(name="Server", members=[SimpleNamespace(id=i) for i in member_ids]),
            response=SimpleNamespace(defer=AsyncMock()),
            followup=SimpleNamespace(send=AsyncMock()),
        )
        await StatsCog.server_stats.callback(self.cog, interaction)
        return interaction.followup.send.call_args.kwargs["embed"]

    @staticmethod
    def logged_field(embed):
        return next(field.value for field in embed.fields if "Games Logged" in field.name)

    async def test_sums_played_tabs_once_per_present_profile_without_using_game_statuses(self):
        await self.db.link_user(10, "PlayerA")
        await self.db.link_user(11, "playera")
        await self.db.link_user(20, "PlayerB")
        await self.db.link_user(30, "AbsentPlayer")
        await self.db.add_guild_admin_follow(1, "FeedOnly", None, 99)
        for slug, status in (("unknown-game", "unknown"), ("wishlist-game", "wishlist")):
            await self.db.upsert_game(slug, slug)
            await self.db.upsert_user_rating(10, slug, None, status)
        counts = {"playera": 100, "playerb": 200}
        self.api.get_user_profile.side_effect = lambda name: SimpleNamespace(played_count=counts[name.lower()])

        embed = await self.stats([10, 11, 20, 99])

        self.assertIn("**300**", self.logged_field(embed))
        self.assertEqual(self.api.get_user_profile.await_count, 2)
        self.assertEqual({call.args[0].lower() for call in self.api.get_user_profile.await_args_list}, set(counts))
        self.assertFalse(any("Completed" in field.name for field in embed.fields))
        self.assertEqual(len(embed.fields), 5)

    async def test_unavailable_profile_counts_are_disclosed_as_a_partial_total(self):
        await self.db.link_user(10, "Available")
        await self.db.link_user(20, "Unavailable")

        def profile(name):
            if name == "Unavailable":
                raise RuntimeError("temporary upstream failure")
            return SimpleNamespace(played_count=12)

        self.api.get_user_profile.side_effect = profile
        embed = await self.stats([10, 20])
        value = self.logged_field(embed)
        self.assertIn("At least **12**", value)
        self.assertIn("1 profile count unavailable", value)

    async def test_missing_or_invalid_counts_never_appear_as_a_verified_zero(self):
        await self.db.link_user(10, "Player")
        for result in (None, SimpleNamespace(played_count=None), SimpleNamespace(played_count=-1), SimpleNamespace(played_count=True)):
            with self.subTest(result=result):
                self.api.get_user_profile.return_value = result
                value = self.logged_field(await self.stats([10]))
                self.assertIn("Unavailable", value)
                self.assertNotIn("**0**", value)

    async def test_true_zero_and_empty_server_are_valid_zero_totals(self):
        await self.db.link_user(10, "Player")
        self.api.get_user_profile.return_value = SimpleNamespace(played_count=0)
        self.assertIn("**0**", self.logged_field(await self.stats([10])))
        self.api.get_user_profile.reset_mock()
        self.assertIn("**0**", self.logged_field(await self.stats([])))
        self.api.get_user_profile.assert_not_awaited()

    async def test_profile_requests_are_bounded(self):
        members = list(range(10, 16))
        for member_id in members:
            await self.db.link_user(member_id, f"Player{member_id}")
        active, peak = 0, 0

        async def profile(name):
            nonlocal active, peak
            active += 1
            peak = max(peak, active)
            await asyncio.sleep(0)
            active -= 1
            return SimpleNamespace(played_count=1)

        self.api.get_user_profile.side_effect = profile
        embed = await self.stats(members)
        self.assertIn("**6**", self.logged_field(embed))
        self.assertEqual(peak, 2)

    async def test_deadline_retains_completed_counts_and_cleans_up_workers(self):
        await self.db.link_user(10, "Fast")
        await self.db.link_user(20, "Slow1")
        await self.db.link_user(30, "Slow2")

        slow_started = asyncio.Event()
        slow_cancelled = 0
        release_slow = asyncio.Event()

        async def profile(name):
            nonlocal slow_cancelled
            if name == "Fast":
                return SimpleNamespace(played_count=42)
            slow_started.set()
            try:
                await release_slow.wait()
            except asyncio.CancelledError:
                slow_cancelled += 1
                raise
            return SimpleNamespace(played_count=99)

        self.cog = StatsCog(
            SimpleNamespace(db=self.db, backloggd=self.api),
            played_count_timeout=0.05,
        )
        self.api.get_user_profile.side_effect = profile

        embed = await self.stats([10, 20, 30])
        value = self.logged_field(embed)
        self.assertIn("At least **42**", value)
        self.assertIn("2 profile counts unavailable", value)
        self.assertGreaterEqual(slow_cancelled, 1)

        # Slots must be completely free for a subsequent request.
        self.api.get_user_profile.side_effect = lambda name: SimpleNamespace(played_count=5)
        next_embed = await self.stats([10, 20])
        self.assertIn("**10**", self.logged_field(next_embed))

    async def test_all_timeout_reports_unavailable_instead_of_zero(self):
        await self.db.link_user(10, "SlowA")
        await self.db.link_user(20, "SlowB")

        block = asyncio.Event()
        cancelled_count = 0

        async def slow_profile(name):
            nonlocal cancelled_count
            try:
                await block.wait()
            except asyncio.CancelledError:
                cancelled_count += 1
                raise

        self.cog = StatsCog(
            SimpleNamespace(db=self.db, backloggd=self.api),
            played_count_timeout=0.05,
        )
        self.api.get_user_profile.side_effect = slow_profile

        embed = await self.stats([10, 20])
        value = self.logged_field(embed)
        self.assertIn("Unavailable", value)
        self.assertIn("2 profile counts unavailable", value)
        self.assertNotIn("**0**", value)
        self.assertEqual(cancelled_count, 2)

    async def test_1000_profiles_use_bounded_worker_set_and_peak_concurrency_two(self):
        profiles = [{"profile_key": f"p{i}", "display_username": f"Player{i}"} for i in range(1000)]
        active = 0
        peak = 0
        worker_tasks = set()

        async def fast_profile(name):
            nonlocal active, peak
            worker_tasks.add(id(asyncio.current_task()))
            active += 1
            peak = max(peak, active)
            if len(worker_tasks) < 2 and active == 1:
                await asyncio.sleep(0)
            active -= 1
            return SimpleNamespace(played_count=1)

        self.api.get_user_profile.side_effect = fast_profile
        result = await self.cog._played_tab_counts([], profiles=profiles)

        self.assertEqual(result["total_logged"], 1000)
        self.assertEqual(result["logged_profiles_counted"], 1000)
        self.assertEqual(result["logged_profiles_missing"], 0)
        self.assertLessEqual(peak, 2)
        self.assertLessEqual(len(worker_tasks), 2)

    async def test_external_cancellation_cleans_up_workers_and_releases_slots(self):
        await self.db.link_user(10, "SlowA")
        await self.db.link_user(20, "SlowB")

        both_entered = asyncio.Event()
        entered = 0
        cancelled = 0
        block = asyncio.Event()

        async def blocked_profile(name):
            nonlocal entered, cancelled
            entered += 1
            if entered == 2:
                both_entered.set()
            try:
                await block.wait()
            except asyncio.CancelledError:
                cancelled += 1
                raise

        self.cog = StatsCog(
            SimpleNamespace(db=self.db, backloggd=self.api),
            played_count_timeout=5.0,
        )
        self.api.get_user_profile.side_effect = blocked_profile

        task = asyncio.create_task(self.cog._played_tab_counts([10, 20]))
        await asyncio.wait_for(both_entered.wait(), timeout=1.0)
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task

        self.assertEqual(cancelled, 2)
        # Slots remain completely released for subsequent lookups.
        self.api.get_user_profile.side_effect = lambda name: SimpleNamespace(played_count=3)
        embed = await self.stats([10, 20])
        self.assertIn("**6**", self.logged_field(embed))
