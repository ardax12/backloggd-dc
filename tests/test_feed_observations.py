import os
os.environ["PYTHON_DOTENV_DISABLED"] = "1"

import asyncio
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from backloggd import ActivityEntry, GameEntry, GameStatus
from bot.cogs.feed import FeedCog
from bot.db import Database
from bot.feed_models import FeedEvent
from bot.services.library_sync import LibrarySyncService


class FeedObservationRefreshTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.db = Database(str(Path(self.directory.name) / "observations.db"), turso_url="")
        await self.db.connect()
        self.service = LibrarySyncService(automatic_debounce_seconds=300, failure_backoff_seconds=300)
        self.now = datetime(2026, 11, 12, 12, 0, tzinfo=timezone.utc)

    async def asyncTearDown(self):
        await self.service.close()
        await self.db.close()
        self.directory.cleanup()

    def _entry(self, event_id, *, action_type="wishlist", rating=None, occurred_at="2026-11-12T12:05:00Z", slug="hades"):
        return FeedEvent.from_activity(ActivityEntry(
            activity_id=event_id,
            game_title="Hades",
            game_slug=slug,
            cover_url=None,
            action_type=action_type,
            action_text=action_type,
            rating=rating,
            review_notes=None,
            date_str="12:05",
            occurred_at=occurred_at,
        ))

    async def test_hidden_old_and_suppressed_actions_trigger_sync_once_per_profile(self):
        await self.db.link_user(10, "Millers")
        await self.db.link_user(11, "millers")
        api = SimpleNamespace(get_user_library=AsyncMock(return_value=[
            GameEntry(slug="hades", title="Hades", rating=4.5, status=GameStatus.WISHLIST)
        ]))
        bot = SimpleNamespace(db=self.db, backloggd=api, library_sync=self.service)
        cog = FeedCog(bot, clock=lambda: self.now)

        hidden = self._entry("wish-1", action_type="wishlist")
        old_session = self._entry("old-1", action_type="completed", occurred_at="2026-10-01T12:00:00Z")
        with patch("bot.cogs.feed.schedule_profile_refresh", wraps=__import__("bot.services.library_sync", fromlist=["schedule_profile_refresh"]).schedule_profile_refresh) as sched:
            self.assertTrue(await cog._observe_profile_activity("millers", "Millers", {"activity": [hidden, old_session], "journal": []}))
            await asyncio.sleep(0.05)
            # Identical repeated poll must not reschedule.
            self.assertFalse(await cog._observe_profile_activity("millers", "Millers", {"activity": [hidden, old_session], "journal": []}))
            self.assertEqual(sched.await_count, 1)

        api.get_user_library.assert_awaited_once_with("Millers", force_refresh=True)
        self.assertEqual(await self.db.get_profile_ratings_dict("millers"), {"hades": 4.5})

    async def test_dirty_followup_runs_when_new_observation_arrives_during_active_sync(self):
        await self.db.link_user(10, "Millers")
        first_started = asyncio.Event()
        release_first = asyncio.Event()
        call_count = 0

        async def fetch_library(username, force_refresh=False):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                first_started.set()
                await release_first.wait()
                return [GameEntry(slug="hades", title="Hades", rating=4.0, status=GameStatus.PLAYED)]
            return [GameEntry(slug="hades", title="Hades", rating=5.0, status=GameStatus.PLAYED)]

        api = SimpleNamespace(get_user_library=AsyncMock(side_effect=fetch_library))
        bot = SimpleNamespace(db=self.db, backloggd=api, library_sync=self.service)
        cog = FeedCog(bot, clock=lambda: self.now)

        first = self._entry("rate-1", action_type="rated", rating=4.0)
        await cog._observe_profile_activity("millers", "Millers", {"activity": [first], "journal": []})
        await first_started.wait()

        second = self._entry("rate-2", action_type="rated", rating=5.0)
        await cog._observe_profile_activity("millers", "Millers", {"activity": [second, first], "journal": []})
        release_first.set()

        state = self.service._profiles["millers"]
        if state.task:
            await state.task
        await asyncio.sleep(0.05)
        self.assertEqual(call_count, 2)
        self.assertEqual(await self.db.get_profile_ratings_dict("millers"), {"hades": 5.0})

    async def test_admin_feed_only_profile_skips_full_library_sync(self):
        await self.db.add_guild_admin_follow(1, "PublicOnly", None, 99)
        api = SimpleNamespace(get_user_library=AsyncMock(return_value=[]))
        bot = SimpleNamespace(db=self.db, backloggd=api, library_sync=self.service)
        cog = FeedCog(bot, clock=lambda: self.now)

        rating = self._entry("rate-pub", action_type="rated", rating=4.5)
        self.assertTrue(await cog._observe_profile_activity("publiconly", "PublicOnly", {"activity": [rating], "journal": []}))
        await asyncio.sleep(0.05)
        api.get_user_library.assert_not_awaited()
        self.assertEqual(await self.db.get_profile_ratings_dict("publiconly"), {})

    async def test_unchanged_polls_during_sync_join_without_dirty_followup(self):
        await self.db.link_user(10, "Millers")
        started, release = asyncio.Event(), asyncio.Event()

        async def fetch_library(*args, **kwargs):
            started.set()
            await release.wait()
            return []

        api = SimpleNamespace(get_user_library=AsyncMock(side_effect=fetch_library))
        cog = FeedCog(SimpleNamespace(db=self.db, backloggd=api, library_sync=self.service), clock=lambda: self.now)
        sources = {"activity": [self._entry("same-rating", action_type="rated", rating=4.0)]}
        await cog._observe_profile_activity("millers", "Millers", sources)
        task = self.service._profiles["millers"].task
        try:
            await asyncio.wait_for(started.wait(), timeout=2)
            for _ in range(3):
                await cog._observe_profile_activity("millers", "Millers", sources)
        finally:
            release.set()
            await task

        api.get_user_library.assert_awaited_once()
        self.assertIsNone(await self.db.get_profile_refresh_pending_token("millers"))

    async def test_library_sync_failure_preserves_cache_and_keeps_source_health_separate(self):
        await self.db.link_user(10, "Millers")
        await self.db.replace_profile_library(
            "Millers",
            [GameEntry(slug="hades", title="Hades", rating=4.5, status=GameStatus.PLAYED)],
            synced_at=datetime(2026, 11, 12, 10, 0, tzinfo=timezone.utc),
        )
        await self.db.record_feed_source_attempt("millers", "activity")
        await self.db.record_feed_source_success("millers", "activity", coverage="exhausted", pages_read=1, boundary_event_id="b1")

        api = SimpleNamespace(get_user_library=AsyncMock(side_effect=RuntimeError("library fetch 503")))
        bot = SimpleNamespace(db=self.db, backloggd=api, library_sync=self.service)
        cog = FeedCog(bot, clock=lambda: self.now)

        await cog._observe_profile_activity("millers", "Millers", {"activity": [self._entry("like-1", action_type="liked")], "journal": []})
        task = self.service._profiles["millers"].task
        if task:
            await task

        info = await self.db.get_profile_sync_info("millers")
        self.assertEqual(info["last_synced_at"], "2026-11-12 10:00:00")
        self.assertIsNotNone(info["last_sync_attempt_at"])
        self.assertIn("library fetch 503", info["last_sync_error"])
        self.assertEqual(await self.db.get_profile_ratings_dict("millers"), {"hades": 4.5})
        source_health = await self.db.get_profile_feed_source("millers", "activity")
        self.assertIsNone(source_health["last_error"])
        self.assertEqual(source_health["coverage"], "exhausted")

    async def test_debounced_observation_retains_pending_refresh_until_cooldown_expires(self):
        from datetime import timedelta

        await self.db.link_user(10, "Millers")
        t0 = datetime(2026, 11, 12, 12, 0, 0, tzinfo=timezone.utc)
        now = t0
        service = LibrarySyncService(
            automatic_debounce_seconds=300,
            failure_backoff_seconds=300,
            clock=lambda: (now - t0).total_seconds(),
        )
        try:
            api = SimpleNamespace(get_user_library=AsyncMock(side_effect=[
                [GameEntry(slug="hades", title="Hades", rating=4.0, status=GameStatus.PLAYED)],
                [GameEntry(slug="hades", title="Hades", rating=5.0, status=GameStatus.PLAYED)],
            ]))
            bot = SimpleNamespace(db=self.db, backloggd=api, library_sync=service)
            cog = FeedCog(bot, clock=lambda: now)

            # t = 0s: first observed rating triggers initial library fetch (#1).
            first = self._entry("rate-1", action_type="rated", rating=4.0, occurred_at="2026-11-12T12:00:00Z")
            self.assertTrue(await cog._observe_profile_activity("millers", "Millers", {"activity": [first], "journal": []}))
            await service._profiles["millers"].task
            self.assertEqual(api.get_user_library.await_count, 1)
            self.assertIsNone(await self.db.get_profile_refresh_pending_token("millers"))

            # t = 60s: new rating observed inside 300s debounce window -> skipped, pending retained.
            now = t0 + timedelta(seconds=60)
            second = self._entry("rate-2", action_type="rated", rating=5.0, occurred_at="2026-11-12T12:01:00Z")
            self.assertTrue(await cog._observe_profile_activity("millers", "Millers", {"activity": [second, first], "journal": []}))
            if service._profiles["millers"].task:
                await service._profiles["millers"].task
            await asyncio.sleep(0)
            self.assertEqual(api.get_user_library.await_count, 1)
            self.assertIsNotNone(await self.db.get_profile_refresh_pending_token("millers"))

            # t = 400s: poll sees no new activity change, but pending refresh triggers fetch (#2).
            now = t0 + timedelta(seconds=400)
            self.assertTrue(await cog._observe_profile_activity("millers", "Millers", {"activity": [second, first], "journal": []}))
            if service._profiles["millers"].task:
                await service._profiles["millers"].task
            await asyncio.sleep(0)
            self.assertEqual(api.get_user_library.await_count, 2)
            self.assertEqual(await self.db.get_profile_ratings_dict("millers"), {"hades": 5.0})
            self.assertIsNone(await self.db.get_profile_refresh_pending_token("millers"))

            # t = 500s: no pending refresh and no new changes -> no 3rd fetch.
            now = t0 + timedelta(seconds=500)
            self.assertFalse(await cog._observe_profile_activity("millers", "Millers", {"activity": [second, first], "journal": []}))
            self.assertEqual(api.get_user_library.await_count, 2)
        finally:
            await service.close()

    async def test_pending_refresh_survives_restart_and_failed_retry_backoff(self):
        from datetime import timedelta

        await self.db.link_user(10, "Millers")
        t0 = datetime(2026, 11, 12, 12, 0, 0, tzinfo=timezone.utc)
        now = t0
        service1 = LibrarySyncService(
            automatic_debounce_seconds=300,
            failure_backoff_seconds=300,
            clock=lambda: (now - t0).total_seconds(),
        )
        service2 = None
        try:
            api = SimpleNamespace(get_user_library=AsyncMock(side_effect=[
                [GameEntry(slug="hades", title="Hades", rating=4.0, status=GameStatus.PLAYED)],
                RuntimeError("upstream 503"),
                [GameEntry(slug="hades", title="Hades", rating=5.0, status=GameStatus.PLAYED)],
            ]))
            cog1 = FeedCog(SimpleNamespace(db=self.db, backloggd=api, library_sync=service1), clock=lambda: now)
            first = self._entry("rate-1", action_type="rated", rating=4.0, occurred_at="2026-11-12T12:00:00Z")
            await cog1._observe_profile_activity("millers", "Millers", {"activity": [first], "journal": []})
            if service1._profiles["millers"].task:
                await service1._profiles["millers"].task

            now = t0 + timedelta(seconds=60)
            second = self._entry("rate-2", action_type="rated", rating=5.0, occurred_at="2026-11-12T12:01:00Z")
            await cog1._observe_profile_activity("millers", "Millers", {"activity": [second, first], "journal": []})
            if service1._profiles["millers"].task:
                await service1._profiles["millers"].task
            await asyncio.sleep(0)
            self.assertEqual(api.get_user_library.await_count, 1)

            # Simulate restart with fresh service & DB connection.
            await service1.close()
            await self.db.close()
            await self.db.connect()
            service2 = LibrarySyncService(
                automatic_debounce_seconds=300,
                failure_backoff_seconds=300,
                clock=lambda: (now - t0).total_seconds(),
            )
            cog2 = FeedCog(SimpleNamespace(db=self.db, backloggd=api, library_sync=service2), clock=lambda: now)

            # t = 400s: retry runs and fails -> pending remains set.
            now = t0 + timedelta(seconds=400)
            self.assertTrue(await cog2._observe_profile_activity("millers", "Millers", {"activity": [second, first], "journal": []}))
            if service2._profiles["millers"].task:
                await service2._profiles["millers"].task
            await asyncio.sleep(0)
            self.assertEqual(api.get_user_library.await_count, 2)
            self.assertIsNotNone(await self.db.get_profile_refresh_pending_token("millers"))

            # t = 460s: within failure backoff -> skipped, still pending.
            now = t0 + timedelta(seconds=460)
            self.assertTrue(await cog2._observe_profile_activity("millers", "Millers", {"activity": [second, first], "journal": []}))
            if service2._profiles["millers"].task:
                await service2._profiles["millers"].task
            await asyncio.sleep(0)
            self.assertEqual(api.get_user_library.await_count, 2)

            # t = 720s: past failure backoff -> retry succeeds and clears pending.
            now = t0 + timedelta(seconds=720)
            self.assertTrue(await cog2._observe_profile_activity("millers", "Millers", {"activity": [second, first], "journal": []}))
            if service2._profiles["millers"].task:
                await service2._profiles["millers"].task
            await asyncio.sleep(0)
            self.assertEqual(api.get_user_library.await_count, 3)
            self.assertEqual(await self.db.get_profile_ratings_dict("millers"), {"hades": 5.0})
            self.assertIsNone(await self.db.get_profile_refresh_pending_token("millers"))
        finally:
            await service1.close()
            if service2 is not None:
                await service2.close()


if __name__ == "__main__":
    unittest.main()
