"""Task 12 parity and race regression tests for shared library refresh behavior."""

import asyncio
import os
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

os.environ["PYTHON_DOTENV_DISABLED"] = "1"
os.environ["TURSO_DATABASE_URL"] = ""
os.environ["TURSO_AUTH_TOKEN"] = ""
os.environ["DEV_GUILD_ID"] = ""

from backloggd import ActivityEntry, GameEntry, GameStatus

from bot.cogs.account import AccountCog
from bot.cogs.feed import FeedCog
from bot.db import Database
from bot.feed_models import FeedEvent
from bot.services.freshness import ensure_profiles_fresh
from bot.services.library_sync import (
    LibrarySyncService,
    schedule_profile_refresh,
    sync_profile_library,
)


def _game(slug: str, title: str, rating: float | None = 4.5, status: GameStatus = GameStatus.PLAYED) -> GameEntry:
    return GameEntry(slug=slug, title=title, rating=rating, status=status)


def _activity(event_id: str, slug: str = "hades", action_type: str = "rated", rating: float | None = 4.5) -> FeedEvent:
    return FeedEvent.from_activity(
        ActivityEntry(
            activity_id=event_id,
            game_title=slug.title(),
            game_slug=slug,
            cover_url=None,
            action_type=action_type,
            action_text=action_type,
            rating=rating,
            review_notes=None,
            date_str="2026-11-12",
            occurred_at="2026-11-12T10:00:00Z",
        )
    )


class LibrarySyncParityAndRaceTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmpdir.cleanup)
        self.db_path = str(Path(self.tmpdir.name) / "sync_parity.db")
        self.db = Database(self.db_path, turso_url="")
        self.addAsyncCleanup(self.db.close)
        await self.db.connect()
        self.now_mono = 1000.0
        self.now_utc = datetime(2026, 11, 12, 12, 0, tzinfo=timezone.utc)
        self.sync = LibrarySyncService(clock=lambda: self.now_mono)
        self.addAsyncCleanup(self.sync.close)

    async def test_account_sync_and_background_sync_share_profile_replacement_contract(self):
        """AccountCog._sync_user_library delegates to sync_profile_library and uses replace_profile_library."""
        db = SimpleNamespace(
            get_profile_refresh_pending_token=AsyncMock(return_value="tok-1"),
            replace_profile_library=AsyncMock(return_value=True),
            record_profile_sync_failure=AsyncMock(),
        )
        api = SimpleNamespace(
            get_user_library=AsyncMock(return_value=[_game("hades", "Hades", 4.5)])
        )
        bot = SimpleNamespace(db=db, backloggd=api, library_sync=self.sync)
        cog = AccountCog(bot)

        count = await cog._sync_user_library(10, "Millers")
        self.assertEqual(count, 1)
        db.replace_profile_library.assert_awaited_once_with(
            "Millers",
            [_game("hades", "Hades", 4.5)],
            require_linked=True,
            clear_pending_token="tok-1",
        )

        # Compare parity with background sync_profile_library on identical boundary.
        db2 = SimpleNamespace(
            get_profile_refresh_pending_token=AsyncMock(return_value="tok-1"),
            replace_profile_library=AsyncMock(return_value=True),
            record_profile_sync_failure=AsyncMock(),
        )
        api2 = SimpleNamespace(
            get_user_library=AsyncMock(return_value=[_game("hades", "Hades", 4.5)])
        )
        bot2 = SimpleNamespace(db=db2, backloggd=api2, library_sync=self.sync)
        count2 = await sync_profile_library(bot2, "Millers")
        self.assertEqual(count2, count)
        self.assertEqual(
            db2.replace_profile_library.call_args,
            db.replace_profile_library.call_args,
        )

    async def test_account_seed_sync_state_shares_canonical_seeding_behavior(self):
        """AccountCog._seed_sync_state seeds both fresh success and failure backoff like schedule_profile_refresh."""
        await self.db.link_user(10, "Millers")
        await self.db.record_profile_sync_failure(
            "Millers",
            "upstream 503",
            attempted_at=datetime.now(timezone.utc) - timedelta(seconds=30),
        )
        bot = SimpleNamespace(db=self.db, backloggd=SimpleNamespace(), library_sync=self.sync)
        cog = AccountCog(bot)
        await cog._seed_sync_state("Millers")

        state = self.sync.get_profile_state("millers")
        self.assertTrue(state["failed_last"])
        self.assertEqual(state["last_error"], "upstream 503")

        skipped = await self.sync.request("millers", AsyncMock(return_value=1), manual=False)
        self.assertEqual(skipped.status, "skipped")
        self.assertEqual(skipped.reason, "backoff")

        manual = await self.sync.request("millers", AsyncMock(return_value=3), manual=True)
        self.assertEqual(manual.status, "success")
        self.assertEqual(manual.game_count, 3)

    async def test_failure_restart_and_manual_bypass_preserve_previous_library(self):
        await self.db.link_user(10, "Millers")
        await self.db.replace_profile_library(
            "Millers", [_game("hades", "Hades", 4.5)], synced_at=self.now_utc - timedelta(hours=2)
        )
        api = SimpleNamespace(get_user_library=AsyncMock(side_effect=RuntimeError("boom 502")))
        bot = SimpleNamespace(db=self.db, backloggd=api, library_sync=self.sync)

        outcome = await (await schedule_profile_refresh(bot, "Millers", manual=True, now=self.now_utc))
        self.assertEqual(outcome.status, "failed")
        self.assertEqual(await self.db.get_profile_ratings_dict("millers"), {"hades": 4.5})

        # Simulate bot restart with a fresh LibrarySyncService.
        restarted_sync = LibrarySyncService(clock=lambda: self.now_mono)
        self.addAsyncCleanup(restarted_sync.close)
        restarted_bot = SimpleNamespace(db=self.db, backloggd=api, library_sync=restarted_sync)

        # Backoff skips automatic refresh immediately after restart (using wall-clock consistent time).
        attempt_now = datetime.now(timezone.utc)
        await self.db.record_profile_sync_failure("Millers", "boom 502", attempted_at=attempt_now)
        auto_task = await schedule_profile_refresh(
            restarted_bot, "Millers", manual=False, now=attempt_now + timedelta(seconds=30)
        )
        auto_outcome = await auto_task
        self.assertEqual(auto_outcome.status, "skipped")
        self.assertEqual(auto_outcome.reason, "backoff")

        # Manual refresh bypasses backoff and succeeds when upstream recovers.
        api.get_user_library = AsyncMock(return_value=[_game("celeste", "Celeste", 5.0)])
        manual_outcome = await (
            await schedule_profile_refresh(restarted_bot, "Millers", manual=True, now=attempt_now + timedelta(seconds=31))
        )
        self.assertEqual(manual_outcome.status, "success")
        self.assertEqual(manual_outcome.game_count, 1)
        self.assertEqual(await self.db.get_profile_ratings_dict("millers"), {"celeste": 5.0})

    async def test_other_cog_joining_running_account_sync_does_not_trigger_second_scrape(self):
        await self.db.link_user(10, "Millers")
        started = asyncio.Event()
        release = asyncio.Event()

        async def fetch_library(username, force_refresh=False):
            started.set()
            await release.wait()
            return [_game("hades", "Hades", 5.0)]

        api = SimpleNamespace(get_user_library=AsyncMock(side_effect=fetch_library))
        bot = SimpleNamespace(db=self.db, backloggd=api, library_sync=self.sync)
        account_cog = AccountCog(bot)

        account_task = account_cog._start_sync_task(10, "Millers", manual=True)
        await asyncio.wait_for(started.wait(), timeout=1.0)

        # Freshness service joining mid-sync shares the active task.
        statuses = await ensure_profiles_fresh(
            bot, [("millers", "Millers")], now=self.now_utc
        )
        self.assertTrue(statuses["millers"].refreshing)

        # Feed schedule joining mid-sync without dirty changes also shares the active task.
        feed_task = await schedule_profile_refresh(bot, "millers", now=self.now_utc)
        self.assertIs(feed_task, account_task)

        release.set()
        outcome = await account_task
        self.assertEqual(outcome.status, "success")
        api.get_user_library.assert_awaited_once_with("Millers", force_refresh=True)

    async def test_initiator_unlink_while_duplicate_remains_keeps_shared_update_via_any_link_guard(self):
        await self.db.link_user(10, "Millers")
        await self.db.link_user(20, "millers")
        started = asyncio.Event()
        release = asyncio.Event()

        async def fetch_library(username, force_refresh=False):
            started.set()
            await release.wait()
            return [_game("balatro", "Balatro", 4.5)]

        api = SimpleNamespace(get_user_library=AsyncMock(side_effect=fetch_library))
        bot = SimpleNamespace(db=self.db, backloggd=api, library_sync=self.sync)
        account_cog = AccountCog(bot)

        task = account_cog._start_sync_task(10, "Millers", manual=True)
        await asyncio.wait_for(started.wait(), timeout=1.0)

        # Initiator unlinks while duplicate member 20 remains connected.
        self.assertTrue(await self.db.unlink_user(10))
        release.set()
        outcome = await task

        self.assertEqual(outcome.status, "success")
        self.assertEqual(outcome.game_count, 1)
        self.assertEqual(await self.db.get_profile_ratings_dict("millers"), {"balatro": 4.5})

    async def test_last_link_removed_and_relink_during_sync_discard_stale_results(self):
        await self.db.link_user(10, "Alpha")
        started = asyncio.Event()
        release = asyncio.Event()

        async def fetch_library(username, force_refresh=False):
            if username == "Alpha":
                started.set()
                await release.wait()
                return [_game("hades", "Hades", 4.0)]
            return [_game("celeste", "Celeste", 5.0)]

        api = SimpleNamespace(get_user_library=AsyncMock(side_effect=fetch_library))
        bot = SimpleNamespace(db=self.db, backloggd=api, library_sync=self.sync)
        account_cog = AccountCog(bot)

        alpha_task = account_cog._start_sync_task(10, "Alpha", manual=True)
        await asyncio.wait_for(started.wait(), timeout=1.0)

        # Last link removed and member relinks to Beta while Alpha fetch is in flight.
        self.assertTrue(await self.db.unlink_user(10))
        await self.db.link_user(10, "Beta")
        beta_task = account_cog._start_sync_task(10, "Beta", manual=True)
        release.set()

        alpha_outcome, beta_outcome = await asyncio.gather(alpha_task, beta_task)
        self.assertEqual(alpha_outcome.status, "failed")
        self.assertEqual(beta_outcome.status, "success")
        self.assertEqual(await self.db.get_profile_ratings_dict("alpha"), {})
        self.assertEqual(await self.db.get_profile_ratings_dict("beta"), {"celeste": 5.0})

    async def test_new_activity_during_running_account_sync_schedules_dirty_followup_and_clears_token(self):
        await self.db.link_user(10, "Millers")
        started = asyncio.Event()
        release = asyncio.Event()
        calls = 0

        async def fetch_library(username, force_refresh=False):
            nonlocal calls
            calls += 1
            if calls == 2:
                started.set()
                await release.wait()
                return [_game("hades", "Hades", 4.0)]
            return [_game("hades", "Hades", 5.0)]

        api = SimpleNamespace(get_user_library=AsyncMock(side_effect=fetch_library))
        bot = SimpleNamespace(db=self.db, backloggd=api, library_sync=self.sync)
        account_cog = AccountCog(bot)
        feed_cog = FeedCog(bot, clock=lambda: self.now_utc)

        # Baseline observation before sync starts (completes as calls == 1).
        await feed_cog._observe_profile_activity("millers", "Millers", {"activity": [_activity("a1", rating=4.0)]})
        if self.sync._profiles.get("millers") and self.sync._profiles["millers"].task:
            await self.sync._profiles["millers"].task

        sync_task = account_cog._start_sync_task(10, "Millers", manual=True)
        await asyncio.wait_for(started.wait(), timeout=1.0)

        # New rating activity observed mid-sync sets pending token and requests dirty follow-up.
        scheduled = await feed_cog._observe_profile_activity(
            "millers", "Millers", {"activity": [_activity("a2", rating=5.0), _activity("a1", rating=4.0)]}
        )
        self.assertTrue(scheduled)
        self.assertIsNotNone(await self.db.get_profile_refresh_pending_token("millers"))

        release.set()
        outcome = await sync_task
        self.assertEqual(calls, 3)
        self.assertEqual(outcome.status, "success")
        self.assertIsNone(await self.db.get_profile_refresh_pending_token("millers"))
        self.assertEqual(await self.db.get_profile_ratings_dict("millers"), {"hades": 5.0})


if __name__ == "__main__":
    unittest.main()
