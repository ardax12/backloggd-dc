import os
os.environ["PYTHON_DOTENV_DISABLED"] = "1"

import tempfile
import unittest
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

from bot.cogs.feed import FeedCog
from bot.db import Database
from feed_samples import completion_event


class FeedSourceHealthTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.db = Database(str(Path(self.directory.name) / "source-health.db"), turso_url="")
        await self.db.connect()
        await self.db.link_user(10, "Millers")

    async def asyncTearDown(self):
        await self.db.close()
        self.directory.cleanup()

    async def test_unsupported_latest_page_is_useful_but_keeps_an_unknown_gap(self):
        api = SimpleNamespace(
            get_user_activity=AsyncMock(return_value=[completion_event()]),
            get_user_journal=AsyncMock(return_value=[]),
        )
        sources = await FeedCog(SimpleNamespace(db=self.db, backloggd=api))._fetch_feed_sources("Millers")

        self.assertEqual(len(sources["activity"]), 1)
        health = await self.db.get_profile_feed_source("millers", "activity")
        self.assertEqual(health["attempts"], 1)
        self.assertEqual(health["successes"], 1)
        self.assertEqual(health["coverage"], "unsupported")
        self.assertEqual(health["pages_read"], 1)
        self.assertIsNotNone(health["last_attempt_at"])
        self.assertIsNotNone(health["last_success_at"])
        self.assertIsNotNone(health["unresolved_gap_since"])
        self.assertIsNone(health["last_continuous_boundary_event_id"])

    async def test_public_snapshot_api_receives_only_the_last_continuous_boundary(self):
        await self.db.record_feed_source_attempt("millers", "activity")
        await self.db.record_feed_source_success(
            "millers", "activity", coverage="exhausted", pages_read=4,
            boundary_event_id="known-old-event",
        )
        snapshot_api = AsyncMock(return_value=SimpleNamespace(
            entries=[replace(completion_event(), activity_id="known-old-event")],
            coverage="boundary_reached", pages_read=2,
        ))
        api = SimpleNamespace(
            get_user_activity_snapshot=snapshot_api,
            get_user_journal=AsyncMock(return_value=[]),
        )

        await FeedCog(SimpleNamespace(db=self.db, backloggd=api))._fetch_feed_sources("Millers")

        snapshot_api.assert_awaited_once_with(
            "Millers", stop_at_ids=("known-old-event",), max_pages=5
        )
        health = await self.db.get_profile_feed_source("millers", "activity")
        self.assertEqual(health["coverage"], "boundary_reached")
        self.assertEqual(health["last_continuous_boundary_event_id"], "known-old-event")
        self.assertIsNone(health["unresolved_gap_since"])

    async def test_unsupported_fetches_do_not_clear_gap_until_a_boundary_is_reached(self):
        await self.db.record_feed_source_attempt("millers", "activity")
        await self.db.record_feed_source_success(
            "millers", "activity", coverage="exhausted", pages_read=3,
            boundary_event_id="boundary-1",
        )
        await self.db.record_feed_source_attempt("millers", "activity")
        await self.db.record_feed_source_success(
            "millers", "activity", coverage="unsupported", pages_read=1,
        )
        unresolved = await self.db.get_profile_feed_source("millers", "activity")
        gap_started = unresolved["unresolved_gap_since"]
        self.assertIsNotNone(gap_started)
        self.assertEqual(unresolved["last_continuous_boundary_event_id"], "boundary-1")

        await self.db.record_feed_source_attempt("millers", "activity")
        await self.db.record_feed_source_success(
            "millers", "activity", coverage="boundary_reached", pages_read=5,
            boundary_event_id="boundary-1",
        )
        recovered = await self.db.get_profile_feed_source("millers", "activity")
        self.assertIsNone(recovered["unresolved_gap_since"])
        self.assertIsNone(recovered["gap_reason"])
        self.assertEqual(recovered["last_continuous_boundary_event_id"], "boundary-1")
        self.assertEqual(recovered["successes"], 3)

    async def test_failure_keeps_last_success_and_boundary_visible(self):
        await self.db.record_feed_source_attempt("millers", "activity")
        await self.db.record_feed_source_success(
            "millers", "activity", coverage="boundary_reached", pages_read=2,
            boundary_event_id="known",
        )
        successful = await self.db.get_profile_feed_source("millers", "activity")
        await self.db.record_feed_source_attempt("millers", "activity")
        await self.db.record_feed_source_failure("millers", "activity", "blocked")
        failed = await self.db.get_profile_feed_source("millers", "activity")

        self.assertEqual(failed["successes"], successful["successes"])
        self.assertEqual(failed["last_success_at"], successful["last_success_at"])
        self.assertEqual(failed["last_continuous_boundary_event_id"], "known")
        self.assertEqual(failed["last_error"], "blocked")
        self.assertEqual(failed["attempts"], 2)

    async def test_invalid_snapshot_is_a_failure_not_an_empty_valid_page(self):
        api = SimpleNamespace(
            get_user_activity_snapshot=AsyncMock(return_value=SimpleNamespace(
                entries=[], coverage="invented", pages_read=1,
            )),
            get_user_journal=AsyncMock(return_value=[]),
        )

        sources = await FeedCog(SimpleNamespace(db=self.db, backloggd=api))._fetch_feed_sources("Millers")

        self.assertNotIn("activity", sources)
        health = await self.db.get_profile_feed_source("millers", "activity")
        self.assertEqual(health["attempts"], 1)
        self.assertEqual(health["successes"], 0)
        self.assertIn("unknown activity coverage", health["last_error"])

    async def test_limited_outage_recovery_records_gap_until_and_closes_on_boundary(self):
        from datetime import datetime, timezone
        await self.db.record_feed_source_attempt("millers", "activity")
        await self.db.record_feed_source_success(
            "millers", "activity", coverage="exhausted", pages_read=2,
            boundary_event_id="b-1",
        )
        gap_until = datetime(2026, 11, 12, 9, 30, tzinfo=timezone.utc)
        await self.db.record_feed_source_attempt("millers", "activity")
        await self.db.record_feed_source_success(
            "millers", "activity", coverage="limited", pages_read=5,
            gap_until=gap_until,
        )
        health = await self.db.get_profile_feed_source("millers", "activity")
        self.assertIsNotNone(health["unresolved_gap_since"])
        self.assertEqual(health["unresolved_gap_until"], "2026-11-12 09:30:00")
        self.assertEqual(health["gap_reason"], "limited")

        await self.db.record_feed_source_attempt("millers", "activity")
        await self.db.record_feed_source_success(
            "millers", "activity", coverage="boundary_reached", pages_read=2,
            boundary_event_id="b-2",
        )
        closed = await self.db.get_profile_feed_source("millers", "activity")
        self.assertIsNone(closed["unresolved_gap_since"])
        self.assertIsNone(closed["unresolved_gap_until"])
        self.assertEqual(closed["last_continuous_boundary_event_id"], "b-2")


if __name__ == "__main__":
    unittest.main()
