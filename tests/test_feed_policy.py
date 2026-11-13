import unittest
from dataclasses import replace
from datetime import datetime, timezone
import tempfile
from pathlib import Path

from bot.feed_models import FeedEvent
from bot.feed_policy import classify_feed_event
from bot.db import Database


def event(action="completed", *, played_date=None, occurred_at=None, **changes):
    base = FeedEvent(
        "event", "Game", "game", None, action, action, None, None, None,
        played_date=played_date,
        occurred_at=occurred_at,
    )
    return replace(base, **changes)


class FeedPolicyTests(unittest.TestCase):
    def setUp(self):
        self.now = datetime(2026, 11, 12, 0, 0, tzinfo=timezone.utc)

    def test_exactly_seven_days_is_eligible_and_eight_days_is_suppressed(self):
        seven = classify_feed_event(event(played_date="2026-11-05"), self.now)
        eight = classify_feed_event(event(played_date="2026-11-04"), self.now)
        self.assertEqual(seven, ("eligible", "session_is_within_seven_utc_calendar_days", "2026-11-05"))
        self.assertEqual(eight[0], "suppressed_old")

    def test_calendar_boundary_uses_injected_utc_clock_and_converts_offsets(self):
        now = datetime.fromisoformat("2026-11-12T00:30:00+03:00")
        candidate = event(occurred_at="2026-11-04T23:30:00-05:00")
        decision = classify_feed_event(candidate, now)
        self.assertEqual(decision[0], "eligible")
        self.assertEqual(decision[2], "2026-11-05")

    def test_future_or_unparseable_session_date_is_held(self):
        future = classify_feed_event(event(played_date="2026-11-13"), self.now)
        unknown = classify_feed_event(event(played_date="not a date"), self.now)
        self.assertEqual(future[0], "held_future")
        self.assertEqual(unknown[0], "held_uncertain")

    def test_missing_session_date_is_held_but_explicit_rating_is_independent(self):
        missing = classify_feed_event(event(), self.now)
        old_rating = classify_feed_event(
            event("rated", played_date="2020-01-01", rating=4.5), self.now
        )
        old_review = classify_feed_event(
            event("reviewed", occurred_at="2010-01-01T00:00:00Z", review_notes="review"), self.now
        )
        self.assertEqual(missing[0], "held_uncertain")
        self.assertEqual(old_rating[0], "eligible")
        self.assertEqual(old_review[0], "eligible")

    def test_non_session_action_does_not_inherit_old_play_date_suppression(self):
        decision = classify_feed_event(event("liked", played_date="2010-01-01"), self.now)
        self.assertEqual(decision[0], "eligible")
        self.assertEqual(decision[1], "not_a_play_session")


class FeedDecisionPersistenceTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.db = Database(str(Path(self.directory.name) / "feed-decisions.db"), turso_url="")
        await self.db.connect()
        await self.db.link_user(10, "Millers")

    async def asyncTearDown(self):
        await self.db.close()
        self.directory.cleanup()

    async def test_first_decision_survives_restart_and_cannot_age_into_announcement(self):
        saved = await self.db.record_feed_event_decision(
            1, "Millers", "old-session", source="activity", event_date="2026-11-04",
            decision="suppressed_old", reason="older_than_cutoff", payload='{"id":"old-session"}',
        )
        await self.db.close()
        await self.db.connect()
        retried = await self.db.record_feed_event_decision(
            1, "millers", "old-session", source="activity", event_date="2026-11-12",
            decision="eligible", reason="new_date", payload='{"id":"old-session"}',
        )
        counts = await self.db.get_feed_decision_counts(1, "millers")

        self.assertEqual(saved["decision"], "suppressed_old")
        self.assertEqual(retried["decision"], "suppressed_old")
        self.assertEqual(retried["event_date"], "2026-11-04")
        self.assertEqual(counts["suppressed_old"], 1)


if __name__ == "__main__":
    unittest.main()
