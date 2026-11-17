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

    async def test_initial_limited_coverage_keeps_extent_unknown_instead_of_using_event_dates(self):
        from datetime import datetime, timezone

        now = datetime(2026, 11, 16, 12, 0, tzinfo=timezone.utc)
        entries = [
            replace(completion_event(), activity_id="old", occurred_at="2026-11-13T08:00:00Z"),
            replace(completion_event(), activity_id="future", occurred_at="2026-11-17T08:00:00Z"),
            replace(completion_event(), activity_id="malformed", occurred_at="not-a-date"),
        ]
        api = SimpleNamespace(
            get_user_activity_snapshot=AsyncMock(return_value=SimpleNamespace(
                entries=entries, coverage="limited", pages_read=1,
            )),
            get_user_journal=AsyncMock(return_value=[]),
        )

        await FeedCog(
            SimpleNamespace(db=self.db, backloggd=api), clock=lambda: now,
        )._fetch_feed_sources("Millers")

        health = await self.db.get_profile_feed_source("millers", "activity")
        self.assertIsNone(health["unresolved_gap_since"])
        self.assertIsNone(health["unresolved_gap_until"])
        self.assertEqual(health["gap_reason"], "limited")

    async def test_failure_after_known_boundary_records_an_open_continuity_gap(self):
        await self.db.record_feed_source_success(
            "millers", "activity", coverage="exhausted", pages_read=2,
            boundary_event_id="known-boundary",
        )
        before = await self.db.get_profile_feed_source("millers", "activity")

        await self.db.record_feed_source_failure("millers", "activity", "blocked")

        after = await self.db.get_profile_feed_source("millers", "activity")
        self.assertEqual(
            after["unresolved_gap_since"], before["last_continuous_boundary_at"],
        )
        self.assertIsNone(after["unresolved_gap_until"])
        self.assertEqual(after["gap_reason"], "source_failure")

    async def test_failure_gap_state_matches_sqlite_and_libturso(self):
        from datetime import datetime, timezone
        from tests.test_feed_batch_writes import CountingTursoClient

        t0 = datetime(2026, 11, 10, 8, 0, tzinfo=timezone.utc)
        t1 = datetime(2026, 11, 11, 9, 0, tzinfo=timezone.utc)
        turso_path = Path(self.directory.name) / "turso-failure-health.db"
        seed = Database(str(turso_path), turso_url="")
        await seed.connect()
        await seed.close()
        turso_db = Database(str(turso_path), turso_url="https://failure-health.turso.io")
        turso_db._client = CountingTursoClient(str(turso_path))
        try:
            for label, db in (("sqlite", self.db), ("turso", turso_db)):
                key = f"failure-{label}"
                await db.record_feed_source_success(
                    key, "activity", coverage="exhausted", pages_read=2,
                    boundary_event_id="known", now=t0,
                )
                await db.record_feed_source_attempt(key, "activity", now=t1)
                await db.record_feed_source_failure(key, "activity", "blocked")
                health = await db.get_profile_feed_source(key, "activity")
                self.assertEqual(health["last_continuous_boundary_at"], "2026-11-10 08:00:00")
                self.assertEqual(health["unresolved_gap_since"], "2026-11-10 08:00:00")
                self.assertIsNone(health["unresolved_gap_until"])
                self.assertEqual(health["gap_reason"], "source_failure")
                self.assertEqual(health["last_attempt_at"], "2026-11-11 09:00:00")
                self.assertEqual(health["last_error"], "blocked")
        finally:
            await turso_db.close()

    async def test_feed_source_health_uses_one_injected_utc_clock(self):
        from datetime import datetime, timedelta, timezone

        now = datetime(2026, 11, 16, 15, 30, tzinfo=timezone(timedelta(hours=3)))
        clock_calls = []

        def clock():
            clock_calls.append(now)
            return now

        api = SimpleNamespace(
            get_user_activity_snapshot=AsyncMock(return_value=SimpleNamespace(
                entries=[], coverage="exhausted", pages_read=1,
            )),
            get_user_journal=AsyncMock(return_value=[]),
        )
        feed = FeedCog(SimpleNamespace(db=self.db, backloggd=api), clock=clock)

        await feed._fetch_feed_sources("Millers")
        self.assertEqual(clock_calls, [now])

        for source in ("activity", "journal"):
            health = await self.db.get_profile_feed_source("millers", source)
            self.assertEqual(health["last_attempt_at"], "2026-11-16 12:30:00")
            self.assertEqual(health["last_success_at"], "2026-11-16 12:30:00")

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
        self.assertIsNone(health["unresolved_gap_since"])
        self.assertIsNone(health["unresolved_gap_until"])
        self.assertEqual(health["gap_reason"], "unsupported")
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
        from datetime import datetime, timezone
        boundary_time = datetime(2026, 11, 10, 8, 0, tzinfo=timezone.utc)
        await self.db.record_feed_source_attempt("millers", "activity")
        await self.db.record_feed_source_success(
            "millers", "activity", coverage="exhausted", pages_read=3,
            boundary_event_id="boundary-1", now=boundary_time,
        )
        await self.db.record_feed_source_attempt("millers", "activity")
        await self.db.record_feed_source_success(
            "millers", "activity", coverage="unsupported", pages_read=1,
        )
        unresolved = await self.db.get_profile_feed_source("millers", "activity")
        self.assertEqual(unresolved["unresolved_gap_since"], "2026-11-10 08:00:00")
        self.assertIsNone(unresolved["unresolved_gap_until"])
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

    async def test_limited_outage_recovery_keeps_open_gap_until_boundary_commit(self):
        from datetime import datetime, timezone
        boundary_time = datetime(2026, 11, 10, 9, 30, tzinfo=timezone.utc)
        await self.db.record_feed_source_attempt("millers", "activity")
        await self.db.record_feed_source_success(
            "millers", "activity", coverage="exhausted", pages_read=2,
            boundary_event_id="b-1", now=boundary_time,
        )
        gap_until = datetime(2026, 11, 12, 9, 30, tzinfo=timezone.utc)
        await self.db.record_feed_source_attempt("millers", "activity")
        await self.db.record_feed_source_success(
            "millers", "activity", coverage="limited", pages_read=5,
            gap_until=gap_until, now=datetime(2026, 11, 12, 12, 0, tzinfo=timezone.utc),
        )
        health = await self.db.get_profile_feed_source("millers", "activity")
        self.assertEqual(health["unresolved_gap_since"], "2026-11-10 09:30:00")
        self.assertIsNone(health["unresolved_gap_until"])
        self.assertEqual(health["gap_reason"], "limited")

        await self.db.record_feed_source_attempt("millers", "activity")
        await self.db.record_feed_source_success(
            "millers", "activity", coverage="boundary_reached", pages_read=2,
            boundary_event_id="b-2", now=datetime(2026, 11, 13, 12, 0, tzinfo=timezone.utc),
        )
        closed = await self.db.get_profile_feed_source("millers", "activity")
        self.assertIsNone(closed["unresolved_gap_since"])
        self.assertIsNone(closed["unresolved_gap_until"])
        self.assertEqual(closed["last_continuous_boundary_event_id"], "b-2")

    async def test_direct_insert_matches_attempt_first_across_coverages_and_deferred_commit(self):
        from datetime import datetime, timedelta, timezone
        from tests.test_feed_batch_writes import CountingTursoClient

        now = datetime(2026, 11, 16, 15, 30, tzinfo=timezone(timedelta(hours=3)))
        gap_until = datetime(2026, 11, 14, 8, 15, tzinfo=timezone.utc)
        cases = [
            ("exhausted", "b-exh", False, True, None),
            ("exhausted", "b-exh", True, False, None),
            ("exhausted", None, True, False, None),
            ("boundary_reached", "b-bnd", False, True, None),
            ("boundary_reached", "b-bnd", True, False, None),
            ("boundary_reached", None, False, False, gap_until),
            ("boundary_reached", None, True, False, gap_until),
            ("limited", None, False, False, gap_until),
            ("limited", None, True, False, gap_until),
            ("unsupported", None, False, False, None),
            ("unsupported", None, True, False, None),
        ]

        turso_path = Path(self.directory.name) / "turso-health.db"
        seed = Database(str(turso_path), turso_url="")
        await seed.connect()
        await seed.close()
        turso_client = CountingTursoClient(str(turso_path))
        turso_db = Database(str(turso_path), turso_url="https://source-health.turso.io")
        turso_db._client = turso_client
        try:
            for backend_label, db in (("sqlite", self.db), ("turso", turso_db)):
                await db.link_user(10, "Millers")
                for idx, (coverage, boundary_id, defer_commit, immediate_commit, case_gap_until) in enumerate(cases):
                    with self.subTest(
                        backend=backend_label,
                        coverage=coverage,
                        boundary_id=boundary_id,
                        defer_commit=defer_commit,
                    ):
                        direct_key = f"direct_{idx}"
                        attempt_key = f"attempt_{idx}"

                        await db.record_feed_source_success(
                            direct_key, "activity", coverage=coverage, pages_read=2,
                            boundary_event_id=boundary_id, defer_boundary_commit=defer_commit,
                            gap_until=case_gap_until, now=now,
                        )
                        await db.record_feed_source_attempt(attempt_key, "activity", now=now)
                        await db.record_feed_source_success(
                            attempt_key, "activity", coverage=coverage, pages_read=2,
                            boundary_event_id=boundary_id, defer_boundary_commit=defer_commit,
                            gap_until=case_gap_until, now=now,
                        )

                        direct_row = await db.get_profile_feed_source(direct_key, "activity")
                        attempt_row = await db.get_profile_feed_source(attempt_key, "activity")
                        self.assertEqual(direct_row["attempts"], 0)
                        self.assertEqual(attempt_row["attempts"], 1)
                        self.assertEqual(direct_row["successes"], 1)
                        self.assertEqual(attempt_row["successes"], 1)
                        self.assertIsNone(direct_row["last_attempt_at"])
                        self.assertEqual(attempt_row["last_attempt_at"], "2026-11-16 12:30:00")

                        for field in (
                            "coverage", "pages_read", "last_continuous_boundary_event_id",
                            "last_success_at", "last_continuous_boundary_at",
                            "unresolved_gap_since", "unresolved_gap_until", "gap_reason", "last_error",
                        ):
                            self.assertEqual(direct_row[field], attempt_row[field], field)
                        self.assertEqual(direct_row["last_success_at"], "2026-11-16 12:30:00")

                        closes_gap = coverage == "exhausted" or (
                            coverage == "boundary_reached" and bool(boundary_id)
                        )
                        if immediate_commit:
                            self.assertEqual(direct_row["last_continuous_boundary_event_id"], boundary_id)
                            self.assertEqual(direct_row["last_continuous_boundary_at"], "2026-11-16 12:30:00")
                            self.assertIsNone(direct_row["unresolved_gap_since"])
                            self.assertIsNone(direct_row["unresolved_gap_until"])
                            self.assertIsNone(direct_row["gap_reason"])
                        elif closes_gap and defer_commit:
                            self.assertIsNone(direct_row["last_continuous_boundary_event_id"])
                            self.assertIsNone(direct_row["last_continuous_boundary_at"])
                            self.assertIsNone(direct_row["unresolved_gap_since"])
                            self.assertIsNone(direct_row["unresolved_gap_until"])
                            self.assertIsNone(direct_row["gap_reason"])
                        else:
                            self.assertIsNone(direct_row["last_continuous_boundary_event_id"])
                            self.assertIsNone(direct_row["last_continuous_boundary_at"])
                            self.assertIsNone(direct_row["unresolved_gap_since"])
                            self.assertIsNone(direct_row["unresolved_gap_until"])
                            self.assertEqual(direct_row["gap_reason"], coverage)

                        await db.commit_feed_source_boundary(
                            direct_key, "activity", coverage=coverage,
                            boundary_event_id=boundary_id, now=now,
                        )
                        await db.commit_feed_source_boundary(
                            attempt_key, "activity", coverage=coverage,
                            boundary_event_id=boundary_id, now=now,
                        )
                        after_direct = await db.get_profile_feed_source(direct_key, "activity")
                        after_attempt = await db.get_profile_feed_source(attempt_key, "activity")
                        if closes_gap:
                            self.assertEqual(after_direct["last_continuous_boundary_event_id"], boundary_id)
                            self.assertEqual(after_attempt["last_continuous_boundary_event_id"], boundary_id)
                            self.assertEqual(after_direct["last_continuous_boundary_at"], "2026-11-16 12:30:00")
                            self.assertEqual(after_attempt["last_continuous_boundary_at"], "2026-11-16 12:30:00")
                            self.assertIsNone(after_direct["unresolved_gap_since"])
                            self.assertIsNone(after_attempt["unresolved_gap_since"])
                            self.assertIsNone(after_direct["gap_reason"])
                        else:
                            self.assertIsNone(after_direct["last_continuous_boundary_event_id"])
                            self.assertIsNone(after_attempt["last_continuous_boundary_event_id"])
                            self.assertIsNone(after_direct["unresolved_gap_since"])
                            self.assertIsNone(after_attempt["unresolved_gap_since"])
                            self.assertIsNone(after_direct["unresolved_gap_until"])
                            self.assertIsNone(after_attempt["unresolved_gap_until"])
                            self.assertEqual(after_direct["gap_reason"], coverage)
        finally:
            await turso_db.close()

    async def test_status_text_keeps_unknown_and_recipient_gaps_open_until_committed_boundary(self):
        from datetime import datetime, timezone
        from tests.test_feed_batch_writes import CountingTursoClient

        turso_path = Path(self.directory.name) / "turso-status-health.db"
        seed = Database(str(turso_path), turso_url="")
        await seed.connect()
        await seed.close()
        turso_db = Database(str(turso_path), turso_url="https://status-health.turso.io")
        turso_db._client = CountingTursoClient(str(turso_path))

        async def source_health_text(db, now):
            await db.link_user(10, "Millers")
            await db.set_guild_feed_preference(1, 10, True)
            await db.set_feed_channel(1, 100)
            await db.reconcile_guild_feed_activity(
                1, ["millers"], now=now, present_member_ids=[10],
            )
            interaction = SimpleNamespace(
                guild=SimpleNamespace(
                    id=1, chunked=True, members=[SimpleNamespace(id=10)],
                ),
                guild_id=1,
                user=SimpleNamespace(id=10),
                response=SimpleNamespace(defer=AsyncMock()),
                followup=SimpleNamespace(send=AsyncMock()),
            )
            feed = FeedCog(SimpleNamespace(db=db), clock=lambda: now)
            await FeedCog.feed_status.callback(feed, interaction)
            return interaction.followup.send.call_args.kwargs["embed"].fields[0].value

        try:
            for backend, db in (("sqlite", self.db), ("turso", turso_db)):
                with self.subTest(backend=backend):
                    unknown_at = datetime(2026, 11, 10, 8, 0, tzinfo=timezone.utc)
                    await db.record_feed_source_success(
                        "millers", "activity", coverage="limited", pages_read=2,
                        gap_until=datetime(2026, 11, 8, 8, 0, tzinfo=timezone.utc),
                        now=unknown_at,
                    )
                    field = await source_health_text(db, unknown_at)
                    self.assertIn("Coverage extent unknown (no continuous boundary)", field)
                    self.assertNotIn("Gap bounds conflict", field)

                    first_boundary = datetime(2026, 11, 10, 9, 0, tzinfo=timezone.utc)
                    await db.record_feed_source_success(
                        "millers", "activity", coverage="exhausted", pages_read=3,
                        boundary_event_id="b-1", now=first_boundary,
                    )
                    limited_at = datetime(2026, 11, 12, 9, 0, tzinfo=timezone.utc)
                    await db.record_feed_source_success(
                        "millers", "activity", coverage="limited", pages_read=5,
                        gap_until=datetime(2026, 11, 9, 9, 0, tzinfo=timezone.utc),
                        now=limited_at,
                    )
                    field = await source_health_text(db, limited_at)
                    self.assertIn("Coverage gap:", field)
                    self.assertIn("→ present", field)
                    self.assertNotIn("Gap bounds conflict", field)
                    await db._execute(
                        """UPDATE profile_feed_source_health SET unresolved_gap_until = ?
                           WHERE profile_key = 'millers' AND source = 'activity'""",
                        ("2026-11-09 09:00:00",),
                    )
                    conflicted = await source_health_text(db, limited_at)
                    self.assertIn("Gap bounds conflict; extent unknown", conflicted)
                    self.assertNotIn("Coverage gap:", conflicted)
                    await db._execute(
                        """UPDATE profile_feed_source_health SET unresolved_gap_until = NULL
                           WHERE profile_key = 'millers' AND source = 'activity'""",
                        (),
                    )

                    recovery_at = datetime(2026, 11, 13, 9, 0, tzinfo=timezone.utc)
                    await db.record_feed_source_success(
                        "millers", "activity", coverage="boundary_reached", pages_read=2,
                        boundary_event_id="b-2", defer_boundary_commit=True,
                        now=recovery_at,
                    )
                    field = await source_health_text(db, recovery_at)
                    self.assertIn("Coverage gap:", field)
                    await db.commit_feed_source_boundary(
                        "millers", "activity", coverage="boundary_reached",
                        boundary_event_id="b-2", now=recovery_at,
                    )
                    field = await source_health_text(db, recovery_at)
                    self.assertNotIn("Coverage gap:", field)

                    fetch_at = datetime(2026, 11, 14, 9, 0, tzinfo=timezone.utc)
                    await db.record_feed_source_success(
                        "millers", "activity", coverage="exhausted", pages_read=1,
                        boundary_event_id="b-3", defer_boundary_commit=True,
                        now=fetch_at,
                    )
                    recipient_gap_at = datetime(2026, 11, 14, 9, 5, tzinfo=timezone.utc)
                    await db.record_feed_target_gap("millers", now=recipient_gap_at)
                    field = await source_health_text(db, recipient_gap_at)
                    self.assertIn("Recipient gap:", field)
                    self.assertIn("→ present", field)

                    retry_at = datetime(2026, 11, 14, 9, 10, tzinfo=timezone.utc)
                    await db.record_feed_source_success(
                        "millers", "activity", coverage="exhausted", pages_read=1,
                        boundary_event_id="b-4", defer_boundary_commit=True,
                        now=retry_at,
                    )
                    self.assertIn(
                        "Recipient gap:", await source_health_text(db, retry_at),
                    )
                    await db.commit_feed_source_boundary(
                        "millers", "activity", coverage="exhausted",
                        boundary_event_id="b-4", now=retry_at,
                    )
                    self.assertNotIn(
                        "Recipient gap:", await source_health_text(db, retry_at),
                    )
        finally:
            await turso_db.close()

    async def test_existing_row_deferred_commit_preserves_prior_gap_and_boundary_until_explicit_commit(self):
        # Prior committed boundary followed by limited outage creates an unresolved gap.
        await self.db.record_feed_source_success(
            "millers", "activity", coverage="exhausted", pages_read=2, boundary_event_id="b-1"
        )
        baseline = await self.db.get_profile_feed_source("millers", "activity")
        await self.db.record_feed_source_attempt("millers", "activity")
        await self.db.record_feed_source_success(
            "millers", "activity", coverage="limited", pages_read=5
        )
        gapped = await self.db.get_profile_feed_source("millers", "activity")
        self.assertEqual(gapped["last_continuous_boundary_event_id"], "b-1")
        self.assertEqual(gapped["last_continuous_boundary_at"], baseline["last_continuous_boundary_at"])
        self.assertIsNotNone(gapped["unresolved_gap_since"])
        self.assertEqual(gapped["gap_reason"], "limited")

        # Recovery fetch with defer_boundary_commit=True must NOT erase gap evidence or advance boundary yet.
        await self.db.record_feed_source_attempt("millers", "activity")
        await self.db.record_feed_source_success(
            "millers",
            "activity",
            coverage="boundary_reached",
            pages_read=3,
            boundary_event_id="b-2",
            defer_boundary_commit=True,
        )
        before_commit = await self.db.get_profile_feed_source("millers", "activity")
        self.assertEqual(before_commit["coverage"], "boundary_reached")
        self.assertEqual(before_commit["last_continuous_boundary_event_id"], "b-1")
        self.assertEqual(before_commit["last_continuous_boundary_at"], baseline["last_continuous_boundary_at"])
        self.assertEqual(before_commit["unresolved_gap_since"], gapped["unresolved_gap_since"])
        self.assertEqual(before_commit["unresolved_gap_until"], gapped["unresolved_gap_until"])
        self.assertEqual(before_commit["gap_reason"], "limited")

        # If a recipient fails before commit, failure keeps prior boundary and gap intact.
        await self.db.record_feed_source_failure("millers", "activity", "channel timeout")
        after_fail = await self.db.get_profile_feed_source("millers", "activity")
        self.assertEqual(after_fail["last_continuous_boundary_event_id"], "b-1")
        self.assertEqual(after_fail["unresolved_gap_since"], gapped["unresolved_gap_since"])
        self.assertEqual(after_fail["gap_reason"], "limited")
        self.assertEqual(after_fail["last_error"], "channel timeout")

        # Explicit commit clears the gap and advances the continuous boundary.
        await self.db.commit_feed_source_boundary(
            "millers", "activity", coverage="boundary_reached", boundary_event_id="b-2"
        )
        committed = await self.db.get_profile_feed_source("millers", "activity")
        self.assertEqual(committed["last_continuous_boundary_event_id"], "b-2")
        self.assertIsNotNone(committed["last_continuous_boundary_at"])
        self.assertIsNone(committed["unresolved_gap_since"])
        self.assertIsNone(committed["unresolved_gap_until"])
        self.assertIsNone(committed["gap_reason"])

    async def test_direct_first_success_deferred_commit_multi_guild_failure_preserves_recovery(self):
        # Simulate direct first success with defer_boundary_commit=True followed by target gap/failure.
        await self.db.record_feed_source_success(
            "millers",
            "activity",
            coverage="exhausted",
            pages_read=1,
            boundary_event_id="b-first",
            defer_boundary_commit=True,
        )
        pending = await self.db.get_profile_feed_source("millers", "activity")
        self.assertIsNone(pending["last_continuous_boundary_event_id"])
        self.assertIsNone(pending["last_continuous_boundary_at"])
        self.assertIsNone(pending["unresolved_gap_since"])

        # Unavailable second guild records target gap rather than closing boundary.
        await self.db.record_feed_target_gap("millers")
        with_target_gap = await self.db.get_profile_feed_source("millers", "activity")
        self.assertIsNone(with_target_gap["last_continuous_boundary_event_id"])
        self.assertIsNotNone(with_target_gap["unresolved_gap_since"])
        self.assertEqual(with_target_gap["gap_reason"], "unavailable_feed_target")

        # Subsequent full recovery with explicit commit clears target gap and commits boundary.
        await self.db.record_feed_source_attempt("millers", "activity")
        await self.db.record_feed_source_success(
            "millers",
            "activity",
            coverage="exhausted",
            pages_read=1,
            boundary_event_id="b-recovered",
            defer_boundary_commit=True,
        )
        mid = await self.db.get_profile_feed_source("millers", "activity")
        self.assertEqual(mid["gap_reason"], "unavailable_feed_target")
        self.assertIsNone(mid["last_continuous_boundary_event_id"])

        await self.db.commit_feed_source_boundary(
            "millers", "activity", coverage="exhausted", boundary_event_id="b-recovered"
        )
        done = await self.db.get_profile_feed_source("millers", "activity")
        self.assertEqual(done["last_continuous_boundary_event_id"], "b-recovered")
        self.assertIsNone(done["unresolved_gap_since"])
        self.assertIsNone(done["gap_reason"])


if __name__ == "__main__":
    unittest.main()
