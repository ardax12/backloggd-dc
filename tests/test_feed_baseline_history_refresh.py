"""Task 7 regressions: refresh baseline history without inventing rating chronology (F4)."""

import os

os.environ["PYTHON_DOTENV_DISABLED"] = "1"
os.environ["TURSO_DATABASE_URL"] = ""
os.environ["TURSO_AUTH_TOKEN"] = ""
os.environ["DEV_GUILD_ID"] = ""

from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path
import sqlite3
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

import discord

from bot.cogs.feed import FeedCog
from bot.db import Database
from bot.feed_models import FeedEvent
from tests.test_feed_batch_writes import CountingTursoClient


def _make_channel(channel_id: int = 100) -> MagicMock:
    channel = MagicMock(spec=discord.TextChannel)
    channel.id = channel_id
    channel.guild = SimpleNamespace(id=1)
    next_id = 900

    async def send(**kwargs):
        nonlocal next_id
        next_id += 1
        return SimpleNamespace(id=next_id)

    channel.send = AsyncMock(side_effect=send)
    partials = {}

    def get_partial(message_id):
        if message_id not in partials:
            partials[message_id] = SimpleNamespace(edit=AsyncMock())
        return partials[message_id]

    channel.get_partial_message = MagicMock(side_effect=get_partial)
    channel.permissions_for.return_value = SimpleNamespace(
        view_channel=True,
        send_messages=True,
        embed_links=True,
    )
    return channel


def _event(
    event_id: str,
    *,
    source: str = "activity",
    action_type: str = "completed",
    game_slug: str = "hades",
    game_title: str = "Hades",
    rating: float | None = None,
    played_date: str | None = "2026-11-12",
    occurred_at: str | None = "2026-11-12T10:00:00Z",
    session_labels: tuple[str, ...] = (),
    review_notes: str | None = None,
    reference: str | None = None,
) -> FeedEvent:
    return FeedEvent(
        activity_id=event_id,
        event_ids=(event_id,),
        source=source,
        action_type=action_type,
        action_text=f"{action_type} {game_title}",
        game_slug=game_slug,
        game_title=game_title,
        occurred_at=occurred_at,
        played_date=played_date,
        date_str="Nov 12, 2026",
        rating=rating,
        review_notes=review_notes,
        has_spoilers=False,
        is_replay=False,
        platform="PC",
        session_labels=session_labels,
        reference=reference,
        cover_url=None,
    )


class FeedBaselineHistoryRefreshTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmpdir.cleanup)
        self.sqlite_db = Database(str(Path(self.tmpdir.name) / "sqlite.db"), turso_url="")
        await self.sqlite_db.connect()
        await self.sqlite_db._conn._execute(
            self.sqlite_db._conn._conn.setlimit, sqlite3.SQLITE_LIMIT_VARIABLE_NUMBER, 999
        )
        self.addAsyncCleanup(self.sqlite_db.close)

        turso_path = Path(self.tmpdir.name) / "turso.db"
        seed = Database(str(turso_path), turso_url="")
        await seed.connect()
        await seed.close()
        self.turso_client = CountingTursoClient(str(turso_path))
        self.turso_db = Database(str(turso_path), turso_url="https://f4-test.turso.io")
        self.turso_db._client = self.turso_client
        self.addAsyncCleanup(self.turso_db.close)

        self.sleep_patch = patch("bot.cogs.feed.asyncio.sleep", new=AsyncMock())
        self.sleep_patch.start()
        self.addCleanup(self.sleep_patch.stop)

    async def _backends(self):
        return [("sqlite", self.sqlite_db), ("turso", self.turso_db)]

    async def test_same_poll_journal_baseline_refreshes_canonical_history_for_earlier_rating(self):
        for label, db in await self._backends():
            with self.subTest(backend=label):
                await db.link_user(10, "Millers")
                channel = _make_channel()
                t1 = datetime(2026, 11, 12, 10, 0, tzinfo=timezone.utc)
                t2_baseline = datetime(2026, 11, 12, 12, 0, tzinfo=timezone.utc)
                t2_publish = datetime(2026, 11, 12, 14, 0, tzinfo=timezone.utc)
                current_now = t1

                cog = FeedCog(SimpleNamespace(db=db), clock=lambda: current_now)

                # Poll 1: activity initializes while journal fails (absent from sources).
                await cog._publish_sources(1, "millers", "Millers", None, channel, {"activity": []})
                channel.send.assert_not_awaited()
                self.assertTrue(await db.has_feed_source(1, "millers", "activity-display-v1"))
                self.assertFalse(await db.has_feed_source(1, "millers", "journal-display-v1"))

                # Poll 2: journal initializes at 12:00Z with rating=3.0 and activity has a later
                # rating at 13:00Z. Refreshed canonical_journal must be visible in this same poll.
                current_now = t2_baseline
                orig_seed = db.seed_feed_source_with_messages

                async def advance_after_seed(*args, **kwargs):
                    nonlocal current_now
                    res = await orig_seed(*args, **kwargs)
                    current_now = t2_publish
                    return res

                db.seed_feed_source_with_messages = advance_after_seed
                try:
                    j_base = _event(
                        "journal_hades_base",
                        source="journal",
                        action_type="completed",
                        rating=3.0,
                        played_date="November, 2026 11",
                        occurred_at=None,
                        session_labels=("Finished",),
                    )
                    act_rate = _event(
                        "act-rate-new",
                        source="activity",
                        action_type="rated",
                        rating=4.5,
                        played_date=None,
                        occurred_at="2026-11-12T13:00:00Z",
                    )
                    seen_policy_events = {}
                    orig_policy = cog._apply_discovery_policy

                    async def spy_policy(guild_id, profile_key, events, saved, now, **kwargs):
                        seen_policy_events["count"] = len(events)
                        return await orig_policy(guild_id, profile_key, events, saved, now, **kwargs)

                    cog._apply_discovery_policy = spy_policy
                    orig_parse = cog._parse_persisted_feed_history
                    parse_calls = []

                    def spy_parse(rows):
                        res = orig_parse(rows)
                        parse_calls.append(res)
                        return res

                    cog._parse_persisted_feed_history = spy_parse
                    await cog._publish_sources(
                        1,
                        "millers",
                        "Millers",
                        None,
                        channel,
                        {"activity": [act_rate], "journal": [j_base]},
                    )
                finally:
                    db.seed_feed_source_with_messages = orig_seed

                # Second parse call after any_first_baseline must expose refreshed journal history.
                self.assertEqual(len(parse_calls), 2)
                refreshed_aliases, _, refreshed_journal, _ = parse_calls[1]
                self.assertIn("journal_hades_base", refreshed_journal)
                self.assertIn("journal_hades_base", refreshed_aliases)
                # Canonical journal + incoming journal entry are deduplicated into one event (+ 1 activity).
                self.assertEqual(seen_policy_events["count"], 2)

                # Journal baseline itself never announces; new rating uses earlier journal baseline.
                channel.send.assert_awaited_once()
                embed = channel.send.call_args.kwargs["embed"]
                self.assertIn("changed their rating", embed.title)
                self.assertIn("3.0", embed.description)
                self.assertIn("4.5", embed.description)

    async def test_same_poll_journal_baseline_later_contemporaneous_or_conflicting_omits_arrow(self):
        for case_name, baseline_at, action_at, journal_entries in (
            (
                "later_observation_than_activity",
                datetime(2026, 11, 12, 12, 0, tzinfo=timezone.utc),
                "2026-11-12T11:30:00Z",
                [
                    _event(
                        "j-base-late",
                        source="journal",
                        action_type="completed",
                        rating=2.5,
                        played_date="November, 2026 10",
                        occurred_at=None,
                        session_labels=("Finished",),
                    )
                ],
            ),
            (
                "contemporaneous_observation",
                datetime(2026, 11, 12, 12, 0, tzinfo=timezone.utc),
                "2026-11-12T12:00:00Z",
                [
                    _event(
                        "j-base-same",
                        source="journal",
                        action_type="completed",
                        rating=2.5,
                        played_date="November, 2026 12",
                        occurred_at=None,
                        session_labels=("Finished",),
                    )
                ],
            ),
            (
                "conflicting_journal_ratings",
                datetime(2026, 11, 12, 12, 0, tzinfo=timezone.utc),
                "2026-11-12T13:00:00Z",
                [
                    _event(
                        "j-base-c1",
                        source="journal",
                        action_type="completed",
                        rating=2.0,
                        played_date="November, 2026 11",
                        occurred_at=None,
                        session_labels=("Finished",),
                    ),
                    _event(
                        "j-base-c2",
                        source="journal",
                        action_type="playing",
                        rating=4.0,
                        played_date="November, 2026 11",
                        occurred_at=None,
                        session_labels=("Started",),
                    ),
                ],
            ),
        ):
            for label, db in await self._backends():
                with self.subTest(case=case_name, backend=label):
                    guild_id = {"sqlite": 10, "turso": 20}[label] + hash(case_name) % 100
                    await db.link_user(10, "Millers")
                    channel = _make_channel()
                    publish_at = datetime(2026, 11, 12, 14, 0, tzinfo=timezone.utc)
                    current_now = baseline_at
                    cog = FeedCog(SimpleNamespace(db=db), clock=lambda: current_now)

                    await cog._publish_sources(guild_id, "millers", "Millers", None, channel, {"activity": []})
                    orig_seed = db.seed_feed_source_with_messages

                    async def advance_after_seed(*args, **kwargs):
                        nonlocal current_now
                        res = await orig_seed(*args, **kwargs)
                        current_now = publish_at
                        return res

                    db.seed_feed_source_with_messages = advance_after_seed
                    try:
                        act_rate = _event(
                            f"act-{case_name}",
                            source="activity",
                            action_type="rated",
                            rating=5.0,
                            played_date=None,
                            occurred_at=action_at,
                        )
                        await cog._publish_sources(
                            guild_id,
                            "millers",
                            "Millers",
                            None,
                            channel,
                            {"activity": [act_rate], "journal": journal_entries},
                        )
                    finally:
                        db.seed_feed_source_with_messages = orig_seed
                    channel.send.assert_awaited_once()
                    embed = channel.send.call_args.kwargs["embed"]
                    self.assertNotIn("changed their rating", embed.title)
                    self.assertNotIn(" → ", embed.description)

    async def test_unknown_journal_observation_time_after_baseline_omits_arrow(self):
        db = self.sqlite_db
        await db.link_user(10, "Millers")
        channel = _make_channel()
        now = datetime(2026, 11, 12, 12, 0, tzinfo=timezone.utc)
        cog = FeedCog(SimpleNamespace(db=db), clock=lambda: now)

        await cog._publish_sources(1, "millers", "Millers", None, channel, {"activity": []})
        j_base = _event(
            "j-base-unknown",
            source="journal",
            action_type="completed",
            rating=2.0,
            played_date="November, 2026 11",
            occurred_at=None,
            session_labels=("Finished",),
        )
        await cog._publish_sources(1, "millers", "Millers", None, channel, {"activity": [], "journal": [j_base]})
        await db._execute(
            "DELETE FROM profile_feed_decisions WHERE guild_id = ? AND profile_key = ? AND event_id = ?",
            (1, "millers", "j-base-unknown"),
        )
        now = datetime(2026, 11, 12, 15, 0, tzinfo=timezone.utc)
        act_rate = _event(
            "act-after-unknown",
            source="activity",
            action_type="rated",
            rating=4.5,
            played_date=None,
            occurred_at="2026-11-12T14:00:00Z",
        )
        await cog._publish_sources(1, "millers", "Millers", None, channel, {"activity": [act_rate], "journal": []})
        channel.send.assert_awaited_once()
        embed = channel.send.call_args.kwargs["embed"]
        self.assertNotIn("changed their rating", embed.title)
        self.assertNotIn(" → ", embed.description)

    async def test_activity_baseline_after_journal_preserves_archive_fresher_events_and_ops_budget(self):
        for label, db in await self._backends():
            with self.subTest(backend=label):
                await db.link_user(10, "Millers")
                channel = _make_channel()
                now = datetime(2026, 11, 12, 12, 0, tzinfo=timezone.utc)
                cog = FeedCog(SimpleNamespace(db=db), clock=lambda: now)

                # Poll 1: journal initializes while activity fails.
                j_old = _event(
                    "journal_celeste",
                    source="journal",
                    action_type="completed",
                    game_slug="celeste",
                    game_title="Celeste",
                    rating=3.0,
                    played_date="November, 2026 11",
                    occurred_at=None,
                    session_labels=("Finished",),
                )
                await cog._publish_sources(1, "millers", "Millers", None, channel, {"journal": [j_old]})
                self.assertTrue(await db.has_feed_source(1, "millers", "journal-display-v1"))
                self.assertFalse(await db.has_feed_source(1, "millers", "activity-display-v1"))

                # Poll 2: a fresh journal event posts, while activity initializes with older baseline.
                now = datetime(2026, 11, 12, 14, 0, tzinfo=timezone.utc)
                act_base = _event(
                    "act-base-1",
                    source="activity",
                    action_type="completed",
                    game_slug="celeste",
                    game_title="Celeste",
                    rating=3.5,
                    played_date="2026-11-11",
                    occurred_at="2026-11-11T10:00:00Z",
                )
                j_new = _event(
                    "journal_hades_new",
                    source="journal",
                    action_type="completed",
                    game_slug="hades",
                    game_title="Hades",
                    rating=4.0,
                    played_date="November, 2026 12",
                    occurred_at=None,
                    session_labels=("Finished",),
                )
                if label == "turso":
                    self.turso_client.reset_counters()
                await cog._publish_sources(
                    1,
                    "millers",
                    "Millers",
                    None,
                    channel,
                    {"activity": [act_base], "journal": [j_old, j_new]},
                )
                # Only j_new announces; j_old and act_base remain baselined and deduplicated.
                channel.send.assert_awaited_once()
                self.assertIn("Hades", channel.send.call_args.kwargs["embed"].title)
                if label == "turso":
                    remote = (
                        self.turso_client.direct_execute_calls
                        + self.turso_client.tx_execute_calls
                        + self.turso_client.batch_calls
                    )
                    self.assertLessEqual(remote, 35)

                # Poll 3: fresher journal edit updates archive in place without overwriting notes;
                # subsequent activity rating > 14:00Z picks up j_new's 4.0 rating.
                now = datetime(2026, 11, 12, 16, 0, tzinfo=timezone.utc)
                j_new_edited = replace(j_new, review_notes="Fresh incoming journal note")
                act_new_rate = _event(
                    "act-hades-rate-2",
                    source="activity",
                    action_type="rated",
                    game_slug="hades",
                    game_title="Hades",
                    rating=5.0,
                    played_date=None,
                    occurred_at="2026-11-12T15:00:00Z",
                )
                await cog._publish_sources(
                    1,
                    "millers",
                    "Millers",
                    None,
                    channel,
                    {"activity": [act_base, act_new_rate], "journal": [j_old, j_new_edited]},
                )
                self.assertEqual(channel.send.await_count, 2)
                rate_embed = channel.send.call_args.kwargs["embed"]
                self.assertIn("changed their rating", rate_embed.title)
                self.assertIn("4.0", rate_embed.description)
                self.assertIn("5.0", rate_embed.description)
                edit_mock = channel.get_partial_message(901).edit
                edit_mock.assert_awaited_once()
                self.assertIn(
                    "Fresh incoming journal note",
                    edit_mock.call_args.kwargs["embed"].fields[0].value,
                )


if __name__ == "__main__":
    unittest.main()
