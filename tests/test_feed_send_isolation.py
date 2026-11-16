"""Regression tests for Task 2: per-event send failure isolation and delivery/boundary safety."""

import asyncio
import os
import tempfile
import unittest
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

os.environ["PYTHON_DOTENV_DISABLED"] = "1"
os.environ["TURSO_DATABASE_URL"] = ""
os.environ["TURSO_AUTH_TOKEN"] = ""
os.environ["DEV_GUILD_ID"] = ""

import aiohttp
import discord
from backloggd import ActivityEntry, JournalEntry

from bot.cogs.feed import FeedCog
from bot.db import Database
from bot.feed_models import FeedEvent
from bot.feed_rules import journal_events


def _http_error(cls, status: int, text: str = "discord error"):
    return cls(SimpleNamespace(status=status, reason="Error"), text)


def _event(
    activity_id: str,
    *,
    slug: str = "hades",
    title: str = "Hades",
    occurred_at: str = "2026-11-12T10:00:00Z",
    rating: float | None = 4.5,
    reference: str | None = None,
) -> FeedEvent:
    return FeedEvent.from_activity(
        ActivityEntry(
            activity_id=activity_id,
            game_title=title,
            game_slug=slug,
            cover_url=None,
            action_type="completed",
            action_text="completed",
            rating=rating,
            review_notes=None,
            date_str="10:00",
            occurred_at=occurred_at,
            reference=reference,
        )
    )


def _make_channel(channel_id: int = 100):
    sent_counter = {"n": 0}
    partials = {}

    async def send_impl(**kwargs):
        sent_counter["n"] += 1
        return SimpleNamespace(id=5000 + sent_counter["n"], kwargs=kwargs)

    def get_partial(message_id):
        if message_id not in partials:
            partials[message_id] = SimpleNamespace(id=message_id, edit=AsyncMock())
        return partials[message_id]

    channel = MagicMock(spec=discord.TextChannel)
    channel.id = channel_id
    channel.permissions_for.return_value = SimpleNamespace(
        view_channel=True, send_messages=True, embed_links=True
    )
    channel.send = AsyncMock(side_effect=send_impl)
    channel.get_partial_message = get_partial
    channel._partials = partials
    return channel


class FeedSendIsolationTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmpdir.cleanup)
        self.db_path = Path(self.tmpdir.name) / "send_isolation.db"
        self.db = Database(str(self.db_path), turso_url="")
        await self.db.connect()
        await self.db.link_user(10, "Millers")
        self.now = datetime(2026, 11, 12, 12, 0, tzinfo=timezone.utc)
        self.channel = _make_channel(100)
        self.cog = FeedCog(SimpleNamespace(db=self.db), clock=lambda: self.now)
        self.sleep = patch("bot.cogs.feed.asyncio.sleep", new=AsyncMock())
        self.sleep.start()
        self.addCleanup(self.sleep.stop)

    async def asyncTearDown(self):
        await self.db.close()

    async def test_first_event_400_rejection_allows_second_event_and_retries_safely(self):
        await self.cog._publish_sources(1, "millers", "Millers", None, self.channel, {"activity": [], "journal": []})
        ev1 = _event("ev-1", slug="hades", title="Hades", occurred_at="2026-11-12T10:00:00Z")
        ev2 = _event("ev-2", slug="celeste", title="Celeste", occurred_at="2026-11-12T10:05:00Z")

        call_num = {"n": 0}

        async def send_impl(**kwargs):
            call_num["n"] += 1
            if call_num["n"] == 1:
                raise _http_error(discord.HTTPException, 400, "bad embed payload")
            return SimpleNamespace(id=5000 + call_num["n"], kwargs=kwargs)

        self.channel.send.side_effect = send_impl

        ok = await self.cog._publish_sources(
            1, "millers", "Millers", None, self.channel, {"activity": [ev1, ev2], "journal": []}
        )

        self.assertFalse(ok)
        self.assertEqual(self.channel.send.await_count, 2)
        self.assertFalse(await self.db.is_journal_entry_delivered(1, "millers", "ev-1"))
        self.assertTrue(await self.db.is_journal_entry_delivered(1, "millers", "ev-2"))
        rows = {r["event_id"]: r for r in await self.db.get_feed_messages(1, "millers")}
        self.assertNotIn("ev-1", rows)
        self.assertEqual(rows["ev-2"]["delivery_state"], "sent")

        # Next poll retries only ev-1; ev-2 is not duplicated.
        self.channel.send.side_effect = None
        self.channel.send.side_effect = lambda **kwargs: SimpleNamespace(id=5099, kwargs=kwargs)
        ok2 = await self.cog._publish_sources(
            1, "millers", "Millers", None, self.channel, {"activity": [ev1, ev2], "journal": []}
        )
        self.assertTrue(ok2)
        self.assertEqual(self.channel.send.await_count, 3)
        self.assertIn("Hades", self.channel.send.call_args.kwargs["embed"].title)
        self.assertTrue(await self.db.is_journal_entry_delivered(1, "millers", "ev-1"))

    async def test_http_and_transport_policy_matrix_isolates_failures_and_protects_unknown_across_restart(self):
        cases = [
            ("403", _http_error(discord.Forbidden, 403, "missing perms"), "released"),
            ("404", _http_error(discord.NotFound, 404, "unknown channel"), "released"),
            ("429_http", _http_error(discord.HTTPException, 429, "rate limited"), "released"),
            ("429_ratelimited", discord.RateLimited(1.5), "released"),
            ("500", _http_error(discord.HTTPException, 500, "internal error"), "unknown"),
            ("503", _http_error(discord.HTTPException, 503, "unavailable"), "unknown"),
            ("timeout", asyncio.TimeoutError("send timed out"), "unknown"),
            ("client_error", aiohttp.ClientConnectionError("reset by peer"), "unknown"),
        ]
        for label, exc, expected in cases:
            with self.subTest(outcome=label), tempfile.TemporaryDirectory() as folder:
                db = Database(str(Path(folder) / f"{label}.db"), turso_url="")
                await db.connect()
                try:
                    await db.link_user(10, "Millers")
                    ch = _make_channel(100)
                    cog = FeedCog(SimpleNamespace(db=db), clock=lambda: self.now)
                    await cog._publish_sources(1, "millers", "Millers", None, ch, {"activity": [], "journal": []})

                    ev1 = _event(f"ev1-{label}", slug="hades", title="Hades", occurred_at="2026-11-12T10:00:00Z")
                    ev2 = _event(f"ev2-{label}", slug="celeste", title="Celeste", occurred_at="2026-11-12T10:05:00Z")
                    calls = {"n": 0}

                    async def send_impl(**kwargs):
                        calls["n"] += 1
                        if calls["n"] == 1:
                            raise exc
                        return SimpleNamespace(id=6000 + calls["n"], kwargs=kwargs)

                    ch.send.side_effect = send_impl
                    ok = await cog._publish_sources(1, "millers", "Millers", None, ch, {"activity": [ev1, ev2], "journal": []})
                    self.assertFalse(ok)
                    # Independent later event MUST still be attempted & delivered once.
                    self.assertEqual(ch.send.await_count, 2)
                    self.assertTrue(await db.is_journal_entry_delivered(1, "millers", ev2.activity_id))

                    rows = {r["event_id"]: r for r in await db.get_feed_messages(1, "millers")}
                    if expected == "released":
                        self.assertNotIn(ev1.activity_id, rows)
                        self.assertFalse(await db.is_journal_entry_delivered(1, "millers", ev1.activity_id))
                    else:
                        self.assertEqual(rows[ev1.activity_id]["delivery_state"], "unknown")
                        self.assertFalse(await db.is_journal_entry_delivered(1, "millers", ev1.activity_id))

                    # Restart and retry with healthy channel.
                    await db.close()
                    await db.connect()
                    ch.send.side_effect = None
                    ch.send.side_effect = lambda **kwargs: SimpleNamespace(id=7000, kwargs=kwargs)
                    ok_after = await cog._publish_sources(1, "millers", "Millers", None, ch, {"activity": [ev1, ev2], "journal": []})
                    self.assertTrue(ok_after)
                    if expected == "released":
                        # Only ev1 retried; ev2 not duplicated.
                        self.assertEqual(ch.send.await_count, 3)
                        self.assertTrue(await db.is_journal_entry_delivered(1, "millers", ev1.activity_id))
                    else:
                        # Unknown outcome stays protected across restart: NO blind resend!
                        self.assertEqual(ch.send.await_count, 2)
                        rows_after = {r["event_id"]: r for r in await db.get_feed_messages(1, "millers")}
                        self.assertEqual(rows_after[ev1.activity_id]["delivery_state"], "unknown")
                finally:
                    await db.close()

    async def test_cancelled_error_propagates_and_does_not_continue_loop(self):
        await self.cog._publish_sources(1, "millers", "Millers", None, self.channel, {"activity": [], "journal": []})
        ev1 = _event("ev-cancel-1", occurred_at="2026-11-12T10:00:00Z")
        ev2 = _event("ev-cancel-2", slug="celeste", title="Celeste", occurred_at="2026-11-12T10:05:00Z")
        self.channel.send.side_effect = asyncio.CancelledError()

        with self.assertRaises(asyncio.CancelledError):
            await self.cog._publish_sources(
                1, "millers", "Millers", None, self.channel, {"activity": [ev1, ev2], "journal": []}
            )
        self.channel.send.assert_awaited_once()

    async def test_aliases_and_discovery_timestamps_survive_definitive_rejection_retry(self):
        await self.cog._publish_sources(1, "millers", "Millers", None, self.channel, {"activity": [], "journal": []})
        # Event played 6 days ago (eligible at T0 = Nov 12); fails 400 on T0, then clock advances
        # past 7 days on retry at T1 = Nov 20. Original observed_at and journal alias must survive!
        act = replace(
            _event("act-retry", occurred_at="2026-11-12T10:00:00Z", reference="/u/Millers/log/retry/"),
            played_date=None,
        )
        j_log = journal_events([
            JournalEntry(
                entry_id="jr", game_title="Hades", game_slug="hades",
                played_date="2026-11-06", platform="PC", rating=4.5, session_labels=("Finished",),
            )
        ], "Millers")[0]
        self.channel.send.side_effect = _http_error(discord.HTTPException, 400, "temporary bad req")

        ok = await self.cog._publish_sources(
            1, "millers", "Millers", None, self.channel, {"activity": [act], "journal": [j_log]}
        )
        self.assertFalse(ok)
        dec_act = await self.db.get_feed_event_decision(1, "millers", "act-retry")
        dec_log = await self.db.get_feed_event_decision(1, "millers", j_log.activity_id)
        self.assertEqual(dec_act["decision"], "eligible")
        self.assertEqual(dec_log["decision"], "eligible")
        first_observed = dec_act["observed_at"]

        # Clock jumps 8 days ahead; retry still uses original observed_at and keeps journal aliases.
        self.now = self.now + timedelta(days=8)
        self.channel.send.side_effect = None
        self.channel.send.side_effect = lambda **kwargs: SimpleNamespace(id=5010, kwargs=kwargs)
        ok2 = await self.cog._publish_sources(
            1, "millers", "Millers", None, self.channel, {"activity": [act], "journal": [j_log]}
        )
        self.assertTrue(ok2)
        self.assertEqual(self.channel.send.await_count, 2)
        dec_after = await self.db.get_feed_event_decision(1, "millers", "act-retry")
        self.assertEqual(dec_after["observed_at"], first_observed)
        msgs = {r["event_id"]: r for r in await self.db.get_feed_messages(1, "millers")}
        self.assertEqual(msgs["act-retry"]["message_id"], 5010)
        self.assertEqual(msgs[j_log.activity_id]["message_id"], 5010)

    async def test_loop_holds_continuous_boundary_when_one_event_fails_and_another_succeeds(self):
        await self.db.set_feed_channel(1, 100)
        await self.db.set_guild_feed_preference(1, 10, True)
        await self.db.reconcile_guild_feed_activity(1, ["millers"], now=self.now)
        await self.db.record_feed_source_attempt("millers", "activity")
        await self.db.record_feed_source_success(
            "millers", "activity", coverage="exhausted", pages_read=1, boundary_event_id="ev-0"
        )
        ev0 = _event("ev-0", slug="old-game", title="Old Game", occurred_at="2026-11-12T08:00:00Z")
        ev1 = _event("ev-1", slug="hades", title="Hades", occurred_at="2026-11-12T12:05:00Z")
        ev2 = _event("ev-2", slug="celeste", title="Celeste", occurred_at="2026-11-12T12:10:00Z")

        guild = SimpleNamespace(
            id=1, name="G1", me=object(), chunked=True,
            members=[SimpleNamespace(id=10)],
            get_channel=lambda cid: self.channel if cid == 100 else None,
        )
        api = SimpleNamespace(
            get_user_activity_snapshot=AsyncMock(side_effect=[
                SimpleNamespace(entries=[ev0], coverage="boundary_reached", pages_read=1),
                SimpleNamespace(entries=[ev2, ev1, ev0], coverage="boundary_reached", pages_read=1),
                SimpleNamespace(entries=[ev2, ev1, ev0], coverage="boundary_reached", pages_read=1),
            ]),
            get_user_journal=AsyncMock(return_value=[]),
        )
        cog = FeedCog(
            SimpleNamespace(db=self.db, backloggd=api, get_guild=lambda gid: guild if gid == 1 else None),
            clock=lambda: self.now,
        )

        # Baseline poll: ev-0
        await cog.journal_feed_loop.coro(cog)
        self.assertEqual((await self.db.get_profile_feed_source("millers", "activity"))["last_continuous_boundary_event_id"], "ev-0")

        # Poll 2: ev-1 fails with 400 while ev-2 succeeds -> ev-2 delivered, boundary stays at ev-0!
        attempt = {"n": 0}

        async def send_poll2(**kwargs):
            attempt["n"] += 1
            if attempt["n"] == 1:
                raise _http_error(discord.HTTPException, 400, "rejected ev-1")
            return SimpleNamespace(id=5100 + attempt["n"], kwargs=kwargs)

        self.channel.send.side_effect = send_poll2
        await cog.journal_feed_loop.coro(cog)
        self.assertEqual(self.channel.send.await_count, 2)
        self.assertTrue(await self.db.is_journal_entry_delivered(1, "millers", "ev-2"))
        self.assertFalse(await self.db.is_journal_entry_delivered(1, "millers", "ev-1"))
        self.assertEqual((await self.db.get_profile_feed_source("millers", "activity"))["last_continuous_boundary_event_id"], "ev-0")

        # Poll 3: retry succeeds -> ev-1 delivered without duplicating ev-2, boundary advances to ev-2!
        self.channel.send.side_effect = lambda **kwargs: SimpleNamespace(id=5200, kwargs=kwargs)
        await cog.journal_feed_loop.coro(cog)
        self.assertEqual(self.channel.send.await_count, 3)
        self.assertTrue(await self.db.is_journal_entry_delivered(1, "millers", "ev-1"))
        self.assertEqual((await self.db.get_profile_feed_source("millers", "activity"))["last_continuous_boundary_event_id"], "ev-2")

    async def test_edit_http_and_ratelimit_failure_still_permits_later_new_send(self):
        await self.cog._publish_sources(1, "millers", "Millers", None, self.channel, {"activity": [], "journal": []})
        ev1 = _event("ev-edit-1", slug="hades", title="Hades", rating=3.0, occurred_at="2026-11-12T10:00:00Z")
        self.assertTrue(await self.cog._publish_sources(1, "millers", "Millers", None, self.channel, {"activity": [ev1], "journal": []}))
        msg1 = 5001

        self.channel.get_partial_message(msg1).edit.side_effect = discord.RateLimited(1.0)
        ev1_updated = replace(ev1, rating=4.5)
        ev2 = _event("ev-after-edit-fail", slug="celeste", title="Celeste", occurred_at="2026-11-12T10:30:00Z")
        ok = await self.cog._publish_sources(
            1, "millers", "Millers", None, self.channel, {"activity": [ev1_updated, ev2], "journal": []}
        )
        self.assertFalse(ok)
        self.assertEqual(self.channel.send.await_count, 2)
        self.assertTrue(await self.db.is_journal_entry_delivered(1, "millers", "ev-after-edit-fail"))
