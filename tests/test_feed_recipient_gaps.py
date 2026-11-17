"""Task 5 regressions: isolate per-guild recipient gaps from source uncertainty (F2)."""

import os

os.environ["PYTHON_DOTENV_DISABLED"] = "1"
os.environ["TURSO_DATABASE_URL"] = ""
os.environ["TURSO_AUTH_TOKEN"] = ""
os.environ["DEV_GUILD_ID"] = ""

import asyncio
from dataclasses import asdict, replace
from datetime import datetime, timezone
import json
from pathlib import Path
import sqlite3
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

import discord

from bot.cogs.feed import FeedCog
from bot.db import Database, _MAX_SQL_BATCH_BYTES, _MAX_SQL_BIND_PARAMS
from bot.db_migrations import run_migrations
from bot.feed_models import FeedEvent
from tests.test_feed_batch_writes import CountingTursoClient


def _activity(
    *,
    activity_id: str,
    slug: str = "hades",
    title: str = "Hades",
    action_type: str = "rated",
    rating: float | None = 4.5,
    played_date: str | None = None,
    occurred_at: str | None = None,
) -> FeedEvent:
    return FeedEvent(
        activity_id=activity_id,
        event_ids=(activity_id,),
        source="activity",
        action_type=action_type,
        game_slug=slug,
        game_title=title,
        cover_url=None,
        action_text=f"{action_type} {title}",
        rating=rating,
        review_notes=None,
        date_str=None,
        occurred_at=occurred_at,
        played_date=played_date,
    )


def _make_channel(channel_id: int, guild_id: int) -> MagicMock:
    channel = MagicMock(spec=discord.TextChannel)
    channel.id = channel_id
    channel.guild = SimpleNamespace(id=guild_id)
    channel.permissions_for.return_value = SimpleNamespace(
        view_channel=True, send_messages=True, embed_links=True
    )
    next_id = 1000 + channel_id

    async def _send(**_kwargs):
        nonlocal next_id
        next_id += 1
        return SimpleNamespace(id=next_id)

    channel.send = AsyncMock(side_effect=_send)
    partial_msgs = {}

    def _get_partial(msg_id: int):
        if msg_id not in partial_msgs:
            partial_msgs[msg_id] = SimpleNamespace(edit=AsyncMock())
        return partial_msgs[msg_id]

    channel.get_partial_message = _get_partial
    return channel


def _make_guild(guild_id: int, member_ids: list[int], channel: MagicMock | None) -> SimpleNamespace:
    return SimpleNamespace(
        id=guild_id,
        name=f"Guild-{guild_id}",
        me=object(),
        chunked=True,
        members=[SimpleNamespace(id=mid) for mid in member_ids],
        get_channel=lambda cid, ch=channel: ch if (ch is not None and cid == ch.id) else None,
    )


def _http_error(cls, status: int, message: str = "error"):
    return cls(SimpleNamespace(status=status, reason=message), {"message": message})


class FeedRecipientGapIsolationTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmpdir.cleanup)
        self.db = Database(str(Path(self.tmpdir.name) / "recipient_gaps.db"), turso_url="")
        await self.db.connect()
        self.addAsyncCleanup(self.db.close)
        self.now = datetime(2026, 11, 13, 12, 0, tzinfo=timezone.utc)
        self.activation_time = datetime(2026, 11, 13, 8, 0, tzinfo=timezone.utc)

    async def _feed_status_embed(self, db: Database, *, guild_id: int, guild: SimpleNamespace, now: datetime):
        interaction = SimpleNamespace(
            guild=guild,
            guild_id=guild_id,
            user=SimpleNamespace(id=10),
            response=SimpleNamespace(defer=AsyncMock()),
            followup=SimpleNamespace(send=AsyncMock()),
        )
        cog = FeedCog(SimpleNamespace(db=db, get_guild=lambda gid: guild if gid == guild_id else None), clock=lambda: now)
        await FeedCog.feed_status.callback(cog, interaction)
        return interaction.followup.send.call_args.kwargs["embed"]

    async def test_shared_profile_healthy_guild_avoids_held_uncertain_and_recipient_gap_status(self):
        await self._assert_shared_profile_isolation()

    async def test_healthy_guild_keeps_continuity_when_pinned_floor_scrolls_off_page(self):
        await self._assert_shared_profile_isolation(floor_visible=False)

    async def test_limited_snapshot_overlap_is_independent_of_pinned_delivery_floor(self):
        await self._assert_shared_profile_isolation(floor_visible=False, coverage="limited", pages_read=2)

    async def test_missing_overlap_retains_source_uncertainty(self):
        await self._assert_shared_profile_isolation(floor_visible=False, overlap=False)

    async def test_restart_does_not_invent_observed_source_continuity(self):
        await self._assert_shared_profile_isolation(floor_visible=False, forget_snapshot=True)

    async def _assert_shared_profile_isolation(self, *, floor_visible=True, overlap=True, forget_snapshot=False, coverage="unsupported", pages_read=1):
        await self.db.link_user(10, "Millers")
        await self.db.set_feed_channel(1, 100)
        await self.db.set_feed_channel(2, 200)
        await self.db.set_guild_feed_preference(1, 10, True, member_ids=[10])
        await self.db.set_guild_feed_preference(2, 10, True, member_ids=[10])
        await self.db.reconcile_guild_feed_activity(1, ["millers"], now=self.activation_time, present_member_ids=[10])
        await self.db.reconcile_guild_feed_activity(2, ["millers"], now=self.activation_time, present_member_ids=[10])

        base_ev = _activity(activity_id="act-0", slug="celeste", title="Celeste", occurred_at="2026-11-13T09:00:00Z")
        for gid in (1, 2):
            await self.db.seed_feed_source(gid, "millers", "activity-display-v1", ["act-0"])
            await self.db.seed_feed_source(gid, "millers", "journal-display-v1", [])
        await self.db.record_feed_source_attempt("millers", "activity", now=self.activation_time)
        await self.db.record_feed_source_success(
            "millers", "activity", coverage="exhausted", pages_read=1, boundary_event_id="act-0", now=self.activation_time,
        )

        ch1 = _make_channel(100, 1)
        g1 = _make_guild(1, [10], ch1)
        g2_unavail = _make_guild(2, [10], None)
        guilds = {1: g1, 2: g2_unavail}

        # Poll 1: G2 is unavailable; dated event arrives -> G1 delivers, G2 stores eligible, floor pinned at act-0.
        ev1 = _activity(activity_id="act-1", slug="hades", title="Hades", occurred_at="2026-11-13T10:00:00Z")
        # Poll 2: G2 still unavailable; undated-action-time rating arrives (safe when source is healthy!).
        ev2 = _activity(activity_id="act-2", slug="balatro", title="Balatro", action_type="rated", rating=5.0, occurred_at=None)

        snapshots = [
            SimpleNamespace(entries=[ev1, base_ev], coverage="boundary_reached", pages_read=1),
            SimpleNamespace(entries=[ev2, *([ev1] if overlap else []), *([base_ev] if floor_visible else [])], coverage="boundary_reached" if floor_visible else coverage, pages_read=pages_read),
        ]
        api = SimpleNamespace(
            get_user_activity_snapshot=AsyncMock(side_effect=lambda *a, **kw: snapshots.pop(0)),
            get_user_journal=AsyncMock(return_value=[]),
        )
        cog = FeedCog(SimpleNamespace(db=self.db, backloggd=api, get_guild=lambda gid: guilds.get(gid)), clock=lambda: self.now)

        with patch("bot.cogs.feed.asyncio.sleep", new=AsyncMock()):
            await cog.journal_feed_loop.coro(cog)
            if forget_snapshot:
                cog._activity_snapshot_meta.clear()
            await cog.journal_feed_loop.coro(cog)

        # Healthy G1 must NOT inherit held_uncertain for ev2 from G2's recipient gap!
        dec1 = await self.db.get_feed_event_decision(1, "millers", "act-2")
        self.assertEqual(dec1["decision"], "held_uncertain" if not overlap or forget_snapshot else "eligible")
        self.assertEqual(ch1.send.await_count, 1 if not overlap or forget_snapshot else 2)

        # Floor stayed pinned at act-0 while G2 was unavailable, and G2 did not send.
        health = await self.db.get_profile_feed_source("millers", "activity")
        self.assertEqual(health["last_continuous_boundary_event_id"], "act-0")

        # Status isolation: G1 is Active with NO Recipient gap, while G2 reports its recipient gap.
        g1_status = await self.db.get_guild_profile_feed_status(1, "millers")
        g2_status = await self.db.get_guild_profile_feed_status(2, "millers")
        self.assertIsNone(g1_status["recipient_gap"])
        self.assertIsNotNone(g2_status["recipient_gap"])

        embed_g1 = await self._feed_status_embed(self.db, guild_id=1, guild=g1, now=self.now)
        self.assertIn("**Effective state:** Active\n", embed_g1.description + "\n")
        self.assertNotIn("Recipient gap:", embed_g1.fields[0].value)

        embed_g2 = await self._feed_status_embed(self.db, guild_id=2, guild=g2_unavail, now=self.now)
        self.assertIn("Recipient gap:", embed_g2.fields[0].value)

    async def test_unavailable_guild_recovers_after_restart_when_entries_leave_page_exactly_once(self):
        await self.db.link_user(10, "Millers")
        await self.db.set_feed_channel(1, 100)
        await self.db.set_feed_channel(2, 200)
        await self.db.set_guild_feed_preference(1, 10, True, member_ids=[10])
        await self.db.set_guild_feed_preference(2, 10, True, member_ids=[10])
        await self.db.reconcile_guild_feed_activity(1, ["millers"], now=self.activation_time, present_member_ids=[10])
        await self.db.reconcile_guild_feed_activity(2, ["millers"], now=self.activation_time, present_member_ids=[10])

        base_ev = _activity(activity_id="act-0", slug="celeste", title="Celeste", occurred_at="2026-11-13T09:00:00Z")
        for gid in (1, 2):
            await self.db.seed_feed_source(gid, "millers", "activity-display-v1", ["act-0"])
            await self.db.seed_feed_source(gid, "millers", "journal-display-v1", [])
        await self.db.record_feed_source_attempt("millers", "activity", now=self.activation_time)
        await self.db.record_feed_source_success(
            "millers", "activity", coverage="exhausted", pages_read=1, boundary_event_id="act-0", now=self.activation_time,
        )

        ch1 = _make_channel(100, 1)
        g1 = _make_guild(1, [10], ch1)
        g2_unavail = _make_guild(2, [10], None)
        guilds = {1: g1, 2: g2_unavail}
        missed_ev = _activity(activity_id="act-missed", slug="disco-elysium", title="Disco Elysium", occurred_at="2026-11-13T10:30:00Z")

        api = SimpleNamespace(
            get_user_activity_snapshot=AsyncMock(return_value=SimpleNamespace(
                entries=[missed_ev, base_ev], coverage="boundary_reached", pages_read=1,
            )),
            get_user_journal=AsyncMock(return_value=[]),
        )
        cog = FeedCog(SimpleNamespace(db=self.db, backloggd=api, get_guild=lambda gid: guilds.get(gid)), clock=lambda: self.now)
        with patch("bot.cogs.feed.asyncio.sleep", new=AsyncMock()):
            await cog.journal_feed_loop.coro(cog)
        self.assertEqual(ch1.send.await_count, 1)

        # Restart DB + new FeedCog; G2 recovers while act-missed has dropped off Backloggd page.
        db_path = self.db.db_path
        await self.db.close()
        db2 = Database(db_path, turso_url="")
        await db2.connect()
        self.addAsyncCleanup(db2.close)

        ch2 = _make_channel(200, 2)
        g2_ok = _make_guild(2, [10], ch2)
        guilds[2] = g2_ok
        api2 = SimpleNamespace(
            get_user_activity_snapshot=AsyncMock(return_value=SimpleNamespace(
                entries=[base_ev], coverage="boundary_reached", pages_read=1,
            )),
            get_user_journal=AsyncMock(return_value=[]),
        )
        cog2 = FeedCog(SimpleNamespace(db=db2, backloggd=api2, get_guild=lambda gid: guilds.get(gid)), clock=lambda: self.now)
        with patch("bot.cogs.feed.asyncio.sleep", new=AsyncMock()):
            await cog2.journal_feed_loop.coro(cog2)
            await cog2.journal_feed_loop.coro(cog2)

        ch2.send.assert_awaited_once()
        self.assertIn("Disco Elysium", ch2.send.call_args.kwargs["embed"].title)
        self.assertEqual(ch1.send.await_count, 1)
        g2_status = await db2.get_guild_profile_feed_status(2, "millers")
        self.assertIsNone(g2_status["recipient_gap"])
        health = await db2.get_profile_feed_source("millers", "activity")
        self.assertIsNone(health["unresolved_gap_since"])

    async def test_active_send_and_edit_failures_record_and_clear_per_guild_recipient_gap(self):
        await self.db.link_user(10, "Millers")
        await self.db.set_feed_channel(1, 100)
        await self.db.set_feed_channel(2, 200)
        await self.db.set_guild_feed_preference(1, 10, True, member_ids=[10])
        await self.db.set_guild_feed_preference(2, 10, True, member_ids=[10])
        await self.db.reconcile_guild_feed_activity(1, ["millers"], now=self.activation_time, present_member_ids=[10])
        await self.db.reconcile_guild_feed_activity(2, ["millers"], now=self.activation_time, present_member_ids=[10])
        for gid in (1, 2):
            await self.db.seed_feed_source(gid, "millers", "activity-display-v1", [])
            await self.db.seed_feed_source(gid, "millers", "journal-display-v1", [])

        ch1 = _make_channel(100, 1)
        ch2 = _make_channel(200, 2)
        g1 = _make_guild(1, [10], ch1)
        g2 = _make_guild(2, [10], ch2)
        guilds = {1: g1, 2: g2}

        # Guild 1 active send fails with 403 -> Guild 1 gets explicit recipient gap, Guild 2 remains healthy.
        ch1.send.side_effect = _http_error(discord.Forbidden, 403, "Missing Permissions")
        ev1 = _activity(activity_id="act-1", slug="hades", title="Hades", rating=4.0, occurred_at="2026-11-13T10:00:00Z")
        api = SimpleNamespace(
            get_user_activity_snapshot=AsyncMock(return_value=SimpleNamespace(
                entries=[ev1], coverage="exhausted", pages_read=1,
            )),
            get_user_journal=AsyncMock(return_value=[]),
        )
        cog = FeedCog(SimpleNamespace(db=self.db, backloggd=api, get_guild=lambda gid: guilds.get(gid)), clock=lambda: self.now)

        with patch("bot.cogs.feed.asyncio.sleep", new=AsyncMock()):
            await cog.journal_feed_loop.coro(cog)

        g1_status = await self.db.get_guild_profile_feed_status(1, "millers")
        g2_status = await self.db.get_guild_profile_feed_status(2, "millers")
        self.assertIsNotNone(g1_status["recipient_gap"])
        self.assertEqual(g1_status["recipient_gap"]["reason"], "delivery_failure")
        self.assertIsNone(g2_status["recipient_gap"])

        embed_g1 = await self._feed_status_embed(self.db, guild_id=1, guild=g1, now=self.now)
        embed_g2 = await self._feed_status_embed(self.db, guild_id=2, guild=g2, now=self.now)
        self.assertIn("Recipient gap:", embed_g1.fields[0].value)
        self.assertNotIn("Recipient gap:", embed_g2.fields[0].value)
        self.assertIn("**Effective state:** Active\n", embed_g2.description + "\n")

        # Guild 1 recovers on retry -> gap clears and boundary advances to act-1.
        ch1.send = AsyncMock(return_value=SimpleNamespace(id=5001))
        with patch("bot.cogs.feed.asyncio.sleep", new=AsyncMock()):
            await cog.journal_feed_loop.coro(cog)
        g1_recovered = await self.db.get_guild_profile_feed_status(1, "millers")
        self.assertIsNone(g1_recovered["recipient_gap"])
        health = await self.db.get_profile_feed_source("millers", "activity")
        self.assertEqual(health["last_continuous_boundary_event_id"], "act-1")

        # Active edit failure on Guild 1 -> Guild 1 gets recipient_gap again until edit succeeds.
        ch1.get_partial_message(5001).edit.side_effect = _http_error(discord.HTTPException, 502, "Bad Gateway")
        ev1_edited = replace(ev1, rating=5.0)
        api.get_user_activity_snapshot.return_value = SimpleNamespace(
            entries=[ev1_edited], coverage="boundary_reached", pages_read=1,
        )
        with patch("bot.cogs.feed.asyncio.sleep", new=AsyncMock()):
            await cog.journal_feed_loop.coro(cog)
        g1_edit_gap = await self.db.get_guild_profile_feed_status(1, "millers")
        g2_no_gap = await self.db.get_guild_profile_feed_status(2, "millers")
        self.assertIsNotNone(g1_edit_gap["recipient_gap"])
        self.assertIsNone(g2_no_gap["recipient_gap"])

        ch1.get_partial_message(5001).edit.side_effect = None
        with patch("bot.cogs.feed.asyncio.sleep", new=AsyncMock()):
            await cog.journal_feed_loop.coro(cog)
        g1_final = await self.db.get_guild_profile_feed_status(1, "millers")
        self.assertIsNone(g1_final["recipient_gap"])

    async def test_real_source_outage_and_limited_coverage_remain_conservative_for_all_guilds(self):
        await self.db.link_user(10, "Millers")
        await self.db.set_feed_channel(1, 100)
        await self.db.set_feed_channel(2, 200)
        await self.db.set_guild_feed_preference(1, 10, True, member_ids=[10])
        await self.db.set_guild_feed_preference(2, 10, True, member_ids=[10])
        for gid in (1, 2):
            await self.db.reconcile_guild_feed_activity(gid, ["millers"], now=self.activation_time, present_member_ids=[10])
            await self.db.seed_feed_source(gid, "millers", "activity-display-v1", ["b-1"])
            await self.db.seed_feed_source(gid, "millers", "journal-display-v1", [])
        await self.db.record_feed_source_attempt("millers", "activity", now=self.activation_time)
        await self.db.record_feed_source_success(
            "millers", "activity", coverage="exhausted", pages_read=1, boundary_event_id="b-1", now=self.activation_time,
        )
        # Simulate genuine source failure + limited coverage recovery.
        await self.db.record_feed_source_attempt("millers", "activity", now=self.activation_time)
        await self.db.record_feed_source_failure("millers", "activity", "503 upstream")

        ch1 = _make_channel(100, 1)
        ch2 = _make_channel(200, 2)
        guilds = {1: _make_guild(1, [10], ch1), 2: _make_guild(2, [10], ch2)}
        undated_ev = _activity(activity_id="act-undated", slug="portal", title="Portal", action_type="rated", rating=4.0, occurred_at=None)
        api = SimpleNamespace(
            get_user_activity_snapshot=AsyncMock(return_value=SimpleNamespace(
                entries=[undated_ev], coverage="limited", pages_read=5,
            )),
            get_user_journal=AsyncMock(return_value=[]),
        )
        cog = FeedCog(SimpleNamespace(db=self.db, backloggd=api, get_guild=lambda gid: guilds.get(gid)), clock=lambda: self.now)
        with patch("bot.cogs.feed.asyncio.sleep", new=AsyncMock()):
            await cog.journal_feed_loop.coro(cog)

        for gid in (1, 2):
            dec = await self.db.get_feed_event_decision(gid, "millers", "act-undated")
            self.assertEqual(dec["decision"], "held_uncertain")
        ch1.send.assert_not_awaited()
        ch2.send.assert_not_awaited()

    async def test_all_unavailable_observes_and_disable_or_unset_clears_gap_preserving_archives(self):
        await self.db.link_user(10, "Millers")
        await self.db.set_feed_channel(1, 100)
        await self.db.set_feed_channel(2, 200)
        await self.db.set_guild_feed_preference(1, 10, True, member_ids=[10])
        await self.db.set_guild_feed_preference(2, 10, True, member_ids=[10])
        await self.db.reconcile_guild_feed_activity(1, ["millers"], now=self.activation_time, present_member_ids=[10])
        await self.db.reconcile_guild_feed_activity(2, ["millers"], now=self.activation_time, present_member_ids=[10])
        for gid in (1, 2):
            await self.db.seed_feed_source(gid, "millers", "activity-display-v1", [])
            await self.db.seed_feed_source(gid, "millers", "journal-display-v1", [])
        archived_payload = json.dumps(asdict(_activity(activity_id="archived-1", slug="celeste", title="Celeste")))
        await self.db.store_feed_message(2, "millers", ["archived-1"], archived_payload, 999, 200, "sent")

        # Poll when ALL guilds are unavailable -> still records durable observations and per-guild gaps.
        unavail_ev = _activity(activity_id="act-all-unavail", slug="hades", title="Hades", occurred_at="2026-11-13T11:00:00Z")
        api = SimpleNamespace(
            get_user_activity_snapshot=AsyncMock(return_value=SimpleNamespace(
                entries=[unavail_ev], coverage="exhausted", pages_read=1,
            )),
            get_user_journal=AsyncMock(return_value=[]),
        )
        cog = FeedCog(SimpleNamespace(db=self.db, backloggd=api, get_guild=lambda gid: None), clock=lambda: self.now)
        with patch("bot.cogs.feed.asyncio.sleep", new=AsyncMock()):
            await cog.journal_feed_loop.coro(cog)

        self.assertIsNotNone((await self.db.get_guild_profile_feed_status(1, "millers"))["recipient_gap"])
        self.assertIsNotNone((await self.db.get_guild_profile_feed_status(2, "millers"))["recipient_gap"])
        self.assertEqual([r["event_id"] for r in await self.db.get_pending_feed_observations(2, "millers")], ["act-all-unavail"])

        # Opt out guild 2 -> guild 2 gap clears while archive remains intact.
        await self.db.disable_guild_feed_preference_and_reconcile(2, 10, member_ids=[10])
        self.assertIsNone((await self.db.get_guild_profile_feed_status(2, "millers"))["recipient_gap"])
        g2_msgs = {r["event_id"]: r for r in await self.db.get_feed_messages(2, "millers")}
        self.assertEqual(g2_msgs["archived-1"]["delivery_state"], "sent")

        # Unset feed channel in guild 1 -> guild 1 gap clears too.
        await self.db.unset_feed_channel(1)
        self.assertIsNone((await self.db.get_guild_profile_feed_status(1, "millers"))["recipient_gap"])

    async def test_multi_guild_sql_budgets_and_sqlite_turso_parity(self):
        turso_path = Path(self.tmpdir.name) / "turso_gaps.db"
        seed = Database(str(turso_path), turso_url="")
        await seed.connect()
        await seed.close()
        turso_client = CountingTursoClient(str(turso_path))
        turso_db = Database(str(turso_path), turso_url="https://recipient-gaps.turso.io")
        turso_db._client = turso_client
        try:
            for backend, db in (("sqlite", self.db), ("turso", turso_db)):
                with self.subTest(backend=backend):
                    guild_ids = list(range(1, 36))
                    await db.link_user(10, "Millers")
                    for gid in guild_ids:
                        await db.set_feed_channel(gid, 1000 + gid)
                        await db.set_guild_feed_preference(gid, 10, True, member_ids=[10])
                        await db.reconcile_guild_feed_activity(gid, ["millers"], now=self.activation_time, present_member_ids=[10])
                        await db.seed_feed_source(gid, "millers", "activity-display-v1", [])
                        await db.seed_feed_source(gid, "millers", "journal-display-v1", [])

                    # 20 healthy, 15 unavailable
                    guilds = {}
                    for gid in guild_ids:
                        ch = _make_channel(1000 + gid, gid) if gid <= 20 else None
                        guilds[gid] = _make_guild(gid, [10], ch)

                    ev = _activity(activity_id="act-scale", slug="hades", title="Hades", occurred_at="2026-11-13T11:30:00Z")
                    api = SimpleNamespace(
                        get_user_activity_snapshot=AsyncMock(return_value=SimpleNamespace(
                            entries=[ev], coverage="exhausted", pages_read=1,
                        )),
                        get_user_journal=AsyncMock(return_value=[]),
                    )
                    cog = FeedCog(SimpleNamespace(db=db, backloggd=api, get_guild=lambda gid, m=guilds: m.get(gid)), clock=lambda: self.now)
                    with patch("bot.cogs.feed.asyncio.sleep", new=AsyncMock()):
                        await cog.journal_feed_loop.coro(cog)

                    api.get_user_activity_snapshot.assert_awaited_once()
                    api.get_user_journal.assert_awaited_once()
                    self.assertIsNone((await db.get_guild_profile_feed_status(1, "millers"))["recipient_gap"])
                    self.assertIsNotNone((await db.get_guild_profile_feed_status(25, "millers"))["recipient_gap"])
                    if backend == "turso":
                        self.assertLessEqual(turso_client.max_params_seen, _MAX_SQL_BIND_PARAMS)
                        self.assertLessEqual(turso_client.max_statement_bytes_seen, _MAX_SQL_BATCH_BYTES)
        finally:
            await turso_db.close()

    async def test_v9_migration_rollback_and_legacy_v8_upgrade(self):
        legacy_path = Path(self.tmpdir.name) / "v8_to_v9.db"
        v8_db = Database(str(legacy_path), turso_url="")
        await v8_db.connect()
        await v8_db.link_user(10, "Millers")
        await v8_db.record_feed_source_success("millers", "activity", coverage="exhausted", pages_read=1, boundary_event_id="b-1")
        await v8_db.record_feed_target_gap("millers", now=self.now)
        # Simulate pre-v9 state by rolling schema_migrations back to v8 and dropping v9 table if present
        await v8_db._execute("DELETE FROM schema_migrations WHERE version >= 9")
        await v8_db._execute("DROP TABLE IF EXISTS guild_profile_feed_recipient_gaps")
        await v8_db.close()

        # Inject a failure during v9 migration -> must roll back cleanly.
        retry_db = Database(str(legacy_path), turso_url="")

        async def fail_v9(execute, fetchone):
            await execute("CREATE TABLE v9_partial (x INTEGER)", ())
            raise RuntimeError("injected v9 interruption")

        try:
            with patch("bot.db_migrations._migrate_guild_recipient_gaps", side_effect=fail_v9):
                with self.assertRaisesRegex(RuntimeError, "injected v9 interruption"):
                    await retry_db.connect()
        finally:
            await retry_db.close()

        conn = sqlite3.connect(legacy_path)
        try:
            self.assertIsNone(conn.execute("SELECT 1 FROM sqlite_master WHERE name = 'v9_partial'").fetchone())
            self.assertEqual(conn.execute("SELECT MAX(version) FROM schema_migrations").fetchone()[0], 8)
        finally:
            conn.close()

        # Retry without fault -> completes cleanly to v9 while preserving source health.
        ok_db = Database(str(legacy_path), turso_url="")
        await ok_db.connect()
        try:
            ver = await ok_db._fetchone("SELECT MAX(version) AS v FROM schema_migrations")
            self.assertGreaterEqual(ver["v"], 9)
            health = await ok_db.get_profile_feed_source("millers", "activity")
            self.assertEqual(health["last_continuous_boundary_event_id"], "b-1")
            self.assertEqual(health["gap_reason"], "unavailable_feed_target")
        finally:
            await ok_db.close()


if __name__ == "__main__":
    unittest.main()
