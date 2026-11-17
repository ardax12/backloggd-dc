"""Task 6 regressions: separate identity resolution from announcement permission (F3)."""

import os

os.environ["PYTHON_DOTENV_DISABLED"] = "1"
os.environ["TURSO_DATABASE_URL"] = ""
os.environ["TURSO_AUTH_TOKEN"] = ""
os.environ["DEV_GUILD_ID"] = ""

import asyncio
from contextlib import asynccontextmanager
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
from bot.feed_models import FeedEvent
from tests.test_feed_batch_writes import CountingTursoClient


def _make_channel(channel_id: int = 100) -> MagicMock:
    channel = MagicMock(spec=discord.TextChannel)
    channel.id = channel_id
    channel.guild = SimpleNamespace(id=1)
    next_id = 900
    partials = {}

    async def _send(**kwargs):
        nonlocal next_id
        next_id += 1
        return SimpleNamespace(id=next_id, kwargs=kwargs)

    def _get_partial(msg_id: int):
        if msg_id not in partials:
            partials[msg_id] = SimpleNamespace(id=msg_id, edit=AsyncMock())
        return partials[msg_id]

    channel.send = AsyncMock(side_effect=_send)
    channel.get_partial_message = _get_partial
    channel._partials = partials
    return channel


def _session_event(
    *,
    activity_id: str,
    slug: str = "hades",
    title: str = "Hades",
    action_type: str = "completed",
    rating: float | None = 4.0,
    review_notes: str | None = None,
    occurred_at: str | None = "2026-11-12T10:00:00Z",
    played_date: str | None = "2026-11-12",
    reference: str | None = None,
    event_ids: tuple[str, ...] | None = None,
) -> FeedEvent:
    return FeedEvent(
        activity_id=activity_id,
        event_ids=event_ids or (activity_id,),
        source="activity",
        action_type=action_type,
        game_slug=slug,
        game_title=title,
        cover_url=None,
        action_text=f"{action_type} {title}",
        rating=rating,
        review_notes=review_notes,
        date_str=played_date,
        occurred_at=occurred_at,
        played_date=played_date,
        reference=reference,
    )


class FeedIdentityPolicyTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmpdir.cleanup)
        self.db_path = str(Path(self.tmpdir.name) / "identity_policy.db")
        self.db = Database(self.db_path, turso_url="")
        await self.db.connect()
        self.addAsyncCleanup(self.db.close)
        await self.db.link_user(10, "Millers")
        await self.db.set_feed_channel(1, 100)
        await self.db.set_guild_feed_preference(1, 10, True, member_ids=[10])
        self.activation_at = datetime(2026, 11, 10, 0, 0, tzinfo=timezone.utc)
        await self.db.reconcile_guild_feed_activity(
            1, ["millers"], now=self.activation_at, present_member_ids=[10],
        )

    async def test_f3_reproduction_legacy_baseline_lacking_decisions_never_becomes_eligible_or_posts(self):
        # Legacy migrated baseline in profile_feed_messages with NO row in profile_feed_decisions.
        # Occurred after activation and within 7 days, so only its baseline row guards against eligibility.
        legacy_base = _session_event(
            activity_id="act-legacy-base",
            occurred_at="2026-11-10T10:00:00Z",
            played_date="2026-11-10",
            reference="/u/Millers/log/legacy-1/",
        )
        await self.db.seed_feed_source(1, "millers", "activity-display-v1", ["act-legacy-base"])
        await self.db.seed_feed_source(1, "millers", "journal-display-v1", [])
        await self.db.seed_feed_message(
            1, "millers", ["act-legacy-base"], json.dumps(asdict(legacy_base), sort_keys=True)
        )
        self.assertIsNone(await self.db.get_feed_event_decision(1, "millers", "act-legacy-base"))

        t1 = datetime(2026, 11, 12, 12, 0, tzinfo=timezone.utc)
        t2 = datetime(2026, 11, 13, 12, 0, tzinfo=timezone.utc)
        channel = _make_channel(100)

        # Poll 1 at T1: raw session ID changes without reference -> held_ambiguous.
        old_ambig = _session_event(
            activity_id="act-reid",
            occurred_at="2026-11-10T10:00:00Z",
            played_date="2026-11-10",
            reference=None,
        )
        with patch("bot.cogs.feed.asyncio.sleep", new=AsyncMock()):
            cog_t1 = FeedCog(SimpleNamespace(db=self.db), clock=lambda: t1)
            await cog_t1._publish_sources(
                1, "millers", "Millers", None, channel, {"activity": [old_ambig], "journal": []}
            )
        channel.send.assert_not_awaited()
        self.assertEqual(
            (await self.db.get_feed_event_decision(1, "millers", "act-reid"))["decision"],
            "held_ambiguous",
        )

        # Poll 2 at T2 with deliver=False (e.g. target unavailable): reference stabilizes to act-legacy-base.
        resolved_event = replace(old_ambig, reference="/u/Millers/log/legacy-1/")
        with patch("bot.cogs.feed.asyncio.sleep", new=AsyncMock()):
            cog_t2 = FeedCog(SimpleNamespace(db=self.db), clock=lambda: t2)
            await cog_t2._publish_sources(
                1, "millers", "Millers", None, None, {"activity": [resolved_event], "journal": []}, deliver=False
            )

        # Decision MUST NOT be forced to 'eligible' and MUST NOT enter get_pending_feed_observations.
        decisions = await self.db.get_feed_event_decisions(1, "millers", ["act-legacy-base", "act-reid"])
        self.assertEqual(decisions["act-reid"]["decision"], "baseline")
        self.assertEqual(decisions["act-legacy-base"]["decision"], "baseline")
        self.assertEqual(decisions["act-reid"]["observed_at"], "2026-11-12 12:00:00")
        self.assertEqual(await self.db.get_pending_feed_observations(1, "millers"), [])

        # Poll 3 with deliver=True: existing baseline still blocks Discord announcement.
        with patch("bot.cogs.feed.asyncio.sleep", new=AsyncMock()):
            ok = await cog_t2._publish_sources(
                1, "millers", "Millers", None, channel, {"activity": [resolved_event], "journal": []}, deliver=True
            )
        self.assertTrue(ok)
        channel.send.assert_not_awaited()

    async def test_held_identity_resolution_passes_through_full_policy_with_original_observed_at(self):
        # Seed sent archive posts with stable references so identity matches after initial held_ambiguous.
        channel = _make_channel(100)
        cog_init = FeedCog(SimpleNamespace(db=self.db), clock=lambda: self.activation_at)
        with patch("bot.cogs.feed.asyncio.sleep", new=AsyncMock()):
            await cog_init._publish_sources(1, "millers", "Millers", None, channel, {"activity": [], "journal": []})

        # Prior baseline events for different games so unreferenced sessions become held_ambiguous first.
        cases = [
            # (slug, game_title, observed_at, resolve_now, played_date, occurred_at, recovery_uncertain, expected_decision)
            ("recent-ok", "Recent OK", datetime(2026, 11, 10, 12, 0, tzinfo=timezone.utc), datetime(2026, 11, 15, 12, 0, tzinfo=timezone.utc), "2026-11-05", "2026-11-10T11:00:00Z", False, "eligible"),
            ("old-sess", "Old Sess", datetime(2026, 11, 12, 12, 0, tzinfo=timezone.utc), datetime(2026, 11, 13, 12, 0, tzinfo=timezone.utc), "2026-10-20", "2026-11-12T10:00:00Z", False, "suppressed_old"),
            ("future-sess", "Future Sess", datetime(2026, 11, 12, 12, 0, tzinfo=timezone.utc), datetime(2026, 11, 13, 12, 0, tzinfo=timezone.utc), "2026-11-25", "2026-11-12T10:00:00Z", False, "held_future"),
            ("pre-act", "Pre Act", datetime(2026, 11, 12, 12, 0, tzinfo=timezone.utc), datetime(2026, 11, 13, 12, 0, tzinfo=timezone.utc), "2026-11-08", "2026-11-08T10:00:00Z", False, "suppressed_pre_activation"),
            ("rec-uncertain", "Rec Uncertain", datetime(2026, 11, 12, 12, 0, tzinfo=timezone.utc), datetime(2026, 11, 13, 12, 0, tzinfo=timezone.utc), None, None, True, "held_uncertain"),
        ]
        # Seed one sent post per game with stable reference so held_ambiguous can match and resolve later.
        for slug, title, obs_at, resolve_now, pdate, occ_at, rec_unc, expected_dec in cases:
            prior_ev = _session_event(
                activity_id=f"prior-{slug}",
                slug=slug,
                title=title,
                occurred_at="2026-11-10T01:00:00Z",
                played_date="2026-11-10",
                reference=f"/u/Millers/log/{slug}/",
            )
            await self.db.store_feed_message(
                1, "millers", [prior_ev.activity_id], json.dumps(asdict(prior_ev), sort_keys=True),
                message_id=700, channel_id=100, delivery_state="sent",
            )

        # Hold unreferenced session events at their respective discovery times.
        for slug, title, obs_at, resolve_now, pdate, occ_at, rec_unc, expected_dec in cases:
            held_ev = _session_event(
                activity_id=f"new-{slug}",
                slug=slug,
                title=title,
                occurred_at=occ_at,
                played_date=pdate,
                reference=None,
            )
            cog_obs = FeedCog(SimpleNamespace(db=self.db), clock=lambda t=obs_at: t)
            with patch("bot.cogs.feed.asyncio.sleep", new=AsyncMock()):
                await cog_obs._publish_sources(1, "millers", "Millers", None, channel, {"activity": [held_ev], "journal": []})
            row = await self.db.get_feed_event_decision(1, "millers", f"new-{slug}")
            self.assertEqual(row["decision"], "held_ambiguous", slug)

        # Now each gains its stable reference at resolve_now (under deliver=False so we check persisted decisions directly).
        for slug, title, obs_at, resolve_now, pdate, occ_at, rec_unc, expected_dec in cases:
            with self.subTest(slug=slug, expected=expected_dec):
                resolved_ev = _session_event(
                    activity_id=f"new-{slug}",
                    slug=slug,
                    title=title,
                    occurred_at=occ_at,
                    played_date=pdate,
                    reference=f"/u/Millers/log/{slug}/",
                )
                cog_res = FeedCog(SimpleNamespace(db=self.db), clock=lambda t=resolve_now: t)
                if rec_unc:
                    cog_res._activity_snapshot_meta["millers"] = {
                        "coverage": "boundary_reached",
                        "pages_read": 3,
                        "prior_boundary_id": "prior-rec-uncertain",
                        "boundary_event_id": None,
                        "had_unresolved_gap": False,
                        "prior_gap_reason": None,
                        "had_source_gap": False,
                        "had_prior_failure": False,
                        "observed_at": resolve_now,
                    }
                with patch("bot.cogs.feed.asyncio.sleep", new=AsyncMock()):
                    await cog_res._publish_sources(
                        1, "millers", "Millers", None, None, {"activity": [resolved_ev], "journal": []}, deliver=False
                    )
                dec = await self.db.get_feed_event_decision(1, "millers", f"new-{slug}")
                self.assertEqual(dec["decision"], expected_dec, f"Got {dec['decision']} ({dec['reason']}) for {slug}")
                expected_ts = obs_at.strftime("%Y-%m-%d %H:%M:%S")
                self.assertEqual(dec["observed_at"], expected_ts)

    async def test_archive_edits_on_old_posted_session_still_apply_after_identity_resolution(self):
        t0 = datetime(2026, 11, 11, 12, 0, tzinfo=timezone.utc)
        t1 = datetime(2026, 11, 12, 12, 0, tzinfo=timezone.utc)
        t2 = datetime(2026, 11, 13, 12, 0, tzinfo=timezone.utc)
        channel = _make_channel(100)
        cog = FeedCog(SimpleNamespace(db=self.db), clock=lambda: t0)
        with patch("bot.cogs.feed.asyncio.sleep", new=AsyncMock()):
            await cog._publish_sources(1, "millers", "Millers", None, channel, {"activity": [], "journal": []})

        sent_ev = _session_event(
            activity_id="act-posted",
            occurred_at="2026-11-11T10:00:00Z",
            played_date="2026-11-11",
            action_type="completed",
            rating=4.0,
            reference="/u/Millers/log/posted-1/",
        )
        with patch("bot.cogs.feed.asyncio.sleep", new=AsyncMock()):
            await cog._publish_sources(1, "millers", "Millers", None, channel, {"activity": [sent_ev], "journal": []})
        self.assertEqual(channel.send.await_count, 1)

        # Next poll: new completed alias for old edited date without reference -> held_ambiguous
        ambig_ev = _session_event(
            activity_id="act-posted-v2",
            occurred_at="2026-11-11T10:00:00Z",
            played_date="2026-10-01",
            action_type="completed",
            rating=4.5,
            review_notes="Edited later!",
            reference=None,
        )
        cog.clock = lambda: t1
        with patch("bot.cogs.feed.asyncio.sleep", new=AsyncMock()):
            await cog._publish_sources(1, "millers", "Millers", None, channel, {"activity": [ambig_ev], "journal": []})
        self.assertEqual(channel.send.await_count, 1)
        self.assertEqual(
            (await self.db.get_feed_event_decision(1, "millers", "act-posted-v2"))["decision"],
            "held_ambiguous",
        )

        # Poll 3: gains stable reference matching sent_ev -> MUST edit message 901 without reposting!
        resolved_ev = replace(ambig_ev, reference="/u/Millers/log/posted-1/")
        cog.clock = lambda: t2
        with patch("bot.cogs.feed.asyncio.sleep", new=AsyncMock()):
            ok = await cog._publish_sources(1, "millers", "Millers", None, channel, {"activity": [resolved_ev], "journal": []})
        self.assertTrue(ok)
        self.assertEqual(channel.send.await_count, 1)
        channel._partials[901].edit.assert_awaited_once()
        self.assertIn("Edited later!", channel._partials[901].edit.call_args.kwargs["embed"].fields[0].value)
        dec = await self.db.get_feed_event_decision(1, "millers", "act-posted-v2")
        self.assertEqual(dec["decision"], "suppressed_old")
        self.assertEqual(dec["observed_at"], "2026-11-12 12:00:00")

    async def test_explicit_rating_independence_stricter_decisions_and_protected_states(self):
        t1 = datetime(2026, 11, 12, 12, 0, tzinfo=timezone.utc)
        t2 = datetime(2026, 11, 13, 12, 0, tzinfo=timezone.utc)
        channel = _make_channel(100)
        cog = FeedCog(SimpleNamespace(db=self.db), clock=lambda: t1)
        with patch("bot.cogs.feed.asyncio.sleep", new=AsyncMock()):
            await cog._publish_sources(1, "millers", "Millers", None, channel, {"activity": [], "journal": []})

        # Existing sent post + protected states + stricter decision
        prior_rating = _session_event(
            activity_id="prior-rate",
            slug="celeste",
            title="Celeste",
            action_type="rated",
            rating=4.0,
            occurred_at="2026-11-11T10:00:00Z",
            played_date="2026-10-01",
            reference="/u/Millers/log/celeste/",
        )
        await self.db.store_feed_message(
            1, "millers", ["prior-rate"], json.dumps(asdict(prior_rating), sort_keys=True),
            message_id=902, channel_id=100, delivery_state="sent",
        )
        # Explicit rating held_ambiguous manually, plus a stricter suppressed_pre_activation decision
        ambig_rating = replace(prior_rating, activity_id="rate-v2", reference=None)
        await self.db.record_feed_event_decision(
            1, "millers", "rate-v2", source="activity", event_date=None,
            decision="held_ambiguous", reason="test", payload=json.dumps(asdict(ambig_rating), sort_keys=True),
            observed_at=t1,
        )
        await self.db.record_feed_event_decision(
            1, "millers", "strict-alias", source="activity", event_date="2026-11-01",
            decision="suppressed_pre_activation", reason="pre_activation",
            payload=json.dumps(asdict(prior_rating), sort_keys=True), observed_at=t1,
        )
        deleted_base = _session_event(
            activity_id="protected-deleted",
            slug="deleted-game",
            title="Deleted Game",
            occurred_at="2026-11-11T10:00:00Z",
            played_date="2026-11-11",
            reference="/u/Millers/log/deleted-game/",
        )
        await self.db.store_feed_message(
            1, "millers", ["protected-deleted"], json.dumps(asdict(deleted_base), sort_keys=True),
            message_id=903, channel_id=100, delivery_state="deleted",
        )
        await self.db.record_feed_event_decision(
            1, "millers", "protected-deleted", source="activity", event_date="2026-11-11",
            decision="held_ambiguous", reason="test", payload="{}", observed_at=t1,
        )

        # Explicit rating resolves to 'eligible' despite old played_date; stricter stays suppressed_pre_activation; deleted stays held_ambiguous.
        cog.clock = lambda: t2
        resolved_rating = replace(ambig_rating, reference="/u/Millers/log/celeste/", rating=5.0)
        strict_ev = _session_event(
            activity_id="strict-alias", slug="hollow", title="Hollow",
            occurred_at="2026-11-12T10:00:00Z", played_date="2026-11-12", reference="/u/Millers/log/hollow/",
        )
        await self.db.store_feed_message(
            1, "millers", ["prior-hollow"], json.dumps(asdict(replace(strict_ev, activity_id="prior-hollow")), sort_keys=True),
            message_id=904, channel_id=100, delivery_state="sent",
        )
        del_ev = deleted_base
        with patch("bot.cogs.feed.asyncio.sleep", new=AsyncMock()):
            await cog._publish_sources(
                1, "millers", "Millers", None, None,
                {"activity": [resolved_rating, strict_ev, del_ev], "journal": []}, deliver=False,
            )

        decs = await self.db.get_feed_event_decisions(1, "millers", ["rate-v2", "strict-alias", "protected-deleted"])
        self.assertEqual(decs["rate-v2"]["decision"], "eligible")
        self.assertEqual(decs["strict-alias"]["decision"], "suppressed_pre_activation")
        self.assertEqual(decs["protected-deleted"]["decision"], "held_ambiguous")

    async def test_rollback_cancellation_and_sqlite_turso_parity(self):
        turso_path = Path(self.tmpdir.name) / "turso_id_policy.db"
        seed = Database(str(turso_path), turso_url="")
        await seed.connect()
        await seed.close()
        turso_client = CountingTursoClient(str(turso_path))
        turso_db = Database(str(turso_path), turso_url="https://id-policy.turso.io")
        turso_db._client = turso_client
        t1 = datetime(2026, 11, 12, 12, 0, tzinfo=timezone.utc)
        t2 = datetime(2026, 11, 13, 12, 0, tzinfo=timezone.utc)
        try:
            for backend, db in (("sqlite", self.db), ("turso", turso_db)):
                for cancel in (False, True):
                    with self.subTest(backend=backend, cancel=cancel):
                        gid = 10 + int(cancel)
                        await db.link_user(10, "Millers")
                        await db.set_feed_channel(gid, 100 + gid)
                        await db.set_guild_feed_preference(gid, 10, True, member_ids=[10])
                        await db.reconcile_guild_feed_activity(gid, ["millers"], now=self.activation_at, present_member_ids=[10])
                        await db.seed_feed_source(gid, "millers", "activity-display-v1", [])
                        await db.seed_feed_source(gid, "millers", "journal-display-v1", [])

                        prior = _session_event(
                            activity_id=f"prior-{gid}", occurred_at="2026-11-11T10:00:00Z",
                            played_date="2026-10-01", reference=f"/u/Millers/log/{gid}/",
                        )
                        await db.store_feed_message(
                            gid, "millers", [prior.activity_id], json.dumps(asdict(prior), sort_keys=True),
                            message_id=800 + gid, channel_id=100 + gid, delivery_state="sent",
                        )
                        old_ev = _session_event(
                            activity_id=f"old-{gid}", occurred_at="2026-11-12T10:00:00Z",
                            played_date="2026-10-01", reference=None,
                        )
                        await db.record_feed_event_decision(
                            gid, "millers", old_ev.activity_id, source="activity", event_date="2026-10-01",
                            decision="held_ambiguous", reason="ambig", payload=json.dumps(asdict(old_ev), sort_keys=True),
                            observed_at=t1,
                        )

                        resolved = replace(old_ev, reference=prior.reference)
                        cog = FeedCog(SimpleNamespace(db=db), clock=lambda: t2)

                        orig_tx = db._transaction
                        gate = asyncio.Event()

                        @asynccontextmanager
                        async def failing_tx():
                            async with orig_tx() as tx:
                                orig_exec = tx.execute

                                async def wrapped(sql, params=()):
                                    if "UPDATE profile_feed_decisions" in sql or "INSERT INTO profile_feed_decisions" in sql:
                                        if cancel:
                                            gate.set()
                                            await asyncio.sleep(10)
                                        else:
                                            raise RuntimeError("injected policy update failure")
                                    return await orig_exec(sql, params)

                                tx.execute = wrapped
                                yield tx

                        db._transaction = failing_tx
                        try:
                            task = asyncio.create_task(
                                cog._publish_sources(
                                    gid, "millers", "Millers", None, None,
                                    {"activity": [resolved], "journal": []}, deliver=False,
                                )
                            )
                            if cancel:
                                await asyncio.wait_for(gate.wait(), timeout=5.0)
                                task.cancel()
                                with self.assertRaises(asyncio.CancelledError):
                                    await task
                            else:
                                with self.assertRaisesRegex(RuntimeError, "injected policy update failure"):
                                    await task
                        finally:
                            db._transaction = orig_tx

                        # Decision rolled back cleanly to held_ambiguous
                        row = await db.get_feed_event_decision(gid, "millers", old_ev.activity_id)
                        self.assertEqual(row["decision"], "held_ambiguous")

                        # Retry without fault -> resolves cleanly to suppressed_old
                        with patch("bot.cogs.feed.asyncio.sleep", new=AsyncMock()):
                            await cog._publish_sources(
                                gid, "millers", "Millers", None, None,
                                {"activity": [resolved], "journal": []}, deliver=False,
                            )
                        row_after = await db.get_feed_event_decision(gid, "millers", old_ev.activity_id)
                        self.assertEqual(row_after["decision"], "suppressed_old")
                        self.assertEqual(row_after["observed_at"], "2026-11-12 12:00:00")
            self.assertLessEqual(turso_client.max_params_seen, _MAX_SQL_BIND_PARAMS)
            self.assertLessEqual(turso_client.max_statement_bytes_seen, _MAX_SQL_BATCH_BYTES)
        finally:
            await turso_db.close()


if __name__ == "__main__":
    unittest.main()
