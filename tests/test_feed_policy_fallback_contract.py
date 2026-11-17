"""Task 9 regressions: explicit safe non-bulk policy adapter contract and bulk parity (F9)."""

import os

os.environ["PYTHON_DOTENV_DISABLED"] = "1"
os.environ["TURSO_DATABASE_URL"] = ""
os.environ["TURSO_AUTH_TOKEN"] = ""
os.environ["DEV_GUILD_ID"] = ""

from dataclasses import asdict
from datetime import datetime, timezone
import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest

from bot.cogs.feed import FeedCog
from bot.db import Database
from bot.feed_models import FeedEvent


def _make_event(
    event_id: str,
    *,
    action: str = "completed",
    slug: str = "hades",
    title: str = "Hades",
    played_date: str | None = "2026-11-12",
    occurred_at: str | None = "2026-11-12T10:00:00Z",
    rating: float | None = None,
    review_notes: str | None = None,
    source: str = "activity",
    reference: str | None = None,
    event_ids: tuple[str, ...] | None = None,
) -> FeedEvent:
    return FeedEvent(
        activity_id=event_id,
        game_title=title,
        game_slug=slug,
        cover_url=None,
        action_type=action,
        action_text=f"{action} {title}",
        rating=rating,
        review_notes=review_notes,
        date_str=played_date,
        played_date=played_date,
        occurred_at=occurred_at,
        source=source,
        reference=reference,
        event_ids=event_ids or (event_id,),
    )


class _LegacySingleRowDbAdapter:
    """Compatibility adapter exposing only single-row decision methods (no bulk API)."""

    def __init__(
        self,
        db: Database,
        *,
        return_none_on_record: bool = False,
        disable_reread: bool = False,
    ):
        self._db = db
        self.return_none_on_record = return_none_on_record
        self.disable_reread = disable_reread
        self.record_calls: list[tuple[str, str]] = []
        self.revise_calls: list[tuple[str, str]] = []

    async def get_feed_event_decision(self, guild_id: int, profile_key: str, event_id: str):
        if self.disable_reread:
            return None
        return await self._db.get_feed_event_decision(guild_id, profile_key, event_id)

    async def record_feed_event_decision(
        self,
        guild_id: int,
        profile_key: str,
        event_id: str,
        *,
        source: str,
        event_date,
        decision: str,
        reason: str,
        payload: str,
        observed_at=None,
    ):
        self.record_calls.append((event_id, decision))
        row = await self._db.record_feed_event_decision(
            guild_id,
            profile_key,
            event_id,
            source=source,
            event_date=event_date,
            decision=decision,
            reason=reason,
            payload=payload,
            observed_at=observed_at,
        )
        if self.return_none_on_record:
            return None
        return row

    async def revise_unsent_eligible_feed_decision(
        self,
        guild_id: int,
        profile_key: str,
        event_id: str,
        *,
        decision: str,
        reason: str,
        event_date,
        payload: str,
        allowed_from=("eligible",),
    ):
        self.revise_calls.append((event_id, decision))
        return await self._db.revise_unsent_eligible_feed_decision(
            guild_id,
            profile_key,
            event_id,
            decision=decision,
            reason=reason,
            event_date=event_date,
            payload=payload,
            allowed_from=allowed_from,
        )


class FeedPolicyFallbackContractTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmpdir.cleanup)
        self.now = datetime(2026, 11, 12, 12, 0, tzinfo=timezone.utc)
        self.activation_at = datetime(2026, 10, 15, 0, 0, tzinfo=timezone.utc)

    async def _make_db(self, name: str) -> Database:
        db = Database(str(Path(self.tmpdir.name) / name), turso_url="")
        await db.connect()
        self.addAsyncCleanup(db.close)
        await db.link_user(10, "Millers")
        await db.set_feed_channel(1, 100)
        await db.set_guild_feed_preference(1, 10, True, member_ids=[10])
        await db.reconcile_guild_feed_activity(1, ["millers"], now=self.activation_at, present_member_ids=[10])
        return db

    async def test_same_poll_alias_tightening_forward_and_reverse_matches_bulk_path(self):
        # Forward tightening with transitive alias chain:
        # ev1 ("a1", "a2") looks eligible -> ev2 ("a2", "a3") also recent EXCEPT "a3" is suppressed_old (or ev3 suppresses a3).
        # Reverse ordering: stricter event arrives first, then eligible alias event arrives second.
        scenarios = (
            (
                "forward_2hop",
                [
                    _make_event("a1", played_date="2026-11-12", occurred_at="2026-11-12T10:00:00Z", event_ids=("a1", "a2")),
                    _make_event("a2", played_date="2026-11-12", occurred_at="2026-11-12T10:00:00Z", event_ids=("a2", "a3")),
                    _make_event("a3", played_date="2026-11-01", occurred_at="2026-11-01T10:00:00Z", event_ids=("a3",)),
                ],
            ),
            (
                "forward_single_then_multi_old",
                [
                    _make_event("f1", played_date="2026-11-12", occurred_at="2026-11-12T10:00:00Z", event_ids=("f1",)),
                    _make_event("f2", played_date="2026-11-01", occurred_at="2026-11-01T10:00:00Z", event_ids=("f1", "f2")),
                ],
            ),
            (
                "reverse_multi_old_then_single",
                [
                    _make_event("r2", played_date="2026-11-01", occurred_at="2026-11-01T10:00:00Z", event_ids=("r1", "r2")),
                    _make_event("r1", played_date="2026-11-12", occurred_at="2026-11-12T10:00:00Z", event_ids=("r1",)),
                ],
            ),
        )

        for label, events in scenarios:
            with self.subTest(scenario=label):
                bulk_db = await self._make_db(f"bulk_{label}.db")
                legacy_db = await self._make_db(f"legacy_{label}.db")
                adapter = _LegacySingleRowDbAdapter(legacy_db)

                bulk_cog = FeedCog(SimpleNamespace(db=bulk_db), clock=lambda: self.now)
                legacy_cog = FeedCog(SimpleNamespace(db=adapter), clock=lambda: self.now)

                bulk_accepted = await bulk_cog._apply_discovery_policy(
                    1, "millers", events, {}, self.now, activation_at=self.activation_at,
                )
                legacy_accepted = await legacy_cog._apply_discovery_policy(
                    1, "millers", events, {}, self.now, activation_at=self.activation_at,
                )

                self.assertEqual([e.activity_id for e in legacy_accepted], [])
                self.assertEqual(
                    [e.activity_id for e in legacy_accepted],
                    [e.activity_id for e in bulk_accepted],
                )
                all_ids = sorted({eid for ev in events for eid in ev.event_ids})
                bulk_decisions = await bulk_db.get_feed_event_decisions(1, "millers", all_ids)
                legacy_decisions = await legacy_db.get_feed_event_decisions(1, "millers", all_ids)
                for eid in all_ids:
                    self.assertEqual(legacy_decisions[eid]["decision"], "suppressed_old")
                    self.assertEqual(legacy_decisions[eid]["decision"], bulk_decisions[eid]["decision"])

    async def test_record_returning_none_does_not_crash_and_preserves_stricter_decisions(self):
        # 1. Pure write-only / No-reread None-returning adapter retains planned decisions in request state
        db_none = await self._make_db("none_noread.db")
        none_adapter = _LegacySingleRowDbAdapter(db_none, return_none_on_record=True, disable_reread=True)
        cog_none = FeedCog(SimpleNamespace(db=none_adapter), clock=lambda: self.now)

        events = [
            _make_event("n1", played_date="2026-11-01", occurred_at="2026-11-01T10:00:00Z", event_ids=("n1", "shared-n")),
            _make_event("n2", played_date="2026-11-12", occurred_at="2026-11-12T10:00:00Z", event_ids=("n2", "shared-n")),
            _make_event("ok1", played_date="2026-11-12", occurred_at="2026-11-12T10:00:00Z", event_ids=("ok1",)),
        ]
        accepted = await cog_none._apply_discovery_policy(
            1, "millers", events, {}, self.now, activation_at=self.activation_at,
        )
        self.assertEqual([e.activity_id for e in accepted], ["ok1"])

        # 2. Adapter where record_feed_event_decision returns None, AND storage already had a stricter decision
        #    inserted concurrently between initial prefetch and record (reread recovers it).
        db_race = await self._make_db("none_race.db")
        race_adapter = _LegacySingleRowDbAdapter(db_race, return_none_on_record=True, disable_reread=False)
        orig_record = race_adapter.record_feed_event_decision

        async def racing_record(guild_id, profile_key, event_id, **kwargs):
            if event_id == "race-b":
                # Simulate a concurrent writer having already stored suppressed_old for race-b before our INSERT OR IGNORE.
                await db_race.record_feed_event_decision(
                    guild_id, profile_key, "race-b",
                    source="journal", event_date="2026-10-01", decision="suppressed_old",
                    reason="concurrent_old", payload="{}", observed_at=self.now,
                )
            return await orig_record(guild_id, profile_key, event_id, **kwargs)

        race_adapter.record_feed_event_decision = racing_record
        cog_race = FeedCog(SimpleNamespace(db=race_adapter), clock=lambda: self.now)

        race_events = [
            _make_event("race-a", played_date="2026-11-12", occurred_at="2026-11-12T10:00:00Z", event_ids=("race-a", "race-b")),
        ]
        race_accepted = await cog_race._apply_discovery_policy(
            1, "millers", race_events, {}, self.now, activation_at=self.activation_at,
        )
        self.assertEqual(race_accepted, [])
        stored_map = await db_race.get_feed_event_decisions(1, "millers", ["race-a", "race-b"])
        self.assertEqual(stored_map["race-b"]["decision"], "suppressed_old")
        self.assertEqual(stored_map["race-a"]["decision"], "suppressed_old")

    async def test_bulk_vs_fallback_parity_with_baseline_protected_resolved_and_mixed_corpus(self):
        bulk_db = await self._make_db("parity_bulk.db")
        legacy_db = await self._make_db("parity_legacy.db")

        # Seed identical initial baselines, protected rows (sent/deleted/pending/unknown), stored stricter & held_ambiguous rows
        for db in (bulk_db, legacy_db):
            base_payload = json.dumps(asdict(_make_event("base-1")), sort_keys=True)
            sent_payload = json.dumps(asdict(_make_event("sent-1", reference="/u/Millers/log/1/")), sort_keys=True)
            del_payload = json.dumps(asdict(_make_event("del-1")), sort_keys=True)
            pend_payload = json.dumps(asdict(_make_event("pend-1")), sort_keys=True)
            unk_payload = json.dumps(asdict(_make_event("unk-1")), sort_keys=True)

            await db.seed_feed_source(1, "millers", "activity-display-v1", ["base-1"])
            await db.seed_feed_message(1, "millers", ["base-1"], base_payload)
            await db.store_feed_message(1, "millers", ["sent-1"], sent_payload, message_id=901, channel_id=100, delivery_state="sent")
            await db.store_feed_message(1, "millers", ["del-1"], del_payload, message_id=902, channel_id=100, delivery_state="deleted")
            await db.begin_feed_delivery(1, "millers", ["pend-1"], pend_payload, 100)
            await db.begin_feed_delivery(1, "millers", ["unk-1"], unk_payload, 100)
            await db.mark_feed_delivery_unknown(1, "millers", ["unk-1"])

            # Ambiguous rows: one old (suppressed upon resolution), one deleted/protected, one recent eligible
            await db.record_feed_event_decision(
                1, "millers", "amb-old", source="activity", event_date="2026-10-20",
                decision="held_ambiguous", reason="amb", payload=json.dumps(asdict(_make_event("amb-old", played_date="2026-10-20", occurred_at="2026-10-20T09:00:00Z"))),
                observed_at=self.now,
            )
            await db.record_feed_event_decision(
                1, "millers", "del-1", source="activity", event_date="2026-11-12",
                decision="held_ambiguous", reason="amb", payload=del_payload, observed_at=self.now,
            )
            await db.record_feed_event_decision(
                1, "millers", "amb-ok", source="activity", event_date="2026-11-11",
                decision="held_ambiguous", reason="amb", payload=json.dumps(asdict(_make_event("amb-ok", played_date="2026-11-11", occurred_at="2026-11-11T09:00:00Z"))),
                observed_at=self.now,
            )
            # Pre-existing stricter alias for tightening test
            await db.record_feed_event_decision(
                1, "millers", "pre-supp", source="journal", event_date="2026-11-01",
                decision="suppressed_old", reason="old", payload="{}", observed_at=self.now,
            )

        bulk_saved = {row["event_id"]: row for row in await bulk_db.get_feed_messages(1, "millers")}
        legacy_saved = {row["event_id"]: row for row in await legacy_db.get_feed_messages(1, "millers")}

        corpus = [
            _make_event("base-1"),
            _make_event("sent-1", reference="/u/Millers/log/1/"),
            _make_event("del-1"),
            _make_event("pend-1"),
            _make_event("unk-1"),
            _make_event("amb-old", played_date="2026-10-20", occurred_at="2026-10-20T09:00:00Z"),
            _make_event("amb-ok", played_date="2026-11-11", occurred_at="2026-11-11T09:00:00Z"),
            _make_event("new-ok", played_date="2026-11-12", occurred_at="2026-11-12T10:00:00Z"),
            _make_event("new-pre", played_date="2026-10-10", occurred_at="2026-10-10T10:00:00Z"),
            _make_event("new-old", played_date="2026-11-01", occurred_at="2026-11-01T10:00:00Z"),
            _make_event("new-future", played_date="2026-11-20", occurred_at="2026-11-20T10:00:00Z"),
            _make_event("new-unc", played_date=None, occurred_at=None),
            _make_event("new-rating", action="rated", rating=4.5, played_date="2020-01-01", occurred_at="2026-10-16T00:00:00Z"),
            _make_event("chain-1", played_date="2026-11-12", occurred_at="2026-11-12T10:00:00Z", event_ids=("chain-1", "chain-2")),
            _make_event("chain-2", played_date="2026-11-12", occurred_at="2026-11-12T10:00:00Z", event_ids=("chain-2", "pre-supp")),
        ]
        resolved = {"amb-old", "amb-ok", "del-1"}

        bulk_cog = FeedCog(SimpleNamespace(db=bulk_db), clock=lambda: self.now)
        adapter = _LegacySingleRowDbAdapter(legacy_db)
        legacy_cog = FeedCog(SimpleNamespace(db=adapter), clock=lambda: self.now)

        bulk_accepted = await bulk_cog._apply_discovery_policy(
            1, "millers", corpus, bulk_saved, self.now,
            activation_at=self.activation_at, resolved_event_ids=resolved,
        )
        legacy_accepted = await legacy_cog._apply_discovery_policy(
            1, "millers", corpus, legacy_saved, self.now,
            activation_at=self.activation_at, resolved_event_ids=resolved,
        )

        self.assertEqual(
            [e.activity_id for e in legacy_accepted],
            [e.activity_id for e in bulk_accepted],
        )
        self.assertEqual(
            [e.activity_id for e in legacy_accepted],
            ["base-1", "sent-1", "del-1", "pend-1", "unk-1", "amb-ok", "new-ok", "new-rating"],
        )

        all_ids = sorted({eid for ev in corpus for eid in ev.event_ids})
        bulk_decisions = await bulk_db.get_feed_event_decisions(1, "millers", all_ids)
        legacy_decisions = await legacy_db.get_feed_event_decisions(1, "millers", all_ids)
        self.assertEqual(set(legacy_decisions.keys()), set(bulk_decisions.keys()))
        for eid in bulk_decisions:
            self.assertEqual(
                legacy_decisions[eid]["decision"],
                bulk_decisions[eid]["decision"],
                f"mismatch for {eid}: legacy={legacy_decisions[eid]['decision']} bulk={bulk_decisions[eid]['decision']}",
            )
        # Protected del-1 must stay held_ambiguous (never overwritten!)
        self.assertEqual(legacy_decisions["del-1"]["decision"], "held_ambiguous")


if __name__ == "__main__":
    unittest.main()
