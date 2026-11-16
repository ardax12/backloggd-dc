"""Regression tests for Task 1: persisted alias indexing, deduplicated canonical history, and merge edits."""

import json
import os
import sqlite3
import tempfile
import unittest
from dataclasses import asdict, replace
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

os.environ["PYTHON_DOTENV_DISABLED"] = "1"
os.environ["TURSO_DATABASE_URL"] = ""
os.environ["TURSO_AUTH_TOKEN"] = ""
os.environ["DEV_GUILD_ID"] = ""

import discord
from backloggd import ActivityEntry, JournalEntry

from bot.cogs.feed import FeedCog
from bot.db import Database
from bot.feed_models import FeedEvent
from bot.feed_rules import journal_events


class _TrackingTursoClient:
    """Minimal Turso client double sharing transaction/row semantics with Database."""

    def __init__(self, sqlite_path: str):
        self.conn = sqlite3.connect(sqlite_path)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA foreign_keys = ON")

    def transaction(self):
        client = self

        class _Tx:
            async def execute(self, query, params=None):
                cursor = client.conn.execute(query, list(params or ()))
                rows = cursor.fetchall() if cursor.description else []
                return SimpleNamespace(rows=rows, rows_affected=cursor.rowcount)

            async def commit(self):
                client.conn.commit()

            async def rollback(self):
                client.conn.rollback()

        return _Tx()

    async def execute(self, query, params=None):
        cursor = self.conn.execute(query, list(params or ()))
        rows = cursor.fetchall() if cursor.description else []
        self.conn.commit()
        return SimpleNamespace(rows=rows, rows_affected=cursor.rowcount)

    async def batch(self, statements):
        results = []
        self.conn.execute("BEGIN IMMEDIATE")
        try:
            for query, params in statements:
                cursor = self.conn.execute(query, list(params or ()))
                rows = cursor.fetchall() if cursor.description else []
                results.append(SimpleNamespace(rows=rows, rows_affected=cursor.rowcount))
            self.conn.commit()
            return results
        except BaseException:
            self.conn.rollback()
            raise

    async def close(self):
        self.conn.close()


def _make_channel(channel_id: int = 100):
    sent = {"next_id": 900}
    partials = {}

    async def send_impl(**kwargs):
        sent["next_id"] += 1
        return SimpleNamespace(id=sent["next_id"], kwargs=kwargs)

    def get_partial(message_id):
        if message_id not in partials:
            partials[message_id] = SimpleNamespace(id=message_id, edit=AsyncMock())
        return partials[message_id]

    channel = MagicMock(spec=discord.TextChannel)
    channel.id = channel_id
    channel.send = AsyncMock(side_effect=send_impl)
    channel.get_partial_message = get_partial
    channel._partials = partials
    return channel


def _base_activity(
    *,
    activity_id: str = "act_1",
    action_type: str = "completed",
    action_text: str = "completed",
    rating: float | None = 3.0,
    review_notes: str | None = None,
    occurred_at: str | None = "2026-11-11T15:48:38Z",
    reference: str | None = "/u/Millers/review/10/",
    played_date: str | None = None,
) -> FeedEvent:
    return replace(
        FeedEvent.from_activity(
            ActivityEntry(
                activity_id=activity_id,
                game_title="Hades",
                game_slug="hades",
                cover_url=None,
                action_type=action_type,
                action_text=action_text,
                rating=rating,
                review_notes=review_notes,
                date_str="15:48",
                occurred_at=occurred_at,
                reference=reference,
            )
        ),
        played_date=played_date,
    )


class FeedAliasIdentityTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmpdir.cleanup)
        self.db_path = Path(self.tmpdir.name) / "alias_identity.db"
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

    async def _publish(self, sources, *, db=None, cog=None, channel=None, guild_id=1):
        target_db = db or self.db
        target_cog = cog or self.cog
        target_channel = channel or self.channel
        return await target_cog._publish_sources(
            guild_id, "millers", "Millers", None, target_channel, sources
        )

    async def test_merged_milestone_rating_and_review_alias_edits_original_post_once(self):
        await self._publish({"activity": [], "journal": []})
        act_1 = _base_activity(activity_id="act_1", action_type="completed", rating=3.0, reference="/u/Millers/log/10/")
        rated_1 = _base_activity(activity_id="rated_1", action_type="rated", action_text="rated", rating=3.0, reference="/u/Millers/log/10/")
        reviewed_1 = _base_activity(
            activity_id="reviewed_1", action_type="reviewed", action_text="reviewed",
            rating=None, review_notes="Initial thoughts", reference="/u/Millers/log/10/",
        )
        await self._publish({"activity": [act_1, rated_1, reviewed_1], "journal": []})
        self.channel.send.assert_awaited_once()
        msg_id = 901

        # Restart before poll 2; rating + review text both change on the secondary aliases.
        await self.db.close()
        await self.db.connect()
        rated_1_edited = replace(rated_1, rating=4.5)
        reviewed_1_edited = replace(reviewed_1, review_notes="Much better on replay!")
        ok = await self._publish({"activity": [act_1, rated_1_edited, reviewed_1_edited], "journal": []})

        self.assertTrue(ok)
        self.channel.send.assert_awaited_once()
        partial = self.channel._partials[msg_id]
        partial.edit.assert_awaited_once()
        edited_embed = partial.edit.call_args.kwargs["embed"]
        self.assertIn("4.5", edited_embed.description)
        self.assertIn("Much better on replay!", edited_embed.fields[0].value)

        for alias in ("act_1", "rated_1", "reviewed_1"):
            dec = await self.db.get_feed_event_decision(1, "millers", alias)
            self.assertNotEqual(dec["decision"], "held_ambiguous")

    async def test_journal_merged_into_activity_edits_original_post_after_second_session_for_same_game(self):
        await self._publish({"activity": [], "journal": []})
        act_1 = _base_activity(
            activity_id="act_1", action_type="completed", rating=4.0,
            occurred_at="2026-11-11T10:00:00Z", reference="/u/Millers/log/1/",
        )
        journal_1 = replace(
            journal_events([
                JournalEntry(
                    entry_id="j1", game_title="Hades", game_slug="hades",
                    played_date="November, 2026 11", platform="PC", rating=4.0,
                    review_notes="First clear", session_labels=("Finished",),
                )
            ], "Millers")[0],
        )
        await self._publish({"activity": [act_1], "journal": [journal_1]})
        self.channel.send.assert_awaited_once()
        first_msg_id = 901

        # Add a second real session for the same game (standalone journal session so
        # canonical journal history contains a prior session for hades).
        journal_2 = journal_events([
            JournalEntry(
                entry_id="j2", game_title="Hades", game_slug="hades",
                played_date="November, 2026 12", platform="PS5", rating=4.5,
                session_labels=("Started",), is_replay=True,
            )
        ], "Millers")[0]
        await self._publish({"activity": [act_1], "journal": [journal_1, journal_2]})
        self.assertEqual(self.channel.send.await_count, 2)
        second_msg_id = 902

        # Restart and simulate activity outage + recovery; journal_1 edits date, platform, and notes.
        await self.db.close()
        await self.db.connect()
        journal_1_edited = replace(
            journal_1,
            played_date="November, 2026 10",
            platform="Steam Deck",
            review_notes="Updated deck notes",
        )
        # Outage where activity is absent, only journal runs.
        ok = await self._publish({"journal": [journal_1_edited, journal_2]})
        self.assertTrue(ok)
        # Later recovery with activity present is a clean no-op without duplicates.
        ok2 = await self._publish({"activity": [act_1], "journal": [journal_1_edited, journal_2]})
        self.assertTrue(ok2)

        self.assertEqual(self.channel.send.await_count, 2)
        self.channel._partials[first_msg_id].edit.assert_awaited_once()
        if second_msg_id in self.channel._partials:
            self.channel._partials[second_msg_id].edit.assert_not_awaited()
        edited_embed = self.channel._partials[first_msg_id].edit.call_args.kwargs["embed"]
        self.assertIn("November, 2026 10", edited_embed.description)
        self.assertIn("Steam Deck", edited_embed.description)
        self.assertIn("Updated deck notes", edited_embed.fields[0].value)
        dec = await self.db.get_feed_event_decision(1, "millers", journal_1.activity_id)
        self.assertNotEqual(dec["decision"], "held_ambiguous")

    async def test_alias_rows_sharing_one_payload_count_as_one_identity_for_changed_alias_with_reference(self):
        await self._publish({"activity": [], "journal": []})
        act_1 = _base_activity(activity_id="act_1", action_type="completed", rating=3.0, reference="/u/Millers/log/99/")
        rated_1 = _base_activity(activity_id="rated_1", action_type="rated", action_text="rated", rating=3.0, reference="/u/Millers/log/99/")
        reviewed_1 = _base_activity(
            activity_id="reviewed_1", action_type="reviewed", action_text="reviewed",
            rating=None, review_notes="First draft", reference="/u/Millers/log/99/",
        )
        await self._publish({"activity": [act_1, rated_1, reviewed_1], "journal": []})
        self.channel.send.assert_awaited_once()

        # Poll 2: raw ID changes to reviewed_1_v2 with the same stable reference (only 1 incoming reference).
        # Because the 3 persisted alias rows represent ONE canonical identity, len(matches) == 1.
        reviewed_1_v2 = replace(reviewed_1, activity_id="reviewed_1_v2", review_notes="Revised draft")
        ok = await self._publish({"activity": [reviewed_1_v2], "journal": []})

        self.assertTrue(ok)
        self.channel.send.assert_awaited_once()
        self.channel._partials[901].edit.assert_awaited_once()
        self.assertIn("Revised draft", self.channel._partials[901].edit.call_args.kwargs["embed"].fields[0].value)
        rows = await self.db.get_feed_messages(1, "millers")
        self.assertEqual({r["event_id"] for r in rows}, {"act_1", "rated_1", "reviewed_1", "reviewed_1_v2"})
        self.assertEqual({r["message_id"] for r in rows}, {901})

    async def test_conservative_ambiguity_still_holds_unknown_references_and_duplicate_sessions(self):
        await self._publish({"activity": [], "journal": []})
        act_1 = _base_activity(activity_id="act_1", action_type="completed", rating=3.0, reference="/u/Millers/log/50/")
        rated_1 = _base_activity(activity_id="rated_1", action_type="rated", action_text="rated", rating=3.0, reference="/u/Millers/log/50/")
        await self._publish({"activity": [act_1, rated_1], "journal": []})
        self.channel.send.assert_awaited_once()

        # Known alias (act_1) still resolves even when an unknown alias shares the incoming reference;
        # the unknown alias itself must still be held by incoming_reference_counts.
        unknown_dup = replace(rated_1, activity_id="unknown_alias_50", rating=5.0)
        await self._publish({"activity": [act_1, unknown_dup], "journal": []})
        self.channel.send.assert_awaited_once()
        if 901 in self.channel._partials:
            self.channel._partials[901].edit.assert_not_awaited()
        self.assertEqual(
            (await self.db.get_feed_event_decision(1, "millers", "unknown_alias_50"))["decision"],
            "held_ambiguous",
        )

        # Two genuinely distinct same-game sessions with distinct stable references stay distinct.
        act_2 = _base_activity(
            activity_id="act_2", action_type="completed", rating=4.0,
            occurred_at="2026-11-12T08:00:00Z", reference="/u/Millers/log/51/",
        )
        await self._publish({"activity": [act_1, act_2], "journal": []})
        self.assertEqual(self.channel.send.await_count, 2)

        # Unknown session without stable reference is held when a prior same-game session exists.
        unreferenced_unknown = _base_activity(
            activity_id="unreferenced_unknown", action_type="completed", rating=4.0,
            occurred_at="2026-11-12T09:00:00Z", reference=None,
        )
        await self._publish({"activity": [unreferenced_unknown], "journal": []})
        self.assertEqual(self.channel.send.await_count, 2)
        self.assertEqual(
            (await self.db.get_feed_event_decision(1, "millers", "unreferenced_unknown"))["decision"],
            "held_ambiguous",
        )

    async def test_deleted_pending_unknown_and_policy_states_preserved_across_adapters(self):
        for adapter in ("sqlite", "turso"):
            with self.subTest(adapter=adapter), tempfile.TemporaryDirectory() as folder:
                db_path = str(Path(folder) / f"{adapter}.db")
                if adapter == "sqlite":
                    db = Database(db_path, turso_url="")
                    await db.connect()
                else:
                    seed = Database(db_path, turso_url="")
                    await seed.connect()
                    await seed.close()
                    db = Database(db_path, turso_url="https://alias-test.turso.io")
                    db._client = _TrackingTursoClient(db_path)

                try:
                    await db.link_user(10, "Millers")
                    channel = _make_channel(100)
                    cog = FeedCog(SimpleNamespace(db=db), clock=lambda: self.now)

                    await self._publish({"activity": [], "journal": []}, db=db, cog=cog, channel=channel)

                    # 1) Deleted post with merged act_del + rated_del never reposts when rated_del is edited.
                    act_del = _base_activity(activity_id="act_del", rating=3.0, reference="/u/Millers/log/del/")
                    rated_del = _base_activity(activity_id="rated_del", action_type="rated", action_text="rated", rating=3.0, reference="/u/Millers/log/del/")
                    await self._publish({"activity": [act_del, rated_del], "journal": []}, db=db, cog=cog, channel=channel)
                    self.assertEqual(channel.send.await_count, 1)

                    channel.get_partial_message(901).edit.side_effect = discord.NotFound(
                        SimpleNamespace(status=404, reason="Not Found"), {"message": "deleted"}
                    )
                    await self._publish({"activity": [act_del, replace(rated_del, rating=4.0)], "journal": []}, db=db, cog=cog, channel=channel)
                    channel.get_partial_message(901).edit.side_effect = None
                    await self._publish({"activity": [act_del, replace(rated_del, rating=4.5)], "journal": []}, db=db, cog=cog, channel=channel)
                    self.assertEqual(channel.send.await_count, 1)
                    del_rows = {r["event_id"]: r for r in await db.get_feed_messages(1, "millers")}
                    self.assertEqual(del_rows["act_del"]["delivery_state"], "deleted")
                    self.assertEqual(del_rows["rated_del"]["delivery_state"], "deleted")

                    # 2) Unknown delivery with merged act_unk + journal_unk stays unknown when journal edits.
                    act_unk = _base_activity(
                        activity_id="act_unk", action_type="completed", rating=4.0,
                        occurred_at="2026-11-12T10:00:00Z", reference="/u/Millers/log/unk/",
                    )
                    journal_unk = journal_events([
                        JournalEntry(
                            entry_id="junk", game_title="Hades", game_slug="hades",
                            played_date="November, 2026 12", platform="PC", rating=4.0,
                            session_labels=("Finished",),
                        )
                    ], "Millers")[0]
                    channel.send.side_effect = RuntimeError("lost response")
                    with self.assertRaises(RuntimeError):
                        await self._publish({"activity": [act_unk], "journal": [journal_unk]}, db=db, cog=cog, channel=channel)
                    channel.send.side_effect = None

                    # Edit journal_unk after restart -> stays unknown, never reposts, no held_ambiguous.
                    await self._publish(
                        {"activity": [act_unk], "journal": [replace(journal_unk, platform="PS5")]},
                        db=db, cog=cog, channel=channel,
                    )
                    self.assertEqual(channel.send.await_count, 2)
                    msg_rows = {r["event_id"]: r for r in await db.get_feed_messages(1, "millers")}
                    self.assertEqual(msg_rows["act_unk"]["delivery_state"], "unknown")
                    self.assertEqual(msg_rows[journal_unk.activity_id]["delivery_state"], "unknown")
                    j_dec = await db.get_feed_event_decision(1, "millers", journal_unk.activity_id)
                    self.assertEqual(j_dec["decision"], "eligible")

                    # 3) Pending delivery protection: a persisted 'pending' row across aliases must resolve
                    # cleanly via persisted_aliases and never re-send.
                    pending_event = _base_activity(
                        activity_id="act_pend", action_type="completed", rating=5.0,
                        occurred_at="2026-11-12T11:00:00Z", reference="/u/Millers/log/pend/",
                    )
                    rated_pend = _base_activity(
                        activity_id="rated_pend", action_type="rated", action_text="rated", rating=5.0,
                        occurred_at="2026-11-12T11:00:00Z", reference="/u/Millers/log/pend/",
                    )
                    pend_payload = json.dumps(asdict(replace(pending_event, event_ids=("act_pend", "rated_pend"))), sort_keys=True)
                    await db.begin_feed_delivery(1, "millers", ("act_pend", "rated_pend"), pend_payload, 100)
                    await self._publish({"activity": [pending_event, replace(rated_pend, rating=4.0)], "journal": []}, db=db, cog=cog, channel=channel)
                    self.assertEqual(channel.send.await_count, 2)
                    pend_dec = await db.get_feed_event_decision(1, "millers", "rated_pend")
                    self.assertTrue(pend_dec is None or pend_dec["decision"] != "held_ambiguous")
                    pend_rows = {r["event_id"]: r for r in await db.get_feed_messages(1, "millers")}
                    self.assertIn(pend_rows["act_pend"]["delivery_state"], {"pending", "unknown"})
                    self.assertIn(pend_rows["rated_pend"]["delivery_state"], {"pending", "unknown"})
                finally:
                    await db.close()
