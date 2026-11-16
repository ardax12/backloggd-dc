import os
os.environ["PYTHON_DOTENV_DISABLED"] = "1"

import json
import tempfile
import unittest
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import discord
from backloggd import ActivityEntry

from bot.cogs.feed import FeedCog
from bot.db import Database
from bot.feed_models import FeedEvent


class FeedPreferenceTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.db = Database(str(Path(self.directory.name) / "preferences.db"), turso_url="")
        await self.db.connect()
        await self.db.link_user(10, "Millers")
        await self.db.link_user(11, "millers")
        await self.db.link_user(20, "PlayerB")

    async def asyncTearDown(self):
        await self.db.close()
        self.directory.cleanup()

    async def test_preferences_are_server_local_and_profiles_are_deduplicated(self):
        self.assertFalse(await self.db.get_guild_feed_preference(1, 10))
        await self.db.set_guild_feed_preference(1, 10, True)
        await self.db.set_guild_feed_preference(1, 11, True)
        await self.db.set_guild_feed_preference(2, 20, True)

        guild_one = await self.db.get_guild_feed_profiles(1, [10, 11, 20])
        guild_two = await self.db.get_guild_feed_profiles(2, [10, 11, 20])
        self.assertEqual([row["profile_key"] for row in guild_one], ["millers"])
        self.assertEqual([row["profile_key"] for row in guild_two], ["playerb"])

    async def test_legacy_reconciliation_only_enrolls_snapshot_scope_and_respects_off(self):
        await self.db.set_feed_channel(1, 100)
        await self.db._execute("DELETE FROM schema_migrations WHERE version >= 3")
        from bot.db_migrations import run_migrations
        await run_migrations(self.db)
        await self.db.link_user(30, "LaterLink")
        self.assertEqual(await self.db.get_legacy_feed_guilds_pending(), [1])
        await self.db.set_guild_feed_preference(1, 10, False)

        self.assertTrue(await self.db.complete_legacy_feed_migration(1, [10, 20, 30, 999]))
        self.assertFalse(await self.db.complete_legacy_feed_migration(1, [10, 20, 30]))
        self.assertFalse(await self.db.get_guild_feed_preference(1, 10))
        self.assertTrue(await self.db.get_guild_feed_preference(1, 20))
        self.assertFalse(await self.db.get_guild_feed_preference(1, 30))
        self.assertEqual(await self.db.get_legacy_feed_guilds_pending(), [])

    async def test_resume_resets_source_baseline_only_on_inactive_transition(self):
        await self.db.seed_feed_source(1, "millers", "activity", ["old"])
        await self.db.seed_feed_message(1, "millers", ["old"], '{"id":"old"}')
        first = await self.db.reconcile_guild_feed_activity(1, ["millers"])
        self.assertEqual(first["activated"], ["millers"])
        self.assertEqual(await self.db._fetchall("SELECT * FROM profile_feed_sources WHERE guild_id = 1"), [])

        await self.db.seed_feed_source(1, "millers", "activity", ["current"])
        still_active = await self.db.reconcile_guild_feed_activity(1, ["millers"])
        self.assertEqual(still_active["activated"], [])
        self.assertEqual(len(await self.db._fetchall("SELECT * FROM profile_feed_sources WHERE guild_id = 1")), 1)

        paused = await self.db.reconcile_guild_feed_activity(1, [])
        self.assertEqual(paused["deactivated"], ["millers"])
        resumed = await self.db.reconcile_guild_feed_activity(1, ["millers"])
        self.assertEqual(resumed["activated"], ["millers"])
        self.assertEqual(await self.db._fetchall("SELECT * FROM profile_feed_sources WHERE guild_id = 1"), [])
        self.assertIsNotNone(await self.db._fetchone(
            "SELECT 1 FROM profile_feed_messages WHERE guild_id = 1 AND profile_key = 'millers' AND event_id = 'old'"
        ))

    async def test_unset_channel_pauses_without_erasing_archive_mappings(self):
        await self.db.set_feed_channel(1, 100)
        await self.db.reconcile_guild_feed_activity(1, ["millers"])
        await self.db.seed_feed_source(1, "millers", "activity", ["old"])
        await self.db.seed_feed_message(1, "millers", ["old"], '{"id":"old"}')

        self.assertTrue(await self.db.unset_feed_channel(1))
        self.assertEqual(await self.db._fetchall("SELECT * FROM profile_feed_sources WHERE guild_id = 1"), [])
        state = await self.db._fetchone(
            "SELECT active FROM guild_profile_feed_activity WHERE guild_id = 1 AND profile_key = 'millers'"
        )
        self.assertFalse(state["active"])
        self.assertIsNotNone(await self.db._fetchone(
            "SELECT 1 FROM profile_feed_messages WHERE event_id = 'old'"
        ))

    def interaction(self, member_id, guild_id=1, members=None):
        return SimpleNamespace(
            user=SimpleNamespace(id=member_id),
            guild_id=guild_id,
            guild=SimpleNamespace(
                id=guild_id,
                chunked=True,
                members=[SimpleNamespace(id=value) for value in (members or [member_id])],
            ),
            response=SimpleNamespace(defer=AsyncMock(), send_message=AsyncMock()),
            followup=SimpleNamespace(send=AsyncMock()),
        )

    async def test_feed_on_requires_link_and_does_not_request_a_username(self):
        interaction = self.interaction(999)
        cog = FeedCog(SimpleNamespace(db=self.db))

        await FeedCog.feed_on.callback(cog, interaction)

        self.assertIn("/link", interaction.followup.send.call_args.args[0])
        self.assertFalse(await self.db.get_guild_feed_preference(1, 999))

    async def test_feed_on_is_local_and_passive_members_are_not_candidates(self):
        cog = FeedCog(SimpleNamespace(db=self.db))
        interaction = self.interaction(10, members=[10, 11, 20])

        await FeedCog.feed_on.callback(cog, interaction)

        self.assertTrue(await self.db.get_guild_feed_preference(1, 10))
        self.assertFalse(await self.db.get_guild_feed_preference(2, 10))
        self.assertEqual(
            [row["profile_key"] for row in await self.db.get_guild_feed_profiles(1, [10, 11, 20])],
            ["millers"],
        )
        self.assertEqual(await self.db.get_guild_feed_profiles(2, [10, 11, 20]), [])

    async def test_opt_out_of_one_duplicate_keeps_shared_profile_active(self):
        await self.db.set_guild_feed_preference(1, 10, True)
        await self.db.set_guild_feed_preference(1, 11, True)
        interaction = self.interaction(10, members=[10, 11])
        cog = FeedCog(SimpleNamespace(db=self.db))

        await FeedCog.feed_off.callback(cog, interaction)

        self.assertFalse(await self.db.get_guild_feed_preference(1, 10))
        self.assertTrue(await self.db.get_guild_feed_preference(1, 11))
        self.assertEqual(
            [row["profile_key"] for row in await self.db.get_guild_feed_profiles(1, [10, 11])],
            ["millers"],
        )
        self.assertIn("another linked member", interaction.followup.send.call_args.args[0])

    async def test_incomplete_legacy_member_snapshot_stays_pending(self):
        await self.db.set_feed_channel(1, 100)
        await self.db._execute("DELETE FROM schema_migrations WHERE version >= 3")
        from bot.db_migrations import run_migrations
        await run_migrations(self.db)
        self.assertEqual(await self.db.get_legacy_feed_guilds_pending(), [1])
        guild = SimpleNamespace(
            id=1, chunked=False, members=[], chunk=AsyncMock(return_value=[])
        )
        cog = FeedCog(SimpleNamespace(db=self.db))
        self.assertIsNone(await cog._authoritative_member_ids(guild))
        self.assertEqual(await self.db.get_legacy_feed_guilds_pending(), [1])

    async def test_unlink_before_poll_resets_baseline_only_when_no_other_source_remains(self):
        await self.db.set_feed_channel(1, 100)
        await self.db.set_guild_feed_preference(1, 10, True)
        await self.db.set_guild_feed_preference(1, 11, True)
        await self.db.reconcile_guild_feed_activity(1, ["millers"])
        await self.db.seed_feed_source(1, "millers", "activity-display-v1", ["old"])

        # Removing one duplicate link preserves active status and source baseline.
        await self.db.unlink_user(10)
        self.assertTrue(await self.db.has_feed_source(1, "millers", "activity-display-v1"))

        # Removing the last source (member 11's link) deactivates immediately and clears baseline,
        # even if member 11 relinks and re-enables before the next poll.
        await self.db.unlink_user(11)
        self.assertFalse(await self.db.has_feed_source(1, "millers", "activity-display-v1"))
        row = await self.db._fetchone(
            "SELECT active FROM guild_profile_feed_activity WHERE guild_id = 1 AND profile_key = 'millers'"
        )
        self.assertEqual(row["active"], 0)

        await self.db.link_user(11, "Millers")
        await self.db.set_guild_feed_preference(1, 11, True)
        self.assertFalse(await self.db.has_feed_source(1, "millers", "activity-display-v1"))

    async def test_legacy_unlink_and_relink_to_different_profile_does_not_grandfather(self):
        await self.db.set_feed_channel(1, 100)
        await self.db._execute("DELETE FROM schema_migrations WHERE version >= 3")
        from bot.db_migrations import run_migrations
        await run_migrations(self.db)
        self.assertEqual(await self.db.get_legacy_feed_guilds_pending(), [1])

        await self.db.unlink_user(10)
        await self.db.link_user(10, "BrandNewProfile")

        self.assertTrue(await self.db.complete_legacy_feed_migration(1, [10, 20]))
        self.assertFalse(await self.db.get_guild_feed_preference(1, 10))
        self.assertTrue(await self.db.get_guild_feed_preference(1, 20))

    @staticmethod
    def _make_channel(channel_id: int = 100):
        channel = MagicMock(spec=discord.TextChannel)
        channel.id = channel_id
        channel.mention = f"<#{channel_id}>"
        channel.permissions_for.return_value = SimpleNamespace(
            view_channel=True, send_messages=True, embed_links=True
        )
        counter = {"n": 0}

        async def _send(**kwargs):
            counter["n"] += 1
            return SimpleNamespace(id=5000 + counter["n"], kwargs=kwargs)

        channel.send = AsyncMock(side_effect=_send)
        channel.get_partial_message = lambda mid: SimpleNamespace(id=mid, edit=AsyncMock())
        return channel

    @staticmethod
    def _activity(event_id: str, slug: str, title: str, occurred_at: str) -> ActivityEntry:
        return ActivityEntry(
            activity_id=event_id,
            game_title=title,
            game_slug=slug,
            cover_url=None,
            action_type="completed",
            action_text="completed",
            rating=4.5,
            review_notes=None,
            date_str="11:30",
            occurred_at=occurred_at,
        )

    async def test_sole_member_off_under_chunk_failure_deactivates_and_re_on_after_restart_baselines_gap(self):
        await self.db.unlink_user(11)  # member 10 is sole source for millers
        ch1 = self._make_channel(100)
        ch2 = self._make_channel(200)
        await self.db.set_feed_channel(1, 100)
        await self.db.set_feed_channel(2, 200)
        await self.db.set_guild_feed_preference(1, 10, True)
        await self.db.set_guild_feed_preference(1, 20, True)  # playerb in guild 1
        await self.db.set_guild_feed_preference(2, 10, True)  # millers in guild 2
        t0 = datetime(2026, 11, 13, 8, 0, tzinfo=timezone.utc)
        await self.db.reconcile_guild_feed_activity(1, ["millers", "playerb"], now=t0)
        await self.db.reconcile_guild_feed_activity(2, ["millers"], now=t0)
        await self.db.seed_feed_source(1, "millers", "activity-display-v1", ["old-1"])
        await self.db.seed_feed_source(1, "millers", "journal-display-v1", [])
        await self.db.seed_feed_source(1, "playerb", "activity-display-v1", [])
        await self.db.seed_feed_source(2, "millers", "activity-display-v1", [])

        old_ev = self._activity("old-1", "old-game", "Old Game", "2026-11-13T07:00:00Z")
        old_payload = json.dumps(asdict(FeedEvent.from_activity(old_ev)), sort_keys=True)
        await self.db.store_feed_message(1, "millers", ("old-1",), old_payload, 500, 100, delivery_state="sent")
        await self.db.store_feed_message(1, "millers", ("del-1",), old_payload, 501, 100, delivery_state="deleted")
        await self.db.store_feed_message(1, "millers", ("pend-1",), old_payload, None, 100, delivery_state="pending")
        await self.db.store_feed_message(1, "millers", ("unk-1",), old_payload, None, 100, delivery_state="unknown")
        await self.db.record_feed_event_decision(
            1, "millers", "unsent-1", source="activity", event_date="2026-11-13",
            decision="eligible", reason="recent", payload=old_payload, observed_at=t0,
        )

        # Guild 1 loses chunking
        broken_guild1 = SimpleNamespace(
            id=1, name="G1", me=object(), chunked=False, members=[], chunk=AsyncMock(return_value=[]),
            get_channel=lambda cid: ch1 if cid == 100 else None,
        )
        off_inter = SimpleNamespace(
            user=SimpleNamespace(id=10), guild_id=1, guild=broken_guild1,
            response=SimpleNamespace(defer=AsyncMock(), send_message=AsyncMock()),
            followup=SimpleNamespace(send=AsyncMock()),
        )
        cog = FeedCog(SimpleNamespace(db=self.db), clock=lambda: datetime(2026, 11, 13, 9, 0, tzinfo=timezone.utc))
        await FeedCog.feed_off.callback(cog, off_inter)

        # Sole source gone -> deactivated & unsent state cleared immediately; archives + other guild/profile untouched
        status1 = await self.db.get_guild_profile_feed_status(1, "millers")
        self.assertFalse(status1["active"])
        self.assertEqual(status1["initialized_sources"], [])
        self.assertIsNone(await self.db.get_feed_event_decision(1, "millers", "unsent-1"))
        rows = await self.db._fetchall(
            "SELECT event_id, delivery_state FROM profile_feed_messages WHERE guild_id = 1 AND profile_key = 'millers'"
        )
        msgs = {r["event_id"]: r for r in rows}
        self.assertEqual(msgs["old-1"]["delivery_state"], "sent")
        self.assertEqual(msgs["del-1"]["delivery_state"], "deleted")
        self.assertEqual(msgs["pend-1"]["delivery_state"], "pending")
        self.assertEqual(msgs["unk-1"]["delivery_state"], "unknown")
        self.assertTrue(await self.db.has_feed_source(1, "playerb", "activity-display-v1"))
        self.assertTrue(await self.db.has_feed_source(2, "millers", "activity-display-v1"))

        # Restart before reenrollment
        await self.db.close()
        restarted_db = Database(str(Path(self.directory.name) / "preferences.db"), turso_url="")
        await restarted_db.connect()
        self.db = restarted_db

        on_inter = SimpleNamespace(
            user=SimpleNamespace(id=10), guild_id=1, guild=broken_guild1,
            response=SimpleNamespace(defer=AsyncMock(), send_message=AsyncMock()),
            followup=SimpleNamespace(send=AsyncMock()),
        )
        cog2 = FeedCog(SimpleNamespace(db=self.db), clock=lambda: datetime(2026, 11, 13, 11, 0, tzinfo=timezone.utc))
        await FeedCog.feed_on.callback(cog2, on_inter)

        # Chunking recovers at 12:00: unseen gap event becomes baseline rather than announced!
        gap_ev = self._activity("gap-1", "hades", "Hades", "2026-11-13T10:00:00Z")
        ok_guild1 = SimpleNamespace(
            id=1, name="G1", me=object(), chunked=True,
            members=[SimpleNamespace(id=10), SimpleNamespace(id=20)],
            get_channel=lambda cid: ch1 if cid == 100 else None,
        )
        ok_guild2 = SimpleNamespace(
            id=2, name="G2", me=object(), chunked=True,
            members=[SimpleNamespace(id=10)],
            get_channel=lambda cid: ch2 if cid == 200 else None,
        )
        millers_entries = [gap_ev, old_ev]

        async def snapshot_for_user(username, **_kwargs):
            if username.lower() == "millers":
                return SimpleNamespace(entries=millers_entries, coverage="exhausted", pages_read=1)
            return SimpleNamespace(entries=[], coverage="exhausted", pages_read=1)

        api = SimpleNamespace(
            get_user_activity_snapshot=AsyncMock(side_effect=snapshot_for_user),
            get_user_journal=AsyncMock(return_value=[]),
        )
        loop_cog = FeedCog(
            SimpleNamespace(db=self.db, backloggd=api, get_guild=lambda gid: ok_guild1 if gid == 1 else ok_guild2),
            clock=lambda: datetime(2026, 11, 13, 12, 0, tzinfo=timezone.utc),
        )
        with patch("bot.cogs.feed.asyncio.sleep", new=AsyncMock()):
            await loop_cog.journal_feed_loop.coro(loop_cog)

        ch1.send.assert_not_awaited()
        gap_dec = await self.db.get_feed_event_decision(1, "millers", "gap-1")
        self.assertEqual(gap_dec["decision"], "baseline")

        # Subsequent truly new event after rejoin IS announced!
        new_ev = self._activity("after-1", "celeste", "Celeste", "2026-11-13T12:05:00Z")
        millers_entries = [new_ev, gap_ev, old_ev]
        loop_cog.clock = lambda: datetime(2026, 11, 13, 12, 10, tzinfo=timezone.utc)
        with patch("bot.cogs.feed.asyncio.sleep", new=AsyncMock()):
            await loop_cog.journal_feed_loop.coro(loop_cog)
        ch1.send.assert_awaited_once()
        self.assertIn("Celeste", ch1.send.call_args.kwargs["embed"].title)

    async def test_off_under_chunk_failure_with_other_stored_member_retains_uncertainty_and_resolves_per_presence(self):
        for other_present in (True, False):
            with self.subTest(other_present=other_present), tempfile.TemporaryDirectory() as folder:
                db = Database(str(Path(folder) / "reconcile.db"), turso_url="")
                await db.connect()
                try:
                    await db.link_user(10, "Millers")
                    await db.link_user(11, "Millers")
                    ch = self._make_channel(100)
                    await db.set_feed_channel(1, 100)
                    await db.set_guild_feed_preference(1, 10, True)
                    await db.set_guild_feed_preference(1, 11, True)
                    t0 = datetime(2026, 11, 13, 8, 0, tzinfo=timezone.utc)
                    await db.reconcile_guild_feed_activity(1, ["millers"], now=t0)
                    await db.seed_feed_source(1, "millers", "activity-display-v1", ["old-1"])
                    await db.seed_feed_source(1, "millers", "journal-display-v1", [])
                    old_ev = self._activity("old-1", "old-game", "Old Game", "2026-11-13T07:00:00Z")
                    old_payload = json.dumps(asdict(FeedEvent.from_activity(old_ev)), sort_keys=True)
                    await db.seed_feed_message(1, "millers", ("old-1",), old_payload)

                    broken_guild = SimpleNamespace(
                        id=1, name="G1", me=object(), chunked=False, members=[], chunk=AsyncMock(return_value=[]),
                        get_channel=lambda cid: ch if cid == 100 else None,
                    )
                    off_inter = SimpleNamespace(
                        user=SimpleNamespace(id=10), guild_id=1, guild=broken_guild,
                        response=SimpleNamespace(defer=AsyncMock(), send_message=AsyncMock()),
                        followup=SimpleNamespace(send=AsyncMock()),
                    )
                    cog = FeedCog(SimpleNamespace(db=db), clock=lambda: datetime(2026, 11, 13, 9, 0, tzinfo=timezone.utc))
                    await FeedCog.feed_off.callback(cog, off_inter)

                    # Uncertainty retained because member 11 exists in DB!
                    self.assertTrue((await db.get_guild_profile_feed_status(1, "millers"))["active"])
                    self.assertTrue(await db.has_feed_source(1, "millers", "activity-display-v1"))

                    # Event occurs during outage; poll runs with deliver=False; user 10 also re-enables before chunk recovers!
                    gap_ev = self._activity("gap-ev", "hades", "Hades", "2026-11-13T10:00:00Z")
                    api = SimpleNamespace(
                        get_user_activity_snapshot=AsyncMock(
                            return_value=SimpleNamespace(entries=[gap_ev, old_ev], coverage="exhausted", pages_read=1)
                        ),
                        get_user_journal=AsyncMock(return_value=[]),
                    )
                    bot = SimpleNamespace(db=db, backloggd=api, get_guild=lambda gid: broken_guild if gid == 1 else None)
                    loop_cog = FeedCog(bot, clock=lambda: datetime(2026, 11, 13, 10, 30, tzinfo=timezone.utc))
                    with patch("bot.cogs.feed.asyncio.sleep", new=AsyncMock()):
                        await loop_cog.journal_feed_loop.coro(loop_cog)
                    ch.send.assert_not_awaited()

                    on_inter = SimpleNamespace(
                        user=SimpleNamespace(id=10), guild_id=1, guild=broken_guild,
                        response=SimpleNamespace(defer=AsyncMock(), send_message=AsyncMock()),
                        followup=SimpleNamespace(send=AsyncMock()),
                    )
                    await FeedCog.feed_on.callback(cog, on_inter)

                    # Restart then recover with authoritative member snapshot
                    await db.close()
                    db = Database(str(Path(folder) / "reconcile.db"), turso_url="")
                    await db.connect()

                    present = [10, 11] if other_present else [10]
                    ok_guild = SimpleNamespace(
                        id=1, name="G1", me=object(), chunked=True,
                        members=[SimpleNamespace(id=m) for m in present],
                        get_channel=lambda cid: ch if cid == 100 else None,
                    )
                    recovered_cog = FeedCog(
                        SimpleNamespace(db=db, backloggd=api, get_guild=lambda gid: ok_guild if gid == 1 else None),
                        clock=lambda: datetime(2026, 11, 13, 12, 0, tzinfo=timezone.utc),
                    )
                    with patch("bot.cogs.feed.asyncio.sleep", new=AsyncMock()):
                        await recovered_cog.journal_feed_loop.coro(recovered_cog)

                    if other_present:
                        # Member 11 stayed present continuously -> gap-ev IS announced!
                        ch.send.assert_awaited_once()
                        self.assertIn("Hades", ch.send.call_args.kwargs["embed"].title)
                    else:
                        # Member 11 had departed -> gap-ev happened during unbacked interval -> baseline, NOT announced!
                        ch.send.assert_not_awaited()
                        gap_dec = await db.get_feed_event_decision(1, "millers", "gap-ev")
                        self.assertEqual(gap_dec["decision"], "baseline")
                finally:
                    await db.close()



if __name__ == "__main__":
    unittest.main()
