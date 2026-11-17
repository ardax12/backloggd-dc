"""Task 10 combined integration story: continuity, shared-target gaps, cancellation/reload, and recovery."""

import os

os.environ["PYTHON_DOTENV_DISABLED"] = "1"
os.environ["TURSO_DATABASE_URL"] = ""
os.environ["TURSO_AUTH_TOKEN"] = ""
os.environ["DEV_GUILD_ID"] = ""

import asyncio
from datetime import datetime, timezone
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

import discord
from backloggd import ActivityEntry

from bot.cogs.account import AccountCog
from bot.cogs.feed import FeedCog
from bot.db import Database
from bot.services.library_sync import LibrarySyncService
from tests.test_interaction_visibility_protocol import StatefulDiscordInteraction


def _act(
    activity_id: str,
    *,
    slug: str = "hades",
    title: str = "Hades",
    action_type: str = "completed",
    rating: float | None = 4.5,
    occurred_at: str | None = None,
) -> ActivityEntry:
    return ActivityEntry(
        activity_id=activity_id,
        game_title=title,
        game_slug=slug,
        cover_url=None,
        action_type=action_type,
        action_text=f"{action_type} {title}",
        rating=rating,
        review_notes=None,
        date_str="10:00",
        occurred_at=occurred_at,
    )


def _make_channel(channel_id: int, guild_id: int, *, base_msg_id: int = 1000) -> MagicMock:
    channel = MagicMock(spec=discord.TextChannel)
    channel.id = channel_id
    channel.mention = f"<#{channel_id}>"
    channel.guild = SimpleNamespace(id=guild_id)
    channel.permissions_for.return_value = SimpleNamespace(
        view_channel=True, send_messages=True, embed_links=True
    )
    counter = {"n": base_msg_id}

    async def _default_send(**kwargs):
        counter["n"] += 1
        return SimpleNamespace(id=counter["n"], kwargs=kwargs)

    channel.send = AsyncMock(side_effect=_default_send)
    channel.get_partial_message = lambda mid: SimpleNamespace(id=mid, edit=AsyncMock())
    return channel


class FeedRecoveryIntegrationStoryTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmpdir.cleanup)
        self.db_path = Path(self.tmpdir.name) / "recovery_story.db"
        self.now = datetime(2026, 11, 10, 8, 0, tzinfo=timezone.utc)
        self._mono_origin = self.now
        orig_sleep = asyncio.sleep

        async def _fast_sleep(*_a, **_kw):
            await orig_sleep(0)

        self.sleep_patch = patch(
            "bot.cogs.feed.asyncio.sleep",
            new=AsyncMock(side_effect=_fast_sleep),
        )
        self.sleep_patch.start()
        self.addCleanup(self.sleep_patch.stop)

    def _make_interaction(self, *, guild, user_id: int, manage_channels: bool = True):
        return StatefulDiscordInteraction(
            name="feed",
            guild=guild,
            user_id=user_id,
            manage_channels=manage_channels,
        )

    async def _drain_library_syncs(self, library_sync: LibrarySyncService):
        tasks = [s.task for s in library_sync._profiles.values() if s.task]
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    async def test_combined_pre_addition_gap_shared_target_failure_cancel_reload_and_recovery(self):
        db = Database(str(self.db_path), turso_url="")
        await db.connect()
        self.addAsyncCleanup(db.close)
        library_sync = LibrarySyncService(
            clock=lambda: (self.now - self._mono_origin).total_seconds()
        )
        self.addAsyncCleanup(library_sync.close)
        ch1 = _make_channel(101, 1, base_msg_id=1000)
        ch2 = _make_channel(202, 2, base_msg_id=2000)

        g1_state = {"members": [10, 99], "chunked": True, "channel": ch1}
        g2_state = {"members": [30, 99], "chunked": True, "channel": ch2}

        # Dynamic guild views so chunked/members updates reflect live state across calls.
        class _DynamicGuild:
            def __init__(self, gid: int, state: dict):
                self.id = gid
                self.name = f"Guild {gid}"
                self.me = SimpleNamespace(id=999)
                self._state = state

            @property
            def chunked(self):
                return self._state["chunked"]

            @property
            def members(self):
                return [SimpleNamespace(id=mid, display_name=f"User{mid}") for mid in self._state["members"]]

            @property
            def member_count(self):
                return len(self._state["members"]) if self._state["chunked"] else None

            async def chunk(self, *, cache: bool = True):
                return self.members

            def get_channel(self, cid: int):
                ch = self._state["channel"]
                return ch if (ch is not None and cid == ch.id) else None

        guild1 = _DynamicGuild(1, g1_state)
        guild2 = _DynamicGuild(2, g2_state)
        guilds = {1: guild1, 2: guild2}

        snapshot_state = {
            "entries": [],
            "coverage": "exhausted",
            "pages_read": 1,
        }

        async def _get_snapshot(username, *, stop_at_ids=(), max_pages=5):
            return SimpleNamespace(
                entries=list(snapshot_state["entries"]),
                coverage=snapshot_state["coverage"],
                pages_read=snapshot_state["pages_read"],
            )

        api = SimpleNamespace(
            get_user_profile=AsyncMock(return_value=SimpleNamespace(username="Millers", avatar_url=None)),
            get_user_library=AsyncMock(return_value=[]),
            get_user_playing=AsyncMock(return_value=[]),
            get_user_backlog=AsyncMock(return_value=[]),
            get_user_wishlist=AsyncMock(return_value=[]),
            get_user_activity_snapshot=AsyncMock(side_effect=_get_snapshot),
            get_user_journal=AsyncMock(return_value=[]),
        )
        bot = SimpleNamespace(
            db=db,
            backloggd=api,
            library_sync=library_sync,
            get_guild=lambda gid: guilds.get(gid),
            guilds=[guild1, guild2],
        )
        account = AccountCog(bot)
        cog1 = FeedCog(bot, clock=lambda: self.now)
        self.addAsyncCleanup(cog1.close)

        # ------------------------------------------------------------------
        # Stage 1: Initial M1 baseline + archived sent announcement in G1
        # ------------------------------------------------------------------
        await FeedCog.setchannel.callback(cog1, self._make_interaction(guild=guild1, user_id=99), ch1)
        await AccountCog.link.callback(account, self._make_interaction(guild=guild1, user_id=10), "Millers")
        await self._drain_library_syncs(library_sync)

        ev_base = _act("act-0", slug="celeste", title="Celeste", occurred_at="2026-11-10T07:00:00Z")
        snapshot_state["entries"] = [ev_base]
        snapshot_state["coverage"] = "exhausted"
        await cog1.journal_feed_loop.coro(cog1)
        ch1.send.assert_not_awaited()

        self.now = datetime(2026, 11, 10, 9, 0, tzinfo=timezone.utc)
        ev_sent_m1 = _act("act-1", slug="hades", title="Hades", occurred_at="2026-11-10T08:30:00Z")
        snapshot_state["entries"] = [ev_sent_m1, ev_base]
        snapshot_state["coverage"] = "boundary_reached"
        await cog1.journal_feed_loop.coro(cog1)
        ch1.send.assert_awaited_once()
        self.assertIn("Hades", ch1.send.call_args.kwargs["embed"].title)

        # ------------------------------------------------------------------
        # Stage 2: M1 silently leaves; under incomplete chunk M2 enables;
        #          /feed status and /feed list stay read-only & honest;
        #          then complete snapshot proves gap -> resets baseline cleanly
        # ------------------------------------------------------------------
        g1_state["members"] = [20, 99]
        g1_state["chunked"] = False

        await AccountCog.link.callback(account, self._make_interaction(guild=None, user_id=20), "Millers")
        await self._drain_library_syncs(library_sync)
        await FeedCog.feed_on.callback(cog1, self._make_interaction(guild=guild1, user_id=20))

        status_inter = self._make_interaction(guild=guild1, user_id=20)
        await FeedCog.feed_status.callback(cog1, status_inter)
        status_desc = status_inter.visible_ephemeral_messages()[0]["embed"].description
        self.assertNotIn("**Effective state:** Active", status_desc)
        self.assertIn("Pending continuity verification", status_desc)
        self.assertIn("continuity unverified", status_desc.lower())

        g1_state["chunked"] = True
        list_inter = self._make_interaction(guild=guild1, user_id=99)
        await FeedCog.feed_list.callback(cog1, list_inter)
        list_desc = list_inter.visible_ephemeral_messages()[0]["view"].pages[0].description
        self.assertIn("Pending continuity verification", list_desc)
        self.assertNotIn("· Active", list_desc)
        list_inter.visible_ephemeral_messages()[0]["view"].stop()

        pending_reconcil = await db._fetchall(
            "SELECT candidate_member_ids FROM guild_profile_feed_reconciliation WHERE guild_id = 1 AND profile_key = 'millers'"
        )
        self.assertEqual(len(pending_reconcil), 1)

        self.now = datetime(2026, 11, 11, 10, 0, tzinfo=timezone.utc)
        ev_inactive_gap = _act("act-gap", slug="hollow-knight", title="Hollow Knight", occurred_at="2026-11-11T09:00:00Z")
        snapshot_state["entries"] = [ev_inactive_gap, ev_sent_m1, ev_base]
        snapshot_state["coverage"] = "exhausted"
        await cog1.journal_feed_loop.coro(cog1)

        self.assertEqual(ch1.send.await_count, 1)
        g1_msgs = {r["event_id"]: r for r in await db.get_feed_messages(1, "millers")}
        self.assertEqual(g1_msgs["act-1"]["delivery_state"], "sent")
        self.assertEqual(g1_msgs["act-gap"]["delivery_state"], "baseline")

        resolved_status_inter = self._make_interaction(guild=guild1, user_id=20)
        await FeedCog.feed_status.callback(cog1, resolved_status_inter)
        self.assertIn("**Effective state:** Active\n", resolved_status_inter.visible_ephemeral_messages()[0]["embed"].description + "\n")

        # ------------------------------------------------------------------
        # Stage 3: Enroll G2 (admin), G2 becomes unavailable while G1 is healthy;
        #          dated + undated entries deliver cleanly in G1 without held_uncertain;
        #          then cancel in-flight G1 send for act-3 mid-send
        # ------------------------------------------------------------------
        await AccountCog.link.callback(account, self._make_interaction(guild=None, user_id=30), "Millers")
        await self._drain_library_syncs(library_sync)
        await FeedCog.setchannel.callback(cog1, self._make_interaction(guild=guild2, user_id=99), ch2)
        await FeedCog.feed_add.callback(cog1, self._make_interaction(guild=guild2, user_id=99), "Millers")
        snapshot_state["coverage"] = "boundary_reached"
        await cog1.journal_feed_loop.coro(cog1)
        ch2.send.assert_not_awaited()

        g2_state["channel"] = None
        self.now = datetime(2026, 11, 12, 10, 0, tzinfo=timezone.utc)
        ev_shared_1 = _act("act-2", slug="balatro", title="Balatro", occurred_at="2026-11-12T09:30:00Z")
        ev_shared_undated = _act("act-u", slug="disco-elysium", title="Disco Elysium", action_type="rated", rating=5.0, occurred_at=None)
        snapshot_state["entries"] = [ev_shared_undated, ev_shared_1, ev_inactive_gap, ev_sent_m1, ev_base]
        snapshot_state["coverage"] = "boundary_reached"
        await cog1.journal_feed_loop.coro(cog1)

        # G1 received both Balatro and undated Disco Elysium; no cross-guild held_uncertain!
        self.assertEqual(ch1.send.await_count, 3)
        self.assertEqual((await db.get_feed_event_decision(1, "millers", "act-u"))["decision"], "eligible")

        # Verify status isolation right here: G1 clean Active, G2 shows Recipient gap
        g1_mid_status = await db.get_guild_profile_feed_status(1, "millers")
        g2_mid_status = await db.get_guild_profile_feed_status(2, "millers")
        self.assertIsNone(g1_mid_status["recipient_gap"])
        self.assertIsNotNone(g2_mid_status["recipient_gap"])

        # Now act-3 arrives while G2 is still unavailable; cancel G1 mid-send on act-3.
        self.now = datetime(2026, 11, 12, 10, 15, tzinfo=timezone.utc)
        ev_cancel = _act("act-3", slug="animal-well", title="Animal Well", occurred_at="2026-11-12T10:10:00Z")
        snapshot_state["entries"] = [ev_cancel, ev_shared_undated, ev_shared_1, ev_inactive_gap, ev_sent_m1, ev_base]
        snapshot_state["coverage"] = "boundary_reached"

        send_started = asyncio.Event()
        allow_unwind = asyncio.Event()

        async def _g1_blocked_send(**kwargs):
            send_started.set()
            try:
                await allow_unwind.wait()
            except asyncio.CancelledError:
                await allow_unwind.wait()
                raise

        ch1.send.side_effect = _g1_blocked_send
        poll_task = asyncio.create_task(cog1.journal_feed_loop.coro(cog1))
        await asyncio.wait_for(send_started.wait(), timeout=2.0)

        self.assertEqual(db._active_pending_deliveries, {(1, "millers", "act-3")})

        poll_task.cancel()
        allow_unwind.set()
        try:
            await poll_task
        except asyncio.CancelledError:
            pass
        await cog1.close()
        await library_sync.close()

        self.assertEqual(db._active_pending_deliveries, set())
        self.assertEqual(cog1._delivery_cleanup_tasks, set())
        self.assertIsNone(cog1._draining_task)

        g1_msgs_mid = {r["event_id"]: r for r in await db.get_feed_messages(1, "millers")}
        self.assertEqual(g1_msgs_mid["act-2"]["delivery_state"], "sent")
        self.assertEqual(g1_msgs_mid["act-u"]["delivery_state"], "sent")
        self.assertEqual(g1_msgs_mid["act-3"]["delivery_state"], "unknown")
        await db.close()

        # ------------------------------------------------------------------
        # Stage 4: Full DB + Cog restart; G2 stays unavailable 1 poll to verify
        #          no G1 duplicates; then act-2/u/3 leave the SDK page, G2 recovers
        #          and delivers missed items once, gap clears, boundary advances.
        # ------------------------------------------------------------------
        db2 = Database(str(self.db_path), turso_url="")
        await db2.connect()
        self.addAsyncCleanup(db2.close)
        library_sync2 = LibrarySyncService(
            clock=lambda: (self.now - self._mono_origin).total_seconds()
        )
        self.addAsyncCleanup(library_sync2.close)

        ch1_reloaded = _make_channel(101, 1, base_msg_id=1100)
        g1_state["channel"] = ch1_reloaded
        bot2 = SimpleNamespace(
            db=db2,
            backloggd=api,
            library_sync=library_sync2,
            get_guild=lambda gid: guilds.get(gid),
            guilds=[guild1, guild2],
        )
        cog2 = FeedCog(bot2, clock=lambda: self.now)
        self.addAsyncCleanup(cog2.close)

        await cog2.journal_feed_loop.coro(cog2)
        ch1_reloaded.send.assert_not_awaited()
        ch2.send.assert_not_awaited()

        st_g1 = self._make_interaction(guild=guild1, user_id=20)
        await FeedCog.feed_status.callback(cog2, st_g1)
        self.assertNotIn("Recipient gap:", st_g1.visible_ephemeral_messages()[0]["embed"].fields[0].value)

        st_g2 = self._make_interaction(guild=guild2, user_id=30)
        await FeedCog.feed_status.callback(cog2, st_g2)
        self.assertIn("Recipient gap:", st_g2.visible_ephemeral_messages()[0]["embed"].fields[0].value)
        unavail_list_g2 = self._make_interaction(guild=guild2, user_id=99)
        await FeedCog.feed_list.callback(cog2, unavail_list_g2)
        unavail_list_view = unavail_list_g2.visible_ephemeral_messages()[0]["view"]
        self.assertIn("Unavailable", unavail_list_view.pages[0].description)
        unavail_list_view.stop()

        g2_state["channel"] = ch2
        self.now = datetime(2026, 11, 12, 12, 5, tzinfo=timezone.utc)
        ev_latest = _act("act-4", slug="metaphor", title="Metaphor", occurred_at="2026-11-12T12:00:00Z")
        snapshot_state["entries"] = [ev_latest]
        snapshot_state["coverage"] = "limited"
        await cog2.journal_feed_loop.coro(cog2)

        # G1 sends ONLY act-4 (act-2/act-u sent, act-3 unknown).
        g1_titles = [call.kwargs["embed"].title for call in ch1_reloaded.send.call_args_list]
        self.assertEqual(len(g1_titles), 1)
        self.assertIn("Metaphor", g1_titles[0])

        # G2 recovers missed act-u, act-2, act-3 AND delivers act-4 ONCE each (no duplicates)!
        g2_titles = [call.kwargs["embed"].title for call in ch2.send.call_args_list]
        self.assertEqual(len(g2_titles), 4)
        self.assertTrue(any("Disco Elysium" in t for t in g2_titles))
        self.assertTrue(any("Balatro" in t for t in g2_titles))
        self.assertTrue(any("Animal Well" in t for t in g2_titles))
        self.assertTrue(any("Metaphor" in t for t in g2_titles))

        g2_status_after = await db2.get_guild_profile_feed_status(2, "millers")
        self.assertIsNone(g2_status_after["recipient_gap"])

        # Final healthy boundary-reaching poll advances shared boundary & shows Active in both guilds
        snapshot_state["entries"] = [ev_latest]
        snapshot_state["coverage"] = "exhausted"
        await cog2.journal_feed_loop.coro(cog2)
        self.assertEqual(len(ch1_reloaded.send.call_args_list), 1)
        self.assertEqual(len(ch2.send.call_args_list), 4)

        health_after = await db2.get_profile_feed_source("millers", "activity")
        self.assertEqual(health_after["last_continuous_boundary_event_id"], "act-4")
        self.assertIsNone(health_after["unresolved_gap_since"])

        # Final /feed status & /feed list confirm truthful Active states without recipient gaps
        final_st_g1 = self._make_interaction(guild=guild1, user_id=20)
        await FeedCog.feed_status.callback(cog2, final_st_g1)
        g1_final_embed = final_st_g1.visible_ephemeral_messages()[0]["embed"]
        self.assertIn("**Effective state:** Active\n", g1_final_embed.description + "\n")
        self.assertIn("Unknown send outcomes: 1", g1_final_embed.fields[1].value)

        final_st_g2 = self._make_interaction(guild=guild2, user_id=30)
        await FeedCog.feed_status.callback(cog2, final_st_g2)
        g2_final_embed = final_st_g2.visible_ephemeral_messages()[0]["embed"]
        self.assertIn("**Effective state:** Active\n", g2_final_embed.description + "\n")
        self.assertNotIn("Recipient gap:", g2_final_embed.fields[0].value)

        final_list_g2 = self._make_interaction(guild=guild2, user_id=99)
        await FeedCog.feed_list.callback(cog2, final_list_g2)
        g2_list_view = final_list_g2.visible_ephemeral_messages()[0]["view"]
        self.assertIn("**Millers** — Admin · Active", g2_list_view.pages[0].description)
        g2_list_view.stop()

        # Clean cog teardown leaks zero tasks or active pending ownership
        await cog2.close()
        self.assertEqual(db2._active_pending_deliveries, set())
        self.assertEqual(cog2._delivery_cleanup_tasks, set())
        self.assertIsNone(cog2._draining_task)


if __name__ == "__main__":
    unittest.main()
