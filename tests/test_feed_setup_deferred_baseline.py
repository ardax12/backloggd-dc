"""Regression tests for prompt /setchannel and deferred per-source baseline initialization."""

import asyncio
import os
import tempfile
import unittest
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

os.environ["PYTHON_DOTENV_DISABLED"] = "1"
os.environ["TURSO_DATABASE_URL"] = ""
os.environ["TURSO_AUTH_TOKEN"] = ""
os.environ["DEV_GUILD_ID"] = ""

from bot.cogs.feed import FeedCog
from bot.db import Database
from bot.feed_models import FeedEvent
from bot.services.library_sync import LibrarySyncService


def _make_interaction(guild, *, user_id=10):
    return SimpleNamespace(
        guild=guild,
        guild_id=guild.id,
        user=SimpleNamespace(id=user_id),
        response=SimpleNamespace(defer=AsyncMock(), is_done=lambda: True),
        followup=SimpleNamespace(send=AsyncMock()),
    )


def _make_channel(
    channel_id=100,
    mention="#feed",
    *,
    view_channel=True,
    send_messages=True,
    embed_links=True,
):
    sent = []

    async def send(*, embed=None, view=None):
        msg_id = 500 + len(sent)
        sent.append((msg_id, embed, view))
        return SimpleNamespace(id=msg_id)

    def permissions_for(me):
        if me is None:
            raise AssertionError("permissions_for must not be invoked with None")
        return SimpleNamespace(
            view_channel=view_channel,
            send_messages=send_messages,
            embed_links=embed_links,
        )

    partial = MagicMock()
    partial.edit = AsyncMock()
    return SimpleNamespace(
        id=channel_id,
        mention=mention,
        permissions_for=MagicMock(side_effect=permissions_for),
        send=AsyncMock(side_effect=send),
        get_partial_message=MagicMock(return_value=partial),
        _sent=sent,
    )


class PromptSetChannelAndDeferredBaselineTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmpdir.cleanup)
        self.now = datetime(2026, 11, 13, 12, 0, tzinfo=timezone.utc)
        self.db = Database(str(Path(self.tmpdir.name) / "feed-setup.db"), turso_url="")
        self.addAsyncCleanup(self.db.close)
        await self.db.connect()
        self.sync = LibrarySyncService()
        self.addAsyncCleanup(self.sync.close)

        await self.db.link_user(10, "Millers")
        await self.db.set_guild_feed_preference(1, 10, True)

        self.guild = SimpleNamespace(
            id=1,
            name="Guild",
            chunked=True,
            member_count=1,
            members=[SimpleNamespace(id=10)],
            me=SimpleNamespace(id=999),
        )
        self.channel = _make_channel(100)
        self.guild.get_channel = lambda cid: self.channel if cid == self.channel.id else None

        self.bot = SimpleNamespace(
            db=self.db,
            library_sync=self.sync,
            backloggd=SimpleNamespace(
                get_user_activity_snapshot=AsyncMock(),
                get_user_journal=AsyncMock(),
            ),
            get_guild=lambda gid: self.guild if gid == 1 else None,
        )
        self.cog = FeedCog(self.bot, clock=lambda: self.now)

    async def test_setchannel_responds_promptly_when_sources_block_and_baselines_on_first_poll(self):
        release_fetch = asyncio.Event()

        async def blocked_activity(username, **kwargs):
            await release_fetch.wait()
            return SimpleNamespace(entries=[], coverage="exhausted", pages_read=1)

        async def blocked_journal(username, **kwargs):
            await release_fetch.wait()
            return []

        self.bot.backloggd.get_user_activity_snapshot = AsyncMock(side_effect=blocked_activity)
        self.bot.backloggd.get_user_journal = AsyncMock(side_effect=blocked_journal)

        interaction = _make_interaction(self.guild)
        # /setchannel must finish promptly without waiting for blocked Backloggd fetches.
        await asyncio.wait_for(
            FeedCog.setchannel.callback(self.cog, interaction, self.channel),
            timeout=0.5,
        )

        self.assertEqual(await self.db.get_feed_channel(1), self.channel.id)
        self.bot.backloggd.get_user_activity_snapshot.assert_not_awaited()
        self.bot.backloggd.get_user_journal.assert_not_awaited()
        reply = interaction.followup.send.call_args.args[0]
        self.assertIn("first successful poll", reply.lower())
        self.assertIn("existing history", reply.lower())

        # Now unblock sources and simulate poll 1 -> seeds existing history with 0 sends.
        release_fetch.set()
        old_act = FeedEvent(
            "act-old-1", "Hades", "hades", None, "completed", "completed Hades",
            5.0, "Loved it", None, occurred_at="2026-11-12T10:00:00Z",
        )
        old_jrn = FeedEvent(
            "journal_j1", "Celeste", "celeste", None, "completed", "completed Celeste",
            4.5, "Great", "2026-11-11", source="journal", session_labels=("Finished",),
        )
        await self.cog._publish_sources(1, "millers", "Millers", None, self.channel, {
            "activity": [old_act],
            "journal": [old_jrn],
        })
        self.channel.send.assert_not_awaited()

        # Poll 2: a genuinely new event sends, while existing history stays silent.
        new_act = FeedEvent(
            "act-new-2", "Balatro", "balatro", None, "completed", "completed Balatro",
            5.0, None, None, occurred_at="2026-11-13T12:05:00Z",
        )
        self.now += timedelta(minutes=5)
        await self.cog._publish_sources(1, "millers", "Millers", None, self.channel, {
            "activity": [new_act, old_act],
            "journal": [old_jrn],
        })
        self.channel.send.assert_awaited_once()
        self.assertIn("Balatro", self.channel.send.call_args.kwargs["embed"].title)

    async def test_failed_journal_on_first_poll_baselines_safely_on_later_poll_without_resetting_activity(self):
        interaction = _make_interaction(self.guild)
        await FeedCog.setchannel.callback(self.cog, interaction, self.channel)

        act_old = FeedEvent(
            "act-1", "Hades", "hades", None, "completed", "completed Hades",
            5.0, None, None, occurred_at="2026-11-12T10:00:00Z",
        )
        # Poll 1: activity succeeds, journal fails -> only activity is baselined.
        await self.cog._publish_sources(1, "millers", "Millers", None, self.channel, {
            "activity": [act_old],
        })
        self.channel.send.assert_not_awaited()
        self.assertTrue(await self.db.has_feed_source(1, "millers", "activity-display-v1"))
        self.assertFalse(await self.db.has_feed_source(1, "millers", "journal-display-v1"))

        # Poll 2: journal arrives with historical entry AND backdated activity milestone.
        jrn_hist = FeedEvent(
            "journal_hist", "Celeste", "celeste", None, "completed", "completed Celeste",
            4.5, None, "2026-11-10", source="journal", session_labels=("Finished",),
        )
        backdated_milestone = FeedEvent(
            "act-backdated", "Celeste", "celeste", None, "completed", "completed Celeste",
            4.5, None, None, occurred_at="2026-11-13T12:04:00Z",
        )
        self.now += timedelta(minutes=5)
        await self.cog._publish_sources(1, "millers", "Millers", None, self.channel, {
            "activity": [backdated_milestone, act_old],
            "journal": [jrn_hist],
        })
        # Historical journal entry baselines silently and suppresses backdated milestone leak.
        self.channel.send.assert_not_awaited()
        self.assertTrue(await self.db.has_feed_source(1, "millers", "activity-display-v1"))
        self.assertTrue(await self.db.has_feed_source(1, "millers", "journal-display-v1"))

        # Next genuine post after both sources are initialized still goes out normally.
        act_new = FeedEvent(
            "act-new", "Hollow Knight", "hollow-knight", None, "completed", "completed Hollow Knight",
            5.0, None, None, occurred_at="2026-11-13T12:10:00Z",
        )
        self.now += timedelta(minutes=5)
        await self.cog._publish_sources(1, "millers", "Millers", None, self.channel, {
            "activity": [act_new, backdated_milestone, act_old],
            "journal": [jrn_hist],
        })
        self.channel.send.assert_awaited_once()
        self.assertIn("Hollow Knight", self.channel.send.call_args.kwargs["embed"].title)

    async def test_channel_change_and_new_profile_and_unavailable_membership_preserve_state(self):
        # Initial setup + poll with posted message.
        await FeedCog.setchannel.callback(self.cog, _make_interaction(self.guild), self.channel)
        act_old = FeedEvent(
            "act-1", "Hades", "hades", None, "completed", "completed Hades",
            5.0, None, None, occurred_at="2026-11-12T10:00:00Z",
        )
        await self.cog._publish_sources(1, "millers", "Millers", None, self.channel, {
            "activity": [act_old], "journal": [],
        })
        act_posted = FeedEvent(
            "act-2", "Balatro", "balatro", None, "completed", "completed Balatro",
            4.0, "Good", None, occurred_at="2026-11-13T12:01:00Z",
        )
        await self.cog._publish_sources(1, "millers", "Millers", None, self.channel, {
            "activity": [act_posted, act_old], "journal": [],
        })
        self.assertEqual(len(self.channel._sent), 1)

        # Switch channel while also simulating a newly enrolled member (PlayerB).
        await self.db.link_user(20, "PlayerB")
        await self.db.set_guild_feed_preference(1, 20, True)
        self.guild.members = [SimpleNamespace(id=10), SimpleNamespace(id=20)]
        self.guild.member_count = 2
        new_channel = _make_channel(200, "#new-feed")

        await FeedCog.setchannel.callback(self.cog, _make_interaction(self.guild), new_channel)
        self.assertEqual(await self.db.get_feed_channel(1), 200)
        # Already-active millers retains source initialization & archive; new playerb awaits first poll.
        self.assertTrue(await self.db.has_feed_source(1, "millers", "activity-display-v1"))
        self.assertFalse(await self.db.has_feed_source(1, "playerb", "activity-display-v1"))

        # Next poll for millers does not replay history, and playerb baselines silently.
        await self.cog._publish_sources(1, "millers", "Millers", None, new_channel, {
            "activity": [act_posted, act_old], "journal": [],
        })
        await self.cog._publish_sources(1, "playerb", "PlayerB", None, new_channel, {
            "activity": [replace(act_old, activity_id="pb-old")], "journal": [],
        })
        new_channel.send.assert_not_awaited()

        # Simulate /setchannel when membership snapshot is unavailable: channel still updates cleanly.
        incomplete_guild = SimpleNamespace(
            id=1, name="Guild", chunked=False, member_count=2, members=[],
            chunk=AsyncMock(), me=SimpleNamespace(id=999),
        )
        third_channel = _make_channel(300, "#third-feed")
        interaction = _make_interaction(incomplete_guild)
        await FeedCog.setchannel.callback(self.cog, interaction, third_channel)
        self.assertEqual(await self.db.get_feed_channel(1), 300)
        reply = interaction.followup.send.call_args.args[0]
        self.assertIn("first successful poll", reply.lower())
        # Active enrollments & archives remain intact despite unavailable membership snapshot.
        self.assertTrue(await self.db.has_feed_source(1, "millers", "activity-display-v1"))

    async def test_setchannel_guards_unavailable_bot_member_and_retains_existing_channel(self):
        await self.db.set_feed_channel(1, 100)
        guild_without_me = SimpleNamespace(
            id=1,
            name="Guild",
            chunked=True,
            member_count=1,
            members=[SimpleNamespace(id=10)],
            me=None,
        )
        candidate = _make_channel(200, "#new-feed")
        interaction = _make_interaction(guild_without_me)

        await FeedCog.setchannel.callback(self.cog, interaction, candidate)

        candidate.permissions_for.assert_not_called()
        self.assertEqual(await self.db.get_feed_channel(1), 100)
        self.bot.backloggd.get_user_activity_snapshot.assert_not_awaited()
        self.bot.backloggd.get_user_journal.assert_not_awaited()
        interaction.followup.send.assert_awaited_once()
        self.assertTrue(interaction.followup.send.call_args.kwargs.get("ephemeral"))
        reply = interaction.followup.send.call_args.args[0]
        self.assertIn("try", reply.lower())

    async def test_setchannel_missing_each_required_permission_retains_existing_channel(self):
        await self.db.set_feed_channel(1, 100)
        cases = [
            ("view_channel", "View Channel"),
            ("send_messages", "Send Messages"),
            ("embed_links", "Embed Links"),
        ]
        for attr, label in cases:
            with self.subTest(missing_perm=attr):
                flags = {"view_channel": True, "send_messages": True, "embed_links": True}
                flags[attr] = False
                candidate = _make_channel(200, "#restricted", **flags)
                interaction = _make_interaction(self.guild)

                await FeedCog.setchannel.callback(self.cog, interaction, candidate)

                candidate.permissions_for.assert_called_once_with(self.guild.me)
                self.assertEqual(await self.db.get_feed_channel(1), 100)
                self.bot.backloggd.get_user_activity_snapshot.assert_not_awaited()
                self.bot.backloggd.get_user_journal.assert_not_awaited()
                interaction.followup.send.assert_awaited_once()
                self.assertTrue(interaction.followup.send.call_args.kwargs.get("ephemeral"))
                self.assertIn(label, interaction.followup.send.call_args.args[0])


if __name__ == "__main__":
    unittest.main()
