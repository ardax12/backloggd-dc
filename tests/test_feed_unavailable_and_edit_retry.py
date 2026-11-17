"""Regression tests for Task 10: unavailable targets, transition logging, and independent retryable feed edits."""

import logging
import os
import tempfile
import unittest
import json
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
from backloggd import ActivityEntry

from bot.cogs.feed import FeedCog
from bot.db import Database
from bot.feed_models import FeedEvent


def _activity(
    *,
    activity_id: str = "act-1",
    slug: str = "hades",
    title: str = "Hades",
    rating: float = 4.5,
    occurred_at: str = "2026-11-13T11:30:00Z",
) -> ActivityEntry:
    return ActivityEntry(
        activity_id=activity_id,
        game_title=title,
        game_slug=slug,
        cover_url=None,
        action_type="completed",
        action_text="completed",
        rating=rating,
        review_notes=None,
        date_str="11:30",
        occurred_at=occurred_at,
    )


def _http_error(cls, status: int, text: str = "discord error"):
    response = SimpleNamespace(status=status, reason="Error")
    return cls(response, text)


def _make_channel(channel_id: int = 100, *, view_channel=True, send_messages=True, embed_links=True):
    channel = MagicMock(spec=discord.TextChannel)
    channel.id = channel_id
    channel.mention = f"<#{channel_id}>"
    channel.permissions_for.return_value = SimpleNamespace(
        view_channel=view_channel,
        send_messages=send_messages,
        embed_links=embed_links,
    )
    sent_counter = {"n": 0}

    async def send_impl(**kwargs):
        sent_counter["n"] += 1
        return SimpleNamespace(id=5000 + sent_counter["n"], kwargs=kwargs)

    channel.send = AsyncMock(side_effect=send_impl)
    partials = {}

    def get_partial(message_id):
        if message_id not in partials:
            partials[message_id] = SimpleNamespace(id=message_id, edit=AsyncMock())
        return partials[message_id]

    channel.get_partial_message = get_partial
    channel._partials = partials
    return channel


def _make_guild(guild_id: int, members: list[int], channel=None):
    return SimpleNamespace(
        id=guild_id,
        name=f"Guild {guild_id}",
        me=object(),
        chunked=True,
        members=[SimpleNamespace(id=mid) for mid in members],
        get_channel=lambda cid: channel if channel is not None and cid == channel.id else None,
    )


class FeedUnavailableTargetTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmpdir.cleanup)
        self.db = Database(str(Path(self.tmpdir.name) / "feed-targets.db"), turso_url="")
        self.addAsyncCleanup(self.db.close)
        await self.db.connect()
        self.activation_time = datetime(2026, 11, 13, 8, 0, tzinfo=timezone.utc)
        self.now = datetime(2026, 11, 13, 12, 0, tzinfo=timezone.utc)
        await self.db.link_user(10, "Millers")
        await self.db.set_feed_channel(1, 100)
        await self.db.set_guild_feed_preference(1, 10, True)
        await self.db.add_guild_admin_follow(1, "PublicWatch", None, 99)
        await self.db.reconcile_guild_feed_activity(1, ["millers"], now=self.activation_time)
        await self.db.seed_feed_source(1, "millers", "activity-display-v1", ["old-1"])
        await self.db.seed_feed_source(1, "millers", "journal-display-v1", [])
        old_payload = json.dumps(
            asdict(FeedEvent.from_activity(_activity(activity_id="old-1", slug="old-game", title="Old Game", occurred_at="2026-11-13T07:00:00Z"))),
            sort_keys=True,
        )
        await self.db.seed_feed_message(1, "millers", ("old-1",), old_payload)

    async def test_missing_guild_and_channel_preserve_state_skip_fetches_and_recover(self):
        event = _activity(slug="hades", title="Hades", rating=4.5, occurred_at="2026-11-13T11:30:00Z")
        api = SimpleNamespace(
            get_user_activity_snapshot=AsyncMock(
                return_value=SimpleNamespace(entries=[event], coverage="exhausted", pages_read=1)
            ),
            get_user_journal=AsyncMock(return_value=[]),
        )
        current_guild = None
        bot = SimpleNamespace(
            db=self.db,
            backloggd=api,
            get_guild=lambda gid: current_guild if gid == 1 else None,
        )
        cog = FeedCog(bot, clock=lambda: self.now)

        # Poll 1 & 2: Guild missing from Discord cache -> active recovery profile (Millers) is
        # observed once per poll (never unactivated PublicWatch), state preserved, warning logged once.
        with patch("bot.cogs.feed.asyncio.sleep", new=AsyncMock()), self.assertLogs("backloggd.cogs.feed", level=logging.WARNING) as logs:
            await cog.journal_feed_loop.coro(cog)
            await cog.journal_feed_loop.coro(cog)

        self.assertEqual(api.get_user_activity_snapshot.await_count, 2)
        self.assertEqual(api.get_user_journal.await_count, 2)
        self.assertTrue(all(call.args[0] == "Millers" for call in api.get_user_activity_snapshot.await_args_list))
        self.assertEqual(len([r for r in logs.records if "unavailable" in r.getMessage().lower()]), 1)
        self.assertEqual(await self.db.get_feed_channel(1), 100)
        self.assertTrue(await self.db.get_guild_feed_preference(1, 10))
        self.assertTrue(await self.db.has_guild_admin_follow(1, "publicwatch"))
        self.assertTrue(await self.db.has_feed_source(1, "millers", "activity-display-v1"))
        self.assertEqual(
            [row["event_id"] for row in await self.db.get_pending_feed_observations(1, "millers")],
            ["act-1"],
        )

        # Poll 3 & 4: Guild returns but channel is missing from cache -> observed without sending, warning once.
        current_guild = _make_guild(1, [10], channel=None)
        with patch("bot.cogs.feed.asyncio.sleep", new=AsyncMock()), self.assertLogs("backloggd.cogs.feed", level=logging.WARNING) as logs2:
            await cog.journal_feed_loop.coro(cog)
            await cog.journal_feed_loop.coro(cog)
        self.assertEqual(api.get_user_activity_snapshot.await_count, 4)
        self.assertEqual(api.get_user_journal.await_count, 4)
        self.assertEqual(len([r for r in logs2.records if "unavailable" in r.getMessage().lower()]), 1)

        # /feed status discloses unavailable channel while keeping preference/admin state intact
        status_interaction = SimpleNamespace(
            guild=current_guild,
            guild_id=1,
            user=SimpleNamespace(id=10),
            response=SimpleNamespace(defer=AsyncMock()),
            followup=SimpleNamespace(send=AsyncMock()),
        )
        await FeedCog.feed_status.callback(cog, status_interaction)
        embed = status_interaction.followup.send.call_args.kwargs["embed"]
        self.assertIn("unavailable", embed.description.lower())

        # Poll 5: Channel returns with full permissions -> recovers and delivers automatically!
        channel = _make_channel(100)
        current_guild = _make_guild(1, [10], channel=channel)
        with patch("bot.cogs.feed.asyncio.sleep", new=AsyncMock()):
            await cog.journal_feed_loop.coro(cog)
        self.assertGreaterEqual(api.get_user_activity_snapshot.await_count, 5)
        channel.send.assert_awaited()

    async def test_missing_view_permission_observes_without_sending_discloses_status_and_recovers(self):
        event = _activity(slug="celeste", title="Celeste", rating=5.0, occurred_at="2026-11-13T11:30:00Z")
        api = SimpleNamespace(
            get_user_activity_snapshot=AsyncMock(
                return_value=SimpleNamespace(entries=[event], coverage="exhausted", pages_read=1)
            ),
            get_user_journal=AsyncMock(return_value=[]),
        )
        channel = _make_channel(100, view_channel=False, send_messages=True, embed_links=True)
        guild = _make_guild(1, [10], channel=channel)
        bot = SimpleNamespace(db=self.db, backloggd=api, get_guild=lambda gid: guild if gid == 1 else None)
        cog = FeedCog(bot, clock=lambda: self.now)

        # Polls 1 & 2 with missing View Channel -> observed without sends, 1 warning logged
        with patch("bot.cogs.feed.asyncio.sleep", new=AsyncMock()), self.assertLogs("backloggd.cogs.feed", level=logging.WARNING) as logs:
            await cog.journal_feed_loop.coro(cog)
            await cog.journal_feed_loop.coro(cog)

        self.assertEqual(api.get_user_activity_snapshot.await_count, 2)
        channel.send.assert_not_awaited()
        self.assertEqual(len([r for r in logs.records if "unavailable" in r.getMessage().lower() or "view channel" in r.getMessage().lower()]), 1)

        # /feed status discloses missing View Channel access
        status_interaction = SimpleNamespace(
            guild=guild,
            guild_id=1,
            user=SimpleNamespace(id=10),
            response=SimpleNamespace(defer=AsyncMock()),
            followup=SimpleNamespace(send=AsyncMock()),
        )
        await FeedCog.feed_status.callback(cog, status_interaction)
        embed = status_interaction.followup.send.call_args.kwargs["embed"]
        self.assertIn("unavailable", embed.description.lower())
        self.assertIn("view channel", embed.description.lower())

        # Restore View Channel -> recovers and delivers
        channel.permissions_for.return_value = SimpleNamespace(
            view_channel=True, send_messages=True, embed_links=True
        )
        with patch("bot.cogs.feed.asyncio.sleep", new=AsyncMock()):
            await cog.journal_feed_loop.coro(cog)
        channel.send.assert_awaited_once()

    async def test_only_unavailable_guild_observes_pins_boundary_and_recovers_after_restart(self):
        # Seed continuous boundary at old-1 before outage
        early_activation = datetime(2026, 11, 1, 8, 0, tzinfo=timezone.utc)
        pre_activation_event = _activity(
            activity_id="pre-act", slug="pre-act-game", title="Pre Act", occurred_at="2026-10-31T10:00:00Z"
        )
        new_event = _activity(
            activity_id="new-2", slug="hades", title="Hades", rating=4.5, occurred_at="2026-11-13T11:30:00Z"
        )
        old_event = _activity(
            activity_id="old-1", slug="old-game", title="Old Game", occurred_at="2026-11-13T07:00:00Z"
        )
        too_old_event = _activity(
            activity_id="very-old", slug="ancient", title="Ancient", occurred_at="2026-11-02T10:00:00Z"
        )
        deleted_event = _activity(
            activity_id="del-1", slug="deleted-game", title="Deleted Game", occurred_at="2026-11-13T11:00:00Z"
        )
        del_payload = json.dumps(asdict(FeedEvent.from_activity(deleted_event)), sort_keys=True)

        for reason in ("missing_channel", "incomplete_chunk", "missing_bot_member", "missing_send_perms", "missing_guild"):
            with self.subTest(reason=reason), tempfile.TemporaryDirectory() as folder:
                db = Database(str(Path(folder) / f"{reason}.db"), turso_url="")
                await db.connect()
                try:
                    await db.link_user(10, "Millers")
                    await db.set_feed_channel(1, 100)
                    await db.set_guild_feed_preference(1, 10, True)
                    await db.reconcile_guild_feed_activity(1, ["millers"], now=early_activation)
                    await db.seed_feed_source(1, "millers", "activity-display-v1", ["old-1"])
                    await db.seed_feed_source(1, "millers", "journal-display-v1", [])
                    old_payload = json.dumps(asdict(FeedEvent.from_activity(old_event)), sort_keys=True)
                    await db.seed_feed_message(1, "millers", ("old-1",), old_payload)
                    await db.store_feed_message(1, "millers", ("del-1",), del_payload, 777, 100, delivery_state="deleted")
                    await db.record_feed_source_attempt("millers", "activity")
                    await db.record_feed_source_success(
                        "millers", "activity", coverage="exhausted", pages_read=1, boundary_event_id="old-1"
                    )

                    ch = _make_channel(100)
                    if reason == "missing_channel":
                        guild = _make_guild(1, [10], channel=None)
                    elif reason == "incomplete_chunk":
                        guild = SimpleNamespace(
                            id=1, name="Guild 1", me=object(), chunked=False, member_count=2,
                            members=[SimpleNamespace(id=10)], chunk=AsyncMock(),
                            get_channel=lambda cid, c=ch: c if cid == 100 else None,
                        )
                    elif reason == "missing_bot_member":
                        guild = SimpleNamespace(
                            id=1, name="Guild 1", me=None, chunked=True, member_count=1,
                            members=[SimpleNamespace(id=10)],
                            get_channel=lambda cid, c=ch: c if cid == 100 else None,
                        )
                    elif reason == "missing_send_perms":
                        ch.permissions_for.return_value = SimpleNamespace(
                            view_channel=True, send_messages=False, embed_links=False
                        )
                        guild = _make_guild(1, [10], channel=ch)
                    else:
                        guild = None

                    api = SimpleNamespace(
                        get_user_activity_snapshot=AsyncMock(
                            return_value=SimpleNamespace(
                                entries=[new_event, deleted_event, too_old_event, pre_activation_event, old_event],
                                coverage="boundary_reached",
                                pages_read=1,
                            )
                        ),
                        get_user_journal=AsyncMock(return_value=[]),
                    )
                    bot = SimpleNamespace(db=db, backloggd=api, get_guild=lambda gid, g=guild: g if gid == 1 else None)
                    cog = FeedCog(bot, clock=lambda: self.now)

                    with patch("bot.cogs.feed.asyncio.sleep", new=AsyncMock()):
                        await cog.journal_feed_loop.coro(cog)

                    api.get_user_activity_snapshot.assert_awaited_once_with("Millers", stop_at_ids=("old-1",), max_pages=5)
                    api.get_user_journal.assert_awaited_once()
                    ch.send.assert_not_awaited()

                    health = await db.get_profile_feed_source("millers", "activity")
                    self.assertEqual(health["last_continuous_boundary_event_id"], "old-1")
                    self.assertEqual(health["last_attempt_at"], "2026-11-13 12:00:00")
                    self.assertEqual(health["last_success_at"], "2026-11-13 12:00:00")
                    self.assertEqual(health["unresolved_gap_since"], "2026-11-13 12:00:00")
                    self.assertIsNone(health["unresolved_gap_until"])
                    self.assertEqual(health["gap_reason"], "unavailable_feed_target")

                    pending = [r["event_id"] for r in await db.get_pending_feed_observations(1, "millers")]
                    self.assertEqual(pending, ["new-2"])
                    very_old_dec = await db.get_feed_event_decision(1, "millers", "very-old")
                    self.assertEqual(very_old_dec["decision"], "suppressed_old")
                    pre_act_dec = await db.get_feed_event_decision(1, "millers", "pre-act")
                    self.assertEqual(pre_act_dec["decision"], "suppressed_pre_activation")

                    # Restart (new FeedCog) and recover while new-2 dropped off latest SDK page
                    recovered_ch = _make_channel(100)
                    recovered_guild = _make_guild(1, [10], channel=recovered_ch)
                    api.get_user_activity_snapshot.return_value = SimpleNamespace(
                        entries=[old_event], coverage="boundary_reached", pages_read=1
                    )
                    recovered_bot = SimpleNamespace(
                        db=db, backloggd=api, get_guild=lambda gid, g=recovered_guild: g if gid == 1 else None
                    )
                    recovered_cog = FeedCog(recovered_bot, clock=lambda: self.now)
                    with patch("bot.cogs.feed.asyncio.sleep", new=AsyncMock()):
                        await recovered_cog.journal_feed_loop.coro(recovered_cog)

                    recovered_ch.send.assert_awaited_once()
                    self.assertIn("Hades", recovered_ch.send.call_args.kwargs["embed"].title)
                    del_rows = [r for r in await db.get_feed_messages(1, "millers") if r["event_id"] == "del-1"]
                    self.assertEqual(del_rows[0]["delivery_state"], "deleted")
                    health_after = await db.get_profile_feed_source("millers", "activity")
                    self.assertIsNone(health_after["unresolved_gap_since"])
                    self.assertEqual(health_after["last_continuous_boundary_at"], "2026-11-13 12:00:00")
                finally:
                    await db.close()

    async def test_multiple_unavailable_guilds_and_mixed_available_deduplicate_fetches(self):
        await self.db.remove_guild_admin_follow(1, "PublicWatch")
        # Set up guild 1 & guild 2, both unavailable -> one fetch, one policy pass per guild
        ch1 = _make_channel(100, view_channel=False)
        ch2 = _make_channel(200)
        g1 = _make_guild(1, [10], channel=ch1)
        g2 = _make_guild(2, [10], channel=None)
        await self.db.set_feed_channel(2, 200)
        await self.db.set_guild_feed_preference(2, 10, True)
        await self.db.reconcile_guild_feed_activity(2, ["millers"], now=self.activation_time)
        await self.db.seed_feed_source(2, "millers", "activity-display-v1", [])
        await self.db.seed_feed_source(2, "millers", "journal-display-v1", [])

        ev = _activity(activity_id="act-multi", slug="balatro", title="Balatro", occurred_at="2026-11-13T11:30:00Z")
        api = SimpleNamespace(
            get_user_activity_snapshot=AsyncMock(
                return_value=SimpleNamespace(entries=[ev], coverage="exhausted", pages_read=1)
            ),
            get_user_journal=AsyncMock(return_value=[]),
        )
        guilds = {1: g1, 2: g2}
        bot = SimpleNamespace(db=self.db, backloggd=api, get_guild=lambda gid: guilds.get(gid))
        cog = FeedCog(bot, clock=lambda: self.now)

        with patch("bot.cogs.feed.asyncio.sleep", new=AsyncMock()):
            await cog.journal_feed_loop.coro(cog)

        api.get_user_activity_snapshot.assert_awaited_once()
        api.get_user_journal.assert_awaited_once()
        ch1.send.assert_not_awaited()
        ch2.send.assert_not_awaited()
        self.assertEqual([r["event_id"] for r in await self.db.get_pending_feed_observations(1, "millers")], ["act-multi"])
        self.assertEqual([r["event_id"] for r in await self.db.get_pending_feed_observations(2, "millers")], ["act-multi"])

        # Now make guild 1 available while guild 2 stays unavailable -> still 1 fetch for this poll, guild 1 sends, guild 2 queues
        ch1.permissions_for.return_value = SimpleNamespace(view_channel=True, send_messages=True, embed_links=True)
        with patch("bot.cogs.feed.asyncio.sleep", new=AsyncMock()):
            await cog.journal_feed_loop.coro(cog)
        self.assertEqual(api.get_user_activity_snapshot.await_count, 2)
        ch1.send.assert_awaited_once()
        ch2.send.assert_not_awaited()
        health = await self.db.get_profile_feed_source("millers", "activity")
        self.assertIsNotNone(health["unresolved_gap_since"])

    async def test_unbaselined_active_profile_in_unavailable_guild_stays_baseline_on_observation(self):
        with tempfile.TemporaryDirectory() as folder:
            db = Database(str(Path(folder) / "unbaselined.db"), turso_url="")
            await db.connect()
            try:
                await db.link_user(10, "Millers")
                await db.set_feed_channel(1, 100)
                await db.set_guild_feed_preference(1, 10, True)
                # Activated (e.g. during /setchannel), but NO source baselined yet
                await db.reconcile_guild_feed_activity(1, ["millers"], now=self.activation_time)
                existing_ev = _activity(
                    activity_id="preexisting-1", slug="celeste", title="Celeste", occurred_at="2026-11-13T11:30:00Z"
                )
                api = SimpleNamespace(
                    get_user_activity_snapshot=AsyncMock(
                        return_value=SimpleNamespace(entries=[existing_ev], coverage="exhausted", pages_read=1)
                    ),
                    get_user_journal=AsyncMock(return_value=[]),
                )
                ch = _make_channel(100)
                guild = _make_guild(1, [10], channel=None)
                bot = SimpleNamespace(db=db, backloggd=api, get_guild=lambda gid: guild if gid == 1 else None)
                cog = FeedCog(bot, clock=lambda: self.now)

                with patch("bot.cogs.feed.asyncio.sleep", new=AsyncMock()):
                    await cog.journal_feed_loop.coro(cog)

                self.assertTrue(await db.has_feed_source(1, "millers", "activity-display-v1"))
                self.assertEqual(await db.get_pending_feed_observations(1, "millers"), [])
                dec = await db.get_feed_event_decision(1, "millers", "preexisting-1")
                self.assertEqual(dec["decision"], "baseline")

                # Recover channel: preexisting-1 is NEVER announced
                guild_ok = _make_guild(1, [10], channel=ch)
                bot.get_guild = lambda gid: guild_ok if gid == 1 else None
                with patch("bot.cogs.feed.asyncio.sleep", new=AsyncMock()):
                    await cog.journal_feed_loop.coro(cog)
                ch.send.assert_not_awaited()
            finally:
                await db.close()

    async def test_no_fetches_when_no_enrolled_recovery_profile_exists(self):
        with tempfile.TemporaryDirectory() as folder:
            db = Database(str(Path(folder) / "no-recovery.db"), turso_url="")
            await db.connect()
            try:
                await db.link_user(10, "OptedOut")
                await db.link_user(20, "NeverActivated")
                await db.set_feed_channel(1, 100)
                await db.set_guild_feed_preference(1, 10, True)
                await db.reconcile_guild_feed_activity(1, ["optedout"], now=self.activation_time)
                await db.set_guild_feed_preference(1, 10, False)  # disabled preference
                await db.set_guild_feed_preference(1, 20, True)   # enabled pref, but never activated

                api = SimpleNamespace(
                    get_user_activity_snapshot=AsyncMock(),
                    get_user_journal=AsyncMock(),
                )
                bot = SimpleNamespace(db=db, backloggd=api, get_guild=lambda gid: None)
                cog = FeedCog(bot, clock=lambda: self.now)

                with patch("bot.cogs.feed.asyncio.sleep", new=AsyncMock()):
                    await cog.journal_feed_loop.coro(cog)

                api.get_user_activity_snapshot.assert_not_awaited()
                api.get_user_journal.assert_not_awaited()
            finally:
                await db.close()

    async def test_unavailable_recipient_source_failure_isolation(self):
        await self.db.link_user(20, "SecondUser")
        await self.db.set_guild_feed_preference(1, 20, True)
        await self.db.reconcile_guild_feed_activity(1, ["millers", "seconduser"], now=self.activation_time)
        await self.db.seed_feed_source(1, "seconduser", "activity-display-v1", [])
        await self.db.seed_feed_source(1, "seconduser", "journal-display-v1", [])

        ev = _activity(activity_id="sec-1", slug="portal", title="Portal", occurred_at="2026-11-13T11:30:00Z")

        async def activity_side_effect(username, **_kw):
            if username == "Millers":
                raise RuntimeError("transient upstream 503")
            return SimpleNamespace(entries=[ev], coverage="exhausted", pages_read=1)

        api = SimpleNamespace(
            get_user_activity_snapshot=AsyncMock(side_effect=activity_side_effect),
            get_user_journal=AsyncMock(return_value=[]),
        )
        bot = SimpleNamespace(db=self.db, backloggd=api, get_guild=lambda gid: None)
        cog = FeedCog(bot, clock=lambda: self.now)

        with patch("bot.cogs.feed.asyncio.sleep", new=AsyncMock()):
            await cog.journal_feed_loop.coro(cog)

        self.assertEqual(api.get_user_activity_snapshot.await_count, 2)
        self.assertEqual([r["event_id"] for r in await self.db.get_pending_feed_observations(1, "seconduser")], ["sec-1"])
        millers_act = await self.db.get_profile_feed_source("millers", "activity")
        self.assertIn("transient upstream 503", millers_act["last_error"])
        self.assertEqual(millers_act["gap_reason"], "unavailable_feed_target")
        # Journal still succeeded independently for Millers
        millers_journal = await self.db.get_profile_feed_source("millers", "journal")
        self.assertEqual(millers_journal["successes"], 1)



class FeedIndependentEditRetryTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmpdir.cleanup)
        self.db = Database(str(Path(self.tmpdir.name) / "feed-edits.db"), turso_url="")
        self.addAsyncCleanup(self.db.close)
        await self.db.connect()
        self.now = datetime(2026, 11, 13, 12, 0, tzinfo=timezone.utc)
        await self.db.link_user(10, "Millers")

    async def test_failed_edit_403_and_5xx_keeps_old_payload_allows_new_sends_and_blocks_boundary_commit(self):
        await self.db.set_feed_channel(1, 100)
        await self.db.set_guild_feed_preference(1, 10, True)
        await self.db.set_feed_channel(2, 200)
        await self.db.set_guild_feed_preference(2, 10, True)

        e1 = _activity(
            slug="hades", title="Hades", rating=4.0, occurred_at="2026-11-13T10:00:00Z", activity_id="act-1"
        )
        e2 = _activity(
            slug="celeste", title="Celeste", rating=5.0, occurred_at="2026-11-13T11:00:00Z", activity_id="act-2"
        )
        # Deleted entry to confirm NotFound suppression still works cleanly
        e_del = _activity(
            slug="portal", title="Portal", rating=4.0, occurred_at="2026-11-13T09:00:00Z", activity_id="act-del"
        )

        # Seed activation + baseline sources in both guilds
        activation_time = datetime(2026, 11, 13, 8, 0, tzinfo=timezone.utc)
        for gid, cid in ((1, 100), (2, 200)):
            await self.db.reconcile_guild_feed_activity(gid, ["millers"], now=activation_time)
            await self.db.seed_feed_source(gid, "millers", "activity-display-v1", [])
            await self.db.seed_feed_source(gid, "millers", "journal-display-v1", [])

        ch1 = _make_channel(100)
        ch2 = _make_channel(200)
        g1 = _make_guild(1, [10], ch1)
        g2 = _make_guild(2, [10], ch2)
        guilds = {1: g1, 2: g2}

        snapshots = [
            SimpleNamespace(entries=[e1, e_del], coverage="exhausted", pages_read=1),
        ]
        api = SimpleNamespace(
            get_user_activity_snapshot=AsyncMock(side_effect=lambda *a, **kw: snapshots.pop(0)),
            get_user_journal=AsyncMock(return_value=[]),
        )
        bot = SimpleNamespace(db=self.db, backloggd=api, get_guild=lambda gid: guilds.get(gid))
        cog = FeedCog(bot, clock=lambda: self.now)

        # Poll 1: delivers e1 and e_del to both guilds cleanly -> boundary advances to "act-1"
        await cog.journal_feed_loop.coro(cog)
        self.assertEqual(ch1.send.await_count, 2)
        self.assertEqual(ch2.send.await_count, 2)
        health = await self.db.get_profile_feed_source("millers", "activity")
        self.assertEqual(health["last_continuous_boundary_event_id"], "act-1")

        # Grab message IDs for e1 and e_del in guild 1
        msgs1 = {r["event_id"]: r for r in await self.db.get_feed_messages(1, "millers")}
        msg_e1_g1 = msgs1["act-1"]["message_id"]
        msg_del_g1 = msgs1["act-del"]["message_id"]
        old_payload_g1 = msgs1["act-1"]["payload"]

        # Configure Guild 1 edits: act-del raises NotFound (deleted manually), act-1 raises 403 Forbidden!
        ch1.get_partial_message(msg_del_g1).edit.side_effect = _http_error(discord.NotFound, 404, "Unknown Message")
        ch1.get_partial_message(msg_e1_g1).edit.side_effect = _http_error(discord.Forbidden, 403, "Missing Access")

        e1_updated = replace(e1, rating=5.0)
        e_del_updated = replace(e_del, rating=5.0)

        # Poll 2: e1 & e_del edited, PLUS new activity e2 arrives!
        snapshots.append(
            SimpleNamespace(
                entries=[e2, e1_updated, e_del_updated],
                coverage="boundary_reached",
                pages_read=1,
            )
        )
        await cog.journal_feed_loop.coro(cog)

        # Guild 1: act-del marked deleted, act-1 kept OLD payload for retry, AND e2 STILL sent!
        msgs1_after = {r["event_id"]: r for r in await self.db.get_feed_messages(1, "millers")}
        self.assertEqual(msgs1_after["act-del"]["delivery_state"], "deleted")
        self.assertEqual(msgs1_after["act-1"]["payload"], old_payload_g1)
        self.assertIn("act-2", msgs1_after)
        self.assertEqual(ch1.send.await_count, 3)

        # Guild 2 succeeded independently for all events (edited e1 & e_del, sent e2)!
        self.assertEqual(ch2.send.await_count, 3)
        msgs2_after = {r["event_id"]: r for r in await self.db.get_feed_messages(2, "millers")}
        self.assertIn("act-2", msgs2_after)

        # Because Guild 1 edit failed, global activity boundary MUST NOT advance past act-1 yet!
        health_after_fail = await self.db.get_profile_feed_source("millers", "activity")
        self.assertEqual(health_after_fail["last_continuous_boundary_event_id"], "act-1")

        # Poll 3: Guild 1 edit now hits 502 once, then Poll 4 succeeds -> boundary advances to "act-2"!
        ch1.get_partial_message(msg_e1_g1).edit.side_effect = _http_error(discord.HTTPException, 502, "Bad Gateway")
        snapshots.append(
            SimpleNamespace(
                entries=[e2, e1_updated, e_del_updated],
                coverage="boundary_reached",
                pages_read=1,
            )
        )
        await cog.journal_feed_loop.coro(cog)
        self.assertEqual(
            (await self.db.get_profile_feed_source("millers", "activity"))["last_continuous_boundary_event_id"],
            "act-1",
        )

        ch1.get_partial_message(msg_e1_g1).edit.side_effect = None
        ch1.get_partial_message(msg_e1_g1).edit.reset_mock()
        snapshots.append(
            SimpleNamespace(
                entries=[e2, e1_updated, e_del_updated],
                coverage="boundary_reached",
                pages_read=1,
            )
        )
        await cog.journal_feed_loop.coro(cog)
        ch1.get_partial_message(msg_e1_g1).edit.assert_awaited_once()
        msgs1_final = {r["event_id"]: r for r in await self.db.get_feed_messages(1, "millers")}
        self.assertNotEqual(msgs1_final["act-1"]["payload"], old_payload_g1)
        # Deleted message is NOT reposted or re-edited
        ch1.get_partial_message(msg_del_g1).edit.assert_awaited_once()
        self.assertEqual(
            (await self.db.get_profile_feed_source("millers", "activity"))["last_continuous_boundary_event_id"],
            "act-2",
        )


if __name__ == "__main__":
    unittest.main()
