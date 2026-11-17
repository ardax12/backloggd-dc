import os
os.environ["PYTHON_DOTENV_DISABLED"] = "1"

import inspect
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
from discord import app_commands

from bot.cogs.feed import FeedCog
from bot.db import Database
from bot.feed_models import FeedEvent
from bot.ui.views import FeedListView


class AdminFeedFollowTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.db = Database(str(Path(self.directory.name) / "admin-feed.db"), turso_url="")
        await self.db.connect()
        self.profile_api = AsyncMock(
            return_value=SimpleNamespace(username="PublicPlayer", avatar_url="https://cdn.test/avatar.png")
        )
        self.bot = SimpleNamespace(
            db=self.db,
            backloggd=SimpleNamespace(get_user_profile=self.profile_api),
        )
        self.cog = FeedCog(self.bot)

    async def asyncTearDown(self):
        await self.db.close()
        self.directory.cleanup()

    def interaction(self, *, guild_id=1, username="PublicPlayer", member_ids=()):
        guild = SimpleNamespace(
            id=guild_id,
            chunked=True,
            members=[SimpleNamespace(id=member_id) for member_id in member_ids],
        ) if guild_id is not None else None
        return SimpleNamespace(
            guild=guild,
            guild_id=guild_id,
            user=SimpleNamespace(id=99),
            permissions=SimpleNamespace(manage_channels=True),
            response=SimpleNamespace(defer=AsyncMock(), send_message=AsyncMock()),
            followup=SimpleNamespace(send=AsyncMock(return_value=SimpleNamespace(edit=AsyncMock()))),
        )

    async def test_add_follows_validated_unlinked_public_profile_without_library_sync(self):
        interaction = self.interaction()

        await FeedCog.feed_add.callback(self.cog, interaction, "@PublicPlayer")

        self.profile_api.assert_awaited_once_with("PublicPlayer")
        candidates = await self.db.get_guild_feed_candidates(1, [])
        self.assertEqual(len(candidates), 1)
        self.assertEqual(candidates[0]["profile_key"], "publicplayer")
        self.assertEqual(candidates[0]["sources"], "admin")
        self.assertEqual(candidates[0]["member_ids"], [])
        count = await self.db._fetchone(
            "SELECT COUNT(*) AS count FROM profile_ratings WHERE profile_key = 'publicplayer'"
        )
        self.assertEqual(count["count"], 0)
        self.assertIn("pending", interaction.followup.send.call_args.args[0].lower())

    async def test_admin_follow_addition_resolves_pending_member_continuity_atomically(self):
        t0 = datetime(2026, 11, 13, 8, 0, tzinfo=timezone.utc)
        for discord_id in (10, 11):
            await self.db.link_user(discord_id, "PublicPlayer")
            await self.db.set_guild_feed_preference(1, discord_id, True)
        await self.db.reconcile_guild_feed_activity(1, ["publicplayer"], now=t0)
        await self.db.seed_feed_source(1, "publicplayer", "activity-display-v1", ["old-event"])
        await self.db.record_feed_event_decision(
            1, "publicplayer", "validated-work", source="activity", event_date="2026-11-13",
            decision="eligible", reason="recent", payload="{}", observed_at=t0,
        )
        await self.db.unlink_user(10)

        await FeedCog.feed_add.callback(
            self.cog, self.interaction(member_ids=[10, 11, 99]), "PublicPlayer",
        )
        self.assertIsNone(await self.db._fetchone(
            "SELECT 1 FROM guild_profile_feed_reconciliation WHERE guild_id = 1 AND profile_key = 'publicplayer'"
        ))
        await self.db.unlink_user(11)

        status = await self.db.get_guild_profile_feed_status(1, "publicplayer")
        self.assertTrue(status["active"])
        self.assertTrue(await self.db.has_feed_source(1, "publicplayer", "activity-display-v1"))
        self.assertEqual(
            (await self.db.get_feed_event_decision(1, "publicplayer", "validated-work"))["decision"],
            "eligible",
        )

    async def test_add_is_case_insensitive_idempotent_and_does_not_replace_creator(self):
        interaction = self.interaction()
        await FeedCog.feed_add.callback(self.cog, interaction, "PublicPlayer")
        interaction2 = self.interaction()
        await FeedCog.feed_add.callback(self.cog, interaction2, "PUBLICPLAYER")

        follows = await self.db.get_guild_admin_follows(1)
        self.assertEqual(len(follows), 1)
        self.assertEqual(follows[0]["added_by"], 99)
        self.assertIn("already followed", interaction2.followup.send.call_args.args[0])

    async def test_missing_profile_and_sdk_failure_do_not_create_subscription(self):
        self.profile_api.return_value = None
        await FeedCog.feed_add.callback(self.cog, self.interaction(), "Missing")
        self.assertEqual(await self.db.get_guild_admin_follows(1), [])

        self.profile_api.side_effect = RuntimeError("offline")
        interaction = self.interaction()
        await FeedCog.feed_add.callback(self.cog, interaction, "Later")
        self.assertIn("temporarily unavailable", interaction.followup.send.call_args.args[0])
        self.assertEqual(await self.db.get_guild_admin_follows(1), [])

    async def test_dm_add_rejected_before_sdk_request(self):
        interaction = self.interaction(guild_id=None)

        await FeedCog.feed_add.callback(self.cog, interaction, "PublicPlayer")

        self.profile_api.assert_not_awaited()
        self.assertEqual(await self.db.get_guild_admin_follows(1), [])

    async def test_admin_follow_and_member_enrollment_are_independent(self):
        await self.db.link_user(10, "PublicPlayer")
        await self.db.set_guild_feed_preference(1, 10, True)
        await self.db.add_guild_admin_follow(1, "PublicPlayer", None, 99)
        interaction = self.interaction(member_ids=[10])

        await FeedCog.feed_remove.callback(self.cog, interaction, "PUBLICPLAYER")

        candidates = await self.db.get_guild_feed_candidates(1, [10])
        self.assertEqual(len(candidates), 1)
        self.assertEqual(candidates[0]["sources"], "member")
        self.assertTrue(await self.db.get_guild_feed_preference(1, 10))
        self.assertIn("member participation", interaction.followup.send.call_args.args[0])

    async def test_admin_mutations_require_manage_channels_before_callback(self):
        interaction = self.interaction()
        interaction.permissions.manage_channels = False
        for name in ("add", "remove", "list"):
            command = FeedCog.feed.get_command(name)
            self.assertTrue(command.checks, name)
            check = command.checks[0]
            with self.assertRaises(app_commands.MissingPermissions):
                result = check(interaction)
                await result if inspect.isawaitable(result) else result
        self.profile_api.assert_not_awaited()

    async def test_feed_list_pages_and_reports_effective_source(self):
        for index in range(25):
            await self.db.add_guild_admin_follow(1, f"player{index:02d}", None, 99)
        interaction = self.interaction(member_ids=[])
        interaction.followup.send.return_value = SimpleNamespace(edit=AsyncMock())

        await FeedCog.feed_list.callback(self.cog, interaction)

        kwargs = interaction.followup.send.call_args.kwargs
        self.assertTrue(kwargs["ephemeral"])
        view = kwargs["view"]
        self.assertIsInstance(view, FeedListView)
        self.assertEqual(len(view.pages), 2)
        self.assertIn("Admin", view.pages[0].description)
        self.assertIn("Pending channel", view.pages[0].description)
        self.assertTrue(view.previous_button.disabled)
        self.assertFalse(view.next_button.disabled)

    async def test_setchannel_unsetchannel_checks_and_error_handler(self):
        interaction = self.interaction()
        interaction.permissions.manage_channels = False
        for cmd in (FeedCog.setchannel, FeedCog.unsetchannel):
            self.assertTrue(getattr(cmd, "guild_only", False), cmd.name)
            self.assertTrue(cmd.checks, cmd.name)
            with self.assertRaises(app_commands.MissingPermissions):
                result = cmd.checks[0](interaction)
                await result if inspect.isawaitable(result) else result

        for name in ("add", "remove", "list"):
            self.assertTrue(getattr(FeedCog.feed.get_command(name), "guild_only", False), name)

        unanswered = SimpleNamespace(
            response=SimpleNamespace(is_done=lambda: False, send_message=AsyncMock()),
            followup=SimpleNamespace(send=AsyncMock()),
        )
        await self.cog.channel_command_error(unanswered, app_commands.MissingPermissions(["manage_channels"]))
        self.assertIn("Manage Channels", unanswered.response.send_message.call_args.args[0])

        deferred = SimpleNamespace(
            response=SimpleNamespace(is_done=lambda: True, send_message=AsyncMock()),
            followup=SimpleNamespace(send=AsyncMock()),
        )
        await self.cog.channel_command_error(deferred, app_commands.NoPrivateMessage())
        self.assertIn("server", deferred.followup.send.call_args.args[0].lower())

    async def test_feed_status_exposes_health_coverage_gaps_suppressions_and_unknown_sends(self):
        await self.db.link_user(10, "Millers")
        await self.db.set_feed_channel(1, 100)
        await self.db.add_guild_admin_follow(1, "Millers", None, 99)
        await self.db.reconcile_guild_feed_activity(1, ["millers"])
        await self.db.seed_feed_source(1, "millers", "activity-display-v1", [])
        await self.db.seed_feed_source(1, "millers", "journal-display-v1", [])
        await self.db.record_feed_source_attempt("millers", "activity")
        await self.db.record_feed_source_success(
            "millers", "activity", coverage="unsupported", pages_read=1,
        )
        await self.db.record_feed_source_attempt("millers", "journal")
        await self.db.record_feed_source_failure("millers", "journal", "timed out")
        await self.db.record_feed_event_decision(
            1, "millers", "old-1", source="activity", event_date="2026-10-01",
            decision="suppressed_old", reason="old", payload="{}",
        )
        await self.db.begin_feed_delivery(1, "millers", ["unk-1"], '{"review_notes":"secret"}', 100)
        await self.db.mark_feed_delivery_unknown(1, "millers", ["unk-1"])

        interaction = self.interaction(member_ids=[10])
        interaction.user = SimpleNamespace(id=10)
        await FeedCog.feed_status.callback(self.cog, interaction)

        embed = interaction.followup.send.call_args.kwargs["embed"]
        self.assertIn("Admin follow", embed.description)
        self.assertIn("Degraded", embed.description)
        self.assertIn("timed out", embed.fields[0].value)
        self.assertIn("Old (>7d): 1", embed.fields[1].value)
        self.assertIn("Unknown send outcomes: 1", embed.fields[1].value)
        self.assertNotIn("secret", embed.fields[1].value)

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
            return SimpleNamespace(id=6000 + counter["n"], kwargs=kwargs)

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

    async def test_sole_admin_remove_under_chunk_failure_deactivates_and_readd_baselines_gap(self):
        ch1 = self._make_channel(100)
        ch2 = self._make_channel(200)
        await self.db.set_feed_channel(1, 100)
        await self.db.set_feed_channel(2, 200)
        await self.db.add_guild_admin_follow(1, "PublicPlayer", None, 99)
        await self.db.add_guild_admin_follow(1, "OtherFollow", None, 99)
        await self.db.add_guild_admin_follow(2, "PublicPlayer", None, 99)
        t0 = datetime(2026, 11, 13, 8, 0, tzinfo=timezone.utc)
        await self.db.reconcile_guild_feed_activity(1, ["publicplayer", "otherfollow"], now=t0)
        await self.db.reconcile_guild_feed_activity(2, ["publicplayer"], now=t0)
        await self.db.seed_feed_source(1, "publicplayer", "activity-display-v1", ["old-1"])
        await self.db.seed_feed_source(1, "otherfollow", "activity-display-v1", [])
        await self.db.seed_feed_source(2, "publicplayer", "activity-display-v1", [])

        old_ev = self._activity("old-1", "old-game", "Old Game", "2026-11-13T07:00:00Z")
        old_payload = json.dumps(asdict(FeedEvent.from_activity(old_ev)), sort_keys=True)
        await self.db.store_feed_message(1, "publicplayer", ("old-1",), old_payload, 500, 100, delivery_state="sent")
        await self.db.store_feed_message(1, "publicplayer", ("del-1",), old_payload, 501, 100, delivery_state="deleted")
        await self.db.record_feed_event_decision(
            1, "publicplayer", "unsent-1", source="activity", event_date="2026-11-13",
            decision="eligible", reason="recent", payload=old_payload, observed_at=t0,
        )

        broken_guild1 = SimpleNamespace(
            id=1, name="G1", me=object(), chunked=False, members=[], chunk=AsyncMock(return_value=[]),
            get_channel=lambda cid: ch1 if cid == 100 else None,
        )
        remove_inter = SimpleNamespace(
            guild=broken_guild1, guild_id=1, user=SimpleNamespace(id=99),
            permissions=SimpleNamespace(manage_channels=True),
            response=SimpleNamespace(defer=AsyncMock(), send_message=AsyncMock()),
            followup=SimpleNamespace(send=AsyncMock()),
        )
        cog = FeedCog(self.bot, clock=lambda: datetime(2026, 11, 13, 9, 0, tzinfo=timezone.utc))
        await FeedCog.feed_remove.callback(cog, remove_inter, "PublicPlayer")

        status = await self.db.get_guild_profile_feed_status(1, "publicplayer")
        self.assertFalse(status["active"])
        self.assertEqual(status["initialized_sources"], [])
        self.assertIsNone(await self.db.get_feed_event_decision(1, "publicplayer", "unsent-1"))
        msgs = {r["event_id"]: r for r in await self.db.get_feed_messages(1, "publicplayer")}
        self.assertEqual(msgs["old-1"]["delivery_state"], "sent")
        self.assertEqual(msgs["del-1"]["delivery_state"], "deleted")
        self.assertTrue(await self.db.has_feed_source(1, "otherfollow", "activity-display-v1"))
        self.assertTrue(await self.db.has_feed_source(2, "publicplayer", "activity-display-v1"))

        # Restart then re-add while chunk still fails, then recover!
        await self.db.close()
        self.db = Database(str(Path(self.directory.name) / "admin-feed.db"), turso_url="")
        await self.db.connect()
        self.bot.db = self.db

        add_inter = SimpleNamespace(
            guild=broken_guild1, guild_id=1, user=SimpleNamespace(id=99),
            permissions=SimpleNamespace(manage_channels=True),
            response=SimpleNamespace(defer=AsyncMock(), send_message=AsyncMock()),
            followup=SimpleNamespace(send=AsyncMock()),
        )
        cog2 = FeedCog(self.bot, clock=lambda: datetime(2026, 11, 13, 11, 0, tzinfo=timezone.utc))
        await FeedCog.feed_add.callback(cog2, add_inter, "PublicPlayer")

        gap_ev = self._activity("gap-1", "hades", "Hades", "2026-11-13T10:00:00Z")
        ok_guild1 = SimpleNamespace(
            id=1, name="G1", me=object(), chunked=True, members=[],
            get_channel=lambda cid: ch1 if cid == 100 else None,
        )
        ok_guild2 = SimpleNamespace(
            id=2, name="G2", me=object(), chunked=True, members=[],
            get_channel=lambda cid: ch2 if cid == 200 else None,
        )
        async def snapshot_for_user(username, **_kwargs):
            if username.lower() == "publicplayer":
                return SimpleNamespace(entries=[gap_ev, old_ev], coverage="exhausted", pages_read=1)
            return SimpleNamespace(entries=[], coverage="exhausted", pages_read=1)

        api = SimpleNamespace(
            get_user_profile=self.profile_api,
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
        self.assertEqual((await self.db.get_feed_event_decision(1, "publicplayer", "gap-1"))["decision"], "baseline")

    async def test_admin_remove_under_chunk_failure_with_departed_member_readd_baselines_gap(self):
        for member_present in (True, False):
            with self.subTest(member_present=member_present), tempfile.TemporaryDirectory() as folder:
                db = Database(str(Path(folder) / "admin-reconcile.db"), turso_url="")
                await db.connect()
                try:
                    await db.link_user(10, "PublicPlayer")
                    ch = self._make_channel(100)
                    await db.set_feed_channel(1, 100)
                    await db.set_guild_feed_preference(1, 10, True)
                    await db.add_guild_admin_follow(1, "PublicPlayer", None, 99)
                    t0 = datetime(2026, 11, 13, 8, 0, tzinfo=timezone.utc)
                    await db.reconcile_guild_feed_activity(1, ["publicplayer"], now=t0)
                    await db.seed_feed_source(1, "publicplayer", "activity-display-v1", ["old-1"])
                    await db.seed_feed_source(1, "publicplayer", "journal-display-v1", [])
                    old_ev = self._activity("old-1", "old-game", "Old Game", "2026-11-13T07:00:00Z")
                    old_payload = json.dumps(asdict(FeedEvent.from_activity(old_ev)), sort_keys=True)
                    await db.seed_feed_message(1, "publicplayer", ("old-1",), old_payload)

                    broken_guild = SimpleNamespace(
                        id=1, name="G1", me=object(), chunked=False, members=[], chunk=AsyncMock(return_value=[]),
                        get_channel=lambda cid: ch if cid == 100 else None,
                    )
                    remove_inter = SimpleNamespace(
                        guild=broken_guild, guild_id=1, user=SimpleNamespace(id=99),
                        permissions=SimpleNamespace(manage_channels=True),
                        response=SimpleNamespace(defer=AsyncMock(), send_message=AsyncMock()),
                        followup=SimpleNamespace(send=AsyncMock()),
                    )
                    bot = SimpleNamespace(db=db, backloggd=SimpleNamespace(get_user_profile=self.profile_api))
                    cog = FeedCog(bot, clock=lambda: datetime(2026, 11, 13, 9, 0, tzinfo=timezone.utc))
                    await FeedCog.feed_remove.callback(cog, remove_inter, "PublicPlayer")

                    # Member 10 stored in DB -> uncertainty retained
                    self.assertTrue((await db.get_guild_profile_feed_status(1, "publicplayer"))["active"])

                    gap_ev = self._activity("gap-admin", "hades", "Hades", "2026-11-13T10:00:00Z")
                    api = SimpleNamespace(
                        get_user_profile=self.profile_api,
                        get_user_activity_snapshot=AsyncMock(
                            return_value=SimpleNamespace(entries=[gap_ev, old_ev], coverage="exhausted", pages_read=1)
                        ),
                        get_user_journal=AsyncMock(return_value=[]),
                    )
                    loop_cog = FeedCog(
                        SimpleNamespace(db=db, backloggd=api, get_guild=lambda gid: broken_guild if gid == 1 else None),
                        clock=lambda: datetime(2026, 11, 13, 10, 30, tzinfo=timezone.utc),
                    )
                    with patch("bot.cogs.feed.asyncio.sleep", new=AsyncMock()):
                        await loop_cog.journal_feed_loop.coro(loop_cog)
                    ch.send.assert_not_awaited()

                    # Admin re-adds while chunk still broken
                    add_inter = SimpleNamespace(
                        guild=broken_guild, guild_id=1, user=SimpleNamespace(id=99),
                        permissions=SimpleNamespace(manage_channels=True),
                        response=SimpleNamespace(defer=AsyncMock(), send_message=AsyncMock()),
                        followup=SimpleNamespace(send=AsyncMock()),
                    )
                    await FeedCog.feed_add.callback(cog, add_inter, "PublicPlayer")

                    # Restart & recover
                    await db.close()
                    db = Database(str(Path(folder) / "admin-reconcile.db"), turso_url="")
                    await db.connect()

                    present = [10] if member_present else [99]
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

                    if member_present:
                        ch.send.assert_awaited_once()
                    else:
                        ch.send.assert_not_awaited()
                        self.assertEqual((await db.get_feed_event_decision(1, "publicplayer", "gap-admin"))["decision"], "baseline")
                finally:
                    await db.close()


if __name__ == "__main__":
    unittest.main()
