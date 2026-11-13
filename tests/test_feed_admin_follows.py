import os
os.environ["PYTHON_DOTENV_DISABLED"] = "1"

import inspect
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import discord
from discord import app_commands

from bot.cogs.feed import FeedCog
from bot.db import Database
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


if __name__ == "__main__":
    unittest.main()
