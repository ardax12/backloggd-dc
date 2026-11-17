"""Task 10 regressions for empty-state and feed enrollment copy."""

import os

os.environ["PYTHON_DOTENV_DISABLED"] = "1"
os.environ["TURSO_DATABASE_URL"] = ""
os.environ["TURSO_AUTH_TOKEN"] = ""
os.environ["DEV_GUILD_ID"] = ""

import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from bot.cogs.feed import FeedCog
from bot.cogs.social import SocialCog
from bot.db import Database


def _interaction(*, user_id=10, guild=None):
    return SimpleNamespace(
        user=SimpleNamespace(id=user_id, display_name=f"User{user_id}"),
        guild=guild,
        guild_id=getattr(guild, "id", None),
        permissions=SimpleNamespace(manage_channels=True),
        response=SimpleNamespace(defer=AsyncMock(), send_message=AsyncMock()),
        followup=SimpleNamespace(send=AsyncMock(return_value=SimpleNamespace(edit=AsyncMock()))),
    )


def _guild(*member_ids):
    return SimpleNamespace(
        id=1,
        chunked=True,
        member_count=len(member_ids),
        members=[SimpleNamespace(id=member_id) for member_id in member_ids],
    )


def _status(*, missing=False):
    return SimpleNamespace(
        missing=missing,
        refreshing=False,
        failed_last=False,
        synced_at=None if missing else datetime(2026, 11, 17, tzinfo=timezone.utc),
    )


class SocialEmptyStateCopyTests(unittest.IsolatedAsyncioTestCase):
    async def _run_twins(self, *, username, user_id=10, linked=False, ratings=None, status=None):
        db = SimpleNamespace(
            get_linked_user=AsyncMock(return_value=(
                {"profile_key": username.lower(), "backloggd_username": username, "avatar_url": None}
                if linked else None
            )),
            get_profile=AsyncMock(return_value=None),
            is_profile_library_subscribed=AsyncMock(return_value=False),
            get_profile_ratings_dict=AsyncMock(return_value=ratings or {}),
        )
        cog = SocialCog(SimpleNamespace(db=db))
        cog._get_or_fetch_ratings = AsyncMock(return_value=ratings or {})
        interaction = _interaction(user_id=user_id, guild=_guild(user_id, 11))
        target_status = status or _status(missing=not linked)
        with patch(
            "bot.cogs.social.ensure_profiles_fresh",
            new=AsyncMock(return_value={username.lower(): target_status}),
        ):
            await SocialCog.taste_twins.callback(cog, interaction, None if user_id == 10 and linked else username)
        return interaction, cog, db

    async def test_live_unlinked_empty_other_user_is_described_as_live(self):
        interaction, cog, _ = await self._run_twins(username="OtherPlayer")

        message = interaction.followup.send.await_args.args[0]
        self.assertIn("OtherPlayer", message)
        self.assertIn("live", message.lower())
        self.assertNotIn("cached", message.lower())
        self.assertNotIn("/refresh", message)
        cog._get_or_fetch_ratings.assert_awaited_once_with("OtherPlayer")

    async def test_linked_missing_library_keeps_fetching_state(self):
        interaction, _, db = await self._run_twins(
            username="OtherPlayer", linked=True, status=_status(missing=True),
        )
        message = interaction.followup.send.await_args.args[0]

        self.assertIn("currently being fetched", message)
        self.assertNotIn("no games", message.lower())
        db.get_profile_ratings_dict.assert_awaited_once_with("OtherPlayer")

    async def test_linked_fetched_empty_library_is_distinct_from_missing(self):
        interaction, _, _ = await self._run_twins(
            username="Caller", linked=True, ratings={}, status=_status(missing=False),
        )
        message = interaction.followup.send.await_args.args[0]

        self.assertIn("Your", message)
        self.assertIn("no logged games", message.lower())
        self.assertNotIn("/refresh", message)

    async def test_non_numeric_library_has_its_own_empty_state(self):
        interaction, _, _ = await self._run_twins(
            username="Caller", linked=True, ratings={"backlog": None}, status=_status(missing=False),
        )
        message = interaction.followup.send.await_args.args[0]

        self.assertIn("Your", message)
        self.assertIn("no numeric ratings", message)

    async def test_compare_empty_copy_identifies_caller_or_other_profile(self):
        for empty_side, expected in (("caller", "Your library"), ("other", "**OtherPlayer**'s library")):
            with self.subTest(empty_side=empty_side):
                db = SimpleNamespace(
                    get_linked_user=AsyncMock(return_value={
                        "profile_key": "caller", "backloggd_username": "Caller", "avatar_url": None,
                    }),
                    is_profile_library_subscribed=AsyncMock(return_value=False),
                )
                cog = SocialCog(SimpleNamespace(db=db))
                target = SimpleNamespace(ratings={} if empty_side == "other" else {"game": 4.0}, titles={})
                caller = SimpleNamespace(ratings={} if empty_side == "caller" else {"game": 4.0}, titles={})
                cog._get_or_fetch_library_snapshot = AsyncMock(side_effect=[target, caller])
                interaction = _interaction()
                statuses = {
                    "otherplayer": _status(missing=False),
                    "caller": _status(missing=False),
                }
                with patch(
                    "bot.cogs.social.ensure_profiles_fresh",
                    new=AsyncMock(return_value=statuses),
                ):
                    await SocialCog.compare.callback(cog, interaction, "OtherPlayer")

                message = interaction.followup.send.await_args.args[0]
                self.assertIn(expected, message)
                self.assertIn("no logged games", message.lower())


class FeedEnrollmentCopyTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.db = Database(str(Path(self.directory.name) / "task10.db"), turso_url="")
        await self.db.connect()
        self.profile_api = AsyncMock(return_value=SimpleNamespace(
            username="PublicPlayer", avatar_url=None,
        ))
        self.bot = SimpleNamespace(
            db=self.db,
            backloggd=SimpleNamespace(get_user_profile=self.profile_api),
        )
        self.cog = FeedCog(self.bot, clock=lambda: datetime(2026, 11, 17, tzinfo=timezone.utc))

    async def asyncTearDown(self):
        await self.db.close()
        self.directory.cleanup()

    async def _initialize_profile(self, *, username="Millers", discord_id=10, admin=False):
        if admin:
            await self.db.add_guild_admin_follow(1, username, None, 99)
        else:
            await self.db.link_user(discord_id, username)
            await self.db.set_guild_feed_preference(1, discord_id, True)
        await self.db.set_feed_channel(1, 50)
        await self.db.reconcile_guild_feed_activity(1, [username.lower()])
        await self.db.seed_feed_source(1, username, "activity-display-v1", ["activity-base"])
        await self.db.seed_feed_source(1, username, "journal-display-v1", ["journal-base"])

    async def test_first_feed_on_enrollment_says_baseline_is_pending(self):
        await self.db.link_user(10, "Millers")
        await self.db.set_feed_channel(1, 50)
        interaction = _interaction(guild=_guild(10))

        await FeedCog.feed_on.callback(self.cog, interaction)

        message = interaction.followup.send.await_args.args[0]
        self.assertIn("baseline", message.lower())
        self.assertIn("first", message.lower())

    async def test_repeated_active_feed_on_does_not_claim_first_check_or_reset(self):
        await self._initialize_profile()
        interaction = _interaction(guild=_guild(10))
        status_calls = 0
        original = self.db.get_guild_profile_feed_status

        async def counted_status(*args, **kwargs):
            nonlocal status_calls
            status_calls += 1
            return await original(*args, **kwargs)

        self.db.get_guild_profile_feed_status = counted_status
        await FeedCog.feed_on.callback(self.cog, interaction)

        message = interaction.followup.send.await_args.args[0]
        self.assertIn("already active", message.lower())
        self.assertNotIn("first check", message.lower())
        self.assertIn("does not reset", message.lower())
        self.assertEqual(status_calls, 1)

    async def test_feed_on_after_proven_inactive_period_says_resume_baseline(self):
        await self._initialize_profile()
        await self.db.disable_guild_feed_preference_and_reconcile(1, 10, member_ids=[10])
        status = await self.db.get_guild_profile_feed_status(1, "millers")
        self.assertFalse(status["active"])
        self.assertIsNotNone(status["last_active_at"])
        self.assertFalse(status.get("pending_reconciliation", False))
        interaction = _interaction(guild=_guild(10))

        await FeedCog.feed_on.callback(self.cog, interaction)

        message = interaction.followup.send.await_args.args[0]
        self.assertIn("resum", message.lower())
        self.assertIn("baseline", message.lower())

    async def test_feed_on_with_unavailable_membership_does_not_infer_resume(self):
        await self._initialize_profile()
        await self.db.link_user(11, "Millers")
        await self.db.set_guild_feed_preference(1, 11, True)
        await self.db.disable_guild_feed_preference_and_reconcile(1, 10, member_ids=None)
        self.assertIsNotNone(await self.db._fetchone(
            "SELECT 1 FROM guild_profile_feed_reconciliation WHERE guild_id = 1 AND profile_key = 'millers'"
        ))
        self.cog._authoritative_member_ids = AsyncMock(return_value=None)
        interaction = _interaction(guild=_guild(10))

        await FeedCog.feed_on.callback(self.cog, interaction)

        message = interaction.followup.send.await_args.args[0]
        self.assertIn("continuity", message.lower())
        self.assertIn("complete member snapshot", message.lower())
        self.assertNotIn("resume baseline", message.lower())

    async def test_repeated_active_feed_add_does_not_claim_first_check(self):
        await self._initialize_profile(username="PublicPlayer", admin=True)
        status_calls = 0
        original = self.db.get_guild_profile_feed_status

        async def counted_status(*args, **kwargs):
            nonlocal status_calls
            status_calls += 1
            return await original(*args, **kwargs)

        self.db.get_guild_profile_feed_status = counted_status
        interaction = _interaction(user_id=99, guild=_guild(99))

        await FeedCog.feed_add.callback(self.cog, interaction, "PublicPlayer")

        message = interaction.followup.send.await_args.args[0]
        self.assertIn("already active", message.lower())
        self.assertNotIn("first check", message.lower())
        self.assertEqual(status_calls, 1)

    async def test_feed_add_pending_without_channel_and_unavailable_membership_are_explicit(self):
        no_channel = _interaction(user_id=99, guild=_guild(99))
        await FeedCog.feed_add.callback(self.cog, no_channel, "PublicPlayer")
        first_message = no_channel.followup.send.await_args.args[0]
        self.assertIn("pending until", first_message.lower())
        self.assertNotIn("first check", first_message.lower())

        self.profile_api.return_value = SimpleNamespace(username="Unavailable", avatar_url=None)
        await self.db.set_feed_channel(1, 50)
        unavailable = _interaction(user_id=99, guild=_guild(99))
        self.cog._authoritative_member_ids = AsyncMock(return_value=None)
        await FeedCog.feed_add.callback(self.cog, unavailable, "Unavailable")
        second_message = unavailable.followup.send.await_args.args[0]
        self.assertIn("complete member snapshot", second_message.lower())
        self.assertIn("no discord delivery", second_message.lower())


if __name__ == "__main__":
    unittest.main()
