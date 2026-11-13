import os
os.environ["PYTHON_DOTENV_DISABLED"] = "1"

import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

from bot.cogs.feed import FeedCog
from bot.db import Database


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


if __name__ == "__main__":
    unittest.main()
