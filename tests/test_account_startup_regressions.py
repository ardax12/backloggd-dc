"""Regression coverage for startup command sync and account-link interaction races."""

import asyncio
import os
from pathlib import Path
import tempfile
import unittest
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

os.environ["PYTHON_DOTENV_DISABLED"] = "1"
os.environ["TURSO_DATABASE_URL"] = ""
os.environ["TURSO_AUTH_TOKEN"] = ""
os.environ["DEV_GUILD_ID"] = ""

from bot.client import BackloggdBot
from bot.cogs.account import AccountCog
from bot.db import Database


class StartupExtensionRegressions(unittest.IsolatedAsyncioTestCase):
    async def test_required_extension_failure_aborts_before_command_sync(self):
        bot = BackloggdBot()
        bot.db.connect = AsyncMock()
        bot.db.close = AsyncMock()
        bot.backloggd.close = AsyncMock()
        bot.library_sync.close = AsyncMock()
        bot.guild_membership.close = AsyncMock()
        bot._connection.application_id = 123
        bot.tree.sync = AsyncMock(return_value=[])
        bot.tree.copy_global_to = MagicMock()

        loaded = []

        async def load_extension(extension):
            loaded.append(extension)
            if extension == "bot.cogs.games":
                raise RuntimeError("extension import failed")

        bot.load_extension = AsyncMock(side_effect=load_extension)
        try:
            with self.assertRaisesRegex(RuntimeError, "bot.cogs.games"):
                await bot.setup_hook()

            self.assertEqual(loaded, ["bot.cogs.account", "bot.cogs.games"])
            bot.tree.sync.assert_not_awaited()
            bot.tree.copy_global_to.assert_not_called()
        finally:
            await bot.close()


class AccountRaceRegressions(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tempdir = tempfile.TemporaryDirectory()
        self.db = Database(str(Path(self.tempdir.name) / "account-races.db"), turso_url="")
        await self.db.connect()

    async def asyncTearDown(self):
        await self.db.close()
        self.tempdir.cleanup()

    async def test_concurrent_link_requests_cannot_replace_the_winning_profile(self):
        both_lookups_started = asyncio.Event()
        release_lookups = asyncio.Event()
        profile_lookups = 0

        async def get_user_profile(username):
            nonlocal profile_lookups
            profile_lookups += 1
            if profile_lookups == 2:
                both_lookups_started.set()
            await release_lookups.wait()
            return SimpleNamespace(username=username, avatar_url=None)

        api = SimpleNamespace(get_user_profile=get_user_profile)
        cog = AccountCog(SimpleNamespace(db=self.db, backloggd=api))
        cog._seed_sync_state = AsyncMock()
        cog._start_sync_task = MagicMock()

        def make_interaction():
            return SimpleNamespace(
                user=SimpleNamespace(id=42),
                guild=None,
                response=SimpleNamespace(defer=AsyncMock()),
                followup=SimpleNamespace(send=AsyncMock()),
            )

        interaction_a = make_interaction()
        interaction_b = make_interaction()
        task_a = asyncio.create_task(AccountCog.link.callback(cog, interaction_a, "Alpha"))
        task_b = asyncio.create_task(AccountCog.link.callback(cog, interaction_b, "Beta"))
        try:
            await asyncio.wait_for(both_lookups_started.wait(), timeout=1)
        finally:
            release_lookups.set()
        await asyncio.gather(task_a, task_b)

        saved_link = await self.db.get_linked_user(42)
        self.assertIn(saved_link["backloggd_username"], {"Alpha", "Beta"})
        sends = [interaction.followup.send for interaction in (interaction_a, interaction_b)]
        success_count = sum("embed" in send.call_args.kwargs for send in sends)
        conflict_messages = [
            send.call_args.args[0]
            for send in sends
            if send.call_args.args and "embed" not in send.call_args.kwargs
        ]
        self.assertEqual(success_count, 1)
        self.assertEqual(len(conflict_messages), 1)
        self.assertIn("linked", conflict_messages[0].lower())
        self.assertEqual(cog._start_sync_task.call_count, 1)
        self.assertEqual(cog._seed_sync_state.await_count, 1)

    async def test_guild_link_uses_member_snapshot_to_preserve_overlapping_feed_continuity(self):
        t0 = datetime(2026, 11, 13, 8, 0, tzinfo=timezone.utc)
        for discord_id in (10, 11):
            await self.db.link_user(discord_id, "Millers")
            await self.db.set_guild_feed_preference(1, discord_id, True)
        await self.db.reconcile_guild_feed_activity(1, ["millers"], now=t0)
        await self.db.seed_feed_source(1, "millers", "activity-display-v1", ["old-event"])
        await self.db.record_feed_event_decision(
            1, "millers", "validated-work", source="activity", event_date="2026-11-13",
            decision="eligible", reason="recent", payload="{}", observed_at=t0,
        )
        await self.db.store_feed_message(
            1, "millers", ("archived-event",), "{}", 501, 100, delivery_state="deleted",
        )

        # M1 unlinks in a DM; membership is unavailable to the global unlink operation.
        await self.db.unlink_user(10)
        pending = await self.db._fetchone(
            "SELECT candidate_member_ids FROM guild_profile_feed_reconciliation WHERE guild_id = 1 AND profile_key = 'millers'"
        )
        self.assertEqual(pending["candidate_member_ids"], "[11]")

        api = SimpleNamespace(
            get_user_profile=AsyncMock(return_value=SimpleNamespace(username="Millers", avatar_url=None))
        )
        bot = SimpleNamespace(db=self.db, backloggd=api)
        cog = AccountCog(bot)
        cog._seed_sync_state = AsyncMock()
        cog._start_sync_task = MagicMock()
        interaction = SimpleNamespace(
            user=SimpleNamespace(id=12, display_name="Member 3"),
            guild=SimpleNamespace(
                id=1, chunked=True, member_count=3,
                members=[SimpleNamespace(id=value, display_name=f"Member {value}") for value in (10, 11, 12)],
            ),
            guild_id=1,
            response=SimpleNamespace(defer=AsyncMock()),
            followup=SimpleNamespace(send=AsyncMock(return_value=object())),
        )
        await AccountCog.link.callback(cog, interaction, "Millers")

        self.assertIsNone(await self.db._fetchone(
            "SELECT 1 FROM guild_profile_feed_reconciliation WHERE guild_id = 1 AND profile_key = 'millers'"
        ))
        await self.db.unlink_user(11)

        status = await self.db.get_guild_profile_feed_status(1, "millers")
        self.assertTrue(status["active"])
        self.assertTrue(await self.db.has_feed_source(1, "millers", "activity-display-v1"))
        work = await self.db.get_feed_event_decision(1, "millers", "validated-work")
        self.assertEqual(work["decision"], "eligible")
        self.assertEqual(work["observed_at"], "2026-11-13 08:00:00")
        archive = {row["event_id"]: row for row in await self.db.get_feed_messages(1, "millers")}
        self.assertEqual(archive["archived-event"]["delivery_state"], "deleted")

    async def test_unlink_defers_before_slow_database_cleanup(self):
        cleanup_started = asyncio.Event()
        release_cleanup = asyncio.Event()

        async def slow_unlink(discord_id):
            cleanup_started.set()
            await release_cleanup.wait()
            return True

        interaction = SimpleNamespace(
            user=SimpleNamespace(id=42),
            response=SimpleNamespace(defer=AsyncMock(), send_message=AsyncMock()),
            followup=SimpleNamespace(send=AsyncMock()),
        )
        cog = AccountCog(SimpleNamespace(db=SimpleNamespace(unlink_user=slow_unlink)))
        unlink_task = asyncio.create_task(AccountCog.unlink.callback(cog, interaction))

        try:
            await asyncio.wait_for(cleanup_started.wait(), timeout=1)
            interaction.response.defer.assert_awaited_once_with(ephemeral=True)
            interaction.followup.send.assert_not_awaited()
        finally:
            release_cleanup.set()
        await unlink_task

        interaction.followup.send.assert_awaited_once()
        self.assertIn("Successfully disconnected", interaction.followup.send.call_args.args[0])
        self.assertTrue(interaction.followup.send.call_args.kwargs["ephemeral"])

    async def test_same_profile_concurrent_requests_in_different_guilds_preserve_enrollment_and_dm_guidance(self):
        both_lookups_started = asyncio.Event()
        release_lookups = asyncio.Event()
        lookups = 0

        async def get_user_profile(username):
            nonlocal lookups
            lookups += 1
            if lookups == 2:
                both_lookups_started.set()
            await release_lookups.wait()
            return SimpleNamespace(username="Millers", avatar_url=None)

        api = SimpleNamespace(get_user_profile=get_user_profile)
        cog = AccountCog(SimpleNamespace(db=self.db, backloggd=api))
        cog._seed_sync_state = AsyncMock()
        cog._start_sync_task = MagicMock()

        def make_interaction(guild_id):
            return SimpleNamespace(
                user=SimpleNamespace(id=42),
                guild=SimpleNamespace(id=guild_id, chunked=True, member_count=1, members=[SimpleNamespace(id=42, display_name="User")]),
                guild_id=guild_id,
                response=SimpleNamespace(defer=AsyncMock()),
                followup=SimpleNamespace(send=AsyncMock(return_value=object())),
            )

        i_g1 = make_interaction(101)
        i_g2 = make_interaction(202)
        t1 = asyncio.create_task(AccountCog.link.callback(cog, i_g1, "Millers"))
        t2 = asyncio.create_task(AccountCog.link.callback(cog, i_g2, "millers"))
        await asyncio.wait_for(both_lookups_started.wait(), timeout=1)
        release_lookups.set()
        await asyncio.gather(t1, t2)

        # Both guilds must be enrolled for user 42
        self.assertTrue(await self.db.get_guild_feed_preference(101, 42))
        self.assertTrue(await self.db.get_guild_feed_preference(202, 42))

        loser_messages = [
            send.call_args.args[0]
            for send in (i_g1.followup.send, i_g2.followup.send)
            if send.call_args.args and "embed" not in send.call_args.kwargs
        ]
        self.assertEqual(len(loser_messages), 1)
        self.assertIn("enabled for this server", loser_messages[0].lower())
        self.assertEqual(cog._start_sync_task.call_count, 1)

        # Also test loser running in DM when guild link wins first: loser must include /feed on guidance
        await self.db.unlink_user(42)
        lookups = 0
        both_in_profile = asyncio.Event()
        release_dm_lookup = asyncio.Event()

        async def staged_profile(username):
            nonlocal lookups
            lookups += 1
            if lookups == 1:
                await both_in_profile.wait()
                return SimpleNamespace(username="Millers", avatar_url=None)
            both_in_profile.set()
            await release_dm_lookup.wait()
            return SimpleNamespace(username="Millers", avatar_url=None)

        i_g4 = make_interaction(404)
        i_dm2 = SimpleNamespace(
            user=SimpleNamespace(id=42),
            guild=None,
            guild_id=None,
            response=SimpleNamespace(defer=AsyncMock()),
            followup=SimpleNamespace(send=AsyncMock(return_value=object())),
        )
        api.get_user_profile = staged_profile
        tg4 = asyncio.create_task(AccountCog.link.callback(cog, i_g4, "Millers"))
        tdm2 = asyncio.create_task(AccountCog.link.callback(cog, i_dm2, "Millers"))
        await asyncio.wait_for(tg4, timeout=1)
        release_dm_lookup.set()
        await asyncio.wait_for(tdm2, timeout=1)

        dm_msg = i_dm2.followup.send.call_args.args[0]
        self.assertIn("/feed on", dm_msg.lower())

    async def test_absent_current_link_after_concurrent_unlink_reports_change_without_relink_or_sync(self):
        # Simulate race: user passes pre-check (no link), another caller links & unlinks,
        # conditional insert loses (returns False) and get_linked_user returns None!
        api = SimpleNamespace(
            get_user_profile=AsyncMock(return_value=SimpleNamespace(username="Millers", avatar_url=None))
        )
        fake_db = SimpleNamespace(
            get_linked_user=AsyncMock(side_effect=[None, None]),
            link_user_if_unlinked=AsyncMock(return_value=False),
            set_guild_feed_preference=AsyncMock(),
        )
        cog = AccountCog(SimpleNamespace(db=fake_db, backloggd=api))
        cog._seed_sync_state = AsyncMock()
        cog._start_sync_task = MagicMock()

        interaction = SimpleNamespace(
            user=SimpleNamespace(id=42),
            guild=SimpleNamespace(id=55, members=[]),
            guild_id=55,
            response=SimpleNamespace(defer=AsyncMock()),
            followup=SimpleNamespace(send=AsyncMock()),
        )
        await AccountCog.link.callback(cog, interaction, "Millers")

        fake_db.set_guild_feed_preference.assert_not_awaited()
        cog._seed_sync_state.assert_not_awaited()
        cog._start_sync_task.assert_not_called()
        msg = interaction.followup.send.call_args.args[0].lower()
        self.assertIn("changed", msg)
        self.assertIn("/link", msg)
        self.assertNotIn("already linked", msg)
        self.assertNotIn("run `/unlink`", msg)

    async def test_whitespace_normalized_consistently_in_link_user_if_unlinked_on_sqlite_and_turso(self):
        from tests.test_db_transaction_adapter import _TrackingTursoClient

        turso_path = Path(self.tempdir.name) / "turso_norm.db"
        seed = Database(str(turso_path), turso_url="")
        await seed.connect()
        await seed.close()
        turso_client = _TrackingTursoClient(str(turso_path))
        turso_db = Database(str(turso_path), turso_url="https://norm-test.turso.io")
        turso_db._client = turso_client
        try:
            for label, db in (("sqlite", self.db), ("turso", turso_db)):
                with self.subTest(backend=label):
                    ok = await db.link_user_if_unlinked(42, "  Millers  ", None)
                    self.assertTrue(ok)
                    # Different Discord member may still link the same profile
                    ok2 = await db.link_user_if_unlinked(43, "  Millers  ", None)
                    self.assertTrue(ok2)

                    linked_row = await db.get_linked_user(42)
                    self.assertEqual(linked_row["profile_key"], "millers")
                    self.assertEqual(linked_row["backloggd_username"], "Millers")
                    profile_row = await db.get_profile("millers")
                    self.assertEqual(profile_row["profile_key"], "millers")
                    self.assertEqual(profile_row["display_username"], "Millers")
        finally:
            await turso_db.close()

    async def test_different_profile_concurrent_links_across_guilds_do_not_enroll_loser_guild(self):
        both_lookups_started = asyncio.Event()
        release_lookups = asyncio.Event()
        lookups = 0

        async def get_user_profile(username):
            nonlocal lookups
            lookups += 1
            if lookups == 2:
                both_lookups_started.set()
            await release_lookups.wait()
            return SimpleNamespace(username=username, avatar_url=None)

        api = SimpleNamespace(get_user_profile=get_user_profile)
        cog = AccountCog(SimpleNamespace(db=self.db, backloggd=api))
        cog._seed_sync_state = AsyncMock()
        cog._start_sync_task = MagicMock()

        def make_interaction(guild_id):
            return SimpleNamespace(
                user=SimpleNamespace(id=42),
                guild=SimpleNamespace(id=guild_id, chunked=True, member_count=1, members=[SimpleNamespace(id=42, display_name="User")]),
                guild_id=guild_id,
                response=SimpleNamespace(defer=AsyncMock()),
                followup=SimpleNamespace(send=AsyncMock(return_value=object())),
            )

        i_alpha = make_interaction(501)
        i_beta = make_interaction(502)
        t_alpha = asyncio.create_task(AccountCog.link.callback(cog, i_alpha, "Alpha"))
        t_beta = asyncio.create_task(AccountCog.link.callback(cog, i_beta, "Beta"))
        await asyncio.wait_for(both_lookups_started.wait(), timeout=1)
        release_lookups.set()
        await asyncio.gather(t_alpha, t_beta)

        saved_link = await self.db.get_linked_user(42)
        if saved_link["backloggd_username"] == "Alpha":
            self.assertTrue(await self.db.get_guild_feed_preference(501, 42))
            self.assertFalse(await self.db.get_guild_feed_preference(502, 42))
        else:
            self.assertEqual(saved_link["backloggd_username"], "Beta")
            self.assertTrue(await self.db.get_guild_feed_preference(502, 42))
            self.assertFalse(await self.db.get_guild_feed_preference(501, 42))
        self.assertEqual(cog._start_sync_task.call_count, 1)
        self.assertEqual(cog._seed_sync_state.await_count, 1)


if __name__ == "__main__":
    unittest.main()
