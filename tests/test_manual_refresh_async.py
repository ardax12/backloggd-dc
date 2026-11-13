"""Regression tests for prompt /refresh confirmation and best-effort status editing."""

import asyncio
import os
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

os.environ["PYTHON_DOTENV_DISABLED"] = "1"
os.environ["TURSO_DATABASE_URL"] = ""
os.environ["TURSO_AUTH_TOKEN"] = ""
os.environ["DEV_GUILD_ID"] = ""

import discord

from bot.cogs.account import AccountCog
from bot.cogs.feed import FeedCog
from bot.db import Database
from bot.feed_models import FeedEvent
from bot.services.library_sync import LibrarySyncService


class _FakeResponse:
    def __init__(self, status: int, reason: str):
        self.status = status
        self.reason = reason


class ManualRefreshAsyncTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmpdir.cleanup)
        self.db = Database(str(Path(self.tmpdir.name) / "refresh.db"), turso_url="")
        self.addAsyncCleanup(self.db.close)
        await self.db.connect()
        self.sync = LibrarySyncService()
        self.addAsyncCleanup(self.sync.close)
        await self.db.link_user(10, "Millers")
        await self.db.link_user(20, "millers")
        await self.db.upsert_game("hades", "Hades")
        await self.db.upsert_user_rating(10, "hades", 4.5, "played")

    def _interaction(self, user_id=10):
        status_message = SimpleNamespace(edit=AsyncMock())
        interaction = SimpleNamespace(
            user=SimpleNamespace(id=user_id, display_name=f"User{user_id}"),
            guild=SimpleNamespace(id=1, name="Guild"),
            guild_id=1,
            response=SimpleNamespace(defer=AsyncMock(), is_done=lambda: True),
            followup=SimpleNamespace(send=AsyncMock(return_value=status_message)),
        )
        return interaction, status_message

    async def test_refresh_responds_promptly_while_fetch_blocked_and_updates_on_completion(self):
        started = asyncio.Event()
        release = asyncio.Event()
        edited = asyncio.Event()

        async def blocked_library(username, force_refresh=False):
            started.set()
            await release.wait()
            return [
                SimpleNamespace(
                    slug="celeste", title="Celeste", cover_url=None,
                    release_year=2018, rating=5.0, status="played", is_liked=True,
                ),
                SimpleNamespace(
                    slug="balatro", title="Balatro", cover_url=None,
                    release_year=2024, rating=4.5, status="played", is_liked=False,
                ),
            ]

        api = SimpleNamespace(get_user_library=AsyncMock(side_effect=blocked_library))
        bot = SimpleNamespace(db=self.db, backloggd=api, library_sync=self.sync)
        cog = AccountCog(bot)

        interaction, status_message = self._interaction(user_id=10)
        original_edit = AsyncMock(side_effect=lambda **kwargs: edited.set())
        status_message.edit = original_edit

        # /refresh returns promptly without awaiting the blocked scrape.
        await asyncio.wait_for(AccountCog.refresh.callback(cog, interaction), timeout=0.5)
        await asyncio.wait_for(started.wait(), timeout=0.5)

        interaction.followup.send.assert_awaited_once()
        initial_embed = interaction.followup.send.call_args.kwargs["embed"]
        self.assertIn("refresh", initial_embed.title.lower())
        self.assertTrue(interaction.followup.send.call_args.kwargs.get("wait"))

        # Concurrent duplicate /refresh joins the running task instead of starting a second scrape.
        interaction2, status_message2 = self._interaction(user_id=20)
        edited2 = asyncio.Event()
        status_message2.edit = AsyncMock(side_effect=lambda **kwargs: edited2.set())
        await asyncio.wait_for(AccountCog.refresh.callback(cog, interaction2), timeout=0.5)
        join_embed = interaction2.followup.send.call_args.kwargs["embed"]
        self.assertIn("running", join_embed.description.lower())

        release.set()
        await asyncio.wait_for(edited.wait(), timeout=1.0)
        await asyncio.wait_for(edited2.wait(), timeout=1.0)

        api.get_user_library.assert_awaited_once_with("Millers", force_refresh=True)
        final_embed = original_edit.call_args.kwargs["embed"]
        self.assertIn("complete", final_embed.title.lower())
        self.assertIn("2", final_embed.description)

        # DB snapshot was atomically replaced.
        ratings = await self.db.get_profile_ratings_dict("millers")
        self.assertEqual(ratings, {"celeste": 5.0, "balatro": 4.5})

    async def test_refresh_continues_when_status_edit_fails_forbidden_or_not_found(self):
        for status_code, reason in ((403, "Forbidden"), (404, "Not Found")):
            async def fetch_library(username, force_refresh=False):
                return [
                    SimpleNamespace(
                        slug="celeste", title="Celeste", cover_url=None,
                        release_year=2018, rating=5.0, status="played", is_liked=False,
                    )
                ]

            api = SimpleNamespace(get_user_library=AsyncMock(side_effect=fetch_library))
            sync = LibrarySyncService()
            bot = SimpleNamespace(db=self.db, backloggd=api, library_sync=sync)
            cog = AccountCog(bot)
            interaction, status_message = self._interaction(user_id=10)
            edit_done = asyncio.Event()

            async def failing_edit(**kwargs):
                edit_done.set()
                if status_code == 403:
                    raise discord.Forbidden(_FakeResponse(403, "Forbidden"), "Missing Access")
                raise discord.NotFound(_FakeResponse(404, "Not Found"), "Unknown Message")

            status_message.edit = AsyncMock(side_effect=failing_edit)
            await AccountCog.refresh.callback(cog, interaction)
            await asyncio.wait_for(edit_done.wait(), timeout=1.0)
            await asyncio.sleep(0)
            # Library replacement still succeeded despite status edit error.
            ratings = await self.db.get_profile_ratings_dict("millers")
            self.assertEqual(ratings, {"celeste": 5.0})
            await sync.close()

    async def test_refresh_preserves_previous_library_on_failure_and_distinguishes_cancel_skip(self):
        api = SimpleNamespace(get_user_library=AsyncMock(side_effect=RuntimeError("503 upstream")))
        bot = SimpleNamespace(db=self.db, backloggd=api, library_sync=self.sync)
        cog = AccountCog(bot)
        interaction, status_message = self._interaction(user_id=10)
        edited = asyncio.Event()
        status_message.edit = AsyncMock(side_effect=lambda **kwargs: edited.set())

        await AccountCog.refresh.callback(cog, interaction)
        await asyncio.wait_for(edited.wait(), timeout=1.0)

        err_embed = status_message.edit.call_args.kwargs["embed"]
        self.assertIn("failed", err_embed.title.lower())
        self.assertIn("previous library was kept", err_embed.description.lower())
        # Previous library kept intact.
        self.assertEqual(await self.db.get_profile_ratings_dict("millers"), {"hades": 4.5})

        # Verify cancellation and skipped status messages are distinct from success.
        cancelled_task = asyncio.create_task(asyncio.sleep(10))
        cancelled_task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await cancelled_task
        cancel_msg = SimpleNamespace(edit=AsyncMock())
        await cog._update_link_status(cancelled_task, "Millers", cancel_msg)
        self.assertIn("stopped", cancel_msg.edit.call_args.kwargs["embed"].title.lower())

        self.sync.seed_success("millers", age_seconds=5, game_count=1)
        skip_task = self.sync.request("millers", AsyncMock(return_value=99), manual=False)
        skip_msg = SimpleNamespace(edit=AsyncMock())
        await skip_task
        await cog._update_link_status(skip_task, "Millers", skip_msg)
        skip_embed = skip_msg.edit.call_args.kwargs["embed"]
        self.assertNotIn("complete", skip_embed.title.lower())
        self.assertIn("cooldown", skip_embed.description.lower())

    async def test_unchanged_poll_during_running_sync_does_not_schedule_followup(self):
        started = asyncio.Event()
        release = asyncio.Event()
        calls = 0

        async def sync_library(username, force_refresh=False):
            nonlocal calls
            calls += 1
            if calls == 2:
                started.set()
                await release.wait()
            return [
                SimpleNamespace(
                    slug="hades", title="Hades", cover_url=None,
                    release_year=2020, rating=5.0, status="played", is_liked=False,
                )
            ]

        api = SimpleNamespace(get_user_library=AsyncMock(side_effect=sync_library))
        bot = SimpleNamespace(db=self.db, backloggd=api, library_sync=self.sync)
        account_cog = AccountCog(bot)
        feed_cog = FeedCog(bot)

        event = FeedEvent(
            "act-1", "Hades", "hades", None, "completed", "completed Hades",
            5.0, None, None, occurred_at="2026-11-12T10:00:00Z",
        )
        # First poll establishes observation baseline and completes initial sync (#1).
        self.assertTrue(await feed_cog._observe_profile_activity("millers", "Millers", {"activity": [event]}))
        await self.sync._profiles["millers"].task
        await asyncio.sleep(0)
        self.assertEqual(calls, 1)
        self.assertIsNone(await self.db.get_profile_refresh_pending_token("millers"))

        # Start manual refresh (#2) which blocks mid-scrape.
        interaction, status_message = self._interaction(user_id=10)
        edited = asyncio.Event()
        status_message.edit = AsyncMock(side_effect=lambda **kwargs: edited.set())
        await AccountCog.refresh.callback(account_cog, interaction)
        await asyncio.wait_for(started.wait(), timeout=0.5)

        # Two unchanged polls while refresh is running must NOT mark dirty_operation or schedule a follow-up.
        scheduled1 = await feed_cog._observe_profile_activity("millers", "Millers", {"activity": [event]})
        scheduled2 = await feed_cog._observe_profile_activity("millers", "Millers", {"activity": [event]})
        self.assertFalse(scheduled1)
        self.assertFalse(scheduled2)
        self.assertIsNone(self.sync._profiles["millers"].dirty_operation)

        release.set()
        await asyncio.wait_for(edited.wait(), timeout=1.0)
        await asyncio.sleep(0)
        self.assertEqual(calls, 2)


if __name__ == "__main__":
    unittest.main()
