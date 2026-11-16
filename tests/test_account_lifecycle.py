"""Lifecycle and shutdown regression tests for Task 7 (AccountCog + BackloggdBot)."""

import os

os.environ["PYTHON_DOTENV_DISABLED"] = "1"
os.environ["TURSO_DATABASE_URL"] = ""
os.environ["TURSO_AUTH_TOKEN"] = ""
os.environ["DEV_GUILD_ID"] = ""

import asyncio
import gc
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, MagicMock

import discord

from bot.client import BackloggdBot
from bot.cogs.account import AccountCog
from bot.db import Database
from bot.services.library_sync import LibrarySyncService


class AccountLifecycleTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmpdir.cleanup)
        self.db = Database(str(Path(self.tmpdir.name) / "lifecycle.db"), turso_url="")
        await self.db.connect()
        self.addAsyncCleanup(self.db.close)

    async def test_owned_fallback_unload_cancels_queued_and_running_syncs_and_is_idempotent(self):
        started = asyncio.Event()
        release = asyncio.Event()
        db_writes_after_close = []
        closed_flag = False

        async def slow_lib(username, force_refresh=False):
            started.set()
            await release.wait()
            if closed_flag:
                db_writes_after_close.append(username)
            return [
                SimpleNamespace(
                    slug="hades",
                    title="Hades",
                    status="played",
                    rating=5.0,
                    cover_url=None,
                    release_year=2020,
                )
            ]

        await self.db.link_user(1, "Alpha", None)
        await self.db.link_user(2, "Beta", None)
        await self.db.link_user(3, "Gamma", None)
        await self.db.replace_user_library(
            1,
            "Alpha",
            [SimpleNamespace(slug="old-game", title="Old Game", status="played", rating=4.0, cover_url=None, release_year=2020)],
        )

        api = SimpleNamespace(get_user_library=AsyncMock(side_effect=slow_lib))
        cog = AccountCog(SimpleNamespace(db=self.db, backloggd=api))
        self.assertTrue(cog._owns_library_sync)

        edits = []
        status_msg = SimpleNamespace(edit=AsyncMock(side_effect=lambda **kw: edits.append(kw)))

        # max_workers=2 -> 2 running + 1 queued
        t1 = cog._start_sync_task(1, "Alpha", status_message=status_msg, manual=True)
        t2 = cog._start_sync_task(2, "Beta", status_message=status_msg, manual=True)
        t3 = cog._start_sync_task(3, "Gamma", status_message=status_msg, manual=True)
        await asyncio.wait_for(started.wait(), timeout=2)

        await asyncio.gather(cog.cog_unload(), cog.close())
        closed_flag = True
        await cog.cog_unload()

        self.assertTrue(cog.library_sync._closed)
        self.assertTrue(t1.done())
        self.assertTrue(t2.done())
        self.assertTrue(t3.done())
        self.assertEqual(cog._status_tasks, set())

        release.set()
        await asyncio.sleep(0)
        self.assertEqual(edits, [])
        self.assertEqual(db_writes_after_close, [])
        # Existing cached archive preserved untouched
        alpha_ratings = await self.db.get_profile_ratings_dict("Alpha")
        self.assertEqual(alpha_ratings, {"old-game": 4.0})

    async def test_unload_drains_active_status_edit_without_closing_shared_coordinator(self):
        sync_started = asyncio.Event()
        release_sync = asyncio.Event()
        edit_started = asyncio.Event()
        release_edit = asyncio.Event()

        async def fetch_alpha(username, force_refresh=False):
            return []

        async def fetch_beta(username, force_refresh=False):
            sync_started.set()
            await release_sync.wait()
            return []

        shared_sync = LibrarySyncService(max_workers=2)
        self.addAsyncCleanup(shared_sync.close)

        await self.db.link_user(1, "Alpha", None)
        await self.db.link_user(2, "Beta", None)

        api_a = SimpleNamespace(get_user_library=AsyncMock(side_effect=fetch_alpha))
        api_b = SimpleNamespace(get_user_library=AsyncMock(side_effect=fetch_beta))

        cog = AccountCog(SimpleNamespace(db=self.db, backloggd=api_a, library_sync=shared_sync))
        self.assertFalse(cog._owns_library_sync)

        async def slow_edit(**kwargs):
            edit_started.set()
            await release_edit.wait()

        status_msg = SimpleNamespace(edit=AsyncMock(side_effect=slow_edit))
        # Trigger sync A that finishes immediately and starts an in-flight status_edit
        await cog._start_sync_task(1, "Alpha", status_message=status_msg, manual=True)
        await asyncio.wait_for(edit_started.wait(), timeout=2)

        # Unrelated subscriber starts a shared sync on the same coordinator
        shared_task = shared_sync.request("beta", lambda: fetch_beta("Beta"), manual=True)
        await asyncio.wait_for(sync_started.wait(), timeout=2)

        # Unloading AccountCog cancels/drains its active status edit, but leaves shared_sync open & beta running!
        await cog.cog_unload()
        self.assertEqual(cog._status_tasks, set())
        self.assertFalse(shared_sync._closed)
        self.assertFalse(shared_task.done())

        release_sync.set()
        outcome = await asyncio.wait_for(shared_task, timeout=2)
        self.assertEqual(outcome.status, "success")

    async def test_completion_callback_racing_shutdown_never_edits_discord_after_close(self):
        sync_started = asyncio.Event()
        release_sync = asyncio.Event()

        async def slow_lib(username, force_refresh=False):
            sync_started.set()
            await release_sync.wait()
            return []

        await self.db.link_user(1, "Alpha", None)
        api = SimpleNamespace(get_user_library=AsyncMock(side_effect=slow_lib))
        shared_sync = LibrarySyncService()
        self.addAsyncCleanup(shared_sync.close)
        cog = AccountCog(SimpleNamespace(db=self.db, backloggd=api, library_sync=shared_sync))

        edits = []
        status_msg = SimpleNamespace(edit=AsyncMock(side_effect=lambda **kw: edits.append(kw)))
        shared_task = cog._start_sync_task(1, "Alpha", status_message=status_msg, manual=True)
        await asyncio.wait_for(sync_started.wait(), timeout=2)

        # Complete sync AND immediately close cog before callbacks run
        release_sync.set()
        await cog.close()
        await shared_task
        await asyncio.sleep(0)
        await asyncio.sleep(0)

        self.assertEqual(edits, [])
        self.assertEqual(cog._status_tasks, set())

    async def test_unexpected_status_edit_exception_is_retrieved_and_logged(self):
        await self.db.link_user(1, "Alpha", None)
        api = SimpleNamespace(get_user_library=AsyncMock(return_value=[]))
        shared_sync = LibrarySyncService()
        self.addAsyncCleanup(shared_sync.close)
        cog = AccountCog(SimpleNamespace(db=self.db, backloggd=api, library_sync=shared_sync))
        self.addAsyncCleanup(cog.close)

        status_msg = SimpleNamespace(edit=AsyncMock(side_effect=RuntimeError("unexpected edit failure")))
        with self.assertLogs("backloggd.cogs.account", level="ERROR") as logs:
            task = cog._start_sync_task(1, "Alpha", status_message=status_msg, manual=True)
            await task
            await asyncio.sleep(0)
            await asyncio.sleep(0)

        self.assertTrue(any("unexpected edit failure" in rec.getMessage() or rec.exc_info for rec in logs.records))
        self.assertEqual(cog._status_tasks, set())
        gc.collect()

    async def test_bot_close_ordering_drains_account_and_sync_before_sdk_db_and_discord(self):
        order = []
        sync_started = asyncio.Event()
        release_sync = asyncio.Event()

        bot = BackloggdBot()
        await bot.db.close()
        bot.db = self.db
        await self.db.link_user(1, "Alpha", None)

        orig_db_close = bot.db.close
        orig_sync_close = bot.library_sync.close
        orig_super_close = discord.ext.commands.Bot.close

        async def db_close_spy():
            order.append("db")
            await orig_db_close()

        async def sync_close_spy():
            order.append("library_sync")
            await orig_sync_close()

        async def backloggd_close_spy():
            order.append("backloggd")

        async def membership_close_spy():
            order.append("guild_membership")

        bot.db.close = db_close_spy
        bot.library_sync.close = sync_close_spy
        bot.backloggd.close = backloggd_close_spy
        bot.guild_membership.close = membership_close_spy

        await bot.load_extension("bot.cogs.account")
        account_cog = bot.get_cog("Account")
        orig_account_close = account_cog.close

        async def account_close_spy():
            order.append("account")
            await orig_account_close()

        account_cog.close = account_close_spy

        async def slow_lib(username, force_refresh=False):
            sync_started.set()
            await release_sync.wait()
            return []

        bot.backloggd.get_user_library = AsyncMock(side_effect=slow_lib)
        bot.backloggd.get_user_playing = AsyncMock(return_value=[])
        bot.backloggd.get_user_backlog = AsyncMock(return_value=[])
        bot.backloggd.get_user_wishlist = AsyncMock(return_value=[])
        post_close_edits = []
        status_msg = SimpleNamespace(
            edit=AsyncMock(side_effect=lambda **kw: post_close_edits.append((list(order), kw)))
        )

        account_cog._start_sync_task(1, "Alpha", status_message=status_msg, manual=True)
        await asyncio.wait_for(sync_started.wait(), timeout=2)

        with unittest.mock.patch.object(
            discord.ext.commands.Bot,
            "close",
            AsyncMock(side_effect=lambda *a, **kw: order.append("discord_http")),
        ):
            await bot.close()

        self.assertLess(order.index("account"), order.index("library_sync"))
        self.assertLess(order.index("library_sync"), order.index("backloggd"))
        self.assertLess(order.index("library_sync"), order.index("db"))
        self.assertLess(order.index("db"), order.index("discord_http"))
        self.assertEqual(post_close_edits, [])
        self.assertEqual(account_cog._status_tasks, set())


if __name__ == "__main__":
    unittest.main()
