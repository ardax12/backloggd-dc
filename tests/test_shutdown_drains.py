"""Task 4 regressions for cancellation-resilient cog drains and complete BackloggdBot teardown (F7)."""

import os

os.environ["PYTHON_DOTENV_DISABLED"] = "1"
os.environ["TURSO_DATABASE_URL"] = ""
os.environ["TURSO_AUTH_TOKEN"] = ""
os.environ["DEV_GUILD_ID"] = ""

import asyncio
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import discord
from backloggd import GameEntry, GameStatus, UserLibrary
from bot.client import BackloggdBot
from bot.cogs.account import AccountCog
from bot.cogs.feed import FeedCog
from bot.cogs.social import SocialCog
from bot.cogs.stats import StatsCog
from bot.db import Database
from bot.services.library_sync import LibrarySyncService


class ShutdownDrainTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmpdir.cleanup)
        self.db = Database(str(Path(self.tmpdir.name) / "shutdown_drains.db"), turso_url="")
        await self.db.connect()
        self.addAsyncCleanup(self.db.close)

    async def test_account_cog_close_survives_primary_and_repeated_cancellation_and_joins_secondary(self):
        sync_started = asyncio.Event()
        sync_cancelled = asyncio.Event()
        allow_unwind = asyncio.Event()
        used_sdk_after_close = False
        closed = False

        async def blocked_lib(username, force_refresh=False):
            sync_started.set()
            try:
                await allow_unwind.wait()
            except asyncio.CancelledError:
                sync_cancelled.set()
                await allow_unwind.wait()
                if closed:
                    nonlocal used_sdk_after_close
                    used_sdk_after_close = True
                raise
            return []

        await self.db.link_user(1, "Alpha", None)
        api = SimpleNamespace(get_user_library=AsyncMock(side_effect=blocked_lib))
        cog = AccountCog(SimpleNamespace(db=self.db, backloggd=api))
        status_msg = SimpleNamespace(edit=AsyncMock())
        sync_task = cog._start_sync_task(1, "Alpha", status_message=status_msg, manual=True)
        await asyncio.wait_for(sync_started.wait(), timeout=2.0)

        primary_close = asyncio.create_task(cog.close())
        secondary_close = asyncio.create_task(cog.close())
        await asyncio.wait_for(sync_cancelled.wait(), timeout=2.0)

        # Cancel primary caller repeatedly; drain must NOT die until child sync unwinds!
        primary_close.cancel()
        await asyncio.sleep(0)
        primary_close.cancel()
        await asyncio.sleep(0)
        self.assertFalse(sync_task.done())
        self.assertFalse(secondary_close.done())

        allow_unwind.set()
        with self.assertRaises(asyncio.CancelledError):
            await primary_close
        await asyncio.wait_for(secondary_close, timeout=2.0)
        closed = True
        await asyncio.sleep(0)
        self.assertTrue(sync_task.done())
        self.assertTrue(cog.library_sync._closed)
        self.assertEqual(cog._status_tasks, set())
        self.assertFalse(used_sdk_after_close)

    async def test_social_cog_close_survives_primary_and_repeated_cancellation_and_joins_secondary(self):
        fetch_started = asyncio.Event()
        fetch_cancelled = asyncio.Event()
        allow_unwind = asyncio.Event()

        async def blocked_fetch(username: str):
            fetch_started.set()
            try:
                await allow_unwind.wait()
            except asyncio.CancelledError:
                fetch_cancelled.set()
                await allow_unwind.wait()
                raise
            return UserLibrary(
                username=username,
                games={"hades": GameEntry(slug="hades", title="Hades", rating=4.5, status=GameStatus.PLAYED)},
            )

        api = SimpleNamespace(get_user_library=AsyncMock(side_effect=blocked_fetch))
        cog = SocialCog(SimpleNamespace(db=self.db, backloggd=api), live_fetch_timeout=10.0)
        waiter = asyncio.create_task(cog._get_or_fetch_ratings("Alice"))
        await asyncio.wait_for(fetch_started.wait(), timeout=2.0)

        primary_close = asyncio.create_task(cog.close())
        secondary_close = asyncio.create_task(cog.close())
        await asyncio.wait_for(fetch_cancelled.wait(), timeout=2.0)

        primary_close.cancel()
        await asyncio.sleep(0)
        primary_close.cancel()
        await asyncio.sleep(0)
        self.assertTrue(cog._tracked_live_tasks)
        self.assertFalse(secondary_close.done())

        allow_unwind.set()
        with self.assertRaises(asyncio.CancelledError):
            await primary_close
        await asyncio.wait_for(secondary_close, timeout=2.0)
        self.assertEqual(cog._tracked_live_tasks, set())
        self.assertEqual(dict(cog._live_library_tasks), {})
        self.assertEqual(dict(cog._live_library_waiters), {})
        with self.assertRaises(asyncio.CancelledError):
            await waiter

    async def test_stats_cog_close_survives_repeated_cancellation(self):
        fetch_started = asyncio.Event()
        fetch_cancelled = asyncio.Event()
        allow_unwind = asyncio.Event()

        async def blocked_profile(_username):
            fetch_started.set()
            try:
                await allow_unwind.wait()
            except asyncio.CancelledError:
                fetch_cancelled.set()
                await allow_unwind.wait()
                raise

        bot = SimpleNamespace(
            db=SimpleNamespace(get_guild_linked_profiles=AsyncMock(return_value=[])),
            backloggd=SimpleNamespace(get_user_profile=blocked_profile),
        )
        cog = StatsCog(bot, profile_count_concurrency=1)
        op_task = asyncio.create_task(cog._played_tab_counts([1], profiles=[{"display_username": "Alice"}]))
        await asyncio.wait_for(fetch_started.wait(), timeout=2.0)

        close_task = asyncio.create_task(cog.close())
        await asyncio.wait_for(fetch_cancelled.wait(), timeout=2.0)
        close_task.cancel()
        await asyncio.sleep(0)
        close_task.cancel()
        await asyncio.sleep(0)
        # Drain must still be waiting on the blocked worker after two cancellations!
        self.assertFalse(close_task.done())

        allow_unwind.set()
        with self.assertRaises(asyncio.CancelledError):
            await close_task
        with self.assertRaises(asyncio.CancelledError):
            await op_task
        self.assertEqual(cog._profile_count_workers, set())

    async def test_bot_close_survives_early_stage_cancellation_concurrent_callers_and_preserves_order(self):
        events: list[str] = []
        feed_entered = asyncio.Event()
        allow_feed_finish = asyncio.Event()

        with patch("bot.client.DATABASE_PATH", str(Path(self.tmpdir.name) / "bot_cancel.db")):
            bot = BackloggdBot()
        await bot.db.close()
        bot.db = self.db

        async def blocked_feed_close():
            feed_entered.set()
            await allow_feed_finish.wait()
            events.append("feed")

        async def rec_close(name: str, orig=None):
            events.append(name)
            if orig is not None:
                await orig()

        orig_sync_close = bot.library_sync.close
        orig_sdk_close = bot.backloggd.close
        orig_db_close = bot.db.close
        orig_membership_close = bot.guild_membership.close

        def _make_recorder(name: str, orig=None):
            async def _rec():
                events.append(name)
                if orig is not None:
                    await orig()
            return _rec

        async def _discord_close():
            events.append("discord")

        await bot.add_cog(FeedCog(bot))
        await bot.add_cog(StatsCog(bot))
        await bot.add_cog(AccountCog(bot))
        await bot.add_cog(SocialCog(bot))

        bot.get_cog("Feed").close = AsyncMock(side_effect=blocked_feed_close)
        bot.get_cog("Stats").close = AsyncMock(side_effect=_make_recorder("stats"))
        bot.get_cog("Account").close = AsyncMock(side_effect=_make_recorder("account"))
        bot.get_cog("Social").close = AsyncMock(side_effect=_make_recorder("social"))
        bot.guild_membership.close = AsyncMock(side_effect=_make_recorder("guild_membership", orig_membership_close))
        bot.library_sync.close = AsyncMock(side_effect=_make_recorder("library_sync", orig_sync_close))
        bot.backloggd.close = AsyncMock(side_effect=_make_recorder("backloggd", orig_sdk_close))
        bot.db.close = AsyncMock(side_effect=_make_recorder("db", orig_db_close))

        with patch.object(discord.ext.commands.Bot, "close", AsyncMock(side_effect=_discord_close)):
            c1 = asyncio.create_task(bot.close())
            c2 = asyncio.create_task(bot.close())
            await asyncio.wait_for(feed_entered.wait(), timeout=2.0)

            # Cancel primary bot.close() caller while feed.close is blocked -> teardown MUST still finish all stages!
            c1.cancel()
            await asyncio.sleep(0)
            c1.cancel()
            await asyncio.sleep(0)
            self.assertEqual(events, [])

            allow_feed_finish.set()
            with self.assertRaises(asyncio.CancelledError):
                await c1
            await asyncio.wait_for(c2, timeout=2.0)
            # Re-entrant idempotent close joins cleanly
            await bot.close()

        self.assertEqual(
            events,
            ["feed", "stats", "account", "social", "guild_membership", "library_sync", "backloggd", "db", "discord"],
        )

    async def test_bot_close_isolates_stage_error_and_closes_later_resources(self):
        events: list[str] = []
        with patch("bot.client.DATABASE_PATH", str(Path(self.tmpdir.name) / "bot_err.db")):
            bot = BackloggdBot()
        await bot.db.close()
        bot.db = self.db

        orig_sync_close = bot.library_sync.close
        orig_sdk_close = bot.backloggd.close
        orig_db_close = bot.db.close
        orig_membership_close = bot.guild_membership.close

        await bot.add_cog(FeedCog(bot))
        await bot.add_cog(StatsCog(bot))
        await bot.add_cog(AccountCog(bot))
        await bot.add_cog(SocialCog(bot))

        async def failing_feed_close():
            events.append("feed_err")
            raise RuntimeError("feed close boom")

        def _make_recorder(name: str, orig=None):
            async def _rec():
                events.append(name)
                if orig is not None:
                    await orig()
            return _rec

        async def _discord_close():
            events.append("discord")

        bot.get_cog("Feed").close = AsyncMock(side_effect=failing_feed_close)
        bot.get_cog("Stats").close = AsyncMock(side_effect=_make_recorder("stats"))
        bot.get_cog("Account").close = AsyncMock(side_effect=_make_recorder("account"))
        bot.get_cog("Social").close = AsyncMock(side_effect=_make_recorder("social"))
        bot.guild_membership.close = AsyncMock(side_effect=_make_recorder("guild_membership", orig_membership_close))
        bot.library_sync.close = AsyncMock(side_effect=_make_recorder("library_sync", orig_sync_close))
        bot.backloggd.close = AsyncMock(side_effect=_make_recorder("backloggd", orig_sdk_close))
        bot.db.close = AsyncMock(side_effect=_make_recorder("db", orig_db_close))

        with patch.object(discord.ext.commands.Bot, "close", AsyncMock(side_effect=_discord_close)):
            with self.assertRaisesRegex(RuntimeError, "feed close boom"):
                await bot.close()

        self.assertEqual(
            events,
            ["feed_err", "stats", "account", "social", "guild_membership", "library_sync", "backloggd", "db", "discord"],
        )


if __name__ == "__main__":
    unittest.main()
