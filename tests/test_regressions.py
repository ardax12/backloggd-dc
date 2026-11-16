import os
os.environ["PYTHON_DOTENV_DISABLED"] = "1"
import sqlite3
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock
from bot.ui.embeds import create_all_ratings_embed, create_profile_embed
from bot.cogs.account import AccountCog

class EmbedTests(unittest.TestCase):
    def test_ratings_fit_field_limits(self):
        rows = [{"backloggd_username": f"TestUser{i:02}", "rating": 4.5, "status": "played"} for i in range(20)]
        embed = create_all_ratings_embed("Hades", "hades", "Guild", rows)
        self.assertTrue(all(len(f.value) <= 1024 for f in embed.fields))
        self.assertLessEqual(len(embed), 6000)

    def test_profile_reads_database_rows(self):
        with sqlite3.connect(":memory:") as conn:
            conn.row_factory = sqlite3.Row
            row = conn.execute("SELECT 'Hades' AS game_title, 4.5 AS rating").fetchone()
        profile = SimpleNamespace(username="Tester", avatar_url=None, bio=None, badges=[], favorite_games=[], played_count=1, playing_count=0, backlog_count=0, wishlist_count=0)
        embed = create_profile_embed(profile, recent_played=[row])
        field = next(f for f in embed.fields if "Recently Played" in f.name)
        self.assertIn("Hades", field.value)
        self.assertNotIn("Unknown Game", field.value)
        conn.close()

class SyncTests(unittest.IsolatedAsyncioTestCase):
    async def test_refresh_does_not_seed_pending_feed_events(self):
        bot = SimpleNamespace(backloggd=SimpleNamespace(get_user_library=AsyncMock(return_value=[]), get_user_journal=AsyncMock(return_value=[])), db=SimpleNamespace(replace_user_library=AsyncMock(return_value=True)), guilds=[])
        await AccountCog(bot)._sync_user_library(1, "Tester")
        bot.backloggd.get_user_journal.assert_not_awaited()

class PaginationTests(unittest.IsolatedAsyncioTestCase):
    async def test_next_and_previous_show_all_members(self):
        from bot.ui.views import RatingsView
        rows = [{"backloggd_username": f"User{i}", "rating": 4.5, "status": "played"} for i in range(31)]
        view = RatingsView("Hades", "hades", "Guild", rows)
        interaction = SimpleNamespace(response=SimpleNamespace(is_done=lambda: True), edit_original_response=AsyncMock())
        self.assertTrue(view.previous_button.disabled)
        await view.next_button.callback(interaction)
        self.assertEqual(view.page, 2)
        self.assertIn("User15", interaction.edit_original_response.call_args.kwargs["embed"].fields[0].value)
        await view.next_button.callback(interaction)
        self.assertTrue(view.next_button.disabled)
        await view.previous_button.callback(interaction)
        self.assertEqual(view.page, 2)
        view.stop()


class FeedTests(unittest.IsolatedAsyncioTestCase):
    async def test_baseline_survives_restart_and_is_per_guild_and_source(self):
        import tempfile
        from pathlib import Path
        from bot.db import Database
        with tempfile.TemporaryDirectory() as directory:
            path = str(Path(directory) / "feed.db")
            db = Database(path, turso_url="")
            await db.connect()
            try:
                await db.link_user(10, "Tester")
                self.assertTrue(await db.seed_feed_source(1, "tester", "activity", ["old"]))
                self.assertTrue(await db.is_journal_entry_delivered(1, "tester", "old"))
                self.assertFalse(await db.seed_feed_source(1, "tester", "activity", ["new"]))
                self.assertFalse(await db.is_journal_entry_delivered(1, "tester", "new"))
                self.assertTrue(await db.seed_feed_source(2, "tester", "activity", ["old"]))
                self.assertTrue(await db.seed_feed_source(1, "tester", "journal", ["journal-old"]))
            finally:
                await db.close()
            await db.connect()
            try:
                self.assertFalse(await db.seed_feed_source(1, "tester", "activity", ["after-restart"]))
                self.assertFalse(await db.is_journal_entry_delivered(1, "tester", "after-restart"))
            finally:
                await db.close()


class DeliveryTests(unittest.IsolatedAsyncioTestCase):
    async def test_poll_baselines_then_delivers_and_retries_failed_send(self):
        import tempfile
        import discord
        from datetime import datetime, timezone
        from pathlib import Path
        from unittest.mock import MagicMock, patch
        from bot.db import Database
        from bot.cogs.feed import FeedCog
        from bot.feed_models import FeedEvent
        def event(identifier, occurred_at="2026-11-12T10:00:00Z", slug="hades"):
            return FeedEvent(
                identifier, "Hades", slug, None, "completed", "completed",
                None, None, None, played_date="2026-11-12", occurred_at=occurred_at,
            )
        with tempfile.TemporaryDirectory() as directory:
            db = Database(str(Path(directory) / "test.db"), turso_url="")
            await db.connect()
            try:
                await db.link_user(10, "Tester")
                await db.set_feed_channel(1, 100)
                await db.set_guild_feed_preference(1, 10, True)
                channel = MagicMock(spec=discord.TextChannel)
                channel.permissions_for.return_value = SimpleNamespace(send_messages=True, embed_links=True)
                channel.id = 100
                channel.send = AsyncMock(return_value=SimpleNamespace(id=999))
                guild = SimpleNamespace(members=[SimpleNamespace(id=10)], me=object(), get_channel=lambda _: channel)
                bot = SimpleNamespace(db=db, get_guild=lambda _: guild)
                cog = FeedCog(bot, clock=lambda: datetime(2026, 11, 12, 12, tzinfo=timezone.utc))
                cog._fetch_feed_sources = AsyncMock(return_value={"activity": [event("old")]})
                with patch("bot.cogs.feed.asyncio.sleep", new=AsyncMock()):
                    await cog.journal_feed_loop.coro(cog)
                    channel.send.assert_not_awaited()
                    cog._fetch_feed_sources.return_value = {
                        "activity": [event("new", "2026-11-12T12:05:00Z", slug="celeste"), event("old")]
                    }
                    channel.send.side_effect = discord.Forbidden(
                        SimpleNamespace(status=403, reason="Forbidden"),
                        {"message": "temporary permission rejection"},
                    )
                    await cog.journal_feed_loop.coro(cog)
                    self.assertFalse(await db.is_journal_entry_delivered(1, "tester", "new"))
                    channel.send.side_effect = None
                    await cog.journal_feed_loop.coro(cog)
                    self.assertTrue(await db.is_journal_entry_delivered(1, "tester", "new"))
                    await cog.journal_feed_loop.coro(cog)
                    self.assertEqual(channel.send.await_count, 2)
            finally:
                await db.close()

    async def test_long_ratings_stay_within_discord_limits(self):
        rows = [{"backloggd_username": "x" * 1000, "rating": 4.5, "status": "backlog"} for _ in range(15)]
        embed = create_all_ratings_embed("g" * 300, "hades", "s" * 300, rows)
        self.assertLessEqual(len(embed.title), 256)
        self.assertLessEqual(len(embed), 6000)
        self.assertTrue(all(len(f.value) <= 1024 for f in embed.fields))
