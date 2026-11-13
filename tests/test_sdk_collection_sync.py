"""Tests for Task 15 multi-collection SDK library composition, precedence, and completeness."""

import os
os.environ["PYTHON_DOTENV_DISABLED"] = "1"
os.environ["TURSO_DATABASE_URL"] = ""
os.environ["TURSO_AUTH_TOKEN"] = ""
os.environ["DEV_GUILD_ID"] = ""

from datetime import datetime, timezone
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock

from backloggd import GameEntry, GameStatus, UserLibrary
from bot.db import Database
from bot.services.library_sync import sync_profile_library


FIXED_NOW = datetime(2026, 11, 13, 12, 0, 0, tzinfo=timezone.utc)


class SdkCollectionSyncTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmpdir.cleanup)
        self.db = Database(str(Path(self.tmpdir.name) / "collections.db"), turso_url="")
        self.addAsyncCleanup(self.db.close)
        await self.db.connect()
        await self.db.link_user(10, "Millers")

    async def test_sync_fetches_played_playing_backlog_wishlist_and_populates_roulette(self):
        played_lib = UserLibrary(
            username="Millers",
            games={
                "elden-ring": GameEntry(
                    slug="elden-ring", title="Elden Ring", rating=5.0, status=GameStatus.PLAYED
                ),
            },
        )
        api = SimpleNamespace(
            get_user_library=AsyncMock(return_value=played_lib),
            get_user_playing=AsyncMock(return_value=[
                GameEntry(slug="balatro", title="Balatro", rating=None, status=GameStatus.PLAYING),
            ]),
            get_user_backlog=AsyncMock(return_value=[
                GameEntry(slug="signalis", title="Signalis", rating=None, status=GameStatus.BACKLOG),
            ]),
            get_user_wishlist=AsyncMock(return_value=[
                GameEntry(slug="silksong", title="Hollow Knight: Silksong", rating=None, status=GameStatus.WISHLIST),
            ]),
        )
        bot = SimpleNamespace(db=self.db, backloggd=api)

        count = await sync_profile_library(bot, "Millers", discord_id=10, synced_at=FIXED_NOW)

        self.assertEqual(count, 4)
        api.get_user_library.assert_awaited_once_with("Millers", force_refresh=True)
        api.get_user_playing.assert_awaited_once_with("Millers")
        api.get_user_backlog.assert_awaited_once_with("Millers")
        api.get_user_wishlist.assert_awaited_once_with("Millers")

        entries = await self.db.get_profile_library_entries("Millers")
        self.assertEqual(entries["elden-ring"], {"rating": 5.0, "status": "played"})
        self.assertEqual(entries["balatro"], {"rating": None, "status": "playing"})
        self.assertEqual(entries["signalis"], {"rating": None, "status": "backlog"})
        self.assertEqual(entries["silksong"], {"rating": None, "status": "wishlist"})

        backlog_rows = await self.db.get_user_backlog(10)
        self.assertEqual({row["slug"] for row in backlog_rows}, {"signalis", "silksong"})

    async def test_deterministic_status_precedence_and_owner_rating_preservation(self):
        played_lib = UserLibrary(
            username="Millers",
            games={
                "replaying": GameEntry(
                    slug="replaying", title="Replaying Game", rating=None, status=GameStatus.PLAYED
                ),
                "rated-played": GameEntry(
                    slug="rated-played", title="Rated Played", rating=4.5, status=GameStatus.PLAYED
                ),
            },
        )
        api = SimpleNamespace(
            get_user_library=AsyncMock(return_value=played_lib),
            get_user_playing=AsyncMock(return_value=[
                # Overlaps 'replaying' (played wins) and has rating preserved
                GameEntry(slug="replaying", title="Replaying Game", rating=4.0, status=GameStatus.PLAYING),
                # Overlaps backlog item (playing wins -> excluded from roulette)
                GameEntry(slug="playing-plus-backlog", title="In Progress", rating=None, status=GameStatus.PLAYING),
            ]),
            get_user_backlog=AsyncMock(return_value=[
                GameEntry(slug="rated-played", title="Rated Played", rating=None, status=GameStatus.BACKLOG),
                GameEntry(slug="playing-plus-backlog", title="In Progress", rating=None, status=GameStatus.BACKLOG),
                GameEntry(slug="backlog-plus-wishlist", title="Queued Game", rating=None, status=GameStatus.BACKLOG),
            ]),
            get_user_wishlist=AsyncMock(return_value=[
                GameEntry(slug="backlog-plus-wishlist", title="Queued Game", rating=None, status=GameStatus.WISHLIST),
            ]),
        )
        bot = SimpleNamespace(db=self.db, backloggd=api)

        count = await sync_profile_library(bot, "Millers", discord_id=10, synced_at=FIXED_NOW)

        self.assertEqual(count, 4)
        entries = await self.db.get_profile_library_entries("Millers")
        # played > playing > backlog > wishlist; rating preserved across overlapping collections
        self.assertEqual(entries["replaying"], {"rating": 4.0, "status": "played"})
        self.assertEqual(entries["rated-played"], {"rating": 4.5, "status": "played"})
        self.assertEqual(entries["playing-plus-backlog"], {"rating": None, "status": "playing"})
        self.assertEqual(entries["backlog-plus-wishlist"], {"rating": None, "status": "backlog"})

    async def test_partial_collection_failure_aborts_without_overwriting_existing_cache(self):
        prior_now = datetime(2026, 11, 12, 10, 0, 0, tzinfo=timezone.utc)
        await self.db.replace_profile_library(
            "Millers",
            [GameEntry(slug="kept-game", title="Kept Game", rating=4.5, status=GameStatus.PLAYED)],
            synced_at=prior_now,
        )

        api = SimpleNamespace(
            get_user_library=AsyncMock(return_value=UserLibrary(username="Millers", games={
                "new-played": GameEntry(slug="new-played", title="New Played", rating=5.0, status=GameStatus.PLAYED),
            })),
            get_user_playing=AsyncMock(return_value=[]),
            get_user_backlog=AsyncMock(side_effect=RuntimeError("backlog 503")),
            get_user_wishlist=AsyncMock(return_value=[]),
        )
        bot = SimpleNamespace(db=self.db, backloggd=api)

        result = await sync_profile_library(bot, "Millers", discord_id=10, synced_at=FIXED_NOW)

        self.assertIsNone(result)
        self.assertEqual(await self.db.get_profile_ratings_dict("Millers"), {"kept-game": 4.5})
        info = await self.db.get_profile_sync_info("Millers")
        self.assertEqual(info["last_synced_at"], "2026-11-12 10:00:00")
        self.assertIn("backlog 503", info["last_sync_error"])

    async def test_playing_collection_at_single_page_limit_is_not_labeled_complete(self):
        prior_now = datetime(2026, 11, 12, 10, 0, 0, tzinfo=timezone.utc)
        await self.db.replace_profile_library(
            "Millers",
            [GameEntry(slug="kept-game", title="Kept Game", rating=4.5, status=GameStatus.PLAYED)],
            synced_at=prior_now,
        )
        # backloggd-py 0.2.1 get_user_playing() only reads page 1 (PAGE_SIZE=70) without has_more.
        full_first_page = [
            GameEntry(slug=f"playing-{i}", title=f"Playing {i}", status=GameStatus.PLAYING)
            for i in range(70)
        ]
        api = SimpleNamespace(
            get_user_library=AsyncMock(return_value=UserLibrary(username="Millers")),
            get_user_playing=AsyncMock(return_value=full_first_page),
            get_user_backlog=AsyncMock(return_value=[]),
            get_user_wishlist=AsyncMock(return_value=[]),
        )
        bot = SimpleNamespace(db=self.db, backloggd=api)

        result = await sync_profile_library(bot, "Millers", discord_id=10, synced_at=FIXED_NOW)

        self.assertIsNone(result)
        self.assertEqual(await self.db.get_profile_ratings_dict("Millers"), {"kept-game": 4.5})
        info = await self.db.get_profile_sync_info("Millers")
        self.assertEqual(info["last_synced_at"], "2026-11-12 10:00:00")
        self.assertIn("get_user_playing", info["last_sync_error"])


if __name__ == "__main__":
    unittest.main()
