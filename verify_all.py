"""End-to-end integration and smoke tests for the Backloggd Discord Bot."""

import asyncio
import os
import sys
import unittest
from pathlib import Path

# Fix Windows console UTF-8 output
sys.stdout.reconfigure(encoding="utf-8")
from bot.db import Database
from bot.ui.helpers import rating_to_stars, format_number, format_status
from bot.ui.embeds import (
    create_game_embed,
    create_profile_embed,
    create_compare_embed,
    create_roulette_embed,
    create_server_top_embed,
    create_server_stats_embed
)
from bot.ui.feed_embed import create_journal_feed_embed
from bot.client import BackloggdBot

TEST_DB_PATH = "data/test_verification.db"

class DummyGame:
    slug = "elden-ring"
    title = "Elden Ring"
    release_year = 2022
    community_rating = 4.5
    summary = "A vast world where open fields with a variety of situations and huge dungeons are seamlessly connected."
    cover_url = "https://example.com/cover.jpg"

    class Stats:
        total_ratings = 142000
        played_count = 98000
        playing_count = 14000
        backlog_count = 62000
        wishlist_count = 35000

    stats = Stats()

class DummyProfile:
    username = "NoxTheGamer"
    avatar_url = "https://example.com/avatar.jpg"
    bio = "Souls veteran and RPG enthusiast."
    badges = ["VIP", "Patron"]
    played_count = 342
    playing_count = 3
    backlog_count = 118
    wishlist_count = 45
    favorite_games = ["Bloodborne", "Chrono Trigger", "Hollow Knight", "Outer Wilds", "Elden Ring"]

class DummyReview:
    review_id = "123"
    game_title = "Metaphor: ReFantazio"
    game_slug = "metaphor-refantazio"
    author = "NoxTheGamer"
    rating = 4.5
    review_text = "An incredible journey that combines the best elements of Atlus's previous works."
    created_at = "Oct 15, 2026"
    like_count = 12
    has_spoilers = False

class DummyJournalEntry:
    entry_id = "test_log_12345"
    game_title = "Sea of Stars"
    game_slug = "sea-of-stars"
    played_date = "2026-10-15"
    platform = "PC"
    rating = 4.5
    is_replay = False
    review_notes = "Phenomenal combat system and pixel art. Loved the music!"
    cover_url = "https://example.com/sea-of-stars.jpg"

class VerificationSuite:
    def __init__(self):
        self.db = Database(TEST_DB_PATH)

    async def run_all(self):
        print("=== [1] Testing Formatting & Helper Functions ===")
        assert rating_to_stars(5.0) == "★★★★★", f"Expected '★★★★★', got {rating_to_stars(5.0)}"
        assert rating_to_stars(4.5) == "★★★★½", f"Expected '★★★★½', got {rating_to_stars(4.5)}"
        assert rating_to_stars(3.0) == "★★★☆☆", f"Expected '★★★☆☆', got {rating_to_stars(3.0)}"
        assert rating_to_stars(None) == "☆☆☆☆☆", f"Expected '☆☆☆☆☆', got {rating_to_stars(None)}"
        assert format_number(142000) == "142.0k", f"Expected '142.0k', got {format_number(142000)}"
        assert format_number(1500000) == "1.5M", f"Expected '1.5M', got {format_number(1500000)}"
        assert format_status("played") == "🎮 Played"
        assert format_status("backlog") == "⏳ Backlog"
        print("✅ Helpers & formatters passed!")

        print("\n=== [2] Testing SQLite Schema & Operations ===")
        if os.path.exists(TEST_DB_PATH):
            os.remove(TEST_DB_PATH)
        await self.db.connect()

        # Link users
        await self.db.link_user(discord_id=1001, username="Nox", avatar_url="https://example.com/nox.png")
        await self.db.link_user(discord_id=1002, username="Kaelen", avatar_url="https://example.com/kaelen.png")
        await self.db.link_user(discord_id=1003, username="Vesper", avatar_url=None)

        linked_user = await self.db.get_linked_user(1001)
        assert linked_user is not None and linked_user["backloggd_username"] == "Nox"

        # Cache game & ratings
        await self.db.upsert_game(
            slug="elden-ring",
            title="Elden Ring",
            cover_url="https://example.com/cover.jpg",
            release_year=2022,
            community_rating=4.5
        )
        await self.db.upsert_game(
            slug="chrono-trigger",
            title="Chrono Trigger",
            cover_url="https://example.com/ct.jpg",
            release_year=1995,
            community_rating=4.8
        )
        await self.db.upsert_game(
            slug="hollow-knight",
            title="Hollow Knight",
            cover_url="https://example.com/hk.jpg",
            release_year=2017,
            community_rating=4.7
        )

        # Ratings
        await self.db.upsert_user_rating(1001, "elden-ring", 5.0, "played")
        await self.db.upsert_user_rating(1002, "elden-ring", 4.0, "played")
        await self.db.upsert_user_rating(1003, "elden-ring", None, "backlog")

        await self.db.upsert_user_rating(1001, "chrono-trigger", 5.0, "played")
        await self.db.upsert_user_rating(1002, "chrono-trigger", 5.0, "played")

        await self.db.upsert_user_rating(1001, "hollow-knight", 4.5, "played")
        await self.db.upsert_user_rating(1002, "hollow-knight", 2.0, "played")

        # Test server member scores query
        server_scores = await self.db.get_server_ratings_for_game([1001, 1002, 1003], "elden-ring")
        assert len(server_scores) == 3, f"Expected 3 server ratings, got {len(server_scores)}"
        print(f"✅ Server ratings query returned {len(server_scores)} members")

        # Test leaderboards
        top_rated = await self.db.get_server_top_rated([1001, 1002, 1003], limit=5, min_ratings=2)
        assert len(top_rated) >= 2, f"Expected at least 2 top games, got {len(top_rated)}"
        assert top_rated[0]["title"] == "Chrono Trigger"  # Avg 5.0

        polarizing = await self.db.get_server_polarizing([1001, 1002, 1003], limit=5, min_ratings=2)
        assert len(polarizing) >= 1
        assert polarizing[0]["title"] == "Hollow Knight"  # Delta 2.5 (4.5 vs 2.0)

        shame = await self.db.get_server_shame_list([1001, 1002, 1003], limit=5)
        assert len(shame) >= 1
        assert shame[0]["title"] == "Elden Ring"  # 1 backlog

        stats = await self.db.get_server_stats([1001, 1002, 1003])
        assert stats["linked_members"] == 3
        assert stats["total_logged"] == 7
        assert stats["total_played"] == 6

        # Test journal feed deduplication
        assert not await self.db.is_journal_entry_seen("log_abc_123")
        await self.db.mark_journal_entry_seen("log_abc_123", 1001)
        assert await self.db.is_journal_entry_seen("log_abc_123")

        await self.db.close()
        if os.path.exists(TEST_DB_PATH):
            os.remove(TEST_DB_PATH)
        print("✅ SQLite database operations verified successfully!")

        print("\n=== [3] Testing Embed Constructors ===")
        # 1. Game Embed
        game_embed = create_game_embed(DummyGame(), server_scores, guild_name="TestServer")
        assert "Elden Ring (2022)" in game_embed.title
        assert len(game_embed.fields) >= 2
        print("✅ Game Embed built successfully")

        # 2. Profile Embed
        profile_embed = create_profile_embed(
            DummyProfile(),
            avg_rating=4.25,
            rated_count=120,
            recent_played=[DummyJournalEntry()],
            latest_review=DummyReview()
        )
        assert "NoxTheGamer" in profile_embed.title
        assert any("Favorite Games" in f.name and "Elden Ring" in f.value for f in profile_embed.fields)
        assert any("Recently Played" in f.name and "Sea of Stars" in f.value for f in profile_embed.fields)
        assert any("Latest Review" in f.name and "Metaphor" in f.value for f in profile_embed.fields)
        print("✅ Profile Embed built successfully")

        # 3. Compare Embed
        shared = [{"title": "Chrono Trigger", "r1": 5.0, "r2": 5.0, "avg": 5.0}]
        disagreements = [{"title": "Hollow Knight", "r1": 4.5, "r2": 2.0, "delta": 2.5}]
        compare_embed = create_compare_embed("Nox", "Kaelen", 0.86, shared, disagreements)
        assert "86% Taste Match" in compare_embed.description
        print("✅ Compare Embed built successfully")

        # 4. Roulette Embed
        roulette_embed = create_roulette_embed("Nox", DummyGame(), remaining_backlog=118)
        assert "Backlog Roulette" in roulette_embed.title
        print("✅ Roulette Embed built successfully")

        # 5. Leaderboard Embed
        top_embed = create_server_top_embed("Highest Rated", top_rated, mode="highest_rated", guild_name="TestServer")
        assert "Chrono Trigger" in top_embed.description
        print("✅ Leaderboard Embed built successfully")

        # 6. Server Stats Embed
        server_stats_embed = create_server_stats_embed(stats, guild_name="TestServer")
        assert len(server_stats_embed.fields) == 5
        print("✅ Server Stats Embed built successfully")

        # 7. Journal Feed Embed
        feed_embed = create_journal_feed_embed("Nox", "https://example.com/nox.png", DummyJournalEntry())
        assert "Sea of Stars" in feed_embed.title
        print("✅ Journal Feed Embed built successfully")

        print("\n=== [4] Testing Bot Client Cog Suite ===")
        bot = BackloggdBot()
        await bot.setup_hook()
        cogs = list(bot.cogs.keys())
        expected_cogs = ['Account', 'Games', 'Users', 'Social', 'Discovery', 'Stats', 'Feed']
        assert all(c in cogs for c in expected_cogs), f"Missing cogs! Got: {cogs}"
        await bot.close()
        print(f"✅ All {len(cogs)} Cogs verified active: {cogs}")

        print("\n🎉 ALL E2E AND SMOKE TESTS PASSED!")

if __name__ == "__main__":
    suite = VerificationSuite()
    asyncio.run(suite.run_all())
