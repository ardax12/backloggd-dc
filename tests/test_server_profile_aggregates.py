import os
os.environ["PYTHON_DOTENV_DISABLED"] = "1"

import tempfile
import unittest
from pathlib import Path

from bot.db import Database
from bot.ui.embeds import create_all_ratings_embed, create_server_stats_embed, create_server_top_embed


class ServerProfileAggregateTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.db = Database(str(Path(self.directory.name) / "stats.db"), turso_url="")
        await self.db.connect()
        await self.db.link_user(10, "Millers")
        await self.db.link_user(11, "millers")
        await self.db.link_user(20, "PlayerB")
        await self.db.upsert_game("shared", "Shared Game")
        await self.db.upsert_game("queued", "Queued Game")
        await self.db.upsert_game("hidden", "Hidden Gem")
        await self.db.upsert_game("solo", "Solo Rating")
        await self.db.upsert_user_rating(10, "shared", 5.0, "played")
        await self.db.upsert_user_rating(20, "shared", 3.0, "played")
        await self.db.upsert_user_rating(10, "queued", None, "backlog")
        await self.db.upsert_user_rating(20, "queued", None, "backlog")
        await self.db.upsert_user_rating(10, "hidden", 5.0, "played")
        await self.db.upsert_user_rating(10, "solo", 4.0, "played")

    async def asyncTearDown(self):
        await self.db.close()
        self.directory.cleanup()

    async def test_game_ratings_return_one_row_per_profile(self):
        rows = await self.db.get_server_ratings_for_game([10, 11, 20], "shared")

        self.assertEqual(len(rows), 2)
        self.assertEqual({row["profile_key"] for row in rows}, {"millers", "playerb"})
        self.assertEqual({row["backloggd_username"] for row in rows}, {"millers", "PlayerB"})

    async def test_leaderboards_aggregate_distinct_profiles_in_every_mode(self):
        top = await self.db.get_server_top_rated([10, 11, 20], min_ratings=2)
        played = await self.db.get_server_most_played([10, 11, 20])
        backlog = await self.db.get_server_shame_list([10, 11, 20])
        polarizing = await self.db.get_server_polarizing([10, 11, 20], min_ratings=2)
        gems = await self.db.get_server_hidden_gems([10, 11, 20])

        self.assertEqual([(row["slug"], row["rating_count"], row["avg_rating"]) for row in top], [
            ("shared", 2, 4.0),
        ])
        self.assertEqual([(row["slug"], row["play_count"]) for row in played], [
            ("shared", 2), ("hidden", 1), ("solo", 1),
        ])
        self.assertEqual([(row["slug"], row["backlog_count"]) for row in backlog], [
            ("queued", 2),
        ])
        self.assertEqual([(row["slug"], row["rating_count"], row["rating_delta"]) for row in polarizing], [
            ("shared", 2, 2.0),
        ])
        self.assertEqual([(row["slug"], row["rating_count"]) for row in gems], [
            ("hidden", 1),
        ])

    async def test_minimum_rating_threshold_counts_profiles_not_connections(self):
        rows = await self.db.get_server_top_rated([10, 11], min_ratings=2)
        polarizing = await self.db.get_server_polarizing([10, 11], min_ratings=2)

        self.assertEqual(rows, [])
        self.assertEqual(polarizing, [])

    async def test_server_stats_report_member_and_profile_counts_separately(self):
        stats = await self.db.get_server_stats([10, 11, 20])

        self.assertEqual(stats["linked_members"], 3)
        self.assertEqual(stats["profile_count"], 2)
        self.assertEqual(stats["total_logged"], 6)
        self.assertEqual(stats["total_played"], 4)
        self.assertEqual(stats["server_avg_rating"], 4.25)
        self.assertEqual(stats["top_game"], "Shared Game (2 profiles)")

        await self.db.unlink_user(10)
        still_shared = await self.db.get_server_stats([10, 11, 20])
        self.assertEqual(still_shared["linked_members"], 2)
        self.assertEqual(still_shared["profile_count"], 2)
        self.assertEqual(still_shared["total_logged"], 6)

        await self.db.unlink_user(11)
        without_millers = await self.db.get_server_stats([10, 11, 20])
        self.assertEqual(without_millers["linked_members"], 1)
        self.assertEqual(without_millers["profile_count"], 1)
        self.assertEqual(without_millers["total_logged"], 2)
        self.assertEqual(without_millers["total_played"], 1)
        self.assertEqual(without_millers["server_avg_rating"], 3.0)
        self.assertEqual(without_millers["top_game"], "Shared Game (1 profile)")

    async def test_rating_and_stats_embeds_label_unique_profiles(self):
        rows = await self.db.get_server_ratings_for_game([10, 11, 20], "shared")
        ratings_embed = create_all_ratings_embed("Shared Game", "shared", "Test Server", rows)
        stats_embed = create_server_stats_embed(await self.db.get_server_stats([10, 11, 20]), "Test Server")
        top_embed = create_server_top_embed("Top", [{
            "slug": "shared", "title": "Shared Game", "avg_rating": 4.0, "rating_count": 2,
        }], "highest_rated", "Test Server")

        self.assertIn("of **2** profiles", ratings_embed.description)
        self.assertTrue(any("Unique Backloggd Profiles" in field.name for field in stats_embed.fields))
        self.assertIn("profile ratings", top_embed.description)


if __name__ == "__main__":
    unittest.main()
