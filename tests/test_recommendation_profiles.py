import os
os.environ["PYTHON_DOTENV_DISABLED"] = "1"

import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from bot.cogs.discovery import DiscoveryCog
from bot.cogs.social import SocialCog
from bot.db import Database
from bot.recommendation_rules import numeric_ratings


class RecommendationProfileDatabaseTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.db = Database(str(Path(self.directory.name) / "recommendations.db"), turso_url="")
        await self.db.connect()
        await self.db.link_user(10, "Millers")
        await self.db.link_user(11, "millers")
        await self.db.link_user(20, "PlayerB")

    async def asyncTearDown(self):
        await self.db.close()
        self.directory.cleanup()

    async def test_server_candidates_are_distinct_profiles_and_can_exclude_target(self):
        rows = await self.db.get_guild_linked_profiles([10, 11, 20])
        without_millers = await self.db.get_guild_linked_profiles(
            [10, 11, 20], exclude_profile_key="MILLERS"
        )

        self.assertEqual([row["profile_key"] for row in rows], ["millers", "playerb"])
        self.assertEqual([row["display_username"] for row in rows], ["millers", "PlayerB"])
        self.assertEqual([row["profile_key"] for row in without_millers], ["playerb"])

    async def test_roulette_candidates_allow_only_unrated_backlog_and_wishlist(self):
        await self.db.upsert_game("backlog", "Backlog")
        await self.db.upsert_game("wishlist", "Wishlist")
        await self.db.upsert_game("played", "Played")
        await self.db.upsert_game("playing", "Playing")
        await self.db.upsert_game("rated-backlog", "Rated Backlog")
        await self.db.upsert_game("unknown", "Unknown")
        await self.db.upsert_user_rating(10, "backlog", None, "backlog")
        await self.db.upsert_user_rating(10, "wishlist", None, "wishlist")
        await self.db.upsert_user_rating(10, "played", None, "played")
        await self.db.upsert_user_rating(10, "playing", None, "playing")
        await self.db.upsert_user_rating(10, "rated-backlog", 3.5, "backlog")
        await self.db.upsert_user_rating(10, "unknown", None, "unknown")

        rows = await self.db.get_user_backlog(10)

        self.assertEqual({row["slug"] for row in rows}, {"backlog", "wishlist"})

    async def test_batch_profile_ratings_keep_library_rows_but_not_duplicate_connections(self):
        await self.db.upsert_game("a", "A")
        await self.db.upsert_user_rating(10, "a", 4.5, "played")
        ratings = await self.db.get_multi_profile_ratings_dict(["millers", "playerb"])

        self.assertEqual(ratings, {"millers": {"a": 4.5}, "playerb": {}})


class RecommendationRuleTests(unittest.TestCase):
    def test_only_numeric_ratings_contribute_to_similarity(self):
        self.assertEqual(
            numeric_ratings({"rated": 4.5, "unrated": None, "int": 3, "bool": True}),
            {"rated": 4.5, "int": 3},
        )
        self.assertEqual(numeric_ratings({"backlog": None, "wishlist": None}), {})


class RecommendationCommandTests(unittest.IsolatedAsyncioTestCase):
    def interaction(self, *, user_id=10, members=None):
        return SimpleNamespace(
            user=SimpleNamespace(id=user_id, display_name="Caller"),
            guild=SimpleNamespace(
                name="Test Server",
                members=members or [SimpleNamespace(id=user_id), SimpleNamespace(id=11), SimpleNamespace(id=20)],
            ),
            response=SimpleNamespace(defer=AsyncMock()),
            followup=SimpleNamespace(send=AsyncMock()),
        )

    async def test_recommendation_candidates_are_requested_by_profile_excluding_callers_profile(self):
        games = {f"shared-{index}": 4.0 for index in range(5)}
        other_games = {**games, "extra": 5.0}
        db = SimpleNamespace(
            get_linked_user=AsyncMock(return_value={
                "profile_key": "millers", "backloggd_username": "Millers",
            }),
            get_profile_ratings_dict=AsyncMock(return_value=games),
            get_guild_linked_profiles=AsyncMock(return_value=[{
                "profile_key": "playerb", "display_username": "PlayerB", "avatar_url": None,
            }]),
            get_multi_profile_ratings_dict=AsyncMock(return_value={"playerb": other_games}),
            get_games_by_slugs=AsyncMock(return_value={"extra": {"title": "Extra Game"}}),
        )
        interaction = self.interaction()

        await DiscoveryCog.recommend.callback(DiscoveryCog(SimpleNamespace(db=db)), interaction)

        db.get_guild_linked_profiles.assert_awaited_once_with(
            [11, 20], exclude_profile_key="millers"
        )
        self.assertTrue(interaction.followup.send.await_args.kwargs["embed"].fields)

    async def test_all_unrated_library_reports_insufficient_numeric_ratings(self):
        db = SimpleNamespace(
            get_linked_user=AsyncMock(return_value={
                "profile_key": "millers", "backloggd_username": "Millers",
            }),
            get_profile_ratings_dict=AsyncMock(return_value={"backlog": None, "wishlist": None}),
            get_guild_linked_profiles=AsyncMock(),
        )
        interaction = self.interaction()

        await DiscoveryCog.recommend.callback(DiscoveryCog(SimpleNamespace(db=db)), interaction)

        message = interaction.followup.send.await_args.args[0]
        self.assertIn("numeric game ratings", message)
        db.get_guild_linked_profiles.assert_not_awaited()

    async def test_disjoint_numeric_ratings_explain_that_profiles_lack_overlap(self):
        caller_games = {f"caller-{index}": 4.0 for index in range(5)}
        peer_games = {f"peer-{index}": 4.5 for index in range(5)}
        db = SimpleNamespace(
            get_linked_user=AsyncMock(return_value={
                "profile_key": "millers", "backloggd_username": "Millers",
            }),
            get_profile_ratings_dict=AsyncMock(return_value=caller_games),
            get_guild_linked_profiles=AsyncMock(return_value=[{
                "profile_key": "playerb", "display_username": "PlayerB", "avatar_url": None,
            }]),
            get_multi_profile_ratings_dict=AsyncMock(return_value={"playerb": peer_games}),
        )
        interaction = self.interaction()

        await DiscoveryCog.recommend.callback(DiscoveryCog(SimpleNamespace(db=db)), interaction)

        message = interaction.followup.send.await_args.args[0]
        self.assertIn("shares at least two numeric ratings", message)
        self.assertIn("do not count toward similarity", message)

    async def test_compare_passes_only_numeric_ratings_to_sdk_similarity(self):
        snapshots = {
            "first": {"shared": 4.0, "unrated-first": None},
            "second": {"shared": 4.5, "unrated-second": None},
        }
        db = SimpleNamespace(
            get_profile_sync_info=AsyncMock(return_value={"last_synced_at": "2026-01-01"}),
            get_profile_ratings_dict=AsyncMock(side_effect=lambda name: snapshots[name.lower()]),
        )
        interaction = self.interaction()

        with patch("bot.cogs.social.calculate_similarity_raw", return_value=(80, 50, 60)) as similarity:
            await SocialCog.compare.callback(
                SocialCog(SimpleNamespace(db=db)), interaction, "First", "Second"
            )

        similarity.assert_called_once_with({"shared": 4.0}, {"shared": 4.5})

    async def test_taste_twins_excludes_target_profile_across_other_member_links(self):
        db = SimpleNamespace(
            get_linked_user=AsyncMock(return_value={
                "profile_key": "millers", "backloggd_username": "Millers", "avatar_url": None,
            }),
            get_profile_ratings_dict=AsyncMock(return_value={"a": 4.0, "b": 5.0}),
            get_guild_linked_profiles=AsyncMock(return_value=[]),
        )
        interaction = self.interaction(members=[SimpleNamespace(id=10), SimpleNamespace(id=11)])

        await SocialCog.taste_twins.callback(SocialCog(SimpleNamespace(db=db)), interaction)

        db.get_guild_linked_profiles.assert_awaited_once_with(
            [11], exclude_profile_key="millers"
        )

    async def test_compare_explains_two_member_links_to_same_profile(self):
        db = SimpleNamespace(get_linked_user=AsyncMock(side_effect=[
            {"profile_key": "millers", "backloggd_username": "Millers"},
            {"profile_key": "millers", "backloggd_username": "millers"},
        ]))
        interaction = self.interaction()

        await SocialCog.compare.callback(
            SocialCog(SimpleNamespace(db=db)), interaction, "<@!10>", "<@!11>"
        )

        message = interaction.followup.send.await_args.args[0]
        self.assertIn("same Backloggd profile", message)


if __name__ == "__main__":
    unittest.main()
