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


class StatusAwareRecommendationTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.db = Database(str(Path(self.directory.name) / "rec-status.db"), turso_url="")
        await self.db.connect()

    async def asyncTearDown(self):
        await self.db.close()
        self.directory.cleanup()

    def test_status_aware_exclusion_rules(self):
        from bot.recommendation_rules import excluded_recommendation_slugs, is_excluded_from_recommendations

        self.assertFalse(is_excluded_from_recommendations("backlog", None))
        self.assertFalse(is_excluded_from_recommendations("wishlist", None))
        self.assertTrue(is_excluded_from_recommendations("backlog", 4.0))
        self.assertTrue(is_excluded_from_recommendations("wishlist", 3.5))
        self.assertTrue(is_excluded_from_recommendations("played", None))
        self.assertTrue(is_excluded_from_recommendations("playing", None))
        self.assertTrue(is_excluded_from_recommendations("unknown", None))
        self.assertTrue(is_excluded_from_recommendations("custom_unmapped", None))

        excluded = excluded_recommendation_slugs({
            "wish": {"status": "wishlist", "rating": None},
            "back": {"status": "backlog", "rating": None},
            "played": {"status": "played", "rating": None},
            "playing": {"status": "playing", "rating": None},
            "rated-wish": {"status": "wishlist", "rating": 4.5},
            "unk": {"status": "unknown", "rating": None},
        })
        self.assertEqual(excluded, {"played", "playing", "rated-wish", "unk"})

    async def test_friend_and_server_recommend_allow_unrated_wishlist_and_backlog_only(self):
        from datetime import datetime, timezone
        from backloggd import GameEntry, GameStatus

        now = datetime(2026, 11, 13, 12, 0, tzinfo=timezone.utc)
        await self.db.link_user(10, "Alice")
        await self.db.link_user(20, "Bob")

        alice_items = [
            GameEntry(slug=f"shared-{i}", title=f"Shared {i}", rating=4.5, status=GameStatus.PLAYED)
            for i in range(5)
        ] + [
            GameEntry(slug="wish-pick", title="Wish Pick", rating=5.0, status=GameStatus.PLAYED),
            GameEntry(slug="back-pick", title="Back Pick", rating=4.5, status=GameStatus.PLAYED),
            GameEntry(slug="played-skip", title="Played Skip", rating=5.0, status=GameStatus.PLAYED),
            GameEntry(slug="playing-skip", title="Playing Skip", rating=5.0, status=GameStatus.PLAYED),
            GameEntry(slug="rated-skip", title="Rated Skip", rating=5.0, status=GameStatus.PLAYED),
            GameEntry(slug="unknown-skip", title="Unknown Skip", rating=5.0, status=GameStatus.PLAYED),
        ]
        bob_items = [
            GameEntry(slug=f"shared-{i}", title=f"Shared {i}", rating=4.5, status=GameStatus.PLAYED)
            for i in range(5)
        ] + [
            GameEntry(slug="wish-pick", title="Wish Pick", rating=None, status=GameStatus.WISHLIST),
            GameEntry(slug="back-pick", title="Back Pick", rating=None, status=GameStatus.BACKLOG),
            GameEntry(slug="played-skip", title="Played Skip", rating=None, status=GameStatus.PLAYED),
            GameEntry(slug="playing-skip", title="Playing Skip", rating=None, status=GameStatus.PLAYING),
            GameEntry(slug="rated-skip", title="Rated Skip", rating=3.5, status=GameStatus.WISHLIST),
            GameEntry(slug="unknown-skip", title="Unknown Skip", rating=None, status=GameStatus.UNKNOWN),
        ]
        await self.db.replace_profile_library("Alice", alice_items, synced_at=now)
        await self.db.replace_profile_library("Bob", bob_items, synced_at=now)

        members = [SimpleNamespace(id=10, display_name="Alice"), SimpleNamespace(id=20, display_name="Bob")]
        cog = DiscoveryCog(SimpleNamespace(db=self.db), clock=lambda: now)

        # Mode 2: Alice recommends to Bob -> Bob's unrated wishlist & backlog are eligible.
        friend_interaction = SimpleNamespace(
            user=SimpleNamespace(id=10, display_name="Alice"),
            guild=SimpleNamespace(name="Server", members=members),
            response=SimpleNamespace(defer=AsyncMock()),
            followup=SimpleNamespace(send=AsyncMock()),
        )
        await DiscoveryCog.recommend.callback(cog, friend_interaction, user=SimpleNamespace(id=20, display_name="Bob"))
        friend_embed = friend_interaction.followup.send.await_args.kwargs["embed"]
        friend_titles = [field.name for field in friend_embed.fields]
        self.assertTrue(any("Wish Pick" in t for t in friend_titles))
        self.assertTrue(any("Back Pick" in t for t in friend_titles))
        for skipped in ("Played Skip", "Playing Skip", "Rated Skip", "Unknown Skip", "Shared 0"):
            self.assertFalse(any(skipped in t for t in friend_titles))

        # Mode 1: Bob asks for server recommendations -> Bob's unrated wishlist & backlog from Alice are eligible.
        server_interaction = SimpleNamespace(
            user=SimpleNamespace(id=20, display_name="Bob"),
            guild=SimpleNamespace(name="Server", members=members),
            response=SimpleNamespace(defer=AsyncMock()),
            followup=SimpleNamespace(send=AsyncMock()),
        )
        await DiscoveryCog.recommend.callback(cog, server_interaction)
        server_embed = server_interaction.followup.send.await_args.kwargs["embed"]
        server_titles = [field.name for field in server_embed.fields]
        self.assertTrue(any("Wish Pick" in t for t in server_titles))
        self.assertTrue(any("Back Pick" in t for t in server_titles))
        for skipped in ("Played Skip", "Playing Skip", "Rated Skip", "Unknown Skip", "Shared 0"):
            self.assertFalse(any(skipped in t for t in server_titles))

    async def test_unknown_status_does_not_claim_already_played_everything(self):
        from datetime import datetime, timezone
        from backloggd import GameEntry, GameStatus

        now = datetime(2026, 11, 13, 12, 0, tzinfo=timezone.utc)
        await self.db.link_user(10, "Alice")
        await self.db.link_user(20, "Bob")

        alice_items = [
            GameEntry(slug=f"shared-{i}", title=f"Shared {i}", rating=4.5, status=GameStatus.PLAYED)
            for i in range(5)
        ] + [
            GameEntry(slug="mystery", title="Mystery", rating=5.0, status=GameStatus.PLAYED),
        ]
        bob_items = [
            GameEntry(slug=f"shared-{i}", title=f"Shared {i}", rating=4.5, status=GameStatus.PLAYED)
            for i in range(5)
        ] + [
            GameEntry(slug="mystery", title="Mystery", rating=None, status=GameStatus.UNKNOWN),
        ]
        await self.db.replace_profile_library("Alice", alice_items, synced_at=now)
        await self.db.replace_profile_library("Bob", bob_items, synced_at=now)

        members = [SimpleNamespace(id=10, display_name="Alice"), SimpleNamespace(id=20, display_name="Bob")]
        cog = DiscoveryCog(SimpleNamespace(db=self.db), clock=lambda: now)

        i_friend = SimpleNamespace(
            user=SimpleNamespace(id=10, display_name="Alice"),
            guild=SimpleNamespace(name="Server", members=members),
            response=SimpleNamespace(defer=AsyncMock()),
            followup=SimpleNamespace(send=AsyncMock()),
        )
        await DiscoveryCog.recommend.callback(cog, i_friend, user=SimpleNamespace(id=20, display_name="Bob"))
        msg_friend = i_friend.followup.send.await_args.args[0]
        self.assertIn("unknown status", msg_friend.lower())
        self.assertNotIn("already", msg_friend.lower())

        i_server = SimpleNamespace(
            user=SimpleNamespace(id=20, display_name="Bob"),
            guild=SimpleNamespace(name="Server", members=members),
            response=SimpleNamespace(defer=AsyncMock()),
            followup=SimpleNamespace(send=AsyncMock()),
        )
        await DiscoveryCog.recommend.callback(cog, i_server)
        msg_server = i_server.followup.send.await_args.args[0]
        self.assertIn("unknown status", msg_server.lower())
        self.assertNotIn("already", msg_server.lower())

    async def test_friend_recommend_without_qualifying_ratings_reports_caller_not_excluded(self):
        from datetime import datetime, timezone
        from backloggd import GameEntry, GameStatus

        now = datetime(2026, 11, 13, 12, 0, tzinfo=timezone.utc)
        await self.db.link_user(10, "Alice_One")
        await self.db.link_user(20, "Bob *Twin*")

        bob_items = [
            GameEntry(slug="shared-low", title="Shared Low", rating=None, status=GameStatus.UNKNOWN),
            GameEntry(slug="other", title="Other", rating=4.5, status=GameStatus.PLAYED),
        ]
        await self.db.replace_profile_library("Bob *Twin*", bob_items, synced_at=now)
        members = [
            SimpleNamespace(id=10, display_name="Alice_One"),
            SimpleNamespace(id=20, display_name="Bob *Twin*"),
            SimpleNamespace(id=30, display_name="Unlinked [Friend](https://evil.example)"),
        ]
        cog = DiscoveryCog(SimpleNamespace(db=self.db), clock=lambda: now)

        # Case 1: Caller has only unrated backlog/wishlist entries (no numeric ratings at all)
        await self.db.replace_profile_library(
            "Alice_One",
            [GameEntry(slug="wish-only", title="Wish Only", rating=None, status=GameStatus.WISHLIST)],
            synced_at=now,
        )
        for target_member in (members[1], members[2]):  # linked friend and unlinked friend
            inter = SimpleNamespace(
                user=SimpleNamespace(id=10, display_name="Alice_One"),
                guild=SimpleNamespace(name="Server", members=members),
                response=SimpleNamespace(defer=AsyncMock()),
                followup=SimpleNamespace(send=AsyncMock()),
            )
            await DiscoveryCog.recommend.callback(cog, inter, user=target_member)
            msg = inter.followup.send.await_args.args[0]
            self.assertIn("do not have any games rated 4.0 or above", msg)
            self.assertNotIn("already played", msg.lower())
            self.assertNotIn("unknown status", msg.lower())

        # Case 2: Caller has ratings all strictly below 4.0 (including a slug unknown in friend's library)
        await self.db.replace_profile_library(
            "Alice_One",
            [
                GameEntry(slug="shared-low", title="Shared Low", rating=3.5, status=GameStatus.PLAYED),
                GameEntry(slug="meh", title="Meh", rating=2.0, status=GameStatus.PLAYED),
            ],
            synced_at=now,
        )
        for target_member in (members[1], members[2]):
            inter = SimpleNamespace(
                user=SimpleNamespace(id=10, display_name="Alice_One"),
                guild=SimpleNamespace(name="Server", members=members),
                response=SimpleNamespace(defer=AsyncMock()),
                followup=SimpleNamespace(send=AsyncMock()),
            )
            await DiscoveryCog.recommend.callback(cog, inter, user=target_member)
            msg = inter.followup.send.await_args.args[0]
            self.assertIn("do not have any games rated 4.0 or above", msg)
            self.assertNotIn("already played", msg.lower())
            self.assertNotIn("unknown status", msg.lower())

        # Case 3: Caller has 4.0+ games, all excluded (played/rated) by linked friend -> escapes friend name
        await self.db.replace_profile_library(
            "Alice_One",
            [GameEntry(slug="other", title="Other", rating=4.5, status=GameStatus.PLAYED)],
            synced_at=now,
        )
        inter_excl = SimpleNamespace(
            user=SimpleNamespace(id=10, display_name="Alice_One"),
            guild=SimpleNamespace(name="Server", members=members),
            response=SimpleNamespace(defer=AsyncMock()),
            followup=SimpleNamespace(send=AsyncMock()),
        )
        await DiscoveryCog.recommend.callback(cog, inter_excl, user=members[1])
        msg_excl = inter_excl.followup.send.await_args.args[0]
        self.assertIn("already played, started, or rated all of your 4.0+ rated games", msg_excl)
        self.assertIn("Bob \\*Twin\\*", msg_excl)

        # Case 4: Same 4.0+ caller library against unlinked friend -> public embed with escaped names
        inter_unlinked = SimpleNamespace(
            user=SimpleNamespace(id=10, display_name="Alice_One"),
            guild=SimpleNamespace(name="Server", members=members),
            response=SimpleNamespace(defer=AsyncMock()),
            followup=SimpleNamespace(send=AsyncMock()),
        )
        await DiscoveryCog.recommend.callback(cog, inter_unlinked, user=members[2])
        embed = inter_unlinked.followup.send.await_args.kwargs["embed"]
        self.assertIn("Other", embed.fields[0].name)
        self.assertIn("Alice\\_One", embed.description)
        self.assertIn("Unlinked \\[Friend\\](https://evil.example)", embed.description)


if __name__ == "__main__":
    unittest.main()
