import os
os.environ["PYTHON_DOTENV_DISABLED"] = "1"

import asyncio
from datetime import datetime, timedelta, timezone
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock

import discord

from bot.cogs.discovery import DiscoveryCog
from bot.cogs.feed import FeedCog
from bot.cogs.games import GamesCog
from bot.cogs.social import SocialCog
from bot.cogs.stats import StatsCog
from bot.cogs.users import UsersCog
from bot.db import Database
from bot.services.library_sync import LibrarySyncService
from bot.ui.embeds import (
    create_compare_embed,
    create_game_embed,
    create_profile_embed,
    create_roulette_embed,
    create_server_stats_embed,
    create_server_top_embed,
    create_taste_twins_embed,
)


class FakeClock:
    def __init__(self, initial: datetime):
        self.now = initial

    def __call__(self) -> datetime:
        return self.now

    def advance(self, **kwargs) -> datetime:
        self.now = self.now + timedelta(**kwargs)
        return self.now


class Entry:
    def __init__(self, slug: str, rating=None, status="played", title=None):
        self.slug = slug
        self.rating = rating
        self.status = status
        self.title = title or slug.replace("-", " ").title()
        self.cover_url = None
        self.release_year = 2024


def assert_discord_embed_limits(testcase: unittest.TestCase, embed: discord.Embed):
    testcase.assertLessEqual(len(embed.title or ""), 256)
    testcase.assertLessEqual(len(embed.description or ""), 4096)
    testcase.assertLessEqual(len(embed.fields), 25)
    for field in embed.fields:
        testcase.assertLessEqual(len(field.name or ""), 256)
        testcase.assertLessEqual(len(field.value or ""), 1024)
    testcase.assertLessEqual(len(embed), 6000)


class CacheFreshnessTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.db = Database(str(Path(self.directory.name) / "freshness.db"), turso_url="")
        await self.db.connect()
        self.clock = FakeClock(datetime(2026, 11, 13, 12, 0, 0, tzinfo=timezone.utc))
        self._mono_origin = self.clock()
        self.library_sync = LibrarySyncService(
            clock=lambda: (self.clock() - self._mono_origin).total_seconds()
        )
        self.api = SimpleNamespace(
            get_user_library=AsyncMock(),
            get_user_profile=AsyncMock(),
            get_user_journal=AsyncMock(return_value=[]),
            get_user_reviews=AsyncMock(return_value=[]),
            search_games=AsyncMock(return_value=[]),
            get_game=AsyncMock(),
        )
        self.bot = SimpleNamespace(
            db=self.db,
            backloggd=self.api,
            library_sync=self.library_sync,
        )

    async def asyncTearDown(self):
        await self.library_sync.close()
        await self.db.close()
        self.directory.cleanup()

    async def _drain_syncs(self):
        tasks = [s.task for s in self.library_sync._profiles.values() if s.task]
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    def _interaction(self, *, user_id=10, members=None, guild_name="Fresh Guild"):
        guild_members = members or [SimpleNamespace(id=10, display_name="A"), SimpleNamespace(id=20, display_name="B")]
        return SimpleNamespace(
            user=SimpleNamespace(id=user_id, display_name=f"Member{user_id}", display_avatar=None),
            guild=SimpleNamespace(id=99, name=guild_name, members=guild_members, get_member=lambda uid: None),
            guild_id=99,
            response=SimpleNamespace(defer=AsyncMock(), send_message=AsyncMock()),
            followup=SimpleNamespace(send=AsyncMock(return_value=SimpleNamespace())),
        )

    async def _seed_profile(self, discord_id: int, username: str, entries: list[Entry], synced_at: datetime | None):
        await self.db.link_user(discord_id, username)
        if synced_at is not None:
            for e in entries:
                await self.db.upsert_game(e.slug, e.title)
            await self.db.replace_profile_library(username, entries, synced_at=synced_at)

    async def test_fresh_vs_stale_compare_serves_promptly_and_schedules_stale_background_refresh(self):
        fresh_time = self.clock() - timedelta(minutes=20)
        stale_time = self.clock() - timedelta(minutes=75)
        await self._seed_profile(10, "Alice", [Entry("hades", 4.5), Entry("elden-ring", 5.0)], fresh_time)
        await self._seed_profile(20, "Bob", [Entry("hades", 4.0), Entry("elden-ring", 3.0)], stale_time)

        started = asyncio.Event()
        release = asyncio.Event()

        async def slow_fetch(username, force_refresh=False):
            started.set()
            await release.wait()
            return [Entry("hades", 4.5), Entry("elden-ring", 3.5)]

        self.api.get_user_library.side_effect = slow_fetch
        cog = SocialCog(self.bot, clock=self.clock)
        interaction = self._interaction()

        # Must return promptly even while Bob's background refresh is blocked on `release`.
        await asyncio.wait_for(SocialCog.compare.callback(cog, interaction, "<@10>", "<@20>"), timeout=2.0)

        await asyncio.wait_for(started.wait(), timeout=1.0)
        embed = interaction.followup.send.call_args.kwargs["embed"]
        assert_discord_embed_limits(self, embed)
        freshness_field = next(f for f in embed.fields if "Cache Freshness" in f.name)
        self.assertIn(f"<t:{int(fresh_time.timestamp())}:R>", freshness_field.value)
        self.assertIn(f"<t:{int(stale_time.timestamp())}:R>", freshness_field.value)
        self.assertIn("refreshing in background", freshness_field.value)
        self.api.get_user_library.assert_awaited_once_with("Bob", force_refresh=True)

        release.set()
        await self._drain_syncs()

    async def test_missing_linked_caches_report_fetching_instead_of_empty_library(self):
        # Link profiles without syncing libraries yet (last_synced_at is None).
        await self.db.link_user(10, "Alice")
        await self.db.link_user(20, "Bob")
        release = asyncio.Event()

        async def pending_fetch(username, force_refresh=False):
            await release.wait()
            return [Entry("hades", 4.5)]

        self.api.get_user_library.side_effect = pending_fetch

        social = SocialCog(self.bot, clock=self.clock)
        discovery = DiscoveryCog(self.bot, clock=self.clock)

        # /compare
        i1 = self._interaction()
        await SocialCog.compare.callback(social, i1, "<@10>", "<@20>")
        self.assertIn("currently being fetched in the background", i1.followup.send.call_args.args[0])
        self.assertNotIn("does not have any logged games", i1.followup.send.call_args.args[0])

        # /roulette
        i2 = self._interaction()
        await DiscoveryCog.roulette.callback(discovery, i2)
        self.assertIn("currently being fetched in the background", i2.followup.send.call_args.args[0])
        self.assertNotIn("no unrated games", i2.followup.send.call_args.args[0].lower())

        # /taste-twins
        i3 = self._interaction()
        await SocialCog.taste_twins.callback(social, i3)
        self.assertIn("currently being fetched in the background", i3.followup.send.call_args.args[0])

        # /recommend server mode & friend mode
        i4 = self._interaction()
        await DiscoveryCog.recommend.callback(discovery, i4)
        self.assertIn("currently being fetched in the background", i4.followup.send.call_args.args[0])

        i5 = self._interaction()
        await DiscoveryCog.recommend.callback(discovery, i5, user=SimpleNamespace(id=20, display_name="Bob"))
        self.assertIn("currently being fetched in the background", i5.followup.send.call_args.args[0])

        release.set()
        await self._drain_syncs()

    async def test_failed_refresh_obeys_five_minute_backoff_with_injected_clock(self):
        stale_time = self.clock() - timedelta(hours=2)
        await self._seed_profile(10, "Alice", [Entry("backlog-1", None, "backlog")], stale_time)
        self.api.get_user_library.side_effect = RuntimeError("upstream 503")
        cog = DiscoveryCog(self.bot, clock=self.clock)

        # First /roulette call triggers background sync which fails at T+0.
        i1 = self._interaction(members=[SimpleNamespace(id=10, display_name="Alice")])
        await DiscoveryCog.roulette.callback(cog, i1)
        await self._drain_syncs()
        self.assertEqual(self.api.get_user_library.await_count, 1)

        # After 2 minutes (< 5 min backoff), retry is suppressed and cached failure state is disclosed.
        self.clock.advance(minutes=2)
        i2 = self._interaction(members=[SimpleNamespace(id=10, display_name="Alice")])
        await DiscoveryCog.roulette.callback(cog, i2)
        await asyncio.sleep(0)
        self.assertEqual(self.api.get_user_library.await_count, 1)
        embed2 = i2.followup.send.call_args.kwargs["embed"]
        freshness2 = next(f.value for f in embed2.fields if "Cache Freshness" in f.name)
        self.assertIn("last refresh failed", freshness2)
        self.assertIn(f"<t:{int(stale_time.timestamp())}:R>", freshness2)

        # After 4 more minutes (total 6 min >= 5 min backoff), next call retries background sync.
        self.clock.advance(minutes=4)
        i3 = self._interaction(members=[SimpleNamespace(id=10, display_name="Alice")])
        await DiscoveryCog.roulette.callback(cog, i3)
        await self._drain_syncs()
        self.assertEqual(self.api.get_user_library.await_count, 2)

    async def test_duplicate_members_count_once_in_aggregate_freshness_across_game_and_stats(self):
        fresh_time = self.clock() - timedelta(minutes=15)
        stale_time = self.clock() - timedelta(minutes=90)
        await self._seed_profile(10, "Millers", [Entry("hades", 5.0), Entry("a", 4.0), Entry("b", 4.0), Entry("c", 4.0), Entry("d", 4.0)], fresh_time)
        await self.db.link_user(11, "millers")  # Duplicate connection to Millers
        await self._seed_profile(20, "PlayerB", [Entry("hades", 4.0), Entry("a", 4.5), Entry("b", 4.5), Entry("c", 4.5), Entry("d", 4.5), Entry("rec-game", 5.0)], stale_time)
        await self.db.link_user(30, "PlayerC")  # Missing cache

        self.api.get_user_library.return_value = [
            Entry("hades", 4.0),
            Entry("a", 4.5),
            Entry("b", 4.5),
            Entry("c", 4.5),
            Entry("d", 4.5),
            Entry("rec-game", 5.0),
        ]
        self.api.search_games.return_value = [SimpleNamespace(slug="hades", title="Hades", release_year=2020)]
        self.api.get_game.return_value = SimpleNamespace(
            slug="hades", title="Hades", release_year=2020, community_rating=4.6, summary="Roguelike",
            cover_url=None, stats=None,
        )

        members = [
            SimpleNamespace(id=10, display_name="M1"),
            SimpleNamespace(id=11, display_name="M2"),
            SimpleNamespace(id=20, display_name="B"),
            SimpleNamespace(id=30, display_name="C"),
        ]
        games_cog = GamesCog(self.bot, clock=self.clock)
        stats_cog = StatsCog(self.bot, clock=self.clock)
        social_cog = SocialCog(self.bot, clock=self.clock)
        discovery_cog = DiscoveryCog(self.bot, clock=self.clock)

        # /game
        ig = self._interaction(members=members)
        await GamesCog.game.callback(games_cog, ig, "Hades")
        game_embed = ig.followup.send.call_args.kwargs["embed"]
        assert_discord_embed_limits(self, game_embed)
        gf = next(f.value for f in game_embed.fields if "Cache Freshness" in f.name)
        self.assertIn("Profiles: 3 (1 fresh, 1 stale (>1h), 1 missing)", gf)
        self.assertIn(f"<t:{int(stale_time.timestamp())}:R>", gf)

        # Let background tasks from /game complete before checking updated fresh state
        await self._drain_syncs()

        # /server-stats keeps 6 metric fields and puts aggregate timestamp in description
        is_stats = self._interaction(members=members)
        await StatsCog.server_stats.callback(stats_cog, is_stats)
        stats_embed = is_stats.followup.send.call_args.kwargs["embed"]
        assert_discord_embed_limits(self, stats_embed)
        self.assertEqual(len(stats_embed.fields), 6)
        self.assertIn(f"<t:{int(fresh_time.timestamp())}:R>", stats_embed.description)

        # /server-top
        is_top = self._interaction(members=members)
        await StatsCog.server_top.callback(stats_cog, is_top, mode="Highest Rated")
        top_embed = is_top.followup.send.call_args.kwargs["embed"]
        assert_discord_embed_limits(self, top_embed)
        tf = next(f.value for f in top_embed.fields if "Cache Freshness" in f.name)
        self.assertIn("Profiles: 3", tf)

        # /taste-twins & /recommend aggregate unique profiles
        it = self._interaction(user_id=10, members=members)
        await SocialCog.taste_twins.callback(social_cog, it)
        twins_embed = it.followup.send.call_args.kwargs["embed"]
        assert_discord_embed_limits(self, twins_embed)
        ttf = next(f.value for f in twins_embed.fields if "Cache Freshness" in f.name)
        self.assertIn("Profiles: 3", ttf)

        ir = self._interaction(user_id=10, members=members)
        await DiscoveryCog.recommend.callback(discovery_cog, ir)
        rec_embed = ir.followup.send.call_args.kwargs["embed"]
        assert_discord_embed_limits(self, rec_embed)
        rf = next(f.value for f in rec_embed.fields if "Cache Freshness" in f.name)
        self.assertIn("Profiles: 3", rf)

    async def test_empty_aggregates_do_not_claim_fresh_data(self):
        stats_cog = StatsCog(self.bot, clock=self.clock)
        games_cog = GamesCog(self.bot, clock=self.clock)
        self.api.search_games.return_value = [SimpleNamespace(slug="hades", title="Hades", release_year=2020)]
        self.api.get_game.return_value = SimpleNamespace(
            slug="hades", title="Hades", release_year=2020, community_rating=4.6, summary="Roguelike",
            cover_url=None, stats=None,
        )

        i1 = self._interaction(members=[SimpleNamespace(id=999, display_name="Unlinked")])
        await StatsCog.server_stats.callback(stats_cog, i1)
        stats_embed = i1.followup.send.call_args.kwargs["embed"]
        self.assertIn("No linked Backloggd profiles in cache yet", stats_embed.description)

        i2 = self._interaction(members=[SimpleNamespace(id=999, display_name="Unlinked")])
        await GamesCog.game.callback(games_cog, i2, "Hades")
        game_embed = i2.followup.send.call_args.kwargs["embed"]
        gf = next(f.value for f in game_embed.fields if "Cache Freshness" in f.name)
        self.assertIn("No linked Backloggd profiles in cache yet", gf)

    async def test_source_healthy_library_stale_separation_on_feed_status_and_profile(self):
        stale_lib_time = self.clock() - timedelta(hours=3)
        healthy_feed_time = self.clock() - timedelta(minutes=2)
        await self._seed_profile(10, "Millers", [Entry("hades", 4.5)], stale_lib_time)
        await self.db.set_feed_channel(99, 555)
        await self.db.set_guild_feed_preference(99, 10, True)
        await self.db.reconcile_guild_feed_activity(99, ["millers"], now=healthy_feed_time)
        await self.db.record_feed_source_success(
            "millers", "activity", coverage="exhausted", pages_read=1
        )
        await self.db.record_feed_source_success(
            "millers", "journal", coverage="complete", pages_read=1
        )

        self.api.get_user_profile.return_value = SimpleNamespace(
            username="Millers", avatar_url=None, bio="Hi", badges=[], favorite_games=[],
            played_count=1, playing_count=0, backlog_count=0, wishlist_count=0,
        )
        self.api.get_user_library.return_value = [Entry("hades", 5.0)]

        feed_cog = FeedCog(SimpleNamespace(db=self.db, backloggd=self.api, guilds=[]), clock=self.clock)
        users_cog = UsersCog(self.bot, clock=self.clock)

        i_status = self._interaction(members=[SimpleNamespace(id=10, display_name="Millers")])
        await FeedCog.feed_status.callback(feed_cog, i_status)
        status_embed = i_status.followup.send.call_args.kwargs["embed"]
        assert_discord_embed_limits(self, status_embed)
        health_val = next(f.value for f in status_embed.fields if "Source Health" in f.name)
        self.assertRegex(health_val, r"<t:\d+:R>")
        self.assertIn("`exhausted`", health_val)
        self.assertIn("separate from library cache freshness", status_embed.footer.text)

        i_profile = self._interaction(members=[SimpleNamespace(id=10, display_name="Millers")])
        await UsersCog.profile.callback(users_cog, i_profile)
        profile_embed = i_profile.followup.send.call_args.kwargs["embed"]
        assert_discord_embed_limits(self, profile_embed)
        pf = next(f.value for f in profile_embed.fields if "Cache Freshness" in f.name)
        self.assertIn(f"<t:{int(stale_lib_time.timestamp())}:R>", pf)
        self.assertIn("refreshing in background", pf)

        await self._drain_syncs()

    def test_embed_builders_respect_discord_limits_and_avoid_footer_timestamps(self):
        long_note = "Oldest contributing sync: <t:1794571200:R> · " + ("stale " * 250)
        long_game = SimpleNamespace(
            slug="very-long-game",
            title="G" * 300,
            release_year=2025,
            community_rating=4.5,
            summary="S" * 500,
            cover_url=None,
            stats=SimpleNamespace(plays_count=10, playing_count=1, backlogs_count=2, wishlists_count=3, total_ratings=50),
        )
        long_profile = SimpleNamespace(
            username="U" * 100,
            avatar_url=None,
            bio="B" * 500,
            badges=["VIP", "Patron"],
            favorite_games=["F" * 100 for _ in range(5)],
            played_count=10,
            playing_count=1,
            backlog_count=2,
            wishlist_count=3,
        )
        embeds = [
            create_game_embed(long_game, [], guild_name="G" * 100, freshness_note=long_note),
            create_profile_embed(long_profile, avg_rating=4.5, rated_count=10, freshness_note=long_note),
            create_compare_embed("A" * 50, "B" * 50, 0.85, [{"title": "X" * 100, "r1": 5.0, "r2": 4.5}], [], freshness_note=long_note),
            create_roulette_embed("A" * 50, long_game, 10, freshness_note=long_note),
            create_server_top_embed("Top", [{"slug": "s", "title": "T" * 100, "avg_rating": 4.5, "rating_count": 3}], "highest_rated", "Server", freshness_note=long_note),
            create_server_stats_embed({"linked_members": 2, "profile_count": 1, "total_logged": 5, "total_played": 3, "server_avg_rating": 4.2, "top_game": "Hades"}, "Server", freshness_note=long_note),
            create_taste_twins_embed("A" * 50, None, "Server", [{"username": "B" * 50, "match_pct": 80, "shared_count": 5}], freshness_note=long_note),
        ]
        for emb in embeds:
            assert_discord_embed_limits(self, emb)
            self.assertNotIn("<t:", emb.footer.text or "")


if __name__ == "__main__":
    unittest.main()
