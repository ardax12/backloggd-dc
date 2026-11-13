"""Regression tests for Task 7: consolidated freshness presentation helpers."""

import os
os.environ["PYTHON_DOTENV_DISABLED"] = "1"
os.environ["TURSO_DATABASE_URL"] = ""
os.environ["TURSO_AUTH_TOKEN"] = ""
os.environ["DEV_GUILD_ID"] = ""

import ast
import asyncio
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

from bot.cogs.discovery import DiscoveryCog
from bot.cogs.games import GamesCog
from bot.cogs.social import SocialCog
from bot.cogs.stats import StatsCog
from bot.db import Database
from bot.services import freshness as freshness_mod
from bot.services.library_sync import LibrarySyncService


class FreshnessConsolidationTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmpdir.cleanup)
        self.db = Database(str(Path(self.tmpdir.name) / "freshness_consolidation.db"), turso_url="")
        await self.db.connect()
        self.addAsyncCleanup(self.db.close)
        self.now = datetime(2026, 11, 13, 12, 0, 0, tzinfo=timezone.utc)
        self.mono = 0.0
        self.library_sync = LibrarySyncService(clock=lambda: self.mono)
        self.addAsyncCleanup(self.library_sync.close)
        self.api = SimpleNamespace(
            get_user_library=AsyncMock(return_value=[]),
            get_user_profile=AsyncMock(return_value=SimpleNamespace(played_count=5)),
            search_games=AsyncMock(return_value=[]),
            get_game=AsyncMock(
                return_value=SimpleNamespace(
                    slug="hades",
                    title="Hades",
                    cover_url=None,
                    release_year=2020,
                    release_date=None,
                    community_rating=4.6,
                    url="https://www.backloggd.com/games/hades/",
                    summary="Roguelike",
                    genres=["Action"],
                    platforms=["PC"],
                    developers=["Supergiant"],
                    stats=SimpleNamespace(total_ratings=10, played=10, playing=1, backlog=2, wishlist=3),
                )
            ),
        )
        self.bot = SimpleNamespace(
            db=self.db,
            backloggd=self.api,
            library_sync=self.library_sync,
        )

    def test_freshness_service_does_not_depend_on_cogs_or_ui(self):
        source_path = Path(freshness_mod.__file__)
        tree = ast.parse(source_path.read_text(encoding="utf-8"))
        imported_modules: list[str] = []
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                imported_modules.extend(alias.name for alias in node.names)
            elif isinstance(node, ast.ImportFrom) and node.module:
                imported_modules.append(node.module)
        for mod in imported_modules:
            self.assertFalse(mod.startswith("bot.cogs"), f"freshness.py must not import {mod}")
            self.assertFalse(mod.startswith("bot.ui"), f"freshness.py must not import {mod}")

    def test_shared_fetching_formatter_handles_states_and_single_escaping(self):
        self.assertTrue(
            hasattr(freshness_mod, "format_fetching_library_message"),
            "freshness.py must export format_fetching_library_message",
        )
        base_kwargs = dict(
            profile_key="bad",
            username="Bad](https://evil.example) *Name* @everyone <#1>",
            synced_at=None,
            attempt_at=None,
            last_error=None,
            missing=True,
            stale=True,
        )
        refreshing = freshness_mod.ProfileFreshness(**base_kwargs, refreshing=True, failed_last=False)
        failed_backoff = freshness_mod.ProfileFreshness(**base_kwargs, refreshing=False, failed_last=True)
        failed_retrying = freshness_mod.ProfileFreshness(**base_kwargs, refreshing=True, failed_last=True)

        msg_refreshing = freshness_mod.format_fetching_library_message(base_kwargs["username"], refreshing)
        msg_failed = freshness_mod.format_fetching_library_message(base_kwargs["username"], failed_backoff)
        msg_retrying = freshness_mod.format_fetching_library_message(base_kwargs["username"], failed_retrying)

        self.assertIn("currently being fetched in the background", msg_refreshing)
        self.assertIn("last background refresh failed", msg_failed)
        self.assertIn("currently being fetched in the background", msg_retrying)
        for msg in (msg_refreshing, msg_failed, msg_retrying):
            self.assertIn("Bad\\](https://evil.example) \\*Name\\*", msg)
            self.assertNotIn("@everyone", msg)
            self.assertNotIn("<#1>", msg)

        normal = freshness_mod.format_fetching_library_message("Alice_One", refreshing)
        self.assertIn("**Alice\\_One**", normal)
        self.assertNotIn("\\\\_", normal)

    async def test_discovery_and_social_missing_cache_messages_match_across_states(self):
        malicious = "Bad](https://evil.example) *Name* @here <#42>"
        release = asyncio.Event()

        async def slow_or_fail(username, force_refresh=False):
            await release.wait()
            raise RuntimeError("boom")

        self.api.get_user_library.side_effect = slow_or_fail
        await self.db.link_user(10, malicious)
        await self.db.link_user(20, "Partner")
        await self.db.replace_profile_library(
            "Partner",
            [SimpleNamespace(slug="hades", title="Hades", rating=5.0, status="played", cover_url=None, release_year=2020, community_rating=4.5, is_liked=False)],
            synced_at=self.now - timedelta(minutes=10),
        )

        discovery = DiscoveryCog(self.bot, clock=lambda: self.now)
        social = SocialCog(self.bot, clock=lambda: self.now)

        def _interaction():
            return SimpleNamespace(
                user=SimpleNamespace(id=10, display_name="Caller"),
                guild=SimpleNamespace(id=1, name="Guild", members=[SimpleNamespace(id=10), SimpleNamespace(id=20)]),
                guild_id=1,
                response=SimpleNamespace(defer=AsyncMock()),
                followup=SimpleNamespace(send=AsyncMock()),
            )

        # 1. Running background refresh state matches across Discovery (/roulette) and Social (/compare)
        i_disc_run = _interaction()
        await DiscoveryCog.roulette.callback(discovery, i_disc_run, None)
        i_soc_run = _interaction()
        await SocialCog.compare.callback(social, i_soc_run, "<@20>", None)

        msg_disc_run = i_disc_run.followup.send.call_args.args[0]
        msg_soc_run = i_soc_run.followup.send.call_args.args[0]
        self.assertEqual(msg_disc_run, msg_soc_run)
        self.assertIn("currently being fetched in the background", msg_disc_run)
        self.assertNotIn("@here", msg_disc_run)
        self.assertNotIn("<#42>", msg_disc_run)

        # Complete the in-flight sync with an upstream failure so cache remains missing and enters backoff.
        release.set()
        tasks = [s.task for s in self.library_sync._profiles.values() if s.task]
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

        # 2. Failed-in-backoff missing state matches across Discovery (/roulette) and Social (/compare)
        self.mono += 30.0
        self.now = self.now + timedelta(seconds=30)
        i_disc_fail = _interaction()
        await DiscoveryCog.roulette.callback(discovery, i_disc_fail, None)
        i_soc_fail = _interaction()
        await SocialCog.compare.callback(social, i_soc_fail, "<@20>", None)

        msg_disc_fail = i_disc_fail.followup.send.call_args.args[0]
        msg_soc_fail = i_soc_fail.followup.send.call_args.args[0]
        self.assertEqual(msg_disc_fail, msg_soc_fail)
        self.assertIn("last background refresh failed", msg_disc_fail)

    async def test_games_and_stats_share_aggregate_freshness_across_empty_and_mixed_states(self):
        self.assertTrue(
            hasattr(freshness_mod, "resolve_guild_aggregate_freshness"),
            "freshness.py must export resolve_guild_aggregate_freshness",
        )

        games = GamesCog(self.bot, clock=lambda: self.now)
        stats = StatsCog(self.bot, clock=lambda: self.now)

        def _interaction(member_ids):
            return SimpleNamespace(
                user=SimpleNamespace(id=member_ids[0] if member_ids else 1),
                guild=SimpleNamespace(
                    id=50,
                    name="Fresh Guild",
                    members=[SimpleNamespace(id=mid) for mid in member_ids],
                ),
                guild_id=50,
                response=SimpleNamespace(defer=AsyncMock()),
                followup=SimpleNamespace(send=AsyncMock()),
            )

        # 1. Empty server
        i_game_empty = _interaction([])
        await GamesCog.game.callback(games, i_game_empty, "slug:hades")
        i_top_empty = _interaction([])
        await StatsCog.server_top.callback(stats, i_top_empty, mode="Highest Rated")

        game_fresh_empty = next(
            f.value for f in i_game_empty.followup.send.call_args.kwargs["embed"].fields if "Cache Freshness" in f.name
        )
        top_fresh_empty = next(
            f.value for f in i_top_empty.followup.send.call_args.kwargs["embed"].fields if "Cache Freshness" in f.name
        )
        self.assertEqual(game_fresh_empty, top_fresh_empty)
        self.assertIn("No linked Backloggd profiles", game_fresh_empty)

        # 2. Mixed fresh + stale + missing + failed-backoff profiles
        fresh_ts = self.now - timedelta(minutes=15)
        stale_ts = self.now - timedelta(hours=3)
        await self.db.link_user(10, "FreshUser")
        await self.db.link_user(20, "StaleFailedUser")
        await self.db.link_user(30, "MissingUser")
        await self.db.replace_profile_library("FreshUser", [], synced_at=fresh_ts)
        await self.db.replace_profile_library("StaleFailedUser", [], synced_at=stale_ts)
        await self.db.record_profile_sync_failure("StaleFailedUser", "timeout", attempted_at=self.now - timedelta(minutes=1))

        release = asyncio.Event()

        async def pending(username, force_refresh=False):
            await release.wait()
            return []

        self.api.get_user_library.side_effect = pending

        i_game = _interaction([10, 20, 30])
        await GamesCog.game.callback(games, i_game, "slug:hades")
        i_top = _interaction([10, 20, 30])
        await StatsCog.server_top.callback(stats, i_top, mode="Highest Rated")
        i_stats = _interaction([10, 20, 30])
        await StatsCog.server_stats.callback(stats, i_stats)

        game_fresh = next(
            f.value for f in i_game.followup.send.call_args.kwargs["embed"].fields if "Cache Freshness" in f.name
        )
        top_fresh = next(
            f.value for f in i_top.followup.send.call_args.kwargs["embed"].fields if "Cache Freshness" in f.name
        )
        stats_desc = i_stats.followup.send.call_args.kwargs["embed"].description

        self.assertEqual(game_fresh, top_fresh)
        self.assertEqual(stats_desc, f"⏱ {game_fresh}")
        self.assertIn("1 fresh, 1 stale (>1h), 1 missing", game_fresh)
        self.assertIn("Last refresh failed: 1", game_fresh)

        release.set()
        tasks = [s.task for s in self.library_sync._profiles.values() if s.task]
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)


if __name__ == "__main__":
    unittest.main()
