"""Regression tests for Task 5: single request-local stats membership/profile context reuse."""

import os
os.environ["PYTHON_DOTENV_DISABLED"] = "1"
os.environ["TURSO_DATABASE_URL"] = ""
os.environ["TURSO_AUTH_TOKEN"] = ""
os.environ["DEV_GUILD_ID"] = ""

import sqlite3
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

from bot.cogs.stats import StatsCog
from bot.db import Database
from bot.services.library_sync import LibrarySyncService


class StatsContextReuseTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmpdir.cleanup)
        self.db = Database(str(Path(self.tmpdir.name) / "stats_context.db"), turso_url="")
        await self.db.connect()
        await self.db._conn._execute(
            self.db._conn._conn.setlimit, sqlite3.SQLITE_LIMIT_VARIABLE_NUMBER, 999
        )
        self.now = datetime(2026, 11, 13, 12, 0, 0, tzinfo=timezone.utc)
        self.library_sync = LibrarySyncService(clock=lambda: 0.0)
        self.addAsyncCleanup(self.library_sync.close)
        self.addAsyncCleanup(self.db.close)
        self.api = SimpleNamespace(
            get_user_profile=AsyncMock(return_value=SimpleNamespace(played_count=12)),
            get_user_library=AsyncMock(return_value=[]),
        )
        self.bot = SimpleNamespace(
            db=self.db,
            backloggd=self.api,
            library_sync=self.library_sync,
        )
        self.cog = StatsCog(self.bot, clock=lambda: self.now)

    def _interaction(self, member_ids: list[int], guild_name: str = "Stats Guild"):
        return SimpleNamespace(
            guild=SimpleNamespace(
                id=101,
                name=guild_name,
                members=[SimpleNamespace(id=mid) for mid in member_ids],
                get_member=lambda uid: None,
            ),
            guild_id=101,
            response=SimpleNamespace(defer=AsyncMock()),
            followup=SimpleNamespace(send=AsyncMock()),
        )

    def _instrument_queries(self):
        queries: list[str] = []
        orig_fetchall = self.db._fetchall
        orig_fetchone = self.db._fetchone

        async def wrapped_fetchall(query, params=()):
            queries.append(query)
            return await orig_fetchall(query, params)

        async def wrapped_fetchone(query, params=()):
            queries.append(query)
            return await orig_fetchone(query, params)

        self.db._fetchall = wrapped_fetchall
        self.db._fetchone = wrapped_fetchone
        return queries

    async def test_server_stats_resolves_identity_once_and_skips_unused_row_totals(self):
        await self.db.link_user(10, "Alice")
        await self.db.link_user(11, "alice")
        await self.db.link_user(20, "Bob")
        await self.db.link_user(30, "Departed")
        await self.db.add_guild_admin_follow(101, "FeedCritic", None, 10)
        await self.db.upsert_game("shared", "Shared Game")
        await self.db.upsert_game("admin-only", "Admin Only Game")
        await self.db.upsert_user_rating(10, "shared", 4.5, "played")
        await self.db.upsert_user_rating(20, "shared", 3.5, "played")
        await self.db.upsert_user_rating(30, "shared", 1.0, "played")
        # Mark Alice and Bob recently synced so no background sync tasks spawn.
        recent = self.now - timedelta(minutes=10)
        await self.db.replace_profile_library(
            "Alice",
            [SimpleNamespace(slug="shared", title="Shared Game", rating=4.5, status="played", cover_url=None, release_year=2020, community_rating=4.0, is_liked=False)],
            synced_at=recent,
        )
        await self.db.replace_profile_library(
            "Bob",
            [SimpleNamespace(slug="shared", title="Shared Game", rating=3.5, status="played", cover_url=None, release_year=2020, community_rating=4.0, is_liked=False)],
            synced_at=recent,
        )

        queries = self._instrument_queries()
        interaction = self._interaction([10, 11, 20])
        await StatsCog.server_stats.callback(self.cog, interaction)

        identity_queries = [q for q in queries if "discord_id IN" in q]
        self.assertEqual(len(identity_queries), 1, f"Expected 1 identity query pass, got {len(identity_queries)}: {identity_queries}")
        self.assertFalse(
            any("COUNT(profile_ratings.game_slug)" in q for q in queries),
            "Freshness evaluation should reuse pre-fetched sync metadata instead of re-querying get_profiles_sync_info",
        )
        self.assertFalse(
            any("SUM(CASE WHEN status = 'played'" in q for q in queries),
            "Discord /server-stats path should not compute unused library-row total_logged/total_played",
        )

        embed = interaction.followup.send.call_args.kwargs["embed"]
        fields = {f.name: f.value for f in embed.fields}
        self.assertIn("**3** members", fields["👥 Linked Members"])
        self.assertIn("**2** profiles", fields["Unique Backloggd Profiles"])
        self.assertIn("**24** games", fields["📚 Games Logged"])
        self.assertIn("**4.00**", fields["⭐ Server Avg Rating"])
        self.assertIn("Shared Game (2 profiles)", fields["👑 Most Played Game"])
        # Only Alice and Bob fetched for Played totals (not Departed or FeedCritic).
        self.assertEqual(self.api.get_user_profile.await_count, 2)

        # Direct DB caller still receives full compatibility schema.
        direct_stats = await self.db.get_server_stats([10, 11, 20])
        self.assertEqual(
            direct_stats,
            {
                "linked_members": 3,
                "profile_count": 2,
                "total_logged": 2,
                "total_played": 2,
                "server_avg_rating": 4.0,
                "top_game": "Shared Game (2 profiles)",
            },
        )

    async def test_server_top_resolves_identity_once_and_preserves_leaderboard_results(self):
        await self.db.link_user(10, "Alice")
        await self.db.link_user(11, "alice")
        await self.db.link_user(20, "Bob")
        recent = self.now - timedelta(minutes=10)
        await self.db.replace_profile_library(
            "Alice",
            [
                SimpleNamespace(slug="g1", title="Game One", rating=5.0, status="played", cover_url=None, release_year=2022, community_rating=4.5, is_liked=False),
                SimpleNamespace(slug="b1", title="Backlog One", rating=None, status="backlog", cover_url=None, release_year=2021, community_rating=4.0, is_liked=False),
            ],
            synced_at=recent,
        )
        await self.db.replace_profile_library(
            "Bob",
            [
                SimpleNamespace(slug="g1", title="Game One", rating=3.0, status="played", cover_url=None, release_year=2022, community_rating=4.5, is_liked=False),
                SimpleNamespace(slug="gem1", title="Hidden One", rating=4.8, status="played", cover_url=None, release_year=2023, community_rating=4.2, is_liked=False),
            ],
            synced_at=recent,
        )

        queries = self._instrument_queries()
        for mode, expected_title in (
            ("Highest Rated", "Game One"),
            ("Most Played", "Game One"),
            ("Most Polarizing", "Game One"),
            ("Hidden Gems", "Hidden One"),
            ("Server Backlog Shame List", "Backlog One"),
        ):
            queries.clear()
            interaction = self._interaction([10, 11, 20])
            await StatsCog.server_top.callback(self.cog, interaction, mode=mode)

            identity_queries = [q for q in queries if "discord_id IN" in q]
            self.assertEqual(len(identity_queries), 1, f"Mode {mode} expected 1 identity query pass, got {len(identity_queries)}")
            embed = interaction.followup.send.call_args.kwargs["embed"]
            self.assertIn(expected_title, embed.description)

        # Direct DB leaderboard callers retain results.
        self.assertEqual([r["slug"] for r in await self.db.get_server_top_rated([10, 11, 20])], ["g1"])
        self.assertEqual([r["slug"] for r in await self.db.get_server_most_played([10, 11, 20])], ["g1", "gem1"])
        self.assertEqual([r["slug"] for r in await self.db.get_server_polarizing([10, 11, 20])], ["g1"])
        self.assertEqual([r["slug"] for r in await self.db.get_server_hidden_gems([10, 11, 20])], ["gem1"])
        self.assertEqual([r["slug"] for r in await self.db.get_server_shame_list([10, 11, 20])], ["b1"])

    async def test_cross_chunk_context_reuse_weighted_averages_and_missing_caches(self):
        # 850 distinct profiles -> 2 SQL chunks (800 + 50), plus duplicate connections and 1 missing cache.
        profiles = []
        linked = []
        recent_sql = (self.now - timedelta(minutes=10)).strftime("%Y-%m-%d %H:%M:%S")
        for idx in range(850):
            key = f"prof_{idx:04d}"
            display = f"Prof_{idx:04d}"
            # Last profile has missing cache (last_synced_at = None).
            synced = None if idx == 849 else recent_sql
            profiles.append((key, display, None, synced, synced, None))
            linked.append((idx + 1, key, display, None))
        # Extra duplicate member connection in chunk 2
        linked.append((1700, "prof_0000", "Prof_0000", None))
        # Departed member not in guild.members
        profiles.append(("departed_prof", "DepartedProf", None, recent_sql, recent_sql, None))
        linked.append((9999, "departed_prof", "DepartedProf", None))
        # Admin-only feed follow
        profiles.append(("admin_only", "AdminOnly", None, recent_sql, recent_sql, None))

        games = [("trap", "Trap Game", None, 2023, 3.0)]
        ratings = [
            ("prof_0000", "trap", 5.0, "played", 0),
            ("prof_0001", "trap", 5.0, "played", 0),
            ("prof_0002", "trap", 5.0, "played", 0),
            ("prof_0800", "trap", 1.0, "played", 0),
            ("prof_0801", "trap", 1.0, "played", 0),
            ("prof_0802", "trap", 1.0, "played", 0),
            ("prof_0803", "trap", 1.0, "played", 0),
            ("prof_0804", "trap", 1.0, "played", 0),
            ("departed_prof", "trap", 1.0, "played", 0),
            ("admin_only", "trap", 1.0, "played", 0),
        ]

        await self.db._conn.executemany(
            "INSERT INTO profiles (profile_key, display_username, avatar_url, last_synced_at, last_sync_attempt_at, last_sync_error) VALUES (?, ?, ?, ?, ?, ?)",
            profiles,
        )
        await self.db._conn.executemany(
            "INSERT INTO linked_users (discord_id, profile_key, backloggd_username, avatar_url) VALUES (?, ?, ?, ?)",
            linked,
        )
        await self.db._conn.execute(
            "INSERT INTO guild_admin_follows (guild_id, profile_key, display_username, avatar_url, added_by) VALUES (101, 'admin_only', 'AdminOnly', NULL, 1)"
        )
        await self.db._conn.executemany(
            "INSERT INTO cached_games (slug, title, cover_url, release_year, community_rating) VALUES (?, ?, ?, ?, ?)",
            games,
        )
        await self.db._conn.executemany(
            "INSERT INTO profile_ratings (profile_key, game_slug, rating, status, is_liked) VALUES (?, ?, ?, ?, ?)",
            ratings,
        )
        await self.db._conn.commit()

        member_ids = list(range(1, 851)) + [1700]
        queries = self._instrument_queries()
        i_top = self._interaction(member_ids)
        await StatsCog.server_top.callback(self.cog, i_top, mode="Highest Rated")

        # 851 member IDs -> 2 chunked queries in a single identity-resolution pass (not 4 across 2 passes).
        identity_queries_top = [q for q in queries if "discord_id IN" in q]
        self.assertEqual(len(identity_queries_top), 2)
        top_embed = i_top.followup.send.call_args.kwargs["embed"]
        # Global weighted avg: (3*5.0 + 5*1.0)/8 = 2.50, excluding departed and admin_only.
        self.assertIn("2.50", top_embed.description)
        freshness_val = next(f.value for f in top_embed.fields if "Cache Freshness" in f.name)
        self.assertIn("850", freshness_val)
        self.assertIn("1 missing", freshness_val)

        queries.clear()
        i_stats = self._interaction(member_ids)
        await StatsCog.server_stats.callback(self.cog, i_stats)
        identity_queries_stats = [q for q in queries if "discord_id IN" in q]
        self.assertEqual(len(identity_queries_stats), 2)
        stats_embed = i_stats.followup.send.call_args.kwargs["embed"]
        fields = {f.name: f.value for f in stats_embed.fields}
        self.assertIn("**851** members", fields["👥 Linked Members"])
        self.assertIn("**850** profiles", fields["Unique Backloggd Profiles"])
        self.assertIn("**2.50**", fields["⭐ Server Avg Rating"])


if __name__ == "__main__":
    unittest.main()
