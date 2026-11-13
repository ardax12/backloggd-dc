"""Regression tests for bounded SQL bind parameters across list lookups."""

import os
import sqlite3
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace

os.environ["PYTHON_DOTENV_DISABLED"] = "1"
os.environ["TURSO_DATABASE_URL"] = ""
os.environ["TURSO_AUTH_TOKEN"] = ""
os.environ["DEV_GUILD_ID"] = ""

from bot.db import Database


class BudgetTursoClient:
    """Turso/libSQL client double enforcing <= 800 bind parameters per statement."""

    def __init__(self, sqlite_path: str, *, max_params: int = 800):
        self.conn = sqlite3.connect(sqlite_path)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA foreign_keys = ON")
        self.max_params = max_params
        self.max_params_seen = 0
        self.closed = False

    def _check_params(self, query: str, params) -> list:
        values = list(params or ())
        count = len(values)
        if count > self.max_params_seen:
            self.max_params_seen = count
        if count > self.max_params:
            raise sqlite3.OperationalError(
                f"SQL bind parameter budget exceeded: {count} > {self.max_params} in {query[:120]}"
            )
        return values

    def transaction(self):
        client = self

        class Tx:
            async def execute(self, query, params=None):
                values = client._check_params(query, params)
                cursor = client.conn.execute(query, values)
                rows = cursor.fetchall() if cursor.description else []
                return SimpleNamespace(rows=rows, rows_affected=cursor.rowcount)

            async def commit(self):
                client.conn.commit()

            async def rollback(self):
                client.conn.rollback()

        return Tx()

    async def execute(self, query, params=None):
        values = self._check_params(query, params)
        cursor = self.conn.execute(query, values)
        client_rows = cursor.fetchall() if cursor.description else []
        self.conn.commit()
        return SimpleNamespace(rows=client_rows, rows_affected=cursor.rowcount)

    async def batch(self, statements):
        results = []
        for query, params in statements:
            values = self._check_params(query, params)
            cursor = self.conn.execute(query, values)
            rows = cursor.fetchall() if cursor.description else []
            results.append(SimpleNamespace(rows=rows, rows_affected=cursor.rowcount))
        self.conn.commit()
        return results

    async def close(self):
        self.closed = True
        self.conn.close()


class BoundedSQLLookupTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmpdir.cleanup)
        self.sqlite_path = Path(self.tmpdir.name) / "sqlite.db"
        self.db = Database(str(self.sqlite_path), turso_url="")
        self.addAsyncCleanup(self.db.close)
        await self.db.connect()
        # Enforce SQLite's historical 999 variable limit on the connection thread.
        await self.db._conn._execute(
            self.db._conn._conn.setlimit, sqlite3.SQLITE_LIMIT_VARIABLE_NUMBER, 999
        )

    async def _seed_large_fixture(self, db: Database, *, member_count: int = 2505, extra_profiles: int = 850):
        profiles = []
        linked = []
        prefs = []
        games = []
        ratings = []

        # Profiles p_0000 .. p_{extra_profiles - 1}
        for idx in range(extra_profiles):
            # Vary casing deliberately so normalization + global sorting are tested.
            display = f"Player_{idx:04d}" if idx % 2 == 0 else f"player_{idx:04d}"
            key = display.lower()
            profiles.append((key, display, None, "2026-11-12 00:00:00", "2026-11-12 00:00:00", None))

        # Member 1 links to Player_0000; member 1600 also links to player_0000 (cross-chunk duplicate + case variant).
        for mid in range(1, member_count + 1):
            if mid in (1, 1600):
                display = "Player_0000" if mid == 1 else "player_0000"
            else:
                p_idx = 1 + ((mid - 1) % (extra_profiles - 1))
                display = profiles[p_idx][1]
            key = display.lower()
            linked.append((mid, key, display, None))
            prefs.append((1, mid, 1))

        # 1200 games so slug batching (>800) is exercised too.
        for g_idx in range(1200):
            slug = f"game-{g_idx:04d}"
            games.append((slug, f"Game {g_idx:04d}", None, 2020, 4.0))

        # Every profile rates game-0000 (some NULL) so ratings shelf > 800 profiles.
        for idx, (key, _, _, _, _, _) in enumerate(profiles):
            rating = None if idx % 10 == 0 else float((idx % 5) + 1)
            ratings.append((key, "game-0000", rating, "played", 0))

        if db.is_turso:
            conn = db._client.conn
            conn.executemany(
                """INSERT OR REPLACE INTO profiles
                   (profile_key, display_username, avatar_url, last_synced_at, last_sync_attempt_at, last_sync_error)
                   VALUES (?, ?, ?, ?, ?, ?)""",
                profiles,
            )
            conn.executemany(
                """INSERT OR REPLACE INTO linked_users
                   (discord_id, profile_key, backloggd_username, avatar_url)
                   VALUES (?, ?, ?, ?)""",
                linked,
            )
            conn.executemany(
                """INSERT OR REPLACE INTO guild_feed_preferences (guild_id, discord_id, enabled)
                   VALUES (?, ?, ?)""",
                prefs,
            )
            conn.executemany(
                """INSERT OR REPLACE INTO cached_games (slug, title, cover_url, release_year, community_rating)
                   VALUES (?, ?, ?, ?, ?)""",
                games,
            )
            conn.executemany(
                """INSERT OR REPLACE INTO profile_ratings (profile_key, game_slug, rating, status, is_liked)
                   VALUES (?, ?, ?, ?, ?)""",
                ratings,
            )
            conn.commit()
        else:
            await db._conn.executemany(
                """INSERT OR REPLACE INTO profiles
                   (profile_key, display_username, avatar_url, last_synced_at, last_sync_attempt_at, last_sync_error)
                   VALUES (?, ?, ?, ?, ?, ?)""",
                profiles,
            )
            await db._conn.executemany(
                """INSERT OR REPLACE INTO linked_users
                   (discord_id, profile_key, backloggd_username, avatar_url)
                   VALUES (?, ?, ?, ?)""",
                linked,
            )
            await db._conn.executemany(
                """INSERT OR REPLACE INTO guild_feed_preferences (guild_id, discord_id, enabled)
                   VALUES (?, ?, ?)""",
                prefs,
            )
            await db._conn.executemany(
                """INSERT OR REPLACE INTO cached_games (slug, title, cover_url, release_year, community_rating)
                   VALUES (?, ?, ?, ?, ?)""",
                games,
            )
            await db._conn.executemany(
                """INSERT OR REPLACE INTO profile_ratings (profile_key, game_slug, rating, status, is_liked)
                   VALUES (?, ?, ?, ?, ?)""",
                ratings,
            )
            await db._conn.commit()
        return [p[0] for p in profiles], [g[0] for g in games]

    async def _exercise_all_lookups(self, db: Database):
        profile_keys, slugs = await self._seed_large_fixture(db)
        all_members = list(range(1, 2506))

        # Boundary sizes: 0, 1, 799, 800, 801, 2505 (with and without exclusions/extra filters)
        for size in (0, 1, 799, 800, 801, 2505):
            subset = all_members[:size]
            users = await db.get_guild_linked_users(subset)
            self.assertEqual(len(users), size)

            profs = await db.get_guild_linked_profiles(subset)
            profs_excl = await db.get_guild_linked_profiles(subset, exclude_profile_key="PLAYER_0000")
            self.assertEqual(
                [r["profile_key"] for r in profs],
                sorted(r["profile_key"] for r in profs),
            )
            self.assertNotIn("player_0000", [r["profile_key"] for r in profs_excl])

            sync_profs = await db.get_guild_linked_profiles_with_sync_info(
                subset, exclude_profile_key="PLAYER_0000"
            )
            self.assertEqual(len(sync_profs), len(profs_excl))
            self.assertNotIn("player_0000", [r["profile_key"] for r in sync_profs])

            conns = await db.get_profile_connections("Player_0000", subset, exclude_discord_id=1)
            if size >= 1600:
                self.assertEqual([r["discord_id"] for r in conns], [1600])

            feed_profs = await db.get_guild_feed_profiles(1, subset)
            self.assertEqual(len(feed_profs), len(profs))

            feed_cands = await db.get_guild_feed_candidates(1, subset)
            self.assertEqual(len(feed_cands), len(profs))

            src_members = await db.get_guild_profile_member_sources(1, "PLAYER_0000", subset)
            if size >= 1600:
                self.assertEqual(src_members, [1, 1600])

        # Verify global cross-chunk profile deduplication across 2505 members (850 distinct profiles).
        all_profs = await db.get_guild_linked_profiles(all_members)
        self.assertEqual(len(all_profs), 850)
        self.assertEqual(
            [r["profile_key"] for r in all_profs],
            sorted(profile_keys),
        )

        # Downstream profile list (> 800 profiles) and slug list (> 800 slugs)
        ratings_by_profile = await db.get_multi_profile_ratings_dict(profile_keys)
        self.assertEqual(len(ratings_by_profile), 850)
        self.assertEqual(ratings_by_profile["player_0001"]["game-0000"], 2.0)

        sync_map = await db.get_profiles_sync_info([k.upper() for k in profile_keys])
        self.assertEqual(len(sync_map), 850)

        games_map = await db.get_games_by_slugs(slugs)
        self.assertEqual(len(games_map), 1200)

        # Ratings shelf with 2505 members resolving to 850 distinct profiles
        shelf = await db.get_server_ratings_for_game(all_members, "game-0000")
        self.assertEqual(len(shelf), 850)
        # Non-null ratings descend before NULL ratings
        non_null_indices = [i for i, r in enumerate(shelf) if r["rating"] is not None]
        null_indices = [i for i, r in enumerate(shelf) if r["rating"] is None]
        self.assertLess(max(non_null_indices), min(null_indices))
        self.assertEqual(shelf[0]["rating"], 5.0)

        # Bounded member lookup (>800 IDs)
        linked_subset = await db.get_guild_linked_users(all_members[:900])
        self.assertEqual(len(linked_subset), 900)

        # Feed alias helpers & observation keys with >800 items
        aliases = [f"ev-{i:04d}" for i in range(900)]
        started = await db.begin_feed_delivery(1, "player_0000", aliases, "{}", 100)
        self.assertTrue(started)
        already = await db.begin_feed_delivery(1, "player_0000", aliases, "{}", 100)
        self.assertFalse(already)
        await db.mark_feed_delivery_unknown(1, "player_0000", aliases)
        await db.mark_feed_message_deleted(1, "player_0000", aliases, '{"deleted":true}')
        await db.release_feed_delivery(1, "player_0000", aliases)

        obs = [(f"obs-{i:04d}", f"fp-{i:04d}") for i in range(900)]
        obs_result = await db.record_profile_activity_observations("player_0000", "activity", obs)
        self.assertEqual(obs_result["new"], 900)

    async def test_all_list_lookups_stay_under_sqlite_999_limit(self):
        await self._exercise_all_lookups(self.db)

    async def test_all_list_lookups_stay_under_turso_800_param_budget(self):
        turso_path = Path(self.tmpdir.name) / "turso.db"
        seed_db = Database(str(turso_path), turso_url="")
        await seed_db.connect()
        await seed_db.close()

        client = BudgetTursoClient(str(turso_path), max_params=800)
        turso_db = Database(str(turso_path), turso_url="https://budget-test.turso.io")
        turso_db._client = client
        try:
            await self._exercise_all_lookups(turso_db)
            self.assertLessEqual(client.max_params_seen, 800)
        finally:
            await turso_db.close()


if __name__ == "__main__":
    unittest.main()
