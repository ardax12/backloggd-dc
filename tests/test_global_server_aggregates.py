"""Regression tests for global leaderboard and server statistics aggregation across SQL chunks."""

import os
import sqlite3
import tempfile
import unittest
from pathlib import Path

os.environ["PYTHON_DOTENV_DISABLED"] = "1"
os.environ["TURSO_DATABASE_URL"] = ""
os.environ["TURSO_AUTH_TOKEN"] = ""
os.environ["DEV_GUILD_ID"] = ""

from bot.db import Database
from tests.test_bounded_sql_lookups import BudgetTursoClient


def _naive_reference(linked_rows, rating_rows, game_map, admin_only_profiles):
    """Simple Python reference calculator over the full unchunked fixture."""
    distinct_members = {row[0] for row in linked_rows}
    distinct_profiles = sorted({row[1] for row in linked_rows})
    profile_set = set(distinct_profiles)
    assert not (profile_set & set(admin_only_profiles))

    # Filter ratings strictly to linked profiles (excluding admin-only feed follows).
    eligible = [r for r in rating_rows if r[0] in profile_set]

    by_game: dict[str, list[tuple[str, float | None, str]]] = {}
    for profile_key, slug, rating, status in eligible:
        by_game.setdefault(slug, []).append((profile_key, rating, status))

    def top_rated(limit=10, min_ratings=2):
        rows = []
        for slug, items in by_game.items():
            rated = [r for _, r, _ in items if r is not None]
            if len(rated) >= min_ratings:
                rows.append({
                    "slug": slug,
                    "title": game_map[slug][0],
                    "avg_rating": sum(rated) / len(rated),
                    "rating_count": len(rated),
                })
        rows.sort(key=lambda r: (-r["avg_rating"], -r["rating_count"], r["slug"]))
        return rows[:limit]

    def most_played(limit=10):
        rows = []
        for slug, items in by_game.items():
            played = [r for _, r, status in items if status == "played"]
            if played:
                rated = [r for r in played if r is not None]
                rows.append({
                    "slug": slug,
                    "title": game_map[slug][0],
                    "play_count": len(played),
                    "avg_rating": (sum(rated) / len(rated)) if rated else None,
                })
        rows.sort(
            key=lambda r: (
                -r["play_count"],
                r["avg_rating"] is None,
                -(r["avg_rating"] if r["avg_rating"] is not None else 0.0),
                r["slug"],
            )
        )
        return rows[:limit]

    def shame_list(limit=10):
        rows = []
        for slug, items in by_game.items():
            backlog = [1 for _, _, status in items if status == "backlog"]
            if backlog:
                rows.append({
                    "slug": slug,
                    "title": game_map[slug][0],
                    "backlog_count": len(backlog),
                })
        rows.sort(key=lambda r: (-r["backlog_count"], r["slug"]))
        return rows[:limit]

    def polarizing(limit=10, min_ratings=2):
        rows = []
        for slug, items in by_game.items():
            rated = [r for _, r, _ in items if r is not None]
            if len(rated) >= min_ratings:
                delta = max(rated) - min(rated)
                if delta > 0:
                    rows.append({
                        "slug": slug,
                        "title": game_map[slug][0],
                        "rating_delta": delta,
                        "avg_rating": sum(rated) / len(rated),
                        "rating_count": len(rated),
                    })
        rows.sort(key=lambda r: (-r["rating_delta"], -r["rating_count"], r["slug"]))
        return rows[:limit]

    def hidden_gems(limit=10):
        rows = []
        for slug, items in by_game.items():
            rated = [r for _, r, _ in items if r is not None]
            if rated and len(rated) <= 2:
                avg = sum(rated) / len(rated)
                if avg >= 4.5:
                    rows.append({
                        "slug": slug,
                        "title": game_map[slug][0],
                        "avg_rating": avg,
                        "rating_count": len(rated),
                    })
        rows.sort(key=lambda r: (-r["avg_rating"], r["rating_count"], r["slug"]))
        return rows[:limit]

    all_rated = [r for _, _, r, _ in eligible if r is not None]
    total_played = sum(1 for _, _, _, s in eligible if s == "played")
    played_by_slug = {}
    for _, slug, _, status in eligible:
        if status == "played":
            played_by_slug[slug] = played_by_slug.get(slug, 0) + 1
    top_game = None
    if played_by_slug:
        best_slug = min(played_by_slug, key=lambda s: (-played_by_slug[s], s))
        cnt = played_by_slug[best_slug]
        top_game = f"{game_map[best_slug][0]} ({cnt} profile{'s' if cnt != 1 else ''})"

    stats = {
        "linked_members": len(distinct_members),
        "profile_count": len(distinct_profiles),
        "total_logged": len(eligible),
        "total_played": total_played,
        "server_avg_rating": round(sum(all_rated) / len(all_rated), 2) if all_rated else None,
        "top_game": top_game,
    }
    return {
        "top_rated": top_rated,
        "most_played": most_played,
        "shame_list": shame_list,
        "polarizing": polarizing,
        "hidden_gems": hidden_gems,
        "stats": stats,
    }


class GlobalServerAggregateTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmpdir.cleanup)
        self.sqlite_path = Path(self.tmpdir.name) / "sqlite.db"
        self.db = Database(str(self.sqlite_path), turso_url="")
        self.addAsyncCleanup(self.db.close)
        await self.db.connect()
        await self.db._conn._execute(
            self.db._conn._conn.setlimit, sqlite3.SQLITE_LIMIT_VARIABLE_NUMBER, 999
        )

    async def _seed_cross_chunk_fixture(self, db: Database):
        # 850 distinct linked profiles -> two unequal chunks (800 and 50).
        # Plus 1 feed-only admin follow whose ratings must be ignored in stats/leaderboards.
        profiles = []
        linked = []
        games = {
            "global-top": ("Global Top", None, 2024, 4.9),
            "trap-top": ("Trap Top", None, 2023, 3.0),
            "min-cross": ("Min Cross", None, 2022, 4.4),
            "polar-cross": ("Polar Cross", None, 2021, 3.5),
            "gem-excluded-count": ("Gem Excluded Count", None, 2020, 4.5),
            "gem-excluded-avg": ("Gem Excluded Avg", None, 2019, 4.2),
            "gem-included-cross": ("Gem Included Cross", None, 2018, 4.5),
            "zero-played": ("Zero Played", None, 2017, 1.0),
            "null-played": ("Null Played", None, 2016, 2.0),
            "shame-cross": ("Shame Cross", None, 2015, 3.8),
            "tie-a": ("Tie A", None, 2014, 4.0),
            "tie-b": ("Tie B", None, 2014, 4.0),
        }

        for idx in range(850):
            display = f"P_{idx:04d}" if idx % 2 == 0 else f"p_{idx:04d}"
            key = display.lower()
            profiles.append((key, display, None))
            linked.append((idx + 1, key, display, None))

        # Add duplicate connections spanning across chunk boundaries (members 1, 900, 1700 -> p_0000;
        # members 901, 1701 -> p_0810).
        linked.append((900, "p_0000", "P_0000", None))
        linked.append((1700, "p_0000", "p_0000", None))
        linked.append((901, "p_0810", "P_0810", None))
        linked.append((1701, "p_0810", "p_0810", None))

        # Feed-only admin profile (NOT in linked_users).
        admin_only = ["feed_only_critic"]
        profiles.append(("feed_only_critic", "FeedOnlyCritic", None))

        # Build profile_ratings carefully:
        # Chunk 1 profiles are p_0000..p_0799; Chunk 2 profiles are p_0800..p_0849.
        ratings = [
            # Global winner across chunks (fails min_ratings=2 in each chunk alone)
            ("p_0000", "global-top", 5.0, "played"),
            ("p_0800", "global-top", 5.0, "played"),
            # Trap average: high in chunk 1 (4.8 * 3), low in chunk 2 (1.0 * 5) -> global avg 2.425
            ("p_0001", "trap-top", 4.8, "played"),
            ("p_0002", "trap-top", 4.8, "played"),
            ("p_0003", "trap-top", 4.8, "played"),
            ("p_0801", "trap-top", 1.0, "played"),
            ("p_0802", "trap-top", 1.0, "played"),
            ("p_0803", "trap-top", 1.0, "played"),
            ("p_0804", "trap-top", 1.0, "played"),
            ("p_0805", "trap-top", 1.0, "played"),
            # Minimum-count crossing: 1 in chunk 1, 1 in chunk 2
            ("p_0004", "min-cross", 4.5, "played"),
            ("p_0806", "min-cross", 4.5, "played"),
            # Polarizing extrema across boundary including a 0.0 rating
            ("p_0005", "polar-cross", 5.0, "played"),
            ("p_0807", "polar-cross", 0.0, "played"),
            # Hidden gems excluded by count (2 in chunk 1 + 1 in chunk 2 = 3 > 2)
            ("p_0006", "gem-excluded-count", 4.5, "played"),
            ("p_0007", "gem-excluded-count", 4.5, "played"),
            ("p_0808", "gem-excluded-count", 4.5, "played"),
            # Hidden gems excluded by global avg (5.0 in chunk 1, 3.0 in chunk 2 -> 4.0)
            ("p_0008", "gem-excluded-avg", 5.0, "played"),
            ("p_0809", "gem-excluded-avg", 3.0, "played"),
            # Hidden gem included across boundary
            ("p_0009", "gem-included-cross", 4.5, "played"),
            ("p_0810", "gem-included-cross", 4.5, "played"),
            # Zero rating vs NULL rating in Most Played (both have play_count=4)
            ("p_0010", "zero-played", 0.0, "played"),
            ("p_0011", "zero-played", 0.0, "played"),
            ("p_0811", "zero-played", 0.0, "played"),
            ("p_0812", "zero-played", 0.0, "played"),
            ("p_0012", "null-played", None, "played"),
            ("p_0013", "null-played", None, "played"),
            ("p_0813", "null-played", None, "played"),
            ("p_0814", "null-played", None, "played"),
            # Tie-breakers (equal rating_count, avg_rating, play_count -> tie-a before tie-b)
            ("p_0014", "tie-b", 3.5, "played"),
            ("p_0815", "tie-b", 3.5, "played"),
            ("p_0015", "tie-a", 3.5, "played"),
            ("p_0816", "tie-a", 3.5, "played"),
            # Feed-only critic ratings MUST NOT affect community stats or leaderboards
            ("feed_only_critic", "gem-included-cross", 1.0, "played"),
            ("feed_only_critic", "shame-cross", None, "backlog"),
        ]
        # Shame list across chunks (6 in chunk 1, 4 in chunk 2 -> 10 total)
        for idx in range(20, 26):
            ratings.append((f"p_{idx:04d}", "shame-cross", None, "backlog"))
        for idx in range(820, 824):
            ratings.append((f"p_{idx:04d}", "shame-cross", None, "backlog"))

        # Make "shame-cross" also top_game? Wait: top_game counts status='played'.
        # Make "trap-top" have 8 played vs others having <= 4. Wait: 8 > 4 so Trap Top is unique top_game!

        game_rows = [(slug, *vals) for slug, vals in games.items()]
        rating_db_rows = [(p, s, r, st, 0) for p, s, r, st in ratings]

        if db.is_turso:
            conn = db._client.conn
            conn.executemany(
                "INSERT OR REPLACE INTO profiles (profile_key, display_username, avatar_url) VALUES (?, ?, ?)",
                profiles,
            )
            conn.executemany(
                "INSERT OR REPLACE INTO linked_users (discord_id, profile_key, backloggd_username, avatar_url) VALUES (?, ?, ?, ?)",
                linked,
            )
            conn.execute(
                "INSERT OR REPLACE INTO guild_admin_follows (guild_id, profile_key, display_username, avatar_url, added_by) VALUES (1, 'feed_only_critic', 'FeedOnlyCritic', NULL, 1)"
            )
            conn.executemany(
                "INSERT OR REPLACE INTO cached_games (slug, title, cover_url, release_year, community_rating) VALUES (?, ?, ?, ?, ?)",
                game_rows,
            )
            conn.executemany(
                "INSERT OR REPLACE INTO profile_ratings (profile_key, game_slug, rating, status, is_liked) VALUES (?, ?, ?, ?, ?)",
                rating_db_rows,
            )
            conn.commit()
        else:
            await db._conn.executemany(
                "INSERT OR REPLACE INTO profiles (profile_key, display_username, avatar_url) VALUES (?, ?, ?)",
                profiles,
            )
            await db._conn.executemany(
                "INSERT OR REPLACE INTO linked_users (discord_id, profile_key, backloggd_username, avatar_url) VALUES (?, ?, ?, ?)",
                linked,
            )
            await db._conn.execute(
                "INSERT OR REPLACE INTO guild_admin_follows (guild_id, profile_key, display_username, avatar_url, added_by) VALUES (1, 'feed_only_critic', 'FeedOnlyCritic', NULL, 1)"
            )
            await db._conn.executemany(
                "INSERT OR REPLACE INTO cached_games (slug, title, cover_url, release_year, community_rating) VALUES (?, ?, ?, ?, ?)",
                game_rows,
            )
            await db._conn.executemany(
                "INSERT OR REPLACE INTO profile_ratings (profile_key, game_slug, rating, status, is_liked) VALUES (?, ?, ?, ?, ?)",
                rating_db_rows,
            )
            await db._conn.commit()

        member_ids = [row[0] for row in linked]
        # Duplicate member ID in caller input to verify deduplication too
        member_ids.append(1)
        ref = _naive_reference(linked, ratings, games, admin_only)
        return member_ids, ref

    async def _assert_matches_reference(self, db: Database):
        member_ids, ref = await self._seed_cross_chunk_fixture(db)

        top = await db.get_server_top_rated(member_ids, limit=10, min_ratings=2)
        ref_top = ref["top_rated"](limit=10, min_ratings=2)
        self.assertEqual(
            [(r["slug"], round(r["avg_rating"], 6), r["rating_count"]) for r in top],
            [(r["slug"], round(r["avg_rating"], 6), r["rating_count"]) for r in ref_top],
        )
        # Specifically verify Global Top ranks #1 with avg 5.0 and count 2, and Trap Top is NOT averaged per chunk
        self.assertEqual(top[0]["slug"], "global-top")
        trap_entry = next(r for r in top if r["slug"] == "trap-top")
        self.assertAlmostEqual(trap_entry["avg_rating"], (3 * 4.8 + 5 * 1.0) / 8, places=6)

        played = await db.get_server_most_played(member_ids, limit=10)
        ref_played = ref["most_played"](limit=10)
        self.assertEqual(
            [(r["slug"], r["play_count"], r["avg_rating"]) for r in played],
            [(r["slug"], r["play_count"], r["avg_rating"]) for r in ref_played],
        )
        # Zero rating (0.0) sorts ahead of NULL rating when play_count ties
        zero_idx = next(i for i, r in enumerate(played) if r["slug"] == "zero-played")
        null_idx = next(i for i, r in enumerate(played) if r["slug"] == "null-played")
        self.assertLess(zero_idx, null_idx)
        self.assertEqual(played[zero_idx]["avg_rating"], 0.0)
        self.assertIsNone(played[null_idx]["avg_rating"])

        # Tie-breaker tie-a before tie-b
        tie_a_idx = next(i for i, r in enumerate(top) if r["slug"] == "tie-a")
        tie_b_idx = next(i for i, r in enumerate(top) if r["slug"] == "tie-b")
        self.assertLess(tie_a_idx, tie_b_idx)

        shame = await db.get_server_shame_list(member_ids, limit=10)
        ref_shame = ref["shame_list"](limit=10)
        self.assertEqual(
            [(r["slug"], r["backlog_count"]) for r in shame],
            [(r["slug"], r["backlog_count"]) for r in ref_shame],
        )

        polar = await db.get_server_polarizing(member_ids, limit=10, min_ratings=2)
        ref_polar = ref["polarizing"](limit=10, min_ratings=2)
        self.assertEqual(
            [(r["slug"], r["rating_delta"], round(r["avg_rating"], 6), r["rating_count"]) for r in polar],
            [(r["slug"], r["rating_delta"], round(r["avg_rating"], 6), r["rating_count"]) for r in ref_polar],
        )
        self.assertEqual(polar[0]["slug"], "polar-cross")
        self.assertEqual(polar[0]["rating_delta"], 5.0)

        gems = await db.get_server_hidden_gems(member_ids, limit=10)
        ref_gems = ref["hidden_gems"](limit=10)
        self.assertEqual(
            [(r["slug"], round(r["avg_rating"], 6), r["rating_count"]) for r in gems],
            [(r["slug"], round(r["avg_rating"], 6), r["rating_count"]) for r in ref_gems],
        )
        gem_slugs = {r["slug"] for r in gems}
        self.assertNotIn("gem-excluded-count", gem_slugs)
        self.assertNotIn("gem-excluded-avg", gem_slugs)
        self.assertIn("gem-included-cross", gem_slugs)

        stats = await db.get_server_stats(member_ids)
        self.assertEqual(stats, ref["stats"])

        # Empty guild and unlinked member handling
        self.assertEqual(await db.get_server_top_rated([]), [])
        self.assertEqual(await db.get_server_most_played([999999]), [])
        self.assertEqual(await db.get_server_shame_list([999999]), [])
        self.assertEqual(await db.get_server_polarizing([999999]), [])
        self.assertEqual(await db.get_server_hidden_gems([999999]), [])
        self.assertEqual(
            await db.get_server_stats([999999]),
            {
                "linked_members": 0,
                "profile_count": 0,
                "total_logged": 0,
                "total_played": 0,
                "server_avg_rating": None,
                "top_game": None,
            },
        )

    async def test_global_aggregation_under_sqlite_999_limit(self):
        await self._assert_matches_reference(self.db)

    async def test_global_aggregation_under_turso_800_budget(self):
        turso_path = Path(self.tmpdir.name) / "turso.db"
        seed_db = Database(str(turso_path), turso_url="")
        await seed_db.connect()
        await seed_db.close()

        client = BudgetTursoClient(str(turso_path), max_params=800)
        turso_db = Database(str(turso_path), turso_url="https://budget-test.turso.io")
        turso_db._client = client
        try:
            await self._assert_matches_reference(turso_db)
            self.assertLessEqual(client.max_params_seen, 800)
        finally:
            await turso_db.close()


if __name__ == "__main__":
    unittest.main()
