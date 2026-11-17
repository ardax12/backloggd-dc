"""Regression tests for Task 8: /compare cached game titles with safe fallback and single-pass escaping."""

import os

os.environ["PYTHON_DOTENV_DISABLED"] = "1"
os.environ["TURSO_DATABASE_URL"] = ""
os.environ["TURSO_AUTH_TOKEN"] = ""
os.environ["DEV_GUILD_ID"] = ""

import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

from backloggd.models import GameEntry, GameStatus, UserLibrary

from bot.cogs.social import SocialCog
from bot.db import Database, _MAX_SQL_BIND_PARAMS


def _make_interaction(user_id: int = 10, guild=None):
    return SimpleNamespace(
        user=SimpleNamespace(id=user_id, display_name=f"User{user_id}", display_avatar=None),
        guild=guild,
        guild_id=getattr(guild, "id", None),
        response=SimpleNamespace(defer=AsyncMock(), is_done=lambda: True),
        followup=SimpleNamespace(send=AsyncMock(return_value=SimpleNamespace(edit=AsyncMock()))),
    )


class CompareCachedTitlesTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmpdir.cleanup)
        self.db = Database(str(Path(self.tmpdir.name) / "compare_titles.db"), turso_url="")
        self.addAsyncCleanup(self.db.close)
        await self.db.connect()

    async def test_compare_resolves_cached_titles_with_single_pass_escaping_and_slug_fallback(self):
        # Seed cached_games directly:
        # - final-fantasy-vii-rebirth -> authentic punctuation / uppercase / Roman numerals
        # - persona-5-royal -> Markdown/mention characters in cached title (must be escaped ONCE, not twice)
        # - blank-title -> empty cached title must fall back cleanly to slug.replace("-", " ").title()
        # - uncached-game -> absent from cached_games must fall back cleanly without live game fetch
        await self.db._execute_batch([
            (
                "INSERT INTO cached_games (slug, title) VALUES (?, ?)",
                ("final-fantasy-vii-rebirth", "FINAL FANTASY VII REBIRTH"),
            ),
            (
                "INSERT INTO cached_games (slug, title) VALUES (?, ?)",
                ("persona-5-royal", "Persona_5 *Royal* [Edition](https://evil.example) @everyone"),
            ),
            (
                "INSERT INTO cached_games (slug, title) VALUES (?, ?)",
                ("blank-title", "   "),
            ),
        ])

        await self.db.link_user(101, "Alpha")
        await self.db.link_user(102, "Beta")
        await self.db.replace_user_library(
            101,
            "Alpha",
            UserLibrary(
                username="Alpha",
                games={
                    "final-fantasy-vii-rebirth": GameEntry(
                        slug="final-fantasy-vii-rebirth",
                        title="FINAL FANTASY VII REBIRTH",
                        rating=5.0,
                        status=GameStatus.PLAYED,
                    ),
                    "persona-5-royal": GameEntry(
                        slug="persona-5-royal",
                        title="Persona_5 *Royal* [Edition](https://evil.example) @everyone",
                        rating=5.0,
                        status=GameStatus.PLAYED,
                    ),
                    "blank-title": GameEntry(
                        slug="blank-title",
                        title="   ",
                        rating=4.5,
                        status=GameStatus.PLAYED,
                    ),
                    "alpha-only": GameEntry(
                        slug="alpha-only",
                        title="Alpha Only",
                        rating=4.0,
                        status=GameStatus.PLAYED,
                    ),
                },
            ),
        )
        await self.db._execute("UPDATE cached_games SET title = '   ' WHERE slug = 'blank-title'")

        await self.db.replace_user_library(
            102,
            "Beta",
            UserLibrary(
                username="Beta",
                games={
                    "final-fantasy-vii-rebirth": GameEntry(
                        slug="final-fantasy-vii-rebirth",
                        title="FINAL FANTASY VII REBIRTH",
                        rating=4.5,
                        status=GameStatus.PLAYED,
                    ),
                    "persona-5-royal": GameEntry(
                        slug="persona-5-royal",
                        title="Persona_5 *Royal* [Edition](https://evil.example) @everyone",
                        rating=1.5,
                        status=GameStatus.PLAYED,
                    ),
                    "blank-title": GameEntry(
                        slug="blank-title",
                        title="   ",
                        rating=4.0,
                        status=GameStatus.PLAYED,
                    ),
                    "beta-only": GameEntry(
                        slug="beta-only",
                        title="Beta Only",
                        rating=5.0,
                        status=GameStatus.PLAYED,
                    ),
                },
            ),
        )
        await self.db._execute("UPDATE cached_games SET title = '   ' WHERE slug = 'blank-title'")

        api = SimpleNamespace(
            get_user_library=AsyncMock(side_effect=AssertionError("Fresh cached libraries must not call SDK")),
            get_game=AsyncMock(side_effect=AssertionError("/compare must never fetch live game metadata")),
        )
        cog = SocialCog(SimpleNamespace(db=self.db, backloggd=api))

        orig_get_games_by_slugs = self.db.get_games_by_slugs
        lookup_calls = []

        async def spy_get_games_by_slugs(slugs):
            lookup_calls.append(list(slugs))
            return await orig_get_games_by_slugs(slugs)

        self.db.get_games_by_slugs = spy_get_games_by_slugs
        try:
            interaction = _make_interaction(user_id=101)
            await SocialCog.compare.callback(cog, interaction, "Beta")
        finally:
            self.db.get_games_by_slugs = orig_get_games_by_slugs

        # Exactly one bounded metadata lookup containing only common slugs
        self.assertEqual(len(lookup_calls), 1)
        self.assertEqual(
            set(lookup_calls[0]),
            {"final-fantasy-vii-rebirth", "persona-5-royal", "blank-title"},
        )

        embed = interaction.followup.send.call_args.kwargs["embed"]
        payload = embed.to_dict()
        shared_field = payload["fields"][0]["value"]
        disagree_field = payload["fields"][1]["value"]

        # Authentic casing and Roman numerals preserved
        self.assertIn("FINAL FANTASY VII REBIRTH", shared_field)
        self.assertNotIn("Final Fantasy Vii Rebirth", shared_field)

        # Fallback to slug.replace("-", " ").title() for blank cached title
        self.assertIn("Blank Title", shared_field)

        # Single-pass escaping in create_compare_embed (no double-escaping '\\_')
        self.assertIn("Persona\\_5 \\*Royal\\* \\[Edition\\](https://evil.example)", disagree_field)
        self.assertNotIn("\\\\_", disagree_field)
        self.assertNotIn("@everyone", disagree_field)

        # Freshness note & view intact
        self.assertTrue(any("Freshness" in f["name"] for f in payload["fields"]))
        view = interaction.followup.send.call_args.kwargs["view"]
        self.assertEqual(len(view.children), 1)
        api.get_game.assert_not_awaited()

    async def test_compare_bounds_metadata_lookup_above_800_common_slugs(self):
        # 850 common slugs across two live libraries -> get_games_by_slugs runs once and chunks <= 800 binds
        games_a = {
            f"slug-{i:04d}": GameEntry(
                slug=f"slug-{i:04d}",
                title=f"Slug {i}",
                rating=4.5 if i < 5 else 3.0,
                status=GameStatus.PLAYED,
            )
            for i in range(850)
        }
        games_b = {
            f"slug-{i:04d}": GameEntry(
                slug=f"slug-{i:04d}",
                title=f"Slug {i}",
                rating=4.5 if i < 5 else 2.0,
                status=GameStatus.PLAYED,
            )
            for i in range(850)
        }
        await self.db._execute(
            "INSERT INTO cached_games (slug, title) VALUES (?, ?)",
            ("slug-0000", "Chrono Trigger: Special"),
        )
        api = SimpleNamespace(
            get_user_library=AsyncMock(
                side_effect=[
                    UserLibrary(username="LiveA", games=games_a),
                    UserLibrary(username="LiveB", games=games_b),
                ]
            ),
            get_game=AsyncMock(side_effect=AssertionError("Must not call get_game")),
        )
        cog = SocialCog(SimpleNamespace(db=self.db, backloggd=api))

        fetchall_param_counts = []
        orig_fetchall = self.db._fetchall

        async def track_fetchall(query, params=()):
            if "cached_games" in query:
                fetchall_param_counts.append(len(params))
            return await orig_fetchall(query, params)

        self.db._fetchall = track_fetchall
        try:
            interaction = _make_interaction(user_id=999)
            await SocialCog.compare.callback(cog, interaction, "LiveA", "LiveB")
        finally:
            self.db._fetchall = orig_fetchall

        # 850 common slugs split into 2 SQL chunks, neither exceeding _MAX_SQL_BIND_PARAMS
        self.assertEqual(len(fetchall_param_counts), 2)
        self.assertTrue(all(count <= _MAX_SQL_BIND_PARAMS for count in fetchall_param_counts))

        embed = interaction.followup.send.call_args.kwargs["embed"]
        # slug-0000 uses cached title; slug-0001 keeps its live canonical title
        self.assertIn("Chrono Trigger: Special", embed.fields[0].value)
        self.assertIn("Slug 1", embed.fields[0].value)

    async def test_compare_uses_canonical_titles_from_uncached_live_libraries(self):
        games_a = {
            "final-fantasy-vii-rebirth": GameEntry(
                slug="final-fantasy-vii-rebirth",
                title="FINAL FANTASY VII: REBIRTH",
                rating=5.0,
                status=GameStatus.PLAYED,
            ),
            "persona-5-royal": GameEntry(
                slug="persona-5-royal",
                title="Persona_5 *Royal* [Edition](https://evil.example) @everyone",
                rating=5.0,
                status=GameStatus.PLAYED,
            ),
        }
        games_b = {
            "final-fantasy-vii-rebirth": GameEntry(
                slug="final-fantasy-vii-rebirth",
                title="Final Fantasy VII: Rebirth",
                rating=4.5,
                status=GameStatus.PLAYED,
            ),
            "persona-5-royal": GameEntry(
                slug="persona-5-royal",
                title="Persona 5 Royal: Special Edition",
                rating=1.5,
                status=GameStatus.PLAYED,
            ),
        }
        api = SimpleNamespace(
            get_user_library=AsyncMock(side_effect=[
                UserLibrary(username="LiveA", games=games_a),
                UserLibrary(username="LiveB", games=games_b),
            ]),
            get_game=AsyncMock(side_effect=AssertionError("compare must not fetch games individually")),
        )
        cog = SocialCog(SimpleNamespace(db=self.db, backloggd=api))
        interaction = _make_interaction(user_id=101)

        await SocialCog.compare.callback(cog, interaction, "LiveA", "LiveB")

        embed = interaction.followup.send.call_args.kwargs["embed"]
        text = "\n".join(field.value for field in embed.fields)
        self.assertIn("FINAL FANTASY VII: REBIRTH", text)
        self.assertNotIn("Final Fantasy Vii Rebirth", text)
        self.assertIn("Persona\\_5 \\*Royal\\* \\[Edition\\](https://evil.example)", text)
        self.assertNotIn("\\\\_", text)
        self.assertNotIn("@everyone", text)
        self.assertEqual(api.get_user_library.await_count, 2)
        api.get_game.assert_not_awaited()
        self.assertEqual(await self.db.get_profile_ratings_dict("LiveA"), {})
        self.assertEqual(await self.db.get_profile_ratings_dict("LiveB"), {})

    async def test_valid_cached_title_precedes_conflicting_live_titles(self):
        await self.db._execute(
            "INSERT INTO cached_games (slug, title) VALUES (?, ?)",
            ("shared-game", "Cached Canonical Title"),
        )
        live_games = {
            "shared-game": GameEntry(
                slug="shared-game", title="Live Version A", rating=5.0, status=GameStatus.PLAYED,
            ),
            "live-only-game": GameEntry(
                slug="live-only-game", title="Live Canonical Title", rating=4.5, status=GameStatus.PLAYED,
            ),
        }
        other_games = {
            "shared-game": GameEntry(
                slug="shared-game", title="Live Version B", rating=1.0, status=GameStatus.PLAYED,
            ),
            "live-only-game": GameEntry(
                slug="live-only-game", title="Live Canonical Title", rating=4.5, status=GameStatus.PLAYED,
            ),
        }
        api = SimpleNamespace(
            get_user_library=AsyncMock(side_effect=[
                UserLibrary(username="LiveA", games=live_games),
                UserLibrary(username="LiveB", games=other_games),
            ]),
            get_game=AsyncMock(side_effect=AssertionError("compare must not fetch games individually")),
        )
        cog = SocialCog(SimpleNamespace(db=self.db, backloggd=api))
        interaction = _make_interaction()

        await SocialCog.compare.callback(cog, interaction, "LiveA", "LiveB")

        text = "\n".join(field.value for field in interaction.followup.send.call_args.kwargs["embed"].fields)
        self.assertIn("Cached Canonical Title", text)
        self.assertNotIn("Live Version A", text)
        self.assertNotIn("Live Version B", text)
        self.assertIn("Live Canonical Title", text)
        api.get_game.assert_not_awaited()

    async def test_live_snapshot_failure_cleans_up_and_allows_retry(self):
        import asyncio

        entered = asyncio.Event()
        release_failure = asyncio.Event()
        both_joined = asyncio.Event()
        calls = 0

        async def fetch_library(username):
            nonlocal calls
            calls += 1
            if calls == 1:
                entered.set()
                await release_failure.wait()
                raise RuntimeError("temporary live library failure")
            return UserLibrary(
                username=username,
                games={
                    "retry-game": GameEntry(
                        slug="retry-game", title="Retry Canonical", rating=4.0,
                        status=GameStatus.PLAYED,
                    )
                },
            )

        class ObservedWaiters(dict):
            def __setitem__(self, key, value):
                super().__setitem__(key, value)
                if value == 2:
                    both_joined.set()

        api = SimpleNamespace(get_user_library=AsyncMock(side_effect=fetch_library))
        cog = SocialCog(SimpleNamespace(db=self.db, backloggd=api), live_fetch_timeout=5.0)
        cog._live_library_waiters = ObservedWaiters()
        first = asyncio.create_task(cog._get_or_fetch_library_snapshot("RetryUser"))
        await entered.wait()
        second = asyncio.create_task(cog._get_or_fetch_library_snapshot("retryuser"))
        await both_joined.wait()

        release_failure.set()
        failed = await asyncio.gather(first, second, return_exceptions=True)
        self.assertTrue(all(isinstance(error, RuntimeError) for error in failed))
        self.assertTrue(all("temporary live library failure" in str(error) for error in failed))
        self.assertEqual(cog._live_library_tasks, {})
        self.assertEqual(cog._live_library_waiters, {})

        retry = await cog._get_or_fetch_library_snapshot("RetryUser")
        self.assertEqual(retry.ratings, {"retry-game": 4.0})
        self.assertEqual(retry.titles, {"retry-game": "Retry Canonical"})
        self.assertEqual(calls, 2)


if __name__ == "__main__":
    unittest.main()
