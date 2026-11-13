"""Regression tests for Task 8: resilient autocomplete choices and bounded fallback cache."""

import os
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

os.environ["PYTHON_DOTENV_DISABLED"] = "1"
os.environ["TURSO_DATABASE_URL"] = ""
os.environ["TURSO_AUTH_TOKEN"] = ""
os.environ["DEV_GUILD_ID"] = ""

from bot.cogs.games import GamesCog


def _fake_interaction():
    return SimpleNamespace(
        response=SimpleNamespace(defer=AsyncMock()),
        followup=SimpleNamespace(send=AsyncMock()),
        guild=None,
        user=SimpleNamespace(id=1),
    )


def _fake_game_details(slug: str, title: str = "Resolved Game"):
    return SimpleNamespace(
        slug=slug,
        title=title,
        cover_url=None,
        release_year=2024,
        release_date=None,
        community_rating=4.3,
        url=f"https://www.backloggd.com/games/{slug}/",
        summary="Test summary",
        genres=["Adventure"],
        platforms=["PC"],
        developers=["Studio"],
        stats=SimpleNamespace(
            total_ratings=10,
            played=100,
            playing=10,
            backlog=50,
            wishlist=25,
        ),
    )


class AutocompletePersistenceTests(unittest.IsolatedAsyncioTestCase):
    async def test_tagged_slug_resolves_after_query_eviction_and_fresh_cog_restart(self):
        slug = "hollow-knight-silksong"
        entry = SimpleNamespace(title="Hollow Knight: Silksong", release_year=2025, slug=slug)
        api1 = SimpleNamespace(search_games=AsyncMock(return_value=[entry]), get_game=AsyncMock())
        cog1 = GamesCog(SimpleNamespace(backloggd=api1, db=SimpleNamespace()))

        choices = await cog1.game_autocomplete(SimpleNamespace(), "silksong")
        self.assertEqual(len(choices), 1)
        selected_value = choices[0].value
        self.assertTrue(selected_value.startswith("slug:"))
        self.assertLessEqual(len(selected_value), 100)

        # Flood query cache well past 500 entries to force eviction of "silksong".
        for i in range(600):
            api1.search_games.return_value = [
                SimpleNamespace(title=f"Other {i}", release_year=2020, slug=f"other-{i}")
            ]
            await cog1.game_autocomplete(SimpleNamespace(), f"other-query-{i}")

        self.assertLessEqual(len(cog1._autocomplete_cache), 500)
        self.assertNotIn("silksong", cog1._autocomplete_cache)

        # Resolve on a brand-new GamesCog instance (simulating bot restart) with NO autocomplete state.
        game_details = _fake_game_details(slug, "Hollow Knight: Silksong")
        api2 = SimpleNamespace(
            search_games=AsyncMock(side_effect=AssertionError("should not search free text")),
            get_game=AsyncMock(return_value=game_details),
        )
        db2 = SimpleNamespace(
            upsert_game=AsyncMock(),
            get_server_ratings_for_game=AsyncMock(return_value=[]),
            get_guild_linked_profiles_with_sync_info=AsyncMock(return_value=[]),
        )
        cog2 = GamesCog(SimpleNamespace(backloggd=api2, db=db2))
        interaction = _fake_interaction()

        await GamesCog.game.callback(cog2, interaction, selected_value)

        api2.get_game.assert_awaited_once_with(slug)
        api2.search_games.assert_not_awaited()
        interaction.followup.send.assert_awaited_once()
        self.assertIn("Silksong", interaction.followup.send.call_args.kwargs["embed"].title)

    async def test_exact_length_bounds_and_unicode_title_truncation(self):
        slug_95 = "a" * 95
        slug_96 = "b" * 96
        long_unicode_title = "日本語タイトル🎮" * 20  # >100 chars with emoji/multi-byte chars
        results = [
            SimpleNamespace(title=long_unicode_title, release_year=2024, slug=slug_95),
            SimpleNamespace(title="Oversized Slug Game", release_year=2024, slug=slug_96),
        ]
        api = SimpleNamespace(search_games=AsyncMock(return_value=results))
        cog = GamesCog(SimpleNamespace(backloggd=api, db=SimpleNamespace()))

        choices = await cog.game_autocomplete(SimpleNamespace(), "unicode")
        self.assertEqual(len(choices), 2)

        # Choice 0: exact 95-char slug fits inline ("slug:" + 95 == 100); title truncated <= 100.
        self.assertEqual(choices[0].value, f"slug:{slug_95}")
        self.assertEqual(len(choices[0].value), 100)
        self.assertLessEqual(len(choices[0].name), 100)
        self.assertTrue(choices[0].name.endswith("..."))

        # Choice 1: 96-char slug exceeds inline 95-char limit -> opaque fallback without truncating slug!
        self.assertFalse(choices[1].value.startswith("slug:"))
        self.assertLessEqual(len(choices[1].value), 100)
        self.assertNotEqual(choices[1].value, f"slug:{slug_96[:95]}")

    async def test_oversized_slug_survives_query_eviction_and_reports_cleanly_after_expiry(self):
        oversized_slug = "extremely-long-edition-title-" + ("x" * 90)
        self.assertGreater(len(oversized_slug), 95)
        now = 1000.0
        api = SimpleNamespace(
            search_games=AsyncMock(return_value=[
                SimpleNamespace(title="Oversized Game", release_year=2023, slug=oversized_slug)
            ]),
            get_game=AsyncMock(return_value=_fake_game_details(oversized_slug, "Oversized Game")),
        )
        db = SimpleNamespace(
            upsert_game=AsyncMock(),
            get_server_ratings_for_game=AsyncMock(return_value=[]),
            get_guild_linked_profiles_with_sync_info=AsyncMock(return_value=[]),
        )
        cog = GamesCog(SimpleNamespace(backloggd=api, db=db), monotonic_clock=lambda: now)

        choices = await cog.game_autocomplete(SimpleNamespace(), "oversized query")
        token = choices[0].value

        # Flooding query cache with 600 searches must NOT wipe the fallback entry.
        for i in range(600):
            api.search_games.return_value = [
                SimpleNamespace(title=f"Filler {i}", release_year=2021, slug=f"filler-{i}")
            ]
            await cog.game_autocomplete(SimpleNamespace(), f"filler-{i}")

        self.assertLessEqual(len(cog._autocomplete_cache), 500)
        interaction = _fake_interaction()
        api.search_games.reset_mock()
        await GamesCog.game.callback(cog, interaction, token)
        api.get_game.assert_awaited_once_with(oversized_slug)
        api.search_games.assert_not_awaited()
        self.assertIn("Oversized Game", interaction.followup.send.call_args.kwargs["embed"].title)

        # Advance time past fallback TTL (e.g., > 1 hour) -> clear reselect message, NO literal token search.
        now += 7200.0
        api.get_game.reset_mock()
        api.search_games.reset_mock()
        expired_interaction = _fake_interaction()
        await GamesCog.game.callback(cog, expired_interaction, token)

        api.get_game.assert_not_awaited()
        api.search_games.assert_not_awaited()
        msg = expired_interaction.followup.send.call_args.args[0]
        self.assertIn("expired", msg.lower())
        self.assertIn("oversized query", msg.lower())
        self.assertIn("autocomplete", msg.lower())
        self.assertTrue(expired_interaction.followup.send.call_args.kwargs.get("ephemeral"))

    async def test_free_text_search_preserved(self):
        entry = SimpleNamespace(title="Celeste", release_year=2018, slug="celeste")
        api = SimpleNamespace(
            search_games=AsyncMock(return_value=[entry]),
            get_game=AsyncMock(return_value=_fake_game_details("celeste", "Celeste")),
        )
        db = SimpleNamespace(
            upsert_game=AsyncMock(),
            get_server_ratings_for_game=AsyncMock(return_value=[]),
            get_guild_linked_profiles_with_sync_info=AsyncMock(return_value=[]),
        )
        cog = GamesCog(SimpleNamespace(backloggd=api, db=db))
        interaction = _fake_interaction()

        await GamesCog.game.callback(cog, interaction, "Celeste")

        api.search_games.assert_awaited_once_with("Celeste")
        api.get_game.assert_awaited_once_with("celeste")
        self.assertIn("Celeste", interaction.followup.send.call_args.kwargs["embed"].title)

    async def test_single_fallback_storage_lru_eviction_and_reselection(self):
        now = 10.0
        slug_a = "long-slug-a-" + ("a" * 95)
        slug_b = "long-slug-b-" + ("b" * 95)
        slug_c = "long-slug-c-" + ("c" * 95)
        api = SimpleNamespace(
            search_games=AsyncMock(),
            get_game=AsyncMock(side_effect=lambda s: _fake_game_details(s, f"Title {s[:11]}")),
        )
        db = SimpleNamespace(
            upsert_game=AsyncMock(),
            get_server_ratings_for_game=AsyncMock(return_value=[]),
            get_guild_linked_profiles_with_sync_info=AsyncMock(return_value=[]),
        )
        cog = GamesCog(SimpleNamespace(backloggd=api, db=db), monotonic_clock=lambda: now)

        self.assertFalse(
            hasattr(cog, "_autocomplete_slugs"),
            "GamesCog must keep _fallback_entries as the single fallback token cache",
        )

        with patch("bot.cogs.games._MAX_FALLBACK_SLUGS", 2):
            api.search_games.return_value = [SimpleNamespace(title="Game A", release_year=2024, slug=slug_a)]
            token_a = (await cog.game_autocomplete(SimpleNamespace(), "query a"))[0].value
            now += 1.0
            api.search_games.return_value = [SimpleNamespace(title="Game B", release_year=2024, slug=slug_b)]
            token_b = (await cog.game_autocomplete(SimpleNamespace(), "query b"))[0].value
            self.assertEqual(len(cog._fallback_entries), 2)

            # Reselect token_a so token_b becomes the least-recently-used entry.
            await GamesCog.game.callback(cog, _fake_interaction(), token_a)
            api.get_game.assert_awaited_with(slug_a)

            now += 1.0
            api.search_games.return_value = [SimpleNamespace(title="Game C", release_year=2024, slug=slug_c)]
            token_c = (await cog.game_autocomplete(SimpleNamespace(), "query c"))[0].value

        self.assertEqual(len(cog._fallback_entries), 2)
        self.assertIn(token_a, cog._fallback_entries)
        self.assertIn(token_c, cog._fallback_entries)
        self.assertNotIn(token_b, cog._fallback_entries)

        # Evicted token_b must prompt reselection cleanly without calling search_games(token_b).
        api.search_games.reset_mock()
        api.get_game.reset_mock()
        evicted_interaction = _fake_interaction()
        await GamesCog.game.callback(cog, evicted_interaction, token_b)
        api.search_games.assert_not_awaited()
        api.get_game.assert_not_awaited()
        self.assertIn("expired", evicted_interaction.followup.send.call_args.args[0].lower())

        # Same query "query b" produces a fresh working replacement token after eviction.
        api.search_games.return_value = [SimpleNamespace(title="Game B", release_year=2024, slug=slug_b)]
        renewed_b = (await cog.game_autocomplete(SimpleNamespace(), "query b"))[0].value
        self.assertNotEqual(renewed_b, token_b)
        await GamesCog.game.callback(cog, _fake_interaction(), renewed_b)
        api.get_game.assert_awaited_once_with(slug_b)


if __name__ == "__main__":
    unittest.main()
