"""Regression tests for Task 2: missed command output escaping across discovery, account, and games."""

import os

os.environ["PYTHON_DOTENV_DISABLED"] = "1"
os.environ["TURSO_DATABASE_URL"] = ""
os.environ["TURSO_AUTH_TOKEN"] = ""
os.environ["DEV_GUILD_ID"] = ""

import asyncio
from datetime import datetime, timezone
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock

import discord

from bot.cogs.account import AccountCog
from bot.cogs.discovery import DiscoveryCog
from bot.cogs.games import GamesCog
from bot.services.library_sync import SyncOutcome


def _make_interaction(user_id: int = 10, *, display_name: str = "Caller", guild=None):
    return SimpleNamespace(
        user=SimpleNamespace(id=user_id, display_name=display_name, display_avatar=None),
        guild=guild,
        guild_id=getattr(guild, "id", None),
        response=SimpleNamespace(defer=AsyncMock(), send_message=AsyncMock()),
        followup=SimpleNamespace(send=AsyncMock(return_value=SimpleNamespace(edit=AsyncMock()))),
    )


class DiscoveryOutputSafetyTests(unittest.IsolatedAsyncioTestCase):
    async def test_roulette_empty_cache_and_fetching_messages_escape_malicious_legacy_names(self):
        malicious_name = "bad](https://evil.example) *user* <@123>"
        db = SimpleNamespace(
            get_linked_user=AsyncMock(return_value={"backloggd_username": malicious_name, "profile_key": "bad"}),
            get_user_backlog=AsyncMock(return_value=[]),
            get_profile_sync_info=AsyncMock(return_value=None),
        )
        bot = SimpleNamespace(db=db, backloggd=SimpleNamespace())
        cog = DiscoveryCog(bot, clock=lambda: datetime(2026, 11, 13, tzinfo=timezone.utc))

        # Fetching / missing cache path
        i_missing = _make_interaction()
        await DiscoveryCog.roulette.callback(cog, i_missing, None)
        msg_missing = i_missing.followup.send.call_args.args[0]
        self.assertIn("bad\\](https://evil.example) \\*user\\*", msg_missing)
        self.assertNotIn("<@123>", msg_missing)

        # Empty backlog with existing sync cache path
        db.get_profile_sync_info.return_value = {
            "last_synced_at": datetime(2026, 11, 13, tzinfo=timezone.utc),
            "game_count": 5,
            "last_error": None,
        }
        i_empty = _make_interaction()
        await DiscoveryCog.roulette.callback(cog, i_empty, None)
        msg_empty = i_empty.followup.send.call_args.args[0]
        self.assertIn("bad\\](https://evil.example) \\*user\\*", msg_empty)
        self.assertNotIn("<@123>", msg_empty)

    async def test_recommend_to_unlinked_friend_with_spoof_display_name_is_safe(self):
        caller_name = "Caller_One"
        spoof_friend_display = "[Click Here](https://evil.example) @everyone <#999>"
        friend = SimpleNamespace(id=20, display_name=spoof_friend_display)
        guild = SimpleNamespace(id=1, name="Guild")

        db = SimpleNamespace(
            get_linked_user=AsyncMock(side_effect=[
                {"backloggd_username": caller_name, "profile_key": "caller_one"},
                None,
            ]),
            get_profile_sync_info=AsyncMock(return_value={
                "last_synced_at": datetime(2026, 11, 13, tzinfo=timezone.utc),
                "game_count": 1,
                "last_error": None,
            }),
            get_profile_ratings_dict=AsyncMock(return_value={"hades": 5.0}),
            get_profile_library_entries=AsyncMock(return_value=None),
            get_games_by_slugs=AsyncMock(return_value={"hades": {"title": "Hades"}}),
        )
        cog = DiscoveryCog(SimpleNamespace(db=db, backloggd=SimpleNamespace()), clock=lambda: datetime(2026, 11, 13, tzinfo=timezone.utc))
        interaction = _make_interaction(user_id=10, guild=guild)

        await DiscoveryCog.recommend.callback(cog, interaction, friend)

        embed = interaction.followup.send.call_args.kwargs["embed"]
        # Unlinked Discord display name must NOT become a Backloggd URL and must be escaped in description
        self.assertNotIn("https://www.backloggd.com/u/[Click", embed.description)
        self.assertNotIn("@everyone", embed.description)
        self.assertNotIn("<#999>", embed.description)
        self.assertIn("\\[Click Here\\](https://evil.example)", embed.description)
        # Normal valid caller name should NOT be double-escaped, while underscore is escaped once
        self.assertIn("**[Caller\\_One](https://www.backloggd.com/u/Caller_One/)**", embed.description)

    async def test_recommend_with_malformed_legacy_caller_and_friend_names_uses_raw_keys_for_db(self):
        caller_raw = "Caller](https://evil.example) *X*"
        friend_raw = "Friend_Name @here"
        friend = SimpleNamespace(id=20, display_name="FriendDisplay")
        guild = SimpleNamespace(id=1, name="Guild")

        db = SimpleNamespace(
            get_linked_user=AsyncMock(side_effect=[
                {"backloggd_username": caller_raw, "profile_key": "caller_key"},
                {"backloggd_username": friend_raw, "profile_key": "friend_key"},
            ]),
            get_profile_sync_info=AsyncMock(return_value={
                "last_synced_at": datetime(2026, 11, 13, tzinfo=timezone.utc),
                "game_count": 2,
                "last_error": None,
            }),
            get_profile_ratings_dict=AsyncMock(side_effect=[
                {"hades": 5.0},
                {"celeste": 4.5},
            ]),
            get_profile_library_entries=AsyncMock(return_value={
                "celeste": {"status": "played", "rating": 4.5},
            }),
            get_games_by_slugs=AsyncMock(return_value={"hades": {"title": "Hades"}}),
        )
        cog = DiscoveryCog(SimpleNamespace(db=db, backloggd=SimpleNamespace()), clock=lambda: datetime(2026, 11, 13, tzinfo=timezone.utc))
        interaction = _make_interaction(user_id=10, guild=guild)

        await DiscoveryCog.recommend.callback(cog, interaction, friend)

        # Raw keys drove DB calls
        db.get_profile_ratings_dict.assert_any_await("caller_key")
        db.get_profile_ratings_dict.assert_any_await("friend_key")

        embed = interaction.followup.send.call_args.kwargs["embed"]
        self.assertIn("Caller\\](https://evil.example) \\*X\\*", embed.description)
        self.assertIn("https://www.backloggd.com/u/Caller%5D%28https%3A%2F%2Fevil.example%29%20%2AX%2A/", embed.description)
        self.assertIn("Caller\\](https://evil.example) \\*X\\*'s Rating:", embed.fields[0].value)
        freshness_field = next(f for f in embed.fields if "Freshness" in f.name)
        self.assertNotIn("@here", freshness_field.value)
        self.assertIn("Friend\\_Name", freshness_field.value)

        # No-results path for friend also escapes friend_raw
        db.get_linked_user.side_effect = [
            {"backloggd_username": caller_raw, "profile_key": "caller_key"},
            {"backloggd_username": friend_raw, "profile_key": "friend_key"},
        ]
        db.get_profile_ratings_dict.side_effect = [
            {"hades": 5.0},
            {"hades": None},
        ]
        db.get_profile_library_entries.side_effect = None
        db.get_profile_library_entries.return_value = {
            "hades": {"status": "unknown", "rating": None},
        }
        i_none = _make_interaction(user_id=10, guild=guild)
        await DiscoveryCog.recommend.callback(cog, i_none, friend)
        none_msg = i_none.followup.send.call_args.args[0]
        self.assertNotIn("@here", none_msg)
        self.assertIn("Friend\\_Name", none_msg)

    async def test_recommend_server_mode_escapes_recommender_and_empty_caller(self):
        caller_raw = "Bad_Caller <@123>"
        other_raw = "Twin](https://evil.example) *Two*"
        guild = SimpleNamespace(
            id=1,
            name="Guild",
            members=[SimpleNamespace(id=10), SimpleNamespace(id=20)],
            chunked=True,
        )
        db = SimpleNamespace(
            get_linked_user=AsyncMock(return_value={"backloggd_username": caller_raw, "profile_key": "bad_caller"}),
            get_profile_sync_info=AsyncMock(return_value={
                "last_synced_at": datetime(2026, 11, 13, tzinfo=timezone.utc),
                "game_count": 5,
                "last_error": None,
            }),
            get_profile_ratings_dict=AsyncMock(return_value={
                "g1": 5.0, "g2": 4.5, "g3": 4.0, "g4": 3.5, "g5": 3.0,
            }),
            get_guild_linked_profiles=AsyncMock(return_value=[
                {"profile_key": "twin", "display_username": other_raw},
            ]),
            get_multi_profile_ratings_dict=AsyncMock(return_value={
                "twin": {"g1": 5.0, "g2": 4.5, "g3": 4.0, "g4": 3.5, "g5": 3.0, "pick": 5.0},
            }),
            get_profile_library_entries=AsyncMock(return_value=None),
            get_games_by_slugs=AsyncMock(return_value={"pick": {"title": "Pick Game"}}),
        )
        cog = DiscoveryCog(SimpleNamespace(db=db, backloggd=SimpleNamespace()), clock=lambda: datetime(2026, 11, 13, tzinfo=timezone.utc))
        interaction = _make_interaction(user_id=10, guild=guild)

        await DiscoveryCog.recommend.callback(cog, interaction, None)

        embed = interaction.followup.send.call_args.kwargs["embed"]
        self.assertIn("**Twin\\](https://evil.example) \\*Two\\***", embed.fields[0].value)


class AccountOutputSafetyTests(unittest.IsolatedAsyncioTestCase):
    async def test_refresh_queued_and_running_and_status_updates_escape_malicious_names(self):
        malicious = "user](https://evil.example) <@123> *x*"
        db = SimpleNamespace(
            get_linked_user=AsyncMock(return_value={"backloggd_username": malicious, "avatar_url": None}),
        )
        library_sync = SimpleNamespace(
            is_refreshing=lambda key: False,
            request=lambda *a, **kw: asyncio.Future(),
        )
        cog = AccountCog(SimpleNamespace(db=db, library_sync=library_sync))
        cog._start_sync_task = lambda *a, **kw: None

        # Queued state
        i_queued = _make_interaction()
        await AccountCog.refresh.callback(cog, i_queued, None)
        q_embed = i_queued.followup.send.call_args.kwargs["embed"]
        self.assertIn("user\\](https://evil.example)", q_embed.description)
        self.assertNotIn("<@123>", q_embed.description)
        self.assertIn("https://www.backloggd.com/u/user%5D%28https%3A%2F%2Fevil.example%29%20%3C%40123%3E%20%2Ax%2A/", q_embed.description)

        # Already-running state
        library_sync.is_refreshing = lambda key: True
        i_running = _make_interaction()
        await AccountCog.refresh.callback(cog, i_running, None)
        r_embed = i_running.followup.send.call_args.kwargs["embed"]
        self.assertIn("user\\](https://evil.example)", r_embed.description)
        self.assertNotIn("<@123>", r_embed.description)

        # Asynchronous _update_link_status completion/failure/skipped/cancelled paths
        status_message = SimpleNamespace(edit=AsyncMock())
        loop = asyncio.get_running_loop()
        for outcome in (
            SyncOutcome(status="success", game_count=10),
            SyncOutcome(status="failed"),
            SyncOutcome(status="skipped", reason="fresh", game_count=10),
            SyncOutcome(status="skipped", reason="cooldown"),
        ):
            fut = loop.create_future()
            fut.set_result(outcome)
            status_message.edit.reset_mock()
            await cog._update_link_status(fut, malicious, status_message)
            edit_embed = status_message.edit.call_args.kwargs["embed"]
            self.assertIn("user\\](https://evil.example)", edit_embed.description)
            self.assertNotIn("<@123>", edit_embed.description)

        cancelled = loop.create_future()
        cancelled.cancel()
        status_message.edit.reset_mock()
        await cog._update_link_status(cancelled, malicious, status_message)
        cancel_embed = status_message.edit.call_args.kwargs["embed"]
        self.assertIn("user\\](https://evil.example)", cancel_embed.description)
        self.assertNotIn("<@123>", cancel_embed.description)


class GamesOutputSafetyTests(unittest.IsolatedAsyncioTestCase):
    async def test_game_search_miss_ambiguity_missing_details_and_error_escape_and_bound_content(self):
        malicious_query = "[click](https://evil.example) @everyone <#123> " + ("*x" * 900)
        api = SimpleNamespace(
            search_games=AsyncMock(return_value=[]),
            get_game=AsyncMock(return_value=None),
        )
        db = SimpleNamespace()
        cog = GamesCog(SimpleNamespace(backloggd=api, db=db))

        # 1. Empty search preserves raw query to SDK and safely bounds response
        i_empty = _make_interaction()
        await GamesCog.game.callback(cog, i_empty, malicious_query)
        api.search_games.assert_awaited_once_with(malicious_query.strip())
        empty_msg = i_empty.followup.send.call_args.args[0]
        self.assertLessEqual(len(empty_msg), 2000)
        self.assertNotIn("@everyone", empty_msg)
        self.assertNotIn("<#123>", empty_msg)
        self.assertIn("\\[click\\](https://evil.example)", empty_msg)

        # 2. Ambiguous search escapes query and malicious candidate titles and preserves instructions
        api.search_games.reset_mock()
        api.search_games.return_value = [
            SimpleNamespace(title="Trap [a](https://evil.example) <#55>", release_year=2024, slug="t1"),
            SimpleNamespace(title="Long " + ("Y" * 800), release_year=2025, slug="t2"),
        ]
        i_ambig = _make_interaction()
        await GamesCog.game.callback(cog, i_ambig, malicious_query)
        ambig_msg = i_ambig.followup.send.call_args.args[0]
        self.assertLessEqual(len(ambig_msg), 2000)
        self.assertNotIn("@everyone", ambig_msg)
        self.assertNotIn("<#55>", ambig_msg)
        self.assertIn("\\[a\\](https://evil.example)", ambig_msg)
        self.assertIn("autocomplete", ambig_msg.lower())

        # 3. Missing game metadata for slug: preserves raw slug to SDK and escapes display
        malicious_slug = "slug:[bad](https://evil.example)<@99>"
        i_missing = _make_interaction()
        await GamesCog.game.callback(cog, i_missing, malicious_slug)
        api.get_game.assert_awaited_once_with("[bad](https://evil.example)<@99>")
        missing_msg = i_missing.followup.send.call_args.args[0]
        self.assertNotIn("<@99>", missing_msg)
        self.assertIn("\\[bad\\](https://evil.example)", missing_msg)

        # 4. Exception handler escapes and bounds title
        api.search_games.side_effect = RuntimeError("boom")
        i_err = _make_interaction()
        await GamesCog.game.callback(cog, i_err, malicious_query)
        err_msg = i_err.followup.send.call_args.args[0]
        self.assertLessEqual(len(err_msg), 2000)
        self.assertNotIn("@everyone", err_msg)
        self.assertIn("\\[click\\](https://evil.example)", err_msg)

    async def test_game_valid_input_is_not_double_escaped(self):
        api = SimpleNamespace(
            search_games=AsyncMock(return_value=[]),
        )
        cog = GamesCog(SimpleNamespace(backloggd=api, db=SimpleNamespace()))
        i = _make_interaction()
        await GamesCog.game.callback(cog, i, "Hollow Knight (2017)")
        msg = i.followup.send.call_args.args[0]
        self.assertIn("**Hollow Knight (2017)**", msg)


if __name__ == "__main__":
    unittest.main()
