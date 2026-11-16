"""Regression tests for Task 8: TasteTwinsView duel callback error boundary."""

import os

os.environ["PYTHON_DOTENV_DISABLED"] = "1"
os.environ["TURSO_DATABASE_URL"] = ""
os.environ["TURSO_AUTH_TOKEN"] = ""
os.environ["DEV_GUILD_ID"] = ""

import asyncio
from datetime import datetime, timezone
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, patch

import discord

from bot.cogs.social import SocialCog
from bot.db import Database, _MAX_SQL_BIND_PARAMS
from tests.test_interaction_visibility_protocol import (
    StatefulDiscordInteraction,
    _FakeHTTPResp,
)


class DuelCallbackBoundaryTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmpdir.cleanup)
        self.db = Database(str(Path(self.tmpdir.name) / "duel.db"), turso_url="")
        await self.db.connect()
        self.addAsyncCleanup(self.db.close)

        await self.db.link_user(10, "Alpha", None)
        await self.db.link_user(20, "Beta", None)
        lib_a = [
            SimpleNamespace(slug=f"g{i}", title=f"Game {i}", status="played", rating=4.5, cover_url=None, release_year=2020)
            for i in range(5)
        ]
        lib_b = [
            SimpleNamespace(slug=f"g{i}", title=f"Game {i}", status="played", rating=4.0, cover_url=None, release_year=2020)
            for i in range(5)
        ]
        await self.db.replace_user_library(10, "Alpha", lib_a)
        await self.db.replace_user_library(20, "Beta", lib_b)

        self.guild = SimpleNamespace(
            id=1,
            name="TestGuild",
            chunked=True,
            member_count=2,
            members=[
                SimpleNamespace(id=10, display_name="Alpha"),
                SimpleNamespace(id=20, display_name="Beta"),
            ],
        )
        self.bot = SimpleNamespace(db=self.db, backloggd=SimpleNamespace())
        self.cog = SocialCog(self.bot, clock=lambda: datetime(2026, 11, 13, tzinfo=timezone.utc))
        self.addAsyncCleanup(self.cog.close)

    async def _render_duel_callback(self):
        inter = StatefulDiscordInteraction(name="taste-twins", guild=self.guild, user_id=10, display_name="Alpha")
        await SocialCog.taste_twins.callback(self.cog, inter, None)
        pub = inter.visible_public_messages()
        self.assertEqual(len(pub), 1)
        self.parent_message = pub[0]
        self.addCleanup(self.assertEqual, self.parent_message, dict(self.parent_message))
        view = pub[0]["view"]
        original_controls = [item.to_component_dict() for item in view.children]
        self.addCleanup(
            lambda: self.assertEqual(
                [item.to_component_dict() for item in view.children], original_controls
            )
        )
        duel_buttons = [
            item for item in view.children
            if getattr(item, "label", "").startswith("Duel with ")
        ]
        self.assertEqual(len(duel_buttons), 1)
        return duel_buttons[0].callback

    def _button_interaction(self, **kwargs):
        return StatefulDiscordInteraction(
            interaction_type=discord.InteractionType.component,
            source_message=self.parent_message,
            **kwargs,
        )

    async def test_duel_callback_unauthorized_member_rejected_privately(self):
        callback = await self._render_duel_callback()
        other = self._button_interaction(guild=self.guild, user_id=99, display_name="Intruder")
        await callback(other)
        self.assertEqual(other.visible_public_messages(), [])
        ephem = other.visible_ephemeral_messages()
        self.assertEqual(len(ephem), 1)
        self.assertIn("only the member", ephem[0]["content"].lower())

    async def test_duel_callback_success_posts_public_compare_embed(self):
        callback = await self._render_duel_callback()
        btn_inter = self._button_interaction(guild=self.guild, user_id=10, display_name="Alpha")
        await callback(btn_inter)
        pub = btn_inter.visible_public_messages()
        self.assertEqual(len(pub), 1)
        self.assertIsNotNone(pub[0]["embed"])
        self.assertIn("alpha", pub[0]["embed"].title.lower())
        self.assertIn("beta", pub[0]["embed"].title.lower())
        self.assertEqual(btn_inter.visible_ephemeral_messages(), [])

    async def test_duel_callback_similarity_or_render_failure_logs_once_and_stays_private(self):
        callback = await self._render_duel_callback()
        btn_inter = self._button_interaction(guild=self.guild, user_id=10, display_name="Alpha")
        secret_err = RuntimeError("SELECT * FROM users WHERE auth_token='tok_secret_999'")

        with patch("bot.cogs.social.calculate_similarity_raw", side_effect=secret_err):
            with self.assertLogs("backloggd.cogs.social", level="ERROR") as logs:
                await callback(btn_inter)

        self.assertEqual(len(logs.records), 1)
        self.assertEqual(btn_inter.visible_public_messages(), [])
        ephem = btn_inter.visible_ephemeral_messages()
        self.assertEqual(len(ephem), 1)
        msg = ephem[0]["content"]
        self.assertIn("duel", msg.lower())
        self.assertNotIn("SELECT", msg)
        self.assertNotIn("tok_secret_999", msg)
        self.assertNotIn("RuntimeError", msg)
        for pub_text in btn_inter.public_text_history:
            self.assertNotIn("SELECT", pub_text)
            self.assertNotIn("tok_secret_999", pub_text)

    async def test_duel_callback_http_failure_on_result_send_does_not_raise_or_recurse(self):
        callback = await self._render_duel_callback()
        btn_inter = self._button_interaction(guild=self.guild, user_id=10, display_name="Alpha")

        orig_send = btn_inter.followup.send
        send_calls = 0

        async def flaky_send(*args, **kwargs):
            nonlocal send_calls
            send_calls += 1
            if send_calls == 1:
                raise discord.HTTPException(_FakeHTTPResp(500), "bad gateway")
            return await orig_send(*args, **kwargs)

        btn_inter.followup.send = flaky_send
        with self.assertLogs("backloggd.cogs.social", level="WARNING") as logs:
            await callback(btn_inter)

        self.assertEqual(len(logs.records), 1)
        self.assertEqual(btn_inter.visible_public_messages(), [])
        ephem = btn_inter.visible_ephemeral_messages()
        self.assertEqual(len(ephem), 1)
        self.assertEqual(send_calls, 2)

    async def test_duel_callback_expired_interaction_on_error_send_does_not_raise_or_recurse(self):
        callback = await self._render_duel_callback()
        btn_inter = self._button_interaction(
            guild=self.guild,
            user_id=10,
            display_name="Alpha",
            edit_original_error=discord.NotFound(_FakeHTTPResp(404), "Unknown Interaction"),
            followup_send_error=discord.NotFound(_FakeHTTPResp(404), "Unknown Webhook"),
        )
        with patch("bot.cogs.social.create_compare_embed", side_effect=RuntimeError("render boom")):
            with self.assertLogs("backloggd.cogs.social", level="ERROR") as logs:
                await callback(btn_inter)

        self.assertEqual(len(logs.records), 1)
        self.assertEqual(btn_inter.visible_ephemeral_messages(), [])
        self.assertEqual(btn_inter.visible_public_messages(), [])

    async def test_duel_callback_cancellation_propagates(self):
        callback = await self._render_duel_callback()
        btn_inter = self._button_interaction(guild=self.guild, user_id=10, display_name="Alpha")
        with patch("bot.cogs.social.calculate_similarity_raw", side_effect=asyncio.CancelledError()):
            with self.assertRaises(asyncio.CancelledError):
                await callback(btn_inter)
        self.assertEqual(btn_inter.visible_ephemeral_messages(), [])
        self.assertEqual(btn_inter.public_text_history, [])

    async def test_duel_callback_resolves_all_shared_titles_with_single_pass_escaping_and_blank_fallback(self):
        lib_a = [
            SimpleNamespace(slug="top-fav", title="Top Fav: Special", status="played", rating=5.0, cover_url=None, release_year=2020),
            SimpleNamespace(slug="top-disagree", title="Top Disagree", status="played", rating=5.0, cover_url=None, release_year=2020),
            SimpleNamespace(slug="final-fantasy-vii-rebirth", title="FINAL FANTASY VII REBIRTH", status="played", rating=4.9, cover_url=None, release_year=2024),
            SimpleNamespace(slug="persona-5-royal", title="Persona_5 *Royal* [Edition](https://evil.example) @everyone", status="played", rating=5.0, cover_url=None, release_year=2020),
            SimpleNamespace(slug="blank-title", title="   ", status="played", rating=4.5, cover_url=None, release_year=2021),
            SimpleNamespace(slug="uncached-slug", title="Uncached Slug", status="played", rating=4.3, cover_url=None, release_year=2022),
        ]
        lib_b = [
            SimpleNamespace(slug="top-fav", title="Top Fav: Special", status="played", rating=5.0, cover_url=None, release_year=2020),
            SimpleNamespace(slug="top-disagree", title="Top Disagree", status="played", rating=1.0, cover_url=None, release_year=2020),
            SimpleNamespace(slug="final-fantasy-vii-rebirth", title="FINAL FANTASY VII REBIRTH", status="played", rating=4.8, cover_url=None, release_year=2024),
            SimpleNamespace(slug="persona-5-royal", title="Persona_5 *Royal* [Edition](https://evil.example) @everyone", status="played", rating=4.0, cover_url=None, release_year=2020),
            SimpleNamespace(slug="blank-title", title="   ", status="played", rating=4.4, cover_url=None, release_year=2021),
            SimpleNamespace(slug="uncached-slug", title="Uncached Slug", status="played", rating=4.2, cover_url=None, release_year=2022),
        ]
        await self.db.replace_user_library(10, "Alpha", lib_a)
        await self.db.replace_user_library(20, "Beta", lib_b)
        await self.db._execute("UPDATE cached_games SET title = '   ' WHERE slug = 'blank-title'")

        self.bot.backloggd = SimpleNamespace(
            get_game=AsyncMock(side_effect=AssertionError("duel callback must never fetch live game metadata"))
        )

        orig_get_games_by_slugs = self.db.get_games_by_slugs
        lookup_calls = []

        async def spy_get_games_by_slugs(slugs):
            lookup_calls.append(set(slugs))
            result = await orig_get_games_by_slugs(slugs)
            result.pop("uncached-slug", None)
            return result

        self.db.get_games_by_slugs = spy_get_games_by_slugs
        try:
            callback = await self._render_duel_callback()
            self.assertEqual(len(lookup_calls), 1)
            self.assertEqual(lookup_calls[0], {"top-fav", "top-disagree"})

            btn_inter = self._button_interaction(guild=self.guild, user_id=10, display_name="Alpha")
            await callback(btn_inter)
        finally:
            self.db.get_games_by_slugs = orig_get_games_by_slugs

        self.assertEqual(len(lookup_calls), 2)
        self.assertEqual(
            lookup_calls[1],
            {"final-fantasy-vii-rebirth", "persona-5-royal", "blank-title", "uncached-slug"},
        )
        self.bot.backloggd.get_game.assert_not_awaited()

        pub = btn_inter.visible_public_messages()
        self.assertEqual(len(pub), 1)
        payload = pub[0]["embed"].to_dict()
        shared_field = payload["fields"][0]["value"]
        disagree_field = payload["fields"][1]["value"]

        self.assertIn("Top Fav: Special", shared_field)
        self.assertIn("FINAL FANTASY VII REBIRTH", shared_field)
        self.assertNotIn("Final Fantasy Vii Rebirth", shared_field)
        self.assertIn("Blank Title", shared_field)
        self.assertIn("Uncached Slug", shared_field)
        self.assertIn("Persona\\_5 \\*Royal\\* \\[Edition\\](https://evil.example)", shared_field)
        self.assertIn("Persona\\_5 \\*Royal\\* \\[Edition\\](https://evil.example)", disagree_field)
        self.assertNotIn("\\\\_", shared_field)
        self.assertNotIn("@everyone", shared_field)
        self.assertNotIn("@everyone", disagree_field)

    async def test_duel_callback_bounds_metadata_lookup_above_800_shared_slugs(self):
        lib_a = [
            SimpleNamespace(
                slug=f"slug-{i:04d}",
                title=f"Title {i}",
                status="played",
                rating=5.0 if i in (0, 2) else (4.5 if i == 3 else 3.0),
                cover_url=None,
                release_year=2020,
            )
            for i in range(850)
        ]
        lib_b = [
            SimpleNamespace(
                slug=f"slug-{i:04d}",
                title=f"Title {i}",
                status="played",
                rating=5.0 if i == 0 else (1.0 if i == 2 else (4.5 if i == 3 else 3.0)),
                cover_url=None,
                release_year=2020,
            )
            for i in range(850)
        ]
        await self.db.replace_user_library(10, "Alpha", lib_a)
        await self.db.replace_user_library(20, "Beta", lib_b)
        await self.db._execute(
            "UPDATE cached_games SET title = ? WHERE slug = ?",
            ("NieR:Automata — Game of the YoRHa", "slug-0003"),
        )

        callback = await self._render_duel_callback()

        fetchall_param_counts = []
        orig_fetchall = self.db._fetchall

        async def track_fetchall(query, params=()):
            if "cached_games" in query:
                fetchall_param_counts.append(len(params))
            return await orig_fetchall(query, params)

        self.db._fetchall = track_fetchall
        try:
            btn_inter = self._button_interaction(guild=self.guild, user_id=10, display_name="Alpha")
            await callback(btn_inter)
        finally:
            self.db._fetchall = orig_fetchall

        self.assertEqual(len(fetchall_param_counts), 2)
        self.assertTrue(all(count <= _MAX_SQL_BIND_PARAMS for count in fetchall_param_counts))
        shared_field = btn_inter.visible_public_messages()[0]["embed"].fields[0].value
        self.assertIn("NieR:Automata — Game of the YoRHa", shared_field)

    async def test_duel_callback_db_metadata_failure_and_cancellation_preserve_parent_post(self):
        # Add an extra shared slug so on_compare must query get_games_by_slugs.
        lib_a = [
            SimpleNamespace(slug=f"g{i}", title=f"Game {i}", status="played", rating=5.0 - (i * 0.1), cover_url=None, release_year=2020)
            for i in range(5)
        ]
        lib_b = [
            SimpleNamespace(slug=f"g{i}", title=f"Game {i}", status="played", rating=4.5, cover_url=None, release_year=2020)
            for i in range(5)
        ]
        await self.db.replace_user_library(10, "Alpha", lib_a)
        await self.db.replace_user_library(20, "Beta", lib_b)

        callback = await self._render_duel_callback()

        with patch.object(
            self.db,
            "get_games_by_slugs",
            side_effect=RuntimeError("SELECT * FROM cached_games WHERE auth_token='secret_db_tok'"),
        ):
            btn_err = self._button_interaction(guild=self.guild, user_id=10, display_name="Alpha")
            with self.assertLogs("backloggd.cogs.social", level="ERROR") as logs:
                await callback(btn_err)

        self.assertEqual(len(logs.records), 1)
        self.assertEqual(btn_err.visible_public_messages(), [])
        ephem = btn_err.visible_ephemeral_messages()
        self.assertEqual(len(ephem), 1)
        self.assertNotIn("secret_db_tok", ephem[0]["content"])
        self.assertEqual(btn_err.message.edits, [])
        self.assertFalse(self.parent_message.get("deleted", False))

        with patch.object(self.db, "get_games_by_slugs", side_effect=asyncio.CancelledError()):
            btn_cancel = self._button_interaction(guild=self.guild, user_id=10, display_name="Alpha")
            with self.assertRaises(asyncio.CancelledError):
                await callback(btn_cancel)

        self.assertEqual(btn_cancel.visible_public_messages(), [])
        self.assertEqual(btn_cancel.visible_ephemeral_messages(), [])
        self.assertEqual(btn_cancel.message.edits, [])
        self.assertFalse(self.parent_message.get("deleted", False))


if __name__ == "__main__":
    unittest.main()
