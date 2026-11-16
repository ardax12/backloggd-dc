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
from unittest.mock import patch

import discord

from bot.cogs.social import SocialCog
from bot.db import Database
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


if __name__ == "__main__":
    unittest.main()
