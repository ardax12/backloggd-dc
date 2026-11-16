"""Regression tests for global/local application-command error boundaries."""

import asyncio
import logging
import os
import sqlite3
import sys
import tempfile
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

os.environ["PYTHON_DOTENV_DISABLED"] = "1"
os.environ["TURSO_DATABASE_URL"] = ""
os.environ["TURSO_AUTH_TOKEN"] = ""
os.environ["DEV_GUILD_ID"] = ""

import discord
from discord import app_commands

from bot.client import BackloggdBot
from bot.cogs.feed import FeedCog
from bot.db import Database


class _FakeHTTPResponse:
    status = 404
    reason = "Not Found"


def make_interaction(
    *,
    name: str,
    options=None,
    guild_id=1,
    guild=None,
    user_id=42,
    manage_channels=True,
):
    done = False

    async def defer(*, ephemeral=False, thinking=False):
        nonlocal done
        done = True

    async def send_message(*args, **kwargs):
        nonlocal done
        done = True

    response = SimpleNamespace(
        is_done=lambda: done,
        defer=AsyncMock(side_effect=defer),
        send_message=AsyncMock(side_effect=send_message),
    )
    followup = SimpleNamespace(send=AsyncMock())
    perms = discord.Permissions.none()
    perms.manage_channels = manage_channels
    if guild is None and guild_id is not None:
        guild = SimpleNamespace(
            id=guild_id,
            name="Test Guild",
            chunked=True,
            member_count=1,
            members=[SimpleNamespace(id=user_id)],
            me=SimpleNamespace(id=999),
        )
    data = {"name": name, "type": 1}
    if options is not None:
        data["options"] = options
    if guild_id is not None:
        data["guild_id"] = str(guild_id)
    return SimpleNamespace(
        type=discord.InteractionType.application_command,
        data=data,
        guild_id=guild_id,
        guild=guild,
        user=SimpleNamespace(id=user_id, display_name="Tester", name="tester"),
        channel=SimpleNamespace(id=10),
        permissions=perms,
        app_permissions=discord.Permissions.all(),
        extras={},
        command=None,
        command_failed=False,
        namespace=None,
        _state=SimpleNamespace(),
        _cs_command=None,
        _cs_namespace=None,
        response=response,
        followup=followup,
    )


class CommandErrorBoundaryTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self._saved_cog_modules = {
            name: mod for name, mod in sys.modules.items() if name.startswith("bot.cogs.")
        }
        self.tmpdir = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self.tmpdir.name, "test.db")
        self.bot = BackloggdBot()
        await self.bot.db.close()
        self.bot.db = Database(self.db_path)
        await self.bot.db.connect()
        self.bot.backloggd = MagicMock()
        self.bot.backloggd.close = AsyncMock()

    async def asyncTearDown(self):
        try:
            await self.bot.close()
        finally:
            for name, mod in self._saved_cog_modules.items():
                sys.modules.setdefault(name, mod)
            self.tmpdir.cleanup()

    async def test_db_failure_before_defer_replies_ephemerally_and_logs_without_leaking(self):
        @self.bot.tree.command(name="pre_defer_db_cmd", description="test")
        async def pre_defer_db_cmd(interaction: discord.Interaction):
            raise sqlite3.OperationalError(
                "SELECT * FROM guild_settings WHERE token='secret_tok_123' /app/data/prod.db"
            )

        interaction = make_interaction(name="pre_defer_db_cmd", guild_id=1)

        with self.assertLogs("backloggd.bot", level="ERROR") as logs:
            await self.bot.tree._call(interaction)

        self.assertTrue(interaction.command_failed)
        interaction.response.send_message.assert_awaited_once()
        interaction.followup.send.assert_not_awaited()
        args = interaction.response.send_message.call_args.args
        kwargs = interaction.response.send_message.call_args.kwargs
        self.assertTrue(kwargs.get("ephemeral"))
        reply = args[0] if args else kwargs.get("content", "")
        self.assertNotIn("SELECT", reply)
        self.assertNotIn("secret_tok_123", reply)
        self.assertNotIn("prod.db", reply)
        self.assertNotIn("OperationalError", reply)
        self.assertEqual(len(logs.records), 1)
        self.assertIn("pre_defer_db_cmd", logs.output[0])

    async def test_unsetchannel_db_failure_after_defer_uses_followup_and_logs_once(self):
        await self.bot.load_extension("bot.cogs.feed")
        self.bot.db.unset_feed_channel = AsyncMock(
            side_effect=sqlite3.OperationalError(
                "SELECT * FROM guild_settings WHERE token='secret_tok_123' /app/data/prod.db"
            )
        )
        interaction = make_interaction(name="unsetchannel", guild_id=1)

        with self.assertLogs("backloggd.bot", level="ERROR") as logs:
            await self.bot.tree._call(interaction)

        self.assertTrue(interaction.command_failed)
        interaction.response.defer.assert_awaited_once_with(ephemeral=True)
        interaction.response.send_message.assert_not_awaited()
        interaction.followup.send.assert_awaited_once()
        args = interaction.followup.send.call_args.args
        kwargs = interaction.followup.send.call_args.kwargs
        self.assertTrue(kwargs.get("ephemeral"))
        reply = args[0] if args else kwargs.get("content", "")
        self.assertNotIn("SELECT", reply)
        self.assertNotIn("secret_tok_123", reply)
        self.assertNotIn("prod.db", reply)
        self.assertNotIn("OperationalError", reply)
        self.assertEqual(len(logs.records), 1)
        self.assertIn("unsetchannel", logs.output[0])

    async def test_db_failure_after_defer_uses_followup_and_logs_once(self):
        await self.bot.load_extension("bot.cogs.feed")
        self.bot.db.get_linked_user = AsyncMock(
            side_effect=RuntimeError("libsql://secret-host?authToken=abc /srv/db.sqlite")
        )
        interaction = make_interaction(
            name="feed",
            options=[{"name": "status", "type": 1, "options": []}],
            guild_id=1,
        )

        with self.assertLogs("backloggd.bot", level="ERROR") as logs:
            await self.bot.tree._call(interaction)

        self.assertTrue(interaction.command_failed)
        interaction.response.defer.assert_awaited_once()
        interaction.response.send_message.assert_not_awaited()
        interaction.followup.send.assert_awaited_once()
        kwargs = interaction.followup.send.call_args.kwargs
        self.assertTrue(kwargs.get("ephemeral"))
        reply = interaction.followup.send.call_args.args[0]
        self.assertNotIn("libsql://", reply)
        self.assertNotIn("authToken", reply)
        self.assertNotIn("db.sqlite", reply)
        self.assertEqual(len(logs.records), 1)
        self.assertIn("feed status", logs.output[0])

    async def test_feed_permission_failure_cooperates_with_global_handler_single_reply_no_error_log(self):
        await self.bot.load_extension("bot.cogs.feed")
        interaction = make_interaction(
            name="unsetchannel",
            guild_id=1,
            manage_channels=False,
        )

        root_logger = logging.getLogger("backloggd")
        with self.assertNoLogs(root_logger, level="ERROR"):
            await self.bot.tree._call(interaction)

        interaction.response.send_message.assert_awaited_once()
        interaction.followup.send.assert_not_awaited()
        reply = interaction.response.send_message.call_args.args[0]
        self.assertIn("Manage Channels", reply)
        self.assertTrue(interaction.response.send_message.call_args.kwargs.get("ephemeral"))

    async def test_feed_dm_failure_cooperates_with_global_handler_single_reply_no_error_log(self):
        await self.bot.load_extension("bot.cogs.feed")
        interaction = make_interaction(
            name="unsetchannel",
            guild_id=None,
            guild=None,
        )

        root_logger = logging.getLogger("backloggd")
        with self.assertNoLogs(root_logger, level="ERROR"):
            await self.bot.tree._call(interaction)

        interaction.response.send_message.assert_awaited_once()
        interaction.followup.send.assert_not_awaited()
        self.assertIn("server", interaction.response.send_message.call_args.args[0].lower())

    async def test_unexpected_feed_error_after_defer_is_not_swallowed(self):
        await self.bot.load_extension("bot.cogs.feed")
        self.bot.db.get_guild_feed_candidates = AsyncMock(
            side_effect=sqlite3.DatabaseError("SELECT secret FROM tokens")
        )
        interaction = make_interaction(
            name="feed",
            options=[{"name": "list", "type": 1, "options": []}],
            guild_id=1,
            manage_channels=True,
        )

        with self.assertLogs("backloggd.bot", level="ERROR") as logs:
            await self.bot.tree._call(interaction)

        interaction.response.defer.assert_awaited_once()
        interaction.followup.send.assert_awaited_once()
        reply = interaction.followup.send.call_args.args[0]
        self.assertNotIn("SELECT", reply)
        self.assertNotIn("tokens", reply)
        self.assertEqual(len(logs.records), 1)
        self.assertIn("feed list", logs.output[0])

    async def test_global_handler_classifies_check_and_cooldown_errors_without_error_log(self):
        @self.bot.tree.command(name="cooldown_cmd", description="test")
        @app_commands.checks.bot_has_permissions(embed_links=True)
        async def cooldown_cmd(interaction: discord.Interaction):
            raise app_commands.CommandOnCooldown(app_commands.Cooldown(1, 10.0), 4.2)

        interaction = make_interaction(name="cooldown_cmd", guild_id=1)
        with self.assertNoLogs(logging.getLogger("backloggd"), level="ERROR"):
            await self.bot.tree._call(interaction)

        interaction.response.send_message.assert_awaited_once()
        reply = interaction.response.send_message.call_args.args[0]
        self.assertIn("cooldown", reply.lower())

        interaction2 = make_interaction(name="cooldown_cmd", guild_id=1)
        await self.bot.tree.on_error(
            interaction2,
            app_commands.BotMissingPermissions(["embed_links"]),
        )
        interaction2.response.send_message.assert_awaited_once()
        self.assertIn("permission", interaction2.response.send_message.call_args.args[0].lower())

    async def test_expired_interaction_during_error_reply_does_not_raise_or_recurse(self):
        await self.bot.load_extension("bot.cogs.account")
        self.bot.db.get_linked_user = AsyncMock(side_effect=RuntimeError("boom"))
        interaction = make_interaction(name="refresh", guild_id=1)
        interaction.response.send_message = AsyncMock(
            side_effect=discord.NotFound(_FakeHTTPResponse(), "Unknown interaction")
        )
        interaction.followup.send = AsyncMock(
            side_effect=discord.NotFound(_FakeHTTPResponse(), "Unknown webhook")
        )

        with self.assertLogs("backloggd", level="ERROR"):
            await self.bot.tree._call(interaction)

    async def test_cancelled_error_is_propagated_not_swallowed(self):
        @self.bot.tree.command(name="cancel_cmd", description="test")
        async def cancel_cmd(interaction: discord.Interaction):
            raise asyncio.CancelledError()

        interaction = make_interaction(name="cancel_cmd", guild_id=1)
        with self.assertRaises(asyncio.CancelledError):
            await self.bot.tree._call(interaction)
        interaction.response.send_message.assert_not_awaited()
        interaction.followup.send.assert_not_awaited()

    async def test_all_cogs_load_and_share_global_error_boundary(self):
        for ext in (
            "bot.cogs.account",
            "bot.cogs.games",
            "bot.cogs.users",
            "bot.cogs.social",
            "bot.cogs.discovery",
            "bot.cogs.stats",
            "bot.cogs.feed",
        ):
            await self.bot.load_extension(ext)

        commands_list = list(self.bot.tree.walk_commands())
        self.assertGreaterEqual(len(commands_list), 15)
        for cmd in commands_list:
            if isinstance(cmd, app_commands.Group):
                continue
            if cmd.on_error is not None:
                # Only FeedCog's channel_command_error is expected, and it must delegate unknown errors.
                self.assertEqual(cmd.binding.__class__.__name__, "FeedCog")


if __name__ == "__main__":
    unittest.main()
