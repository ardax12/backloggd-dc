"""Regression tests for Task 11: gateway intents, command copy, and expired view cleanup."""

import os
os.environ["PYTHON_DOTENV_DISABLED"] = "1"
os.environ["TURSO_DATABASE_URL"] = ""
os.environ["TURSO_AUTH_TOKEN"] = ""
os.environ["DEV_GUILD_ID"] = ""

from pathlib import Path
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock

import discord

from bot.client import BackloggdBot
from bot.cogs.account import AccountCog
from bot.cogs.discovery import DiscoveryCog
from bot.ui.views import RouletteView


def _http_response(status: int, reason: str = "Error"):
    return SimpleNamespace(status=status, reason=reason)


class IntentAndCopyTests(unittest.IsolatedAsyncioTestCase):
    def test_bot_disables_unused_message_content_intent_and_keeps_members(self):
        bot = BackloggdBot()
        self.assertTrue(bot.intents.members)
        self.assertFalse(bot.intents.message_content)

        readme = (Path(__file__).parents[1] / "README.md").read_text(encoding="utf-8")
        intents_section = readme.split("### 2. Configure Gateway Intents", 1)[1].split("### 3.", 1)[0]
        self.assertIn("Server Members Intent", intents_section)
        self.assertNotIn("- **Message Content Intent**", intents_section)

    def test_member_selector_descriptions_do_not_advertise_raw_usernames(self):
        refresh_param = next(p for p in AccountCog.refresh.parameters if p.name == "user")
        roulette_param = next(p for p in DiscoveryCog.roulette.parameters if p.name == "user")
        self.assertEqual(refresh_param.type, discord.AppCommandOptionType.user)
        self.assertEqual(roulette_param.type, discord.AppCommandOptionType.user)
        self.assertNotIn("username", refresh_param.description.lower())
        self.assertNotIn("backloggd user", roulette_param.description.lower())
        self.assertIn("member", refresh_param.description.lower())
        self.assertIn("member", roulette_param.description.lower())

    async def test_unlinked_target_grammar_for_caller_vs_other_member(self):
        db = SimpleNamespace(get_linked_user=AsyncMock(return_value=None))
        bot = SimpleNamespace(db=db, backloggd=SimpleNamespace())
        account_cog = AccountCog(bot)
        discovery_cog = DiscoveryCog(bot)

        def _interaction(user_id=1):
            return SimpleNamespace(
                user=SimpleNamespace(id=user_id, display_name="Caller"),
                guild=SimpleNamespace(id=10, name="Guild"),
                response=SimpleNamespace(defer=AsyncMock()),
                followup=SimpleNamespace(send=AsyncMock()),
            )

        other = SimpleNamespace(id=2, display_name="@everyone*")

        # Caller unlinked -> "You do not ..."
        i_self_refresh = _interaction()
        await AccountCog.refresh.callback(account_cog, i_self_refresh, None)
        msg_self_refresh = i_self_refresh.followup.send.call_args.args[0]
        self.assertIn("You do not have a linked Backloggd account", msg_self_refresh)

        i_self_roulette = _interaction()
        await DiscoveryCog.roulette.callback(discovery_cog, i_self_roulette, None)
        msg_self_roulette = i_self_roulette.followup.send.call_args.args[0]
        self.assertIn("You do not have a linked Backloggd account", msg_self_roulette)

        # Other member unlinked -> "<escaped> does not ...", never "do not"
        i_other_refresh = _interaction()
        await AccountCog.refresh.callback(account_cog, i_other_refresh, other)
        msg_other_refresh = i_other_refresh.followup.send.call_args.args[0]
        self.assertIn("does not have a linked Backloggd account", msg_other_refresh)
        self.assertNotIn("do not have", msg_other_refresh)
        self.assertNotIn("@everyone", msg_other_refresh)
        self.assertIn("everyone\\*", msg_other_refresh)
        mentions = i_other_refresh.followup.send.call_args.kwargs.get("allowed_mentions")
        self.assertIsNotNone(mentions)
        self.assertFalse(mentions.users)
        self.assertFalse(mentions.everyone)

        i_other_roulette = _interaction()
        await DiscoveryCog.roulette.callback(discovery_cog, i_other_roulette, other)
        msg_other_roulette = i_other_roulette.followup.send.call_args.args[0]
        self.assertIn("does not have a linked Backloggd account", msg_other_roulette)
        self.assertNotIn("do not have", msg_other_roulette)
        self.assertNotIn("@everyone", msg_other_roulette)
        self.assertIn("everyone\\*", msg_other_roulette)
        mentions2 = i_other_roulette.followup.send.call_args.kwargs.get("allowed_mentions")
        self.assertIsNotNone(mentions2)
        self.assertFalse(mentions2.users)
        self.assertFalse(mentions2.everyone)


class ExpiredViewCleanupTests(unittest.IsolatedAsyncioTestCase):
    async def test_timeout_cleanup_disables_controls_and_handles_expected_http_errors(self):
        # Success case
        view = RouletteView(1, "https://example.test/game", AsyncMock())
        edit_ok = AsyncMock()
        view.message = SimpleNamespace(edit=edit_ok)
        await view.on_timeout()
        self.assertTrue(view.reroll_button.disabled)
        edit_ok.assert_awaited_once_with(view=view)

        # Expected Discord HTTP errors should be handled cleanly after disabling controls
        for exc in (
            discord.NotFound(_http_response(404, "Not Found"), "Unknown Message"),
            discord.Forbidden(_http_response(403, "Forbidden"), "Missing Access"),
            discord.HTTPException(_http_response(502, "Bad Gateway"), "Upstream error"),
        ):
            v = RouletteView(1, "https://example.test/game", AsyncMock())
            v.message = SimpleNamespace(edit=AsyncMock(side_effect=exc))
            await v.on_timeout()
            self.assertTrue(v.reroll_button.disabled)

        # Unrelated programming error must NOT be swallowed
        v_bug = RouletteView(1, "https://example.test/game", AsyncMock())
        v_bug.message = SimpleNamespace(edit=AsyncMock(side_effect=TypeError("unexpected bug")))
        with self.assertRaises(TypeError):
            await v_bug.on_timeout()


if __name__ == "__main__":
    unittest.main()
