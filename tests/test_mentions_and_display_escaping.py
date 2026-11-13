"""Regression tests for Task 1: default AllowedMentions and safe display escaping."""

import os

os.environ["PYTHON_DOTENV_DISABLED"] = "1"
os.environ["TURSO_DATABASE_URL"] = ""
os.environ["TURSO_AUTH_TOKEN"] = ""
os.environ["DEV_GUILD_ID"] = ""

from types import SimpleNamespace
import unittest

import discord
from discord.webhook import async_ as webhook_async

from bot.client import BackloggdBot
from bot.input_validation import escape_display_text
from bot.ui.embeds import (
    create_roulette_embed,
    create_server_stats_embed,
    create_server_top_embed,
    create_taste_twins_embed,
)


class _RecordingWebhookAdapter:
    def __init__(self):
        self.calls = []

    async def create_interaction_response(
        self,
        interaction_id,
        token,
        *,
        session,
        proxy=None,
        proxy_auth=None,
        params,
    ):
        self.calls.append(("interaction_response", params.payload))
        return {
            "interaction": {
                "id": str(interaction_id),
                "type": 2,
                "name": "test",
                "user": {"id": "5", "username": "u", "discriminator": "0"},
            }
        }

    async def execute_webhook(
        self,
        webhook_id,
        token,
        *,
        session,
        proxy=None,
        proxy_auth=None,
        payload=None,
        multipart=None,
        files=None,
        thread_id=None,
        wait=False,
        with_components=False,
    ):
        self.calls.append(("webhook", payload))
        return {
            "id": "99",
            "channel_id": "4",
            "author": {"id": "2", "username": "bot", "discriminator": "0", "bot": True},
            "content": payload.get("content", "") if payload else "",
            "timestamp": "2026-01-01T00:00:00+00:00",
            "tts": False,
            "mention_everyone": False,
            "mentions": [],
            "mention_roles": [],
            "attachments": [],
            "embeds": [],
            "pinned": False,
            "type": 0,
        }


def _make_interaction(bot: BackloggdBot, *, is_component: bool = False) -> discord.Interaction:
    base = {
        "id": "100",
        "application_id": "200",
        "token": "tok",
        "version": 1,
        "channel_id": "4",
        "attachment_size_limit": 8388608,
        "user": {"id": "5", "username": "u", "discriminator": "0"},
    }
    if is_component:
        base["type"] = 3
        base["data"] = {"custom_id": "btn:1", "component_type": 2}
        base["message"] = {
            "id": "55",
            "channel_id": "4",
            "author": {"id": "200", "username": "bot", "discriminator": "0", "bot": True},
            "content": "initial",
            "timestamp": "2026-01-01T00:00:00+00:00",
            "tts": False,
            "mention_everyone": False,
            "mentions": [],
            "mention_roles": [],
            "attachments": [],
            "embeds": [],
            "pinned": False,
            "type": 0,
        }
    else:
        base["type"] = 2
        base["data"] = {"id": "3", "name": "test", "type": 1}
    return discord.Interaction(data=base, state=bot._connection)


class DefaultAllowedMentionsTests(unittest.IsolatedAsyncioTestCase):
    async def test_bot_default_allowed_mentions_disables_all_parsing(self):
        bot = BackloggdBot()
        self.addAsyncCleanup(bot.close)

        self.assertIsNotNone(bot.allowed_mentions)
        self.assertFalse(bot.allowed_mentions.everyone)
        self.assertFalse(bot.allowed_mentions.users)
        self.assertFalse(bot.allowed_mentions.roles)
        self.assertFalse(bot.allowed_mentions.replied_user)
        self.assertEqual(bot.allowed_mentions.to_dict(), {"parse": []})

    async def test_initial_followup_and_button_edit_serialize_none_allowed_mentions(self):
        bot = BackloggdBot()
        self.addAsyncCleanup(bot.close)

        adapter = _RecordingWebhookAdapter()
        token = webhook_async.async_context.set(adapter)
        self.addCleanup(webhook_async.async_context.reset, token)

        slash_interaction = _make_interaction(bot, is_component=False)
        await slash_interaction.response.send_message("Hello @everyone <@123>")
        await slash_interaction.followup.send("Followup @here <@&456>")

        button_interaction = _make_interaction(bot, is_component=True)
        await button_interaction.response.edit_message(content="Updated @everyone")

        self.assertEqual(len(adapter.calls), 3)

        kind0, payload0 = adapter.calls[0]
        self.assertEqual(kind0, "interaction_response")
        self.assertEqual(payload0["data"]["allowed_mentions"], {"parse": []})

        kind1, payload1 = adapter.calls[1]
        self.assertEqual(kind1, "webhook")
        self.assertEqual(payload1["allowed_mentions"], {"parse": []})

        kind2, payload2 = adapter.calls[2]
        self.assertEqual(kind2, "interaction_response")
        self.assertEqual(payload2["data"]["allowed_mentions"], {"parse": []})


class DisplayEscapingTests(unittest.TestCase):
    def test_escape_display_text_neutralizes_link_injection_and_all_mention_tokens(self):
        # Closing-bracket link injection inside [label](url)
        injected = "foo](https://evil.example)"
        escaped = escape_display_text(injected)
        self.assertIn("\\]", escaped)
        self.assertEqual(escaped, "foo\\](https://evil.example)")

        # Full markdown link & URL underscores
        full_link = "[click](https://evil.example/a_b)"
        escaped_link = escape_display_text(full_link)
        self.assertEqual(escaped_link, "\\[click\\](https://evil.example/a\\_b)")

        # Trailing backslash right before ] must not unescape the ]
        backslash_break = "foo\\](https://evil.example)"
        escaped_bs = escape_display_text(backslash_break)
        self.assertEqual(escaped_bs, "foo\\\\\\](https://evil.example)")

        # Short and full user/role/channel mentions plus everyone/here
        for raw_token in (
            "@everyone",
            "@here",
            "<@123>",
            "<@!123>",
            "<@&456>",
            "<#789>",
            "<@123456789012345678>",
            "<#123456789012345678>",
        ):
            safe = escape_display_text(raw_token)
            self.assertNotIn(raw_token, safe)

    def test_escape_display_text_handles_markdown_line_prefixes_and_preserves_unicode(self):
        self.assertEqual(escape_display_text("alpha_name"), "alpha\\_name")
        self.assertEqual(escape_display_text("`code` ||spoiler|| ~~s~~ *b*"), "\\`code\\` \\|\\|spoiler\\|\\| \\~\\~s\\~\\~ \\*b\\*")
        self.assertEqual(escape_display_text("# Heading\n> Quote\n- Item"), "\\# Heading\n\\> Quote\n\\- Item")

        # Ordinary parentheses, mid-line hyphens, colons, and Unicode are untouched
        clean_title = "Spider-Man: Café (2024) 🎮"
        self.assertEqual(escape_display_text(clean_title), clean_title)

    def test_markdown_link_and_description_embeds_escape_untrusted_text_safely(self):
        malicious_title = "Evil](https://evil.example) *Game* <@123>"
        roulette_embed = create_roulette_embed(
            "Player_1",
            SimpleNamespace(title=malicious_title, release_year=2024, slug="evil", cover_url=None, community_rating=4.5),
            remaining_backlog=3,
        )
        # Embed title is non-Markdown: no literal backslash noise
        self.assertEqual(roulette_embed.title, "🎲 Unplayed Game Roulette for Player_1")
        # Embed description uses Markdown link: must escape ] and * and mention token
        self.assertIn("Evil\\](https://evil.example) \\*Game\\*", roulette_embed.description)
        self.assertNotIn("<@123>", roulette_embed.description)

        top_embed = create_server_top_embed(
            "Highest Rated",
            [{"title": "Trap [x](https://evil.example)", "slug": "trap", "avg_rating": 4.5, "rating_count": 2}],
            "highest_rated",
            "Guild",
        )
        self.assertIn("\\[x\\](https://evil.example)", top_embed.description)

        twins_embed = create_taste_twins_embed(
            "Alpha",
            None,
            "@everyone [Guild](https://evil.example)",
            [],
        )
        self.assertNotIn("@everyone", twins_embed.description)
        self.assertIn("\\[Guild\\](https://evil.example)", twins_embed.description)

        stats_embed_empty = create_server_stats_embed(
            {"linked_members": 1, "profile_count": 1, "total_logged": 0, "server_avg_rating": None, "top_game": None},
            "Guild",
        )
        self.assertEqual(stats_embed_empty.fields[4].value, "*N/A*")

        stats_embed_malicious = create_server_stats_embed(
            {"linked_members": 1, "profile_count": 1, "total_logged": 1, "server_avg_rating": 4.0, "top_game": "@everyone *Top*"},
            "Guild",
        )
        self.assertNotIn("@everyone", stats_embed_malicious.fields[4].value)
        self.assertIn("\\*Top\\*", stats_embed_malicious.fields[4].value)


if __name__ == "__main__":
    unittest.main()
