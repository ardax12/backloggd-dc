"""Regression tests for Task 5: context-safe SDK text and image presentation."""

import os

os.environ["PYTHON_DOTENV_DISABLED"] = "1"
os.environ["TURSO_DATABASE_URL"] = ""
os.environ["TURSO_AUTH_TOKEN"] = ""
os.environ["DEV_GUILD_ID"] = ""

import re
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock

import discord

from bot.cogs.account import AccountCog
from bot.ui.embeds import create_game_embed, create_profile_embed
from bot.ui.feed_embed import create_journal_feed_embed
from bot.ui.helpers import clean_image_url


def _assert_embed_payload_limits(testcase: unittest.TestCase, embed: discord.Embed) -> dict:
    payload = embed.to_dict()
    testcase.assertLessEqual(len(payload.get("title", "")), 256)
    testcase.assertLessEqual(len(payload.get("description", "")), 4096)
    for field in payload.get("fields", []):
        testcase.assertLessEqual(len(field.get("name", "")), 256)
        testcase.assertLessEqual(len(field.get("value", "")), 1024)
    testcase.assertLessEqual(len(embed), 6000)
    return payload


def _unescaped_double_pipes(text: str) -> list[int]:
    positions = []
    i = 0
    while i < len(text):
        if text[i] == "\\":
            i += 2
            continue
        if text.startswith("||", i):
            positions.append(i)
            i += 2
            continue
        i += 1
    return positions


class FeedEmbedSafetyTests(unittest.TestCase):
    def test_feed_metadata_and_review_notes_escape_and_quote_safely(self):
        entry = SimpleNamespace(
            game_slug="hades",
            game_title="Hades",
            action_type="reviewed",
            rating=4.5,
            previous_rating=None,
            platform="PC [fake](https://evil.example) @everyone",
            is_replay=False,
            session_labels=("Finished ||spoiler||", "<#123> *x*"),
            played_date="2026-11-13 [bad](https://evil.example)",
            occurred_at=None,
            date_str="Today @here *now*",
            review_notes="First ||breakout|| line\nSecond line [click](https://evil.example) @everyone\r\n> quoted <#55>\\",
            has_spoilers=True,
            cover_url="/images/hades.png",
        )
        embed = create_journal_feed_embed("Player_One", "//cdn.example/avatar.png", entry)
        payload = _assert_embed_payload_limits(self, embed)

        # Plain-text slots stay free of Markdown backslash noise
        self.assertEqual(payload["author"]["name"], "Player_One")
        self.assertEqual(payload["author"]["icon_url"], "https://cdn.example/avatar.png")
        self.assertEqual(payload["thumbnail"]["url"], "https://backloggd.com/images/hades.png")

        desc = payload["description"]
        self.assertNotIn("@everyone", desc)
        self.assertNotIn("@here", desc)
        self.assertNotIn("<#123>", desc)
        self.assertIn("\\[fake\\](https://evil.example)", desc)
        self.assertIn("\\|\\|spoiler\\|\\|", desc)
        self.assertIn("\\[bad\\](https://evil.example)", desc)
        self.assertIn("\\*now\\*", desc)

        notes_val = payload["fields"][0]["value"]
        lines = notes_val.split("\n")
        self.assertGreaterEqual(len(lines), 3)
        self.assertTrue(all(line.startswith("> ") for line in lines), notes_val)
        self.assertNotIn("@everyone", notes_val)
        self.assertNotIn("<#55>", notes_val)
        self.assertIn("\\[click\\](https://evil.example)", notes_val)
        self.assertIn("\\|\\|breakout\\|\\|", notes_val)
        # Exactly one trusted outer spoiler opener + closer
        self.assertEqual(_unescaped_double_pipes(notes_val), [2, len(notes_val) - 2])

    def test_long_escaped_spoiler_notes_clamp_cleanly_without_breaking_escapes_or_closer(self):
        entry = SimpleNamespace(
            game_slug="hades",
            game_title="Hades",
            action_type="reviewed",
            rating=5.0,
            previous_rating=None,
            platform=None,
            is_replay=False,
            session_labels=(),
            played_date=None,
            occurred_at="2026-11-13T10:00:00Z",
            date_str=None,
            review_notes=("*|" * 400) + "\\",
            has_spoilers=True,
            cover_url=None,
        )
        embed = create_journal_feed_embed("Tester", None, entry)
        payload = _assert_embed_payload_limits(self, embed)
        notes_val = payload["fields"][0]["value"]

        self.assertTrue(notes_val.startswith("> ||"))
        self.assertTrue(notes_val.endswith("...||"))
        self.assertEqual(_unescaped_double_pipes(notes_val), [2, len(notes_val) - 2])
        body_before_suffix = notes_val[:-5]
        trailing_slashes = len(body_before_suffix) - len(body_before_suffix.rstrip("\\"))
        self.assertEqual(trailing_slashes % 2, 0)

    def test_genuine_feed_labels_dates_and_non_spoiler_multiline_notes_remain_clean(self):
        entry = SimpleNamespace(
            game_slug="hades",
            game_title="Hades",
            action_type="playing",
            rating=None,
            previous_rating=None,
            platform="PC",
            is_replay=False,
            session_labels=("Started",),
            played_date="2026-11-13",
            occurred_at="2026-11-14T00:00:00Z",
            date_str=None,
            review_notes="Great combat.\nReally solid soundtrack.",
            has_spoilers=False,
            cover_url=None,
        )
        embed = create_journal_feed_embed("Millers", None, entry)
        payload = _assert_embed_payload_limits(self, embed)
        self.assertIn("**Session:** Started", payload["description"])
        self.assertIn("**Played date:** 2026-11-13", payload["description"])
        self.assertEqual(payload["fields"][0]["value"], "> Great combat.\n> Really solid soundtrack.")


class ProfileAndGameEmbedSafetyTests(unittest.TestCase):
    def test_game_embed_escapes_and_clamps_summary_safely(self):
        game = SimpleNamespace(
            slug="hades",
            title="Hades_II",
            release_year=2025,
            community_rating=4.5,
            summary="Play *[now](https://evil.example)* @everyone <#99> " + ("*" * 300),
            cover_url="/covers/hades.jpg",
            stats=SimpleNamespace(total_ratings=10, plays_count=5, playing_count=1, backlogs_count=2, wishlists_count=3),
        )
        embed = create_game_embed(game, [], guild_name="Guild")
        payload = _assert_embed_payload_limits(self, embed)

        self.assertEqual(payload["title"], "Hades_II (2025)")
        desc = payload["description"]
        self.assertNotIn("@everyone", desc)
        self.assertNotIn("<#99>", desc)
        self.assertIn("\\[now\\](https://evil.example)", desc)
        # Wrapped in *...* without leaving an odd trailing backslash before ...*
        self.assertTrue(desc.endswith("...*"), desc)
        before_suffix = desc[:-4]
        trailing_slashes = len(before_suffix) - len(before_suffix.rstrip("\\"))
        self.assertEqual(trailing_slashes % 2, 0)

    def test_profile_embed_escapes_bio_favorites_recents_and_spoiler_review(self):
        profile = SimpleNamespace(
            username="Hero_Name",
            avatar_url="javascript:alert(1)",
            bio="Check [bio](https://evil.example) @everyone <#1> *star* ||pipe||\\",
            badges=["VIP"],
            favorite_games=[
                "Fav [One](https://evil.example) @here",
                SimpleNamespace(title="Fav *Two* ||x||"),
            ],
            played_count=5,
            playing_count=1,
            backlog_count=2,
            wishlist_count=3,
        )
        recents = [
            SimpleNamespace(
                game_title="Recent [Title](https://evil.example) @everyone",
                rating=4.0,
                played_date="2026-11-13 <#7>",
                is_replay=False,
            )
        ]
        review = SimpleNamespace(
            game_title="Review [Game](https://evil.example) *X*",
            rating=5.0,
            like_count=3,
            has_spoilers=True,
            review_text="Secret ||breakout|| [link](https://evil.example) @everyone " + ("*" * 300),
        )
        embed = create_profile_embed(profile, recent_played=recents, latest_review=review)
        payload = _assert_embed_payload_limits(self, embed)

        self.assertEqual(payload["title"], "Hero_Name [VIP]")
        self.assertEqual(payload["thumbnail"]["url"], "https://backloggd.b-cdn.net/no_avatar.jpg")

        desc = payload["description"]
        self.assertNotIn("@everyone", desc)
        self.assertNotIn("<#1>", desc)
        self.assertIn("\\[bio\\](https://evil.example)", desc)
        self.assertIn("\\|\\|pipe\\|\\|", desc)

        favs = next(f["value"] for f in payload["fields"] if "Favorite Games" in f["name"])
        self.assertNotIn("@here", favs)
        self.assertIn("\\[One\\](https://evil.example)", favs)
        self.assertIn("\\*Two\\* \\|\\|x\\|\\|", favs)

        recent_val = next(f["value"] for f in payload["fields"] if "Recently Played" in f["name"])
        self.assertNotIn("@everyone", recent_val)
        self.assertNotIn("<#7>", recent_val)
        self.assertIn("\\[Title\\](https://evil.example)", recent_val)

        rev_val = next(f["value"] for f in payload["fields"] if "Latest Review" in f["name"])
        self.assertNotIn("@everyone", rev_val)
        self.assertIn("\\[Game\\](https://evil.example)", rev_val)
        self.assertIn("\\|\\|breakout\\|\\|", rev_val)
        self.assertIn("\\[link\\](https://evil.example)", rev_val)
        # Exactly one outer spoiler wrapper, intact closer after clamping
        pipes = _unescaped_double_pipes(rev_val)
        self.assertEqual(len(pipes), 2, rev_val)
        self.assertTrue(rev_val.endswith('..."||') or rev_val.endswith('" ||') or rev_val.endswith('"||'), rev_val)

    def test_profile_and_game_embeds_preserve_legitimate_prose(self):
        game = SimpleNamespace(
            slug="celeste",
            title="Celeste",
            release_year=2018,
            community_rating=4.8,
            summary="Help Madeline survive her journey to the top of Celeste Mountain.",
            cover_url=None,
            stats=None,
        )
        g_payload = create_game_embed(game, []).to_dict()
        self.assertIn("*Help Madeline survive her journey to the top of Celeste Mountain.*", g_payload["description"])

        profile = SimpleNamespace(
            username="NormalUser",
            avatar_url=None,
            bio="Casual RPG fan.",
            badges=[],
            favorite_games=["Hollow Knight"],
            played_count=1,
            playing_count=0,
            backlog_count=0,
            wishlist_count=0,
        )
        p_payload = create_profile_embed(
            profile,
            recent_played=[SimpleNamespace(game_title="Hades", rating=5.0, played_date="2026-11-13", is_replay=False)],
            latest_review=SimpleNamespace(game_title="Hades", rating=5.0, like_count=0, has_spoilers=False, review_text="Crisp combat."),
        ).to_dict()
        self.assertIn("> Casual RPG fan.", p_payload["description"])
        recent_val = next(f["value"] for f in p_payload["fields"] if "Recently Played" in f["name"])
        self.assertIn("*(2026-11-13)*", recent_val)
        rev_val = next(f["value"] for f in p_payload["fields"] if "Latest Review" in f["name"])
        self.assertIn('> "Crisp combat."', rev_val)


class ImageUrlAndAccountThumbnailTests(unittest.IsolatedAsyncioTestCase):
    def test_clean_image_url_normalizes_safe_urls_and_rejects_invalid(self):
        self.assertEqual(clean_image_url("/avatars/1.png"), "https://backloggd.com/avatars/1.png")
        self.assertEqual(clean_image_url("//cdn.backloggd.com/1.png"), "https://cdn.backloggd.com/1.png")
        self.assertEqual(clean_image_url("https://example.com/a.png"), "https://example.com/a.png")
        self.assertEqual(clean_image_url("http://example.com/a.png"), "http://example.com/a.png")

        for bad in (
            None,
            "",
            "   ",
            "not a url",
            "just-text",
            "javascript:alert(1)",
            "ftp://example.com/a.png",
            "data:image/png;base64,AAAA",
            "https://bad host/a.png",
            "https:///missing-host.png",
            "https://user@",
        ):
            with self.subTest(url=bad):
                self.assertIsNone(clean_image_url(bad))

    async def test_account_link_and_refresh_normalize_relative_and_omit_invalid_thumbnails(self):
        for avatar_input, expected_thumb, stored_raw in (
            ("/u/avatar.png", "https://backloggd.com/u/avatar.png", "/u/avatar.png"),
            ("//cdn.example/avatar.png", "https://cdn.example/avatar.png", "//cdn.example/avatar.png"),
            ("javascript:alert(1)", None, "javascript:alert(1)"),
            ("not a url", None, "not a url"),
        ):
            with self.subTest(avatar_input=avatar_input):
                db = SimpleNamespace(
                    get_linked_user=AsyncMock(return_value=None),
                    link_user_if_unlinked=AsyncMock(return_value=True),
                    set_guild_feed_preference=AsyncMock(),
                    get_profile_sync_info=AsyncMock(return_value=None),
                )
                backloggd = SimpleNamespace(
                    get_user_profile=AsyncMock(
                        return_value=SimpleNamespace(username="Tester", avatar_url=avatar_input)
                    )
                )
                library_sync = SimpleNamespace(
                    seed_profile_state=lambda *a, **kw: None,
                    is_refreshing=lambda *a: False,
                    request=lambda *a, **kw: None,
                )
                cog = AccountCog(SimpleNamespace(db=db, backloggd=backloggd, library_sync=library_sync))
                cog._start_sync_task = lambda *a, **kw: None

                i_link = SimpleNamespace(
                    user=SimpleNamespace(id=1, display_name="User"),
                    guild=None,
                    guild_id=None,
                    response=SimpleNamespace(defer=AsyncMock()),
                    followup=SimpleNamespace(send=AsyncMock(return_value=SimpleNamespace(edit=AsyncMock()))),
                )
                await AccountCog.link.callback(cog, i_link, "Tester")
                db.link_user_if_unlinked.assert_awaited_once_with(
                    discord_id=1, username="Tester", avatar_url=stored_raw
                )
                link_payload = i_link.followup.send.call_args.kwargs["embed"].to_dict()
                if expected_thumb is None:
                    self.assertNotIn("thumbnail", link_payload)
                else:
                    self.assertEqual(link_payload["thumbnail"]["url"], expected_thumb)

                db.get_linked_user.return_value = {"backloggd_username": "Tester", "avatar_url": stored_raw}
                i_refresh = SimpleNamespace(
                    user=SimpleNamespace(id=1, display_name="User"),
                    guild=None,
                    guild_id=None,
                    response=SimpleNamespace(defer=AsyncMock()),
                    followup=SimpleNamespace(send=AsyncMock(return_value=SimpleNamespace(edit=AsyncMock()))),
                )
                await AccountCog.refresh.callback(cog, i_refresh, None)
                refresh_payload = i_refresh.followup.send.call_args.kwargs["embed"].to_dict()
                if expected_thumb is None:
                    self.assertNotIn("thumbnail", refresh_payload)
                else:
                    self.assertEqual(refresh_payload["thumbnail"]["url"], expected_thumb)


if __name__ == "__main__":
    unittest.main()
