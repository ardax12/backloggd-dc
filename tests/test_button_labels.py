"""Regression tests for Task 4: Discord 80-character button label limits."""

import os

os.environ["PYTHON_DOTENV_DISABLED"] = "1"
os.environ["TURSO_DATABASE_URL"] = ""
os.environ["TURSO_AUTH_TOKEN"] = ""
os.environ["DEV_GUILD_ID"] = ""

from datetime import datetime, timezone
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock

from bot.cogs.social import SocialCog
from bot.input_validation import MAX_USERNAME_LENGTH, normalize_backloggd_username
from bot.ui.helpers import build_backloggd_user_url
from bot.ui.views import LinkButtonView, TasteTwinsView


def _serialized_buttons(view) -> list[dict]:
    buttons = []
    for row in view.to_components():
        buttons.extend(row.get("components", []))
    return buttons


class ButtonLabelLimitTests(unittest.IsolatedAsyncioTestCase):
    async def test_link_button_view_enforces_80_char_limit_across_boundaries_and_extra_buttons(self):
        url = "https://www.backloggd.com/u/" + ("a" * 100) + "/"
        extra_url_long = "https://backloggd.com/u/" + ("b" * 100) + "/review/999/"

        for length in (79, 80):
            label = "a_" * (length // 2) + ("b" if length % 2 else "")
            self.assertEqual(len(label), length)
            view = LinkButtonView(label=label, url=url)
            comps = _serialized_buttons(view)
            self.assertEqual(comps[0]["label"], label)
            self.assertEqual(comps[0]["url"], url)
            self.assertNotIn("\\_", comps[0]["label"])

        for length in (81, 120):
            label = "u_" + ("x" * (length - 2))
            self.assertEqual(len(label), length)
            view = LinkButtonView(
                label=label,
                url=url,
                extra_buttons=[
                    ("ok_" + ("y" * 76), url),
                    ("exact_" + ("z" * 74), url),
                    ("over81_" + ("w" * 74), extra_url_long),
                    ("way_over_" + ("k" * 105), extra_url_long),
                ],
            )
            comps = _serialized_buttons(view)
            self.assertEqual(len(comps), 5)
            for comp in comps:
                self.assertLessEqual(len(comp["label"]), 80)
                self.assertNotIn("\\_", comp["label"])
            self.assertEqual(comps[0]["url"], url)
            self.assertTrue(comps[0]["label"].endswith("..."))
            self.assertEqual(len(comps[1]["label"]), 79)
            self.assertEqual(len(comps[2]["label"]), 80)
            self.assertTrue(comps[3]["label"].endswith("..."))
            self.assertEqual(comps[3]["url"], extra_url_long)
            self.assertTrue(comps[4]["label"].endswith("..."))
            self.assertEqual(comps[4]["url"], extra_url_long)

    async def test_link_button_view_handles_unicode_and_emoji_plain_text_labels(self):
        unicode_label = "View " + ("🎮Café_Éclair_" * 8)
        self.assertGreater(len(unicode_label), 80)
        url = build_backloggd_user_url("Café_Éclair_User")
        view = LinkButtonView(label=unicode_label, url=url)
        comps = _serialized_buttons(view)
        self.assertLessEqual(len(comps[0]["label"]), 80)
        self.assertTrue(comps[0]["label"].endswith("..."))
        self.assertNotIn("\\_", comps[0]["label"])
        self.assertEqual(comps[0]["url"], url)

    async def test_taste_twins_view_enforces_80_char_limit_on_duel_button(self):
        user_url = build_backloggd_user_url("Owner")
        for twin_len in (69, 70, 71, 100):
            twin_name = "t_" + ("a" * (twin_len - 2))
            view = TasteTwinsView(user_url=user_url, top_twin_name=twin_name, on_compare=AsyncMock())
            comps = _serialized_buttons(view)
            self.assertEqual(len(comps), 2)
            self.assertEqual(comps[0]["url"], user_url)
            duel_label = comps[1]["label"]
            self.assertLessEqual(len(duel_label), 80)
            self.assertNotIn("\\_", duel_label)
            if len(f"Duel with {twin_name}") <= 80:
                self.assertEqual(duel_label, f"Duel with {twin_name}")
            else:
                self.assertTrue(duel_label.endswith("..."))

    async def test_compare_and_taste_twins_commands_with_100_char_usernames_preserve_identity_and_bound_labels(self):
        long_user1 = "u1_" + ("a" * (MAX_USERNAME_LENGTH - 3))
        long_user2 = "u2_" + ("b" * (MAX_USERNAME_LENGTH - 3))
        self.assertEqual(len(long_user1), 100)
        self.assertEqual(normalize_backloggd_username(long_user1), long_user1)
        self.assertEqual(normalize_backloggd_username(long_user2), long_user2)

        shared_ratings = {f"game-{i}": 4.5 for i in range(6)}
        db = SimpleNamespace(
            is_profile_library_subscribed=AsyncMock(return_value=True),
            get_profile_sync_info=AsyncMock(return_value={
                "last_synced_at": datetime(2026, 11, 13, tzinfo=timezone.utc),
                "game_count": 6,
                "last_error": None,
            }),
            get_profile_ratings_dict=AsyncMock(side_effect=[shared_ratings, shared_ratings, shared_ratings]),
            get_linked_user=AsyncMock(return_value={"backloggd_username": long_user1, "avatar_url": None}),
            get_guild_linked_profiles=AsyncMock(return_value=[
                {"profile_key": long_user2.lower(), "display_username": long_user2, "avatar_url": None}
            ]),
            get_multi_profile_ratings_dict=AsyncMock(return_value={long_user2.lower(): shared_ratings}),
            get_games_by_slugs=AsyncMock(return_value={}),
        )
        cog = SocialCog(
            SimpleNamespace(db=db, backloggd=SimpleNamespace()),
            clock=lambda: datetime(2026, 11, 13, tzinfo=timezone.utc),
        )

        # /compare output view
        cmp_inter = SimpleNamespace(
            user=SimpleNamespace(id=10, display_name="Caller"),
            guild=None,
            guild_id=None,
            response=SimpleNamespace(defer=AsyncMock()),
            followup=SimpleNamespace(send=AsyncMock()),
        )
        await SocialCog.compare.callback(cog, cmp_inter, long_user1, long_user2)
        cmp_view = cmp_inter.followup.send.call_args.kwargs["view"]
        cmp_comps = _serialized_buttons(cmp_view)
        self.assertLessEqual(len(cmp_comps[0]["label"]), 80)
        self.assertEqual(cmp_comps[0]["url"], build_backloggd_user_url(long_user1))

        # /taste-twins output view + duel callback view
        guild = SimpleNamespace(
            id=1,
            name="Guild",
            chunked=True,
            member_count=2,
            members=[SimpleNamespace(id=10), SimpleNamespace(id=20)],
        )
        twins_inter = SimpleNamespace(
            user=SimpleNamespace(id=10, display_name="Caller"),
            guild=guild,
            guild_id=1,
            response=SimpleNamespace(defer=AsyncMock()),
            followup=SimpleNamespace(send=AsyncMock(return_value=SimpleNamespace())),
        )
        await SocialCog.taste_twins.callback(cog, twins_inter, None)
        twins_view = twins_inter.followup.send.call_args.kwargs["view"]
        twins_comps = _serialized_buttons(twins_view)
        for comp in twins_comps:
            self.assertLessEqual(len(comp["label"]), 80)
        self.assertEqual(twins_comps[0]["url"], build_backloggd_user_url(long_user1))

        duel_btn = [child for child in twins_view.children if getattr(child, "url", None) is None][0]
        btn_inter = SimpleNamespace(
            user=SimpleNamespace(id=10),
            response=SimpleNamespace(defer=AsyncMock()),
            followup=SimpleNamespace(send=AsyncMock()),
        )
        await duel_btn.callback(btn_inter)
        duel_view = btn_inter.followup.send.call_args.kwargs["view"]
        duel_comps = _serialized_buttons(duel_view)
        self.assertLessEqual(len(duel_comps[0]["label"]), 80)
        self.assertEqual(duel_comps[0]["url"], build_backloggd_user_url(long_user2))


if __name__ == "__main__":
    unittest.main()
