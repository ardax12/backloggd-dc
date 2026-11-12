import os
os.environ["PYTHON_DOTENV_DISABLED"] = "1"

import asyncio
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock

from bot.cogs.account import AccountCog
from bot.cogs.games import GamesCog
from bot.cogs.users import UsersCog
from bot.ui.views import RouletteView


class AccountUxTests(unittest.IsolatedAsyncioTestCase):
    async def test_link_sync_updates_the_original_status_message_on_success(self):
        edited = asyncio.Event()

        class StatusMessage:
            async def edit(self, **kwargs):
                self.embed = kwargs["embed"]
                edited.set()

        cog = AccountCog(SimpleNamespace(db=None))
        cog._sync_user_library = AsyncMock(return_value=24)
        message = StatusMessage()
        cog._start_sync_task(1, "Tester", status_message=message)

        await asyncio.wait_for(edited.wait(), timeout=1)

        self.assertIn("24", message.embed.description)
        self.assertIn("complete", message.embed.title.lower())

    async def test_link_sync_reports_failure_on_the_original_status_message(self):
        edited = asyncio.Event()

        class StatusMessage:
            async def edit(self, **kwargs):
                self.embed = kwargs["embed"]
                edited.set()

        cog = AccountCog(SimpleNamespace(db=None))
        cog._sync_user_library = AsyncMock(return_value=None)
        message = StatusMessage()
        cog._start_sync_task(1, "Tester", status_message=message)

        await asyncio.wait_for(edited.wait(), timeout=1)

        self.assertIn("could not", message.embed.description.lower())
        self.assertIn("refresh", message.embed.description.lower())

    async def test_link_verification_failure_suggests_retry_instead_of_spelling(self):
        db = SimpleNamespace(
            get_linked_user=AsyncMock(return_value=None),
            get_linked_user_by_username=AsyncMock(return_value=None),
        )
        api = SimpleNamespace(get_user_profile=AsyncMock(side_effect=RuntimeError("service unavailable")))
        cog = AccountCog(SimpleNamespace(db=db, backloggd=api))
        interaction = SimpleNamespace(
            response=SimpleNamespace(defer=AsyncMock()),
            followup=SimpleNamespace(send=AsyncMock()),
            user=SimpleNamespace(id=1),
        )

        await AccountCog.link.callback(cog, interaction, "Tester")

        message = interaction.followup.send.call_args.args[0]
        self.assertIn("try again", message.lower())
        self.assertNotIn("check the spelling", message.lower())


class ViewUxTests(unittest.IsolatedAsyncioTestCase):
    async def test_expired_reroll_view_disables_controls_and_edits_its_message(self):
        edited = AsyncMock()
        view = RouletteView(1, "https://example.test/game", AsyncMock())
        view.message = SimpleNamespace(edit=edited)

        await view.on_timeout()

        self.assertTrue(view.reroll_button.disabled)
        edited.assert_awaited_once_with(view=view)


class GameSearchUxTests(unittest.IsolatedAsyncioTestCase):
    async def test_autocomplete_value_marks_a_selected_game_slug(self):
        game = SimpleNamespace(title="One", release_year=2024, slug="one")
        cog = GamesCog(SimpleNamespace(backloggd=SimpleNamespace(search_games=AsyncMock(return_value=[game]))))

        choices = await cog.game_autocomplete(SimpleNamespace(), "one")

        self.assertEqual(cog._autocomplete_slugs[choices[0].value], "one")

    async def test_ambiguous_free_text_search_asks_user_to_choose(self):
        games = [
            SimpleNamespace(title="Control", slug="control"),
            SimpleNamespace(title="Control Ultimate Edition", slug="control-ultimate-edition"),
        ]
        api = SimpleNamespace(search_games=AsyncMock(return_value=games), get_game=AsyncMock())
        db = SimpleNamespace(upsert_game=AsyncMock())
        cog = GamesCog(SimpleNamespace(backloggd=api, db=db))
        interaction = SimpleNamespace(
            response=SimpleNamespace(defer=AsyncMock()),
            followup=SimpleNamespace(send=AsyncMock()),
            guild=None,
            user=SimpleNamespace(id=1),
        )

        await GamesCog.game.callback(cog, interaction, "Control")

        api.get_game.assert_not_awaited()
        message = interaction.followup.send.call_args.args[0]
        self.assertIn("Control Ultimate Edition", message)
        self.assertIn("autocomplete", message.lower())


class ProfileUxTests(unittest.IsolatedAsyncioTestCase):
    async def test_profile_service_failure_is_not_reported_as_missing_account(self):
        api = SimpleNamespace(
            get_user_profile=AsyncMock(side_effect=RuntimeError("service unavailable")),
            get_user_journal=AsyncMock(return_value=[]),
            get_user_reviews=AsyncMock(return_value=[]),
        )
        db = SimpleNamespace(
            get_linked_user=AsyncMock(return_value={"backloggd_username": "Tester"}),
        )
        cog = UsersCog(SimpleNamespace(db=db, backloggd=api))
        interaction = SimpleNamespace(
            response=SimpleNamespace(defer=AsyncMock()),
            user=SimpleNamespace(id=1, display_avatar=None), guild=None,
            followup=SimpleNamespace(send=AsyncMock()),
        )

        await UsersCog.profile.callback(cog, interaction, None)

        message = interaction.followup.send.call_args.args[0]
        self.assertIn("try again", message.lower())
        self.assertNotIn("could not be found", message.lower())

    async def test_empty_journal_does_not_label_cached_ratings_as_recently_played(self):
        profile = SimpleNamespace(
            username="Tester", avatar_url=None, bio=None, badges=[], favorite_games=[],
            played_count=1, playing_count=0, backlog_count=1, wishlist_count=0,
        )
        cached = [{"game_title": "Backlogged Game", "rating": 4.0, "status": "backlog"}]
        api = SimpleNamespace(
            get_user_profile=AsyncMock(return_value=profile),
            get_user_journal=AsyncMock(return_value=[]),
            get_user_reviews=AsyncMock(return_value=[]),
        )
        db = SimpleNamespace(
            get_linked_user=AsyncMock(return_value={"backloggd_username": "Tester"}),
            get_user_stats=AsyncMock(return_value={"rated_count": 1, "average_rating": 4.0}),
            get_user_recent_ratings=AsyncMock(return_value=cached),
        )
        cog = UsersCog(SimpleNamespace(db=db, backloggd=api))
        user = SimpleNamespace(id=1, display_avatar=None)
        interaction = SimpleNamespace(
            response=SimpleNamespace(defer=AsyncMock()), user=user, guild=None,
            followup=SimpleNamespace(send=AsyncMock()),
        )

        await UsersCog.profile.callback(cog, interaction, None)

        embed = interaction.followup.send.call_args.kwargs["embed"]
        self.assertNotIn("Recently Played", [field.name for field in embed.fields])
        db.get_user_recent_ratings.assert_not_awaited()


if __name__ == "__main__":
    unittest.main()
