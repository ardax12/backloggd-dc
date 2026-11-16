import os
os.environ["PYTHON_DOTENV_DISABLED"] = "1"

import asyncio
import unittest
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

from backloggd.models import GameEntry, GameStatus, UserLibrary
from bot.cogs.account import AccountCog
from bot.cogs.games import GamesCog
from bot.cogs.social import SocialCog
from bot.cogs.users import UsersCog
from bot.services.library_sync import LibrarySyncService
from bot.ui.views import RouletteView


class AccountUxTests(unittest.IsolatedAsyncioTestCase):
    async def test_member_syncs_join_the_same_profile_task(self):
        started = asyncio.Event()
        release = asyncio.Event()

        async def fetch_library(username, force_refresh=False):
            started.set()
            await release.wait()
            return ["game"]

        db = SimpleNamespace(replace_user_library=AsyncMock(return_value=True))
        api = SimpleNamespace(get_user_library=AsyncMock(side_effect=fetch_library))
        service = LibrarySyncService()
        cog = AccountCog(SimpleNamespace(db=db, backloggd=api, library_sync=service))

        first = cog._start_sync_task(10, "Millers")
        await started.wait()
        second = cog._start_sync_task(11, "millers")
        self.assertIs(first, second)

        release.set()
        await first
        api.get_user_library.assert_awaited_once_with("Millers", force_refresh=True)
        db.replace_user_library.assert_awaited_once_with(10, "Millers", ["game"])
        await service.close()

    async def test_persisted_profile_sync_is_seeded_as_fresh_after_restart(self):
        db = SimpleNamespace(get_profile_sync_info=AsyncMock(return_value={
            "last_synced_at": datetime.now(timezone.utc),
            "game_count": 8,
        }))
        cog = AccountCog(SimpleNamespace(db=db))
        calls = 0

        async def refresh():
            nonlocal calls
            calls += 1
            return 9

        await cog._seed_sync_state("Millers")
        outcome = await cog.library_sync.request(
            "millers", refresh, freshness_window_seconds=3600
        )

        self.assertEqual(outcome.status, "skipped")
        self.assertEqual(outcome.reason, "fresh")
        self.assertEqual(outcome.game_count, 8)
        self.assertEqual(calls, 0)
        await cog.library_sync.close()

    async def test_profile_username_resolution_uses_profile_data_not_a_link_owner(self):
        profile = SimpleNamespace(
            username="Millers", avatar_url=None, bio=None, badges=[], favorite_games=[],
            played_count=0, playing_count=0, backlog_count=0, wishlist_count=0,
        )
        db = SimpleNamespace(
            get_profile_stats=AsyncMock(return_value={"rated_count": 1, "average_rating": 4.0}),
            get_linked_user_by_username=AsyncMock(side_effect=AssertionError("owner lookup is ambiguous")),
        )
        api = SimpleNamespace(
            get_user_profile=AsyncMock(return_value=profile),
            get_user_journal=AsyncMock(return_value=[]),
            get_user_reviews=AsyncMock(return_value=[]),
        )
        cog = UsersCog(SimpleNamespace(db=db, backloggd=api))
        interaction = SimpleNamespace(
            response=SimpleNamespace(defer=AsyncMock()),
            followup=SimpleNamespace(send=AsyncMock()),
            user=SimpleNamespace(id=1, display_avatar=None),
            guild=None,
        )

        await UsersCog.profile.callback(cog, interaction, "Millers")

        db.get_profile_stats.assert_awaited_once_with("Millers")
        db.get_linked_user_by_username.assert_not_awaited()
        interaction.followup.send.assert_awaited_once()

    async def test_social_rating_cache_is_looked_up_by_profile_key(self):
        db = SimpleNamespace(
            get_profile_sync_info=AsyncMock(return_value={"last_synced_at": "2026-01-01", "game_count": 1}),
            get_profile_ratings_dict=AsyncMock(return_value={"hades": 4.5}),
            get_linked_user_by_username=AsyncMock(side_effect=AssertionError("owner lookup is ambiguous")),
        )
        api = SimpleNamespace(get_user_library=AsyncMock())
        ratings = await SocialCog(SimpleNamespace(db=db, backloggd=api))._get_or_fetch_ratings("Millers")

        self.assertEqual(ratings, {"hades": 4.5})
        db.get_profile_ratings_dict.assert_awaited_once_with("Millers")
        db.get_linked_user_by_username.assert_not_awaited()
        api.get_user_library.assert_not_awaited()

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

    async def test_duplicate_profile_notice_names_only_other_members_in_current_guild(self):
        db = SimpleNamespace(
            get_linked_user=AsyncMock(return_value=None),
            link_user_if_unlinked=AsyncMock(return_value=True),
            get_profile_connections=AsyncMock(return_value=[{"discord_id": 2}]),
            get_profile_sync_info=AsyncMock(return_value=None),
        )
        profile = SimpleNamespace(username="Millers", avatar_url=None)
        api = SimpleNamespace(get_user_profile=AsyncMock(return_value=profile))
        cog = AccountCog(SimpleNamespace(db=db, backloggd=api))
        cog._start_sync_task = MagicMock()
        interaction = SimpleNamespace(
            response=SimpleNamespace(defer=AsyncMock()),
            followup=SimpleNamespace(send=AsyncMock(return_value=object())),
            user=SimpleNamespace(id=1),
            guild=SimpleNamespace(members=[
                SimpleNamespace(id=1, display_name="Current member"),
                SimpleNamespace(id=2, display_name="*Same server*"),
                SimpleNamespace(id=3, display_name="Unlinked member"),
            ]),
        )

        await AccountCog.link.callback(cog, interaction, "Millers")

        db.get_profile_connections.assert_awaited_once_with(
            "Millers", [1, 2, 3], exclude_discord_id=1
        )
        embed = interaction.followup.send.call_args.kwargs["embed"]
        self.assertIn("\\*Same server\\*", embed.fields[0].value)
        self.assertNotIn("Unlinked member", embed.fields[0].value)
        self.assertIn("does not verify profile ownership", embed.description)
        allowed_mentions = interaction.followup.send.call_args.kwargs["allowed_mentions"]
        self.assertFalse(allowed_mentions.everyone)
        self.assertFalse(allowed_mentions.users)
        self.assertFalse(allowed_mentions.roles)

    async def test_duplicate_profile_notice_does_not_query_connections_in_dm(self):
        db = SimpleNamespace(
            get_linked_user=AsyncMock(return_value=None),
            link_user_if_unlinked=AsyncMock(return_value=True),
            get_profile_connections=AsyncMock(return_value=[{"discord_id": 2}]),
            get_profile_sync_info=AsyncMock(return_value=None),
        )
        api = SimpleNamespace(
            get_user_profile=AsyncMock(return_value=SimpleNamespace(username="Millers", avatar_url=None))
        )
        cog = AccountCog(SimpleNamespace(db=db, backloggd=api))
        cog._start_sync_task = MagicMock()
        interaction = SimpleNamespace(
            response=SimpleNamespace(defer=AsyncMock()),
            followup=SimpleNamespace(send=AsyncMock(return_value=object())),
            user=SimpleNamespace(id=1),
            guild=None,
        )

        await AccountCog.link.callback(cog, interaction, "Millers")

        db.get_profile_connections.assert_not_awaited()
        embed = interaction.followup.send.call_args.kwargs["embed"]
        self.assertEqual(len(embed.fields), 0)


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

        self.assertEqual(choices[0].value, "slug:one")
        fresh_cog = GamesCog(SimpleNamespace(
            backloggd=SimpleNamespace(search_games=AsyncMock(), get_game=AsyncMock(return_value=None)),
            db=SimpleNamespace(),
        ))
        interaction = SimpleNamespace(
            response=SimpleNamespace(defer=AsyncMock()),
            followup=SimpleNamespace(send=AsyncMock()),
            guild=None,
            user=SimpleNamespace(id=1),
        )
        await GamesCog.game.callback(fresh_cog, interaction, choices[0].value)
        fresh_cog.bot.backloggd.search_games.assert_not_awaited()
        fresh_cog.bot.backloggd.get_game.assert_awaited_once_with("one")

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
            get_profile_stats=AsyncMock(return_value={"rated_count": 1, "average_rating": 4.0}),
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

    async def test_profile_uses_sdk_canonical_username_for_stats_and_single_subscription_lookup(self):
        profile = SimpleNamespace(
            username="Canonical_Name", avatar_url=None, bio=None, badges=[], favorite_games=[],
            played_count=4, playing_count=0, backlog_count=0, wishlist_count=0,
        )

        async def stats_by_username(name: str):
            if name == "Canonical_Name":
                return {"total_games": 5, "rated_count": 3, "average_rating": 4.25}
            return {"total_games": 0, "rated_count": 0, "average_rating": None}

        sync_service = LibrarySyncService()
        db = SimpleNamespace(
            get_profile_sync_info=AsyncMock(return_value=None),
            is_profile_library_subscribed=AsyncMock(return_value=True),
            get_profile_stats=AsyncMock(side_effect=stats_by_username),
            record_profile_sync_attempt=AsyncMock(),
            replace_user_library=AsyncMock(return_value=True),
        )
        api = SimpleNamespace(
            get_user_profile=AsyncMock(return_value=profile),
            get_user_journal=AsyncMock(return_value=[]),
            get_user_reviews=AsyncMock(return_value=[]),
            get_user_library=AsyncMock(return_value=UserLibrary(username="Canonical_Name", games={})),
        )
        cog = UsersCog(SimpleNamespace(db=db, backloggd=api, library_sync=sync_service))
        interaction = SimpleNamespace(
            response=SimpleNamespace(defer=AsyncMock()),
            followup=SimpleNamespace(send=AsyncMock()),
            user=SimpleNamespace(id=1, display_avatar=None),
            guild=None,
        )

        try:
            await UsersCog.profile.callback(cog, interaction, "legacy_input")
            await asyncio.sleep(0)
        finally:
            await sync_service.close()

        db.get_profile_stats.assert_awaited_once_with("Canonical_Name")
        self.assertEqual(db.is_profile_library_subscribed.await_count, 1)
        api.get_user_library.assert_awaited_once_with("Canonical_Name", force_refresh=True)
        embed = interaction.followup.send.call_args.kwargs["embed"]
        view = interaction.followup.send.call_args.kwargs["view"]
        self.assertEqual(embed.url, "https://www.backloggd.com/u/Canonical_Name/")
        self.assertEqual(view.children[0].url, "https://www.backloggd.com/u/Canonical_Name/")
        stats_field = next(f for f in embed.fields if "Average Rating" in f.name)
        self.assertIn("4.25", stats_field.value)
        self.assertIn("3", stats_field.value)

    async def test_shared_profile_canonical_stats_and_relink_guards(self):
        import tempfile
        from pathlib import Path
        from bot.db import Database

        with tempfile.TemporaryDirectory() as tmpdir:
            db = Database(str(Path(tmpdir) / "profile_canon.db"), turso_url="")
            await db.connect()
            try:
                await db.link_user(10, "  User_One.1  ")
                await db.link_user(20, "  user_one.1  ")
                await db.replace_user_library(
                    10,
                    "User_One.1",
                    UserLibrary(
                        username="User_One.1",
                        games={
                            "hades": GameEntry(slug="hades", title="Hades", rating=4.5, status=GameStatus.PLAYED),
                        },
                    ),
                )

                # Shared connections exclude caller safely
                conns = await db.get_profile_connections("USER_ONE.1", [10, 20, 30], exclude_discord_id=10)
                self.assertEqual([row["discord_id"] for row in conns], [20])

                # Same-profile relink notice preserves nonexclusive link and safe escaping/URL
                account_cog = AccountCog(SimpleNamespace(db=db, backloggd=SimpleNamespace()))
                i_same = SimpleNamespace(
                    response=SimpleNamespace(defer=AsyncMock()),
                    followup=SimpleNamespace(send=AsyncMock()),
                    user=SimpleNamespace(id=10),
                    guild=None,
                )
                await AccountCog.link.callback(account_cog, i_same, "  user_one.1 ")
                same_msg = i_same.followup.send.call_args.args[0]
                self.assertIn("User\\_One.1", same_msg)
                self.assertIn("https://www.backloggd.com/u/User_One.1/", same_msg)

                # Different-profile relink guard refuses overwrite without /unlink
                i_diff = SimpleNamespace(
                    response=SimpleNamespace(defer=AsyncMock()),
                    followup=SimpleNamespace(send=AsyncMock()),
                    user=SimpleNamespace(id=10),
                    guild=None,
                )
                await AccountCog.link.callback(account_cog, i_diff, "OtherUser")
                diff_msg = i_diff.followup.send.call_args.args[0]
                self.assertIn("/unlink", diff_msg)
                self.assertEqual((await db.get_linked_user(10))["profile_key"], "user_one.1")

                # /profile queried with lowercase spelling uses canonical SDK username & 1 sub check
                profile = SimpleNamespace(
                    username="User_One.1", avatar_url=None, bio=None, badges=[], favorite_games=[],
                    played_count=1, playing_count=0, backlog_count=0, wishlist_count=0,
                )
                api = SimpleNamespace(
                    get_user_profile=AsyncMock(return_value=profile),
                    get_user_journal=AsyncMock(return_value=[]),
                    get_user_reviews=AsyncMock(return_value=[]),
                )
                orig_sub = db.is_profile_library_subscribed
                sub_calls = 0

                async def counted_sub(name: str):
                    nonlocal sub_calls
                    sub_calls += 1
                    return await orig_sub(name)

                db.is_profile_library_subscribed = counted_sub
                users_cog = UsersCog(SimpleNamespace(db=db, backloggd=api))
                i_prof = SimpleNamespace(
                    response=SimpleNamespace(defer=AsyncMock()),
                    followup=SimpleNamespace(send=AsyncMock()),
                    user=SimpleNamespace(id=99, display_avatar=None),
                    guild=None,
                )
                await UsersCog.profile.callback(users_cog, i_prof, "user_one.1")
                self.assertEqual(sub_calls, 1)
                embed = i_prof.followup.send.call_args.kwargs["embed"]
                self.assertEqual(embed.url, "https://www.backloggd.com/u/User_One.1/")
                self.assertIn("4.5", "\n".join(f.value for f in embed.fields))
            finally:
                await db.close()


if __name__ == "__main__":
    unittest.main()
