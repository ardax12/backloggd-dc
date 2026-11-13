"""Regression tests for Task 9: username validation, safe escaping, and bounded coalesced live compares."""

import asyncio
import os
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

os.environ["PYTHON_DOTENV_DISABLED"] = "1"
os.environ["TURSO_DATABASE_URL"] = ""
os.environ["TURSO_AUTH_TOKEN"] = ""
os.environ["DEV_GUILD_ID"] = ""

from backloggd.exceptions import UserNotFoundError
from backloggd.models import GameEntry, GameStatus, UserLibrary

from bot.cogs.account import AccountCog
from bot.cogs.feed import FeedCog
from bot.cogs.social import SocialCog
from bot.cogs.users import UsersCog
from bot.db import Database
from bot.ui.helpers import build_backloggd_game_url, build_backloggd_user_url


def _make_interaction(user_id: int = 10, guild=None):
    return SimpleNamespace(
        user=SimpleNamespace(id=user_id, display_name=f"User{user_id}", display_avatar=None),
        guild=guild,
        guild_id=getattr(guild, "id", None),
        response=SimpleNamespace(defer=AsyncMock(), is_done=lambda: True),
        followup=SimpleNamespace(send=AsyncMock(return_value=SimpleNamespace(edit=AsyncMock()))),
    )


class UsernameValidationAndEscapingTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmpdir.cleanup)
        self.db = Database(str(Path(self.tmpdir.name) / "validation.db"), turso_url="")
        self.addAsyncCleanup(self.db.close)
        await self.db.connect()

    async def test_invalid_inputs_make_zero_sdk_calls_across_account_users_social_feed(self):
        api = SimpleNamespace(
            get_user_profile=AsyncMock(side_effect=AssertionError("SDK must not be called")),
            get_user_library=AsyncMock(side_effect=AssertionError("SDK must not be called")),
            get_user_journal=AsyncMock(side_effect=AssertionError("SDK must not be called")),
            get_user_reviews=AsyncMock(side_effect=AssertionError("SDK must not be called")),
        )
        bot = SimpleNamespace(db=self.db, backloggd=api)
        account_cog = AccountCog(bot)
        users_cog = UsersCog(bot)
        social_cog = SocialCog(bot)
        feed_cog = FeedCog(bot)
        guild = SimpleNamespace(id=1, name="Guild")

        invalid_inputs = [
            "   ",
            "https://evil.example/u/Millers/",
            "https://www.backloggd.com/u/Millers/games",
            "https://www.backloggd.com/u/Millers/?tab=played#top",
            "../admin",
            "Millers/reviews",
            "Millers?foo=1",
            "Millers\n@everyone",
            "[click](https://evil.example)",
            "`Millers`",
            "x" * 101,
        ]
        for bad in invalid_inputs:
            with self.subTest(bad=bad):
                await AccountCog.link.callback(account_cog, _make_interaction(), bad)
                await UsersCog.profile.callback(users_cog, _make_interaction(), bad)
                await SocialCog.compare.callback(social_cog, _make_interaction(), bad, "ValidOther")
                await SocialCog.taste_twins.callback(social_cog, _make_interaction(guild=guild), bad)
                await FeedCog.feed_add.callback(feed_cog, _make_interaction(guild=guild), bad)
                await FeedCog.feed_remove.callback(feed_cog, _make_interaction(guild=guild), bad)

        api.get_user_profile.assert_not_awaited()
        api.get_user_library.assert_not_awaited()
        api.get_user_journal.assert_not_awaited()
        api.get_user_reviews.assert_not_awaited()

    async def test_valid_at_prefix_profile_url_and_discord_mention_work_and_encode_urls(self):
        profile = SimpleNamespace(
            username="user_name.1",
            avatar_url=None,
            bio=None,
            badges=[],
            favorite_games=[],
            played_count=1,
            playing_count=0,
            backlog_count=0,
            wishlist_count=0,
        )
        api = SimpleNamespace(
            get_user_profile=AsyncMock(return_value=profile),
            get_user_journal=AsyncMock(return_value=[]),
            get_user_reviews=AsyncMock(return_value=[]),
        )
        bot = SimpleNamespace(db=self.db, backloggd=api)
        users_cog = UsersCog(bot)
        account_cog = AccountCog(bot)
        account_cog._start_sync_task = lambda *a, **kw: None

        # @name input passes normalized "user_name.1" without "@"
        i_at = _make_interaction(user_id=10)
        await AccountCog.link.callback(account_cog, i_at, " @user_name.1 ")
        api.get_user_profile.assert_awaited_once_with("user_name.1")

        # Valid Backloggd URL extracts exact username
        api.get_user_profile.reset_mock()
        i_url = _make_interaction(user_id=20)
        await UsersCog.profile.callback(users_cog, i_url, "https://www.backloggd.com/u/user_name.1/")
        api.get_user_profile.assert_awaited_once_with("user_name.1")

        # URL helper encodes path segments safely
        self.assertEqual(
            build_backloggd_user_url("user name?x=1#y"),
            "https://www.backloggd.com/u/user%20name%3Fx%3D1%23y/",
        )
        self.assertEqual(
            build_backloggd_game_url("game/slug?bad=1"),
            "https://www.backloggd.com/games/game%2Fslug%3Fbad%3D1/",
        )

    async def test_typed_user_not_found_errors_report_missing_profile_clearly(self):
        api = SimpleNamespace(
            get_user_profile=AsyncMock(side_effect=UserNotFoundError("404")),
            get_user_journal=AsyncMock(side_effect=UserNotFoundError("404")),
            get_user_reviews=AsyncMock(side_effect=UserNotFoundError("404")),
            get_user_library=AsyncMock(side_effect=UserNotFoundError("404")),
        )
        bot = SimpleNamespace(db=self.db, backloggd=api)
        users_cog = UsersCog(bot)
        social_cog = SocialCog(bot)

        i_profile = _make_interaction()
        await UsersCog.profile.callback(users_cog, i_profile, "MissingUser")
        msg = i_profile.followup.send.call_args.args[0]
        self.assertIn("could not be found", msg.lower())

        i_compare = _make_interaction()
        lib_other = UserLibrary(
            username="Other",
            games={"hades": GameEntry(slug="hades", title="Hades", rating=4.5, status=GameStatus.PLAYED)},
        )
        api.get_user_library.side_effect = [UserNotFoundError("404"), lib_other]
        await SocialCog.compare.callback(social_cog, i_compare, "MissingUser", "Other")
        comp_msg = i_compare.followup.send.call_args.args[0]
        self.assertIn("could not be found", comp_msg.lower())

    async def test_discord_mention_resolution_and_escaping_across_commands(self):
        await self.db.link_user(77, "alpha_name")
        await self.db.link_user(88, "beta_name")
        await self.db.replace_user_library(
            77,
            "alpha_name",
            UserLibrary(
                username="alpha_name",
                games={"hades": GameEntry(slug="hades", title="Hades", rating=5.0, status=GameStatus.PLAYED)},
            ),
        )
        await self.db.replace_user_library(
            88,
            "beta_name",
            UserLibrary(
                username="beta_name",
                games={"hades": GameEntry(slug="hades", title="Hades", rating=4.0, status=GameStatus.PLAYED)},
            ),
        )

        profile = SimpleNamespace(
            username="alpha_name",
            avatar_url=None,
            bio=None,
            badges=[],
            favorite_games=[],
            played_count=1,
            playing_count=0,
            backlog_count=0,
            wishlist_count=0,
        )
        api = SimpleNamespace(
            get_user_profile=AsyncMock(return_value=profile),
            get_user_journal=AsyncMock(return_value=[]),
            get_user_reviews=AsyncMock(return_value=[]),
            get_user_library=AsyncMock(side_effect=AssertionError("Cached fresh library should not trigger live fetch")),
        )
        bot = SimpleNamespace(db=self.db, backloggd=api)
        users_cog = UsersCog(bot)
        social_cog = SocialCog(bot)

        # Discord mention resolves to linked Backloggd user in /profile
        i_prof = _make_interaction(user_id=10)
        await UsersCog.profile.callback(users_cog, i_prof, "<@!77>")
        api.get_user_profile.assert_awaited_once_with("alpha_name")

        # Discord mentions resolve in /compare and escaped names appear in embed
        i_cmp = _make_interaction(user_id=77)
        await SocialCog.compare.callback(social_cog, i_cmp, "<@88>")
        sent_embed = i_cmp.followup.send.call_args.kwargs["embed"]
        self.assertIn("alpha\\_name", sent_embed.fields[0].value)
        self.assertIn("beta\\_name", sent_embed.fields[0].value)


class CoalescedBoundedLiveCompareTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmpdir.cleanup)
        self.db = Database(str(Path(self.tmpdir.name) / "compare.db"), turso_url="")
        self.addAsyncCleanup(self.db.close)
        await self.db.connect()

    async def test_duplicate_live_fetches_coalesce_and_large_library_completes_without_persisting_cache(self):
        started = asyncio.Event()
        release = asyncio.Event()
        calls = []

        large_games = {
            f"game-{i}": GameEntry(
                slug=f"game-{i}",
                title=f"Game {i}",
                rating=4.5 if i < 200 else 3.0,
                status=GameStatus.PLAYED,
            )
            for i in range(1200)
        }

        async def fetch_library(username: str):
            calls.append(username.lower())
            if username.lower() == "alpha":
                started.set()
                await release.wait()
                return UserLibrary(username="Alpha", games=large_games)
            return UserLibrary(username="Beta", games={k: large_games[k] for k in list(large_games)[:50]})

        api = SimpleNamespace(get_user_library=AsyncMock(side_effect=fetch_library))
        cog = SocialCog(SimpleNamespace(db=self.db, backloggd=api), live_fetch_timeout=2.0)

        t1 = asyncio.create_task(cog._get_or_fetch_ratings("Alpha"))
        await asyncio.wait_for(started.wait(), timeout=0.5)
        t2 = asyncio.create_task(cog._get_or_fetch_ratings("alpha"))
        for _ in range(50):
            if cog._live_library_waiters.get("alpha") == 2:
                break
            await asyncio.sleep(0.01)
        self.assertEqual(cog._live_library_waiters.get("alpha"), 2)
        release.set()

        r1, r2 = await asyncio.gather(t1, t2)
        self.assertEqual(len(r1), 1200)
        self.assertEqual(r1, r2)
        # Coalesced: only one upstream get_user_library("Alpha") call for both callers!
        self.assertEqual(calls.count("alpha"), 1)
        # No partial/live-fetched subscribed cache written to DB!
        self.assertEqual(await self.db.get_profile_ratings_dict("alpha"), {})
        self.assertEqual(cog._live_library_tasks, {})

    async def test_live_fetch_concurrency_limit_and_timeout_cleanup(self):
        active = 0
        peak_active = 0
        block = asyncio.Event()

        async def gated_fetch(username: str):
            nonlocal active, peak_active
            active += 1
            peak_active = max(peak_active, active)
            try:
                if username.lower() == "slow":
                    await block.wait()
                else:
                    await asyncio.sleep(0.02)
                return UserLibrary(
                    username=username,
                    games={"hades": GameEntry(slug="hades", title="Hades", rating=4.0, status=GameStatus.PLAYED)},
                )
            finally:
                active -= 1

        api = SimpleNamespace(get_user_library=AsyncMock(side_effect=gated_fetch))
        cog = SocialCog(
            SimpleNamespace(db=self.db, backloggd=api),
            live_fetch_concurrency=2,
            live_fetch_timeout=0.08,
        )

        # 4 distinct profiles simultaneously -> concurrency stays <= 2.
        results = await asyncio.gather(
            cog._get_or_fetch_ratings("u1"),
            cog._get_or_fetch_ratings("u2"),
            cog._get_or_fetch_ratings("u3"),
            cog._get_or_fetch_ratings("u4"),
        )
        self.assertEqual(len(results), 4)
        self.assertLessEqual(peak_active, 2)
        self.assertEqual(cog._live_library_tasks, {})

        # Slow fetch times out, /compare returns retryable message, and slot/task bookkeeping cleans up.
        interaction = _make_interaction()
        await SocialCog.compare.callback(cog, interaction, "slow", "u1")
        msg = interaction.followup.send.call_args.args[0]
        self.assertIn("timed out", msg.lower())
        self.assertIn("try again", msg.lower())
        block.set()
        await asyncio.sleep(0.02)
        self.assertEqual(cog._live_library_tasks, {})

        # Cancellation while blocked also releases slot cleanly.
        block.clear()
        task = asyncio.create_task(cog._get_or_fetch_ratings("slow"))
        await asyncio.sleep(0.01)
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task
        await asyncio.sleep(0.01)
        self.assertEqual(cog._live_library_tasks, {})
        # Semaphore slots remain usable afterwards.
        ok = await cog._get_or_fetch_ratings("u5")
        self.assertEqual(ok, {"hades": 4.0})


if __name__ == "__main__":
    unittest.main()
