import os
os.environ["PYTHON_DOTENV_DISABLED"] = "1"

import asyncio
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

from bot.cogs.stats import StatsCog
from bot.db import Database


class ServerPlayedTotalTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.db = Database(str(Path(self.directory.name) / "stats.db"), turso_url="")
        await self.db.connect()
        self.api = SimpleNamespace(get_user_profile=AsyncMock())
        self.cog = StatsCog(SimpleNamespace(db=self.db, backloggd=self.api))

    async def asyncTearDown(self):
        await self.db.close()
        self.directory.cleanup()

    async def stats(self, member_ids):
        interaction = SimpleNamespace(
            guild=SimpleNamespace(name="Server", members=[SimpleNamespace(id=i) for i in member_ids]),
            response=SimpleNamespace(defer=AsyncMock()),
            followup=SimpleNamespace(send=AsyncMock()),
        )
        await StatsCog.server_stats.callback(self.cog, interaction)
        return interaction.followup.send.call_args.kwargs["embed"]

    @staticmethod
    def logged_field(embed):
        return next(field.value for field in embed.fields if "Games Logged" in field.name)

    async def test_sums_played_tabs_once_per_present_profile_without_using_game_statuses(self):
        await self.db.link_user(10, "PlayerA")
        await self.db.link_user(11, "playera")
        await self.db.link_user(20, "PlayerB")
        await self.db.link_user(30, "AbsentPlayer")
        await self.db.add_guild_admin_follow(1, "FeedOnly", None, 99)
        for slug, status in (("unknown-game", "unknown"), ("wishlist-game", "wishlist")):
            await self.db.upsert_game(slug, slug)
            await self.db.upsert_user_rating(10, slug, None, status)
        counts = {"playera": 100, "playerb": 200}
        self.api.get_user_profile.side_effect = lambda name: SimpleNamespace(played_count=counts[name.lower()])

        embed = await self.stats([10, 11, 20, 99])

        self.assertIn("**300**", self.logged_field(embed))
        self.assertEqual(self.api.get_user_profile.await_count, 2)
        self.assertEqual({call.args[0].lower() for call in self.api.get_user_profile.await_args_list}, set(counts))
        self.assertFalse(any("Completed" in field.name for field in embed.fields))
        self.assertEqual(len(embed.fields), 5)

    async def test_unavailable_profile_counts_are_disclosed_as_a_partial_total(self):
        await self.db.link_user(10, "Available")
        await self.db.link_user(20, "Unavailable")

        def profile(name):
            if name == "Unavailable":
                raise RuntimeError("temporary upstream failure")
            return SimpleNamespace(played_count=12)

        self.api.get_user_profile.side_effect = profile
        embed = await self.stats([10, 20])
        value = self.logged_field(embed)
        self.assertIn("At least **12**", value)
        self.assertIn("1 profile count unavailable", value)

    async def test_missing_or_invalid_counts_never_appear_as_a_verified_zero(self):
        await self.db.link_user(10, "Player")
        for result in (None, SimpleNamespace(played_count=None), SimpleNamespace(played_count=-1), SimpleNamespace(played_count=True)):
            with self.subTest(result=result):
                self.api.get_user_profile.return_value = result
                value = self.logged_field(await self.stats([10]))
                self.assertIn("Unavailable", value)
                self.assertNotIn("**0**", value)

    async def test_true_zero_and_empty_server_are_valid_zero_totals(self):
        await self.db.link_user(10, "Player")
        self.api.get_user_profile.return_value = SimpleNamespace(played_count=0)
        self.assertIn("**0**", self.logged_field(await self.stats([10])))
        self.api.get_user_profile.reset_mock()
        self.assertIn("**0**", self.logged_field(await self.stats([])))
        self.api.get_user_profile.assert_not_awaited()

    async def test_profile_requests_are_bounded(self):
        members = list(range(10, 16))
        for member_id in members:
            await self.db.link_user(member_id, f"Player{member_id}")
        active, peak = 0, 0

        async def profile(name):
            nonlocal active, peak
            active += 1
            peak = max(peak, active)
            await asyncio.sleep(0)
            active -= 1
            return SimpleNamespace(played_count=1)

        self.api.get_user_profile.side_effect = profile
        embed = await self.stats(members)
        self.assertIn("**6**", self.logged_field(embed))
        self.assertEqual(peak, 2)
