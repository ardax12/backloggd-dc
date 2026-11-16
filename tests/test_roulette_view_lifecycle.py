"""Regression tests for Task 4: roulette view retirement, reroll serialization, and timeout safety."""

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
from unittest.mock import MagicMock, patch

import discord

from bot.cogs.discovery import DiscoveryCog
from bot.db import Database
from tests.test_interaction_visibility_protocol import (
    StatefulDiscordInteraction,
    _FakeHTTPResp,
)


def _component_interaction(parent_message: dict, **kwargs) -> StatefulDiscordInteraction:
    return StatefulDiscordInteraction(
        interaction_type=discord.InteractionType.component,
        source_message=parent_message,
        **kwargs,
    )


def _reroll_button(view: discord.ui.View) -> discord.ui.Button:
    for item in view.children:
        if isinstance(item, discord.ui.Button) and (item.label or "").startswith("Reroll"):
            return item
    raise AssertionError("Reroll button not found")


def _url_button(view: discord.ui.View) -> discord.ui.Button:
    for item in view.children:
        if isinstance(item, discord.ui.Button) and getattr(item, "url", None):
            return item
    raise AssertionError("URL button not found")


class RouletteViewLifecycleTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmpdir.cleanup)
        self.db = Database(str(Path(self.tmpdir.name) / "roulette.db"), turso_url="")
        await self.db.connect()
        self.addAsyncCleanup(self.db.close)
        await self.db.link_user(10, "Alpha", None)
        await self.db.replace_user_library(
            10,
            "Alpha",
            [
                SimpleNamespace(
                    slug="game-a",
                    title="Game A",
                    status="backlog",
                    rating=None,
                    cover_url=None,
                    release_year=2020,
                ),
                SimpleNamespace(
                    slug="game-b",
                    title="Game B",
                    status="backlog",
                    rating=None,
                    cover_url=None,
                    release_year=2021,
                ),
                SimpleNamespace(
                    slug="game-c",
                    title="Game C",
                    status="wishlist",
                    rating=None,
                    cover_url=None,
                    release_year=2022,
                ),
            ],
        )
        self.guild = SimpleNamespace(
            id=1,
            name="TestGuild",
            chunked=True,
            member_count=2,
            members=[
                SimpleNamespace(id=10, display_name="Alpha"),
                SimpleNamespace(id=99, display_name="Intruder"),
            ],
        )
        self.cog = DiscoveryCog(
            SimpleNamespace(db=self.db, backloggd=MagicMock()),
            clock=lambda: datetime(2026, 11, 13, tzinfo=timezone.utc),
        )

    @staticmethod
    def _pick_sequence(slugs: list[str]):
        it = iter(slugs)
        def chooser(items):
            target = next(it)
            for row in items:
                if row["slug"] == target:
                    return row
            return items[0]
        return chooser

    async def test_successive_rerolls_retire_old_views_and_ignore_stale_timeouts(self):
        with patch("bot.cogs.discovery.random.choice", side_effect=self._pick_sequence(["game-a", "game-b", "game-c"])):
            slash = StatefulDiscordInteraction(name="roulette", guild=self.guild, user_id=10, display_name="Alpha")
            await DiscoveryCog.roulette.callback(self.cog, slash, None)
            parent = slash.visible_public_messages()[0]
            view_a = parent["view"]
            self.addCleanup(view_a.stop)

            click_b = _component_interaction(parent, guild=self.guild, user_id=10, display_name="Alpha")
            await view_a._scheduled_task(_reroll_button(view_a), click_b)
            self.assertEqual(click_b.response.type, discord.InteractionResponseType.deferred_message_update)
            view_b = parent["view"]
            self.addCleanup(view_b.stop)
            self.assertIsNot(view_b, view_a)
            self.assertTrue(view_a.is_finished())
            self.assertIn("game-b", _url_button(view_b).url)

            click_c = _component_interaction(parent, guild=self.guild, user_id=10, display_name="Alpha")
            await view_b._scheduled_task(_reroll_button(view_b), click_c)
            view_c = parent["view"]
            self.addCleanup(view_c.stop)
            self.assertIsNot(view_c, view_b)
            self.assertTrue(view_b.is_finished())
            self.assertIn("game-c", _url_button(view_c).url)

            # Manually trigger stale timeouts for view_a and view_b -> must NOT overwrite view_c!
            await view_a.on_timeout()
            await view_b.on_timeout()
            self.assertIs(parent["view"], view_c)
            self.assertIn("game-c", _url_button(parent["view"]).url)
            self.assertFalse(_reroll_button(parent["view"]).disabled)

            # Unauthorized click on view_c keeps view_c intact and replies ephemerally.
            unauth = _component_interaction(parent, guild=self.guild, user_id=99, display_name="Intruder")
            await view_c._scheduled_task(_reroll_button(view_c), unauth)
            self.assertEqual(len(unauth.visible_ephemeral_messages()), 1)
            self.assertIs(parent["view"], view_c)
            self.assertFalse(_reroll_button(view_c).disabled)

            # Only current view_c timeout disables current controls (keeping link enabled).
            await view_c.on_timeout()
            self.assertIs(parent["view"], view_c)
            self.assertTrue(_reroll_button(view_c).disabled)
            self.assertFalse(_url_button(view_c).disabled)

    async def test_concurrent_rerolls_serialize_and_prevent_stale_second_replacement(self):
        with patch("bot.cogs.discovery.random.choice", side_effect=self._pick_sequence(["game-a", "game-b", "game-c"])):
            slash = StatefulDiscordInteraction(name="roulette", guild=self.guild, user_id=10, display_name="Alpha")
            await DiscoveryCog.roulette.callback(self.cog, slash, None)
            parent = slash.visible_public_messages()[0]
            view_a = parent["view"]
            self.addCleanup(view_a.stop)

            edit1_started = asyncio.Event()
            release_edit1 = asyncio.Event()

            click1 = _component_interaction(parent, guild=self.guild, user_id=10, display_name="Alpha")
            orig_edit1 = click1.edit_original_response

            async def slow_edit1(**kwargs):
                edit1_started.set()
                await release_edit1.wait()
                return await orig_edit1(**kwargs)

            click1.edit_original_response = slow_edit1
            click2 = _component_interaction(parent, guild=self.guild, user_id=10, display_name="Alpha")

            t1 = asyncio.create_task(view_a._scheduled_task(_reroll_button(view_a), click1))
            await asyncio.wait_for(edit1_started.wait(), timeout=2.0)

            t2 = asyncio.create_task(view_a._scheduled_task(_reroll_button(view_a), click2))
            await asyncio.sleep(0)
            # Second click should have deferred immediately before waiting on reroll lock.
            self.assertTrue(click2.responded)
            self.assertEqual(click2.response.type, discord.InteractionResponseType.deferred_message_update)

            release_edit1.set()
            await asyncio.gather(t1, t2)

            self.addCleanup(parent["view"].stop)
            # Only view_b (from click1) should be active; click2 was for superseded view_a.
            self.assertTrue(view_a.is_finished())
            self.assertIn("game-b", _url_button(parent["view"]).url)
            self.assertIn("Game B", parent["embed"].description)

    async def test_failed_and_cancelled_reroll_stops_new_view_and_preserves_old_view(self):
        created_views = []
        from bot.ui.views import RouletteView as _RealRouletteView

        def spy_view(*args, **kwargs):
            v = _RealRouletteView(*args, **kwargs)
            created_views.append(v)
            self.addCleanup(v.stop)
            return v

        with (
            patch("bot.cogs.discovery.random.choice", side_effect=self._pick_sequence(["game-a", "game-b", "game-c", "game-b"])),
            patch("bot.cogs.discovery.RouletteView", side_effect=spy_view),
        ):
            slash = StatefulDiscordInteraction(name="roulette", guild=self.guild, user_id=10, display_name="Alpha")
            await DiscoveryCog.roulette.callback(self.cog, slash, None)
            parent = slash.visible_public_messages()[0]
            view_a = parent["view"]

            # 1) HTTP failure during edit stops newly constructed view_b and leaves view_a usable.
            edit1_started = asyncio.Event()
            release_edit1 = asyncio.Event()
            fail_click = _component_interaction(parent, guild=self.guild, user_id=10, display_name="Alpha")

            async def failing_edit(**kwargs):
                edit1_started.set()
                await release_edit1.wait()
                raise discord.HTTPException(_FakeHTTPResp(500), "edit failed")

            fail_click.edit_original_response = failing_edit
            retry_click = _component_interaction(parent, guild=self.guild, user_id=10, display_name="Alpha")

            with self.assertLogs("backloggd.ui.views", level="WARNING"):
                t_fail = asyncio.create_task(view_a._scheduled_task(_reroll_button(view_a), fail_click))
                await asyncio.wait_for(edit1_started.wait(), timeout=2.0)
                t_retry = asyncio.create_task(view_a._scheduled_task(_reroll_button(view_a), retry_click))
                await asyncio.sleep(0)
                release_edit1.set()
                await asyncio.gather(t_fail, t_retry)

            # First replacement failed -> its newly created view was stopped; old view stayed usable so retry succeeded!
            self.assertEqual(len(created_views), 3)
            failed_view = created_views[1]
            succeeded_view = created_views[2]
            self.assertTrue(failed_view.is_finished())
            self.assertIsNone(failed_view.message)
            self.assertEqual(len(fail_click.visible_ephemeral_messages()), 1)
            self.assertIs(parent["view"], succeeded_view)
            self.assertTrue(view_a.is_finished())

            # 2) Cancellation during edit stops new candidate view and propagates CancelledError.
            cancel_click = _component_interaction(parent, guild=self.guild, user_id=10, display_name="Alpha")

            async def cancelled_edit(**kwargs):
                raise asyncio.CancelledError()

            cancel_click.edit_original_response = cancelled_edit
            with self.assertRaises(asyncio.CancelledError):
                await succeeded_view._scheduled_task(_reroll_button(succeeded_view), cancel_click)

            self.assertEqual(len(created_views), 4)
            cancelled_new_view = created_views[3]
            self.assertTrue(cancelled_new_view.is_finished())
            self.assertIsNone(cancelled_new_view.message)
            self.assertIs(parent["view"], succeeded_view)
            self.assertFalse(succeeded_view.is_finished())


if __name__ == "__main__":
    unittest.main()
