"""Regression tests for Task 2 (shared View error boundary) and Task 3 (pagination state consistency)."""

import os

os.environ["PYTHON_DOTENV_DISABLED"] = "1"
os.environ["TURSO_DATABASE_URL"] = ""
os.environ["TURSO_AUTH_TOKEN"] = ""
os.environ["DEV_GUILD_ID"] = ""

import asyncio
from datetime import datetime, timezone
import logging
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

import discord

from bot.cogs.discovery import DiscoveryCog
from bot.cogs.feed import FeedCog
from bot.cogs.games import GamesCog
from bot.cogs.social import SocialCog
from bot.db import Database
from bot.ui.views import (
    ExpiringView,
    FeedListView,
    GameCardView,
    LinkButtonView,
    RatingsView,
    RouletteView,
    TasteTwinsView,
)
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


def _find_button(view: discord.ui.View, label_prefix: str) -> discord.ui.Button:
    for item in view.children:
        if isinstance(item, discord.ui.Button) and (item.label or "").startswith(label_prefix):
            return item
    raise AssertionError(f"Button starting with {label_prefix!r} not found in {view.children!r}")


class ViewCallbackBoundaryTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmpdir.cleanup)
        self.db = Database(str(Path(self.tmpdir.name) / "views.db"), turso_url="")
        await self.db.connect()
        self.addAsyncCleanup(self.db.close)
        self.guild = SimpleNamespace(
            id=1,
            name="TestGuild",
            chunked=True,
            member_count=2,
            members=[
                SimpleNamespace(id=10, display_name="Alpha"),
                SimpleNamespace(id=20, display_name="Beta"),
            ],
        )

    def test_view_inheritance_scope(self):
        self.assertTrue(issubclass(RouletteView, ExpiringView))
        self.assertTrue(issubclass(TasteTwinsView, ExpiringView))
        self.assertTrue(issubclass(GameCardView, ExpiringView))
        self.assertTrue(issubclass(RatingsView, ExpiringView))
        self.assertTrue(issubclass(FeedListView, ExpiringView))
        self.assertFalse(issubclass(LinkButtonView, ExpiringView))

    async def test_roulette_reroll_dispatch_success_render_failure_and_unauthorized_http_failure(self):
        await self.db.link_user(10, "Alpha", None)
        await self.db.replace_user_library(
            10,
            "Alpha",
            [
                SimpleNamespace(
                    slug="hades",
                    title="Hades",
                    status="backlog",
                    rating=None,
                    cover_url=None,
                    release_year=2020,
                )
            ],
        )
        bot = SimpleNamespace(db=self.db, backloggd=MagicMock())
        cog = DiscoveryCog(bot, clock=lambda: datetime(2026, 11, 13, tzinfo=timezone.utc))

        slash = StatefulDiscordInteraction(name="roulette", guild=self.guild, user_id=10, display_name="Alpha")
        await DiscoveryCog.roulette.callback(cog, slash, None)
        parent = slash.visible_public_messages()[0]
        view = parent["view"]
        self.addCleanup(view.stop)
        reroll_btn = _find_button(view, "Reroll")

        # 1. Success path via actual View._scheduled_task dispatch
        ok_click = _component_interaction(parent, guild=self.guild, user_id=10, display_name="Alpha")
        await view._scheduled_task(reroll_btn, ok_click)
        self.assertEqual(parent["embed"].title, slash.visible_public_messages()[0]["embed"].title)
        new_view = parent["view"]
        self.addCleanup(new_view.stop)

        # 2. Render failure caught by shared View boundary -> logged once, safe private reply, parent preserved
        snapshot_before = dict(parent)
        reroll_btn2 = _find_button(new_view, "Reroll")
        fail_click = _component_interaction(parent, guild=self.guild, user_id=10, display_name="Alpha")
        with patch(
            "bot.cogs.discovery.create_roulette_embed",
            side_effect=RuntimeError("SELECT * FROM users WHERE auth_token='secret_roulette_tok'"),
        ):
            with self.assertLogs("backloggd.ui.views", level="ERROR") as logs:
                await new_view._scheduled_task(reroll_btn2, fail_click)

        self.assertEqual(len(logs.records), 1)
        self.assertEqual(parent, snapshot_before)
        ephem = fail_click.visible_ephemeral_messages()
        self.assertEqual(len(ephem), 1)
        self.assertNotIn("SELECT", ephem[0]["content"])
        self.assertNotIn("secret_roulette_tok", ephem[0]["content"])
        self.assertNotIn("RuntimeError", ephem[0]["content"])
        self.assertEqual(fail_click.visible_public_messages(), [])

        # 3. Unauthorized click where response.send_message raises HTTPException -> handled cleanly without recursion
        unauth_click = _component_interaction(parent, guild=self.guild, user_id=99, display_name="Intruder")
        send_attempts = 0
        orig_send = unauth_click.response.send_message

        async def flaky_send(*args, **kwargs):
            nonlocal send_attempts
            send_attempts += 1
            if send_attempts == 1:
                raise discord.HTTPException(_FakeHTTPResp(502), "bad gateway")
            return await orig_send(*args, **kwargs)

        unauth_click.response.send_message = flaky_send
        with self.assertLogs("backloggd.ui.views", level="WARNING") as warn_logs:
            await new_view._scheduled_task(reroll_btn2, unauth_click)
        self.assertEqual(len(warn_logs.records), 1)
        self.assertEqual(send_attempts, 2)
        self.assertEqual(len(unauth_click.visible_ephemeral_messages()), 1)

        # 4. Expired interaction during error reporting does not raise or recurse
        expired_click = _component_interaction(
            parent,
            guild=self.guild,
            user_id=99,
            display_name="Intruder",
            send_message_error=discord.NotFound(_FakeHTTPResp(404), "Unknown Interaction"),
            followup_send_error=discord.NotFound(_FakeHTTPResp(404), "Unknown Webhook"),
        )
        with self.assertLogs("backloggd.ui.views", level="WARNING"):
            await new_view._scheduled_task(reroll_btn2, expired_click)
        self.assertEqual(expired_click.visible_ephemeral_messages(), [])

    async def test_game_card_view_all_dispatch_success_failure_and_deferred_update_preservation(self):
        await self.db.upsert_game("hades", "Hades", None, 2020, 4.6)
        members = []
        for idx in range(1, 16):
            await self.db.link_user(idx, f"Player{idx}", None)
            await self.db.upsert_user_rating(idx, "hades", 4.5, "played")
            members.append(SimpleNamespace(id=idx, display_name=f"Player{idx}"))
        big_guild = SimpleNamespace(
            id=1, name="TestGuild", chunked=True, member_count=len(members), members=members
        )
        game_obj = SimpleNamespace(
            slug="hades", title="Hades", release_year=2020, community_rating=4.6,
            cover_url=None, summary="Escape from hell.",
            stats=SimpleNamespace(plays_count=100, backlogs_count=50, wishlists_count=20),
        )
        bot = SimpleNamespace(
            db=self.db,
            backloggd=SimpleNamespace(
                search_games=AsyncMock(return_value=[game_obj]),
                get_game=AsyncMock(return_value=game_obj),
            ),
        )
        cog = GamesCog(bot, clock=lambda: datetime(2026, 11, 13, tzinfo=timezone.utc))

        slash = StatefulDiscordInteraction(name="game", guild=big_guild, user_id=1, display_name="Player1")
        await GamesCog.game.callback(cog, slash, "slug:hades")
        parent = slash.visible_public_messages()[0]
        view = parent["view"]
        self.addCleanup(view.stop)
        view_all_btn = _find_button(view, "View All Ratings")

        # 1. Success via View._scheduled_task
        ok_click = _component_interaction(parent, guild=big_guild, user_id=1)
        await view._scheduled_task(view_all_btn, ok_click)
        ephem = ok_click.visible_ephemeral_messages()
        self.assertEqual(len(ephem), 1)
        self.assertIsInstance(ephem[0]["view"], RatingsView)
        self.addCleanup(ephem[0]["view"].stop)

        # 2. Render failure in on_view_all -> caught cleanly by ExpiringView.on_error
        snapshot_before = dict(parent)
        fail_click = _component_interaction(parent, guild=big_guild, user_id=1)
        with patch(
            "bot.ui.embeds.create_all_ratings_embed",
            side_effect=RuntimeError("render boom tok_secret_game"),
        ):
            with self.assertLogs("backloggd.ui.views", level="ERROR") as logs:
                await view._scheduled_task(view_all_btn, fail_click)
        self.assertEqual(len(logs.records), 1)
        self.assertEqual(parent, snapshot_before)
        err_ephem = fail_click.visible_ephemeral_messages()
        self.assertEqual(len(err_ephem), 1)
        self.assertNotIn("tok_secret_game", err_ephem[0]["content"])

    async def test_ratings_view_dispatch_error_before_and_after_message_update_defer_preserves_parent(self):
        rows = [{"backloggd_username": f"User{i}", "rating": 4.5, "status": "played"} for i in range(35)]
        view = RatingsView("Hades", "hades", "Guild", rows)
        self.addCleanup(view.stop)
        parent = {
            "id": 99,
            "kind": "original",
            "ephemeral": True,
            "content": "Initial content",
            "embed": view.embed(),
            "view": view,
            "deleted": False,
        }
        snapshot_before = dict(parent)
        next_btn = view.next_button

        # 1. Error before acknowledgment
        fail_pre = _component_interaction(parent, guild=self.guild, user_id=10)
        with patch.object(view, "embed", side_effect=RuntimeError("embed fail")):
            with self.assertLogs("backloggd.ui.views", level="ERROR") as logs:
                await view._scheduled_task(next_btn, fail_pre)
        self.assertEqual(len(logs.records), 1)
        self.assertEqual(parent, snapshot_before)
        self.assertEqual(len(fail_pre.visible_ephemeral_messages()), 1)

        # 2. Error AFTER a component deferred_message_update -> parent MUST NOT be edited or deleted!
        from bot.command_errors import defer_interaction

        fail_deferred = _component_interaction(parent, guild=self.guild, user_id=10)
        fail_deferred.edit_original_response = AsyncMock(side_effect=discord.HTTPException(_FakeHTTPResp(500), "edit failed"))
        fail_deferred.delete_original_response = AsyncMock(side_effect=AssertionError("must not delete parent"))
        await defer_interaction(fail_deferred)
        self.assertEqual(fail_deferred.response.type, discord.InteractionResponseType.deferred_message_update)
        with self.assertLogs("backloggd.ui.views", level="WARNING") as warn_logs:
            # The acknowledged edit fails; private error reporting must not retry editing or delete the parent.
            await view._scheduled_task(next_btn, fail_deferred)
        self.assertEqual(len(warn_logs.records), 1)
        fail_deferred.edit_original_response.assert_awaited_once()
        fail_deferred.delete_original_response.assert_not_awaited()
        self.assertEqual(parent, snapshot_before)
        self.assertFalse(parent["deleted"])
        self.assertEqual(len(fail_deferred.visible_ephemeral_messages()), 1)

        # 3. Success still works after failures
        ok_next = _component_interaction(parent, guild=self.guild, user_id=10)
        await view._scheduled_task(next_btn, ok_next)
        self.assertEqual(view.page, 2)
        self.assertFalse(view.previous_button.disabled)

    async def test_feed_list_dispatch_errors_unauthorized_and_deferred_update(self):
        for index in range(25):
            await self.db.add_guild_admin_follow(1, f"player{index:02d}", None, 99)
        bot = SimpleNamespace(db=self.db, backloggd=MagicMock())
        cog = FeedCog(bot, clock=lambda: datetime(2026, 11, 13, tzinfo=timezone.utc))
        slash = StatefulDiscordInteraction(name="feed", guild=self.guild, user_id=10, manage_channels=True)
        await FeedCog.feed_list.callback(cog, slash)
        ephem_initial = slash.visible_ephemeral_messages()
        self.assertEqual(len(ephem_initial), 1)
        parent = ephem_initial[0]
        view = parent["view"]
        self.addCleanup(view.stop)
        snapshot_before = dict(parent)

        # 1. Unauthorized click with HTTP failure -> warning logged once, non-recursive
        unauth = _component_interaction(
            parent,
            guild=self.guild,
            user_id=99,
            send_message_error=discord.NotFound(_FakeHTTPResp(404), "Unknown Interaction"),
            followup_send_error=discord.NotFound(_FakeHTTPResp(404), "Unknown Webhook"),
        )
        with self.assertLogs("backloggd.ui.views", level="WARNING") as logs:
            await view._scheduled_task(view.next_button, unauth)
        self.assertEqual(len(logs.records), 2)
        self.assertIn("Discord HTTP failure in component", logs.records[0].getMessage())
        self.assertIn("Failed to send private reply", logs.records[1].getMessage())
        self.assertEqual(view.page, 0)
        self.assertEqual(parent, snapshot_before)

        # 2. Failed edit -> logged once, safe private reply, parent intact
        fail_edit = _component_interaction(
            parent,
            guild=self.guild,
            user_id=10,
            edit_original_error=discord.HTTPException(_FakeHTTPResp(500), "Edit failed"),
        )
        with self.assertLogs("backloggd.ui.views", level="WARNING") as logs:
            await view._scheduled_task(view.next_button, fail_edit)
        self.assertEqual(len(logs.records), 1)
        self.assertEqual(parent, snapshot_before)
        self.assertEqual(len(fail_edit.visible_ephemeral_messages()), 1)

        # 3. CancelledError propagates cleanly out of _scheduled_task and on_error
        cancel_click = _component_interaction(parent, guild=self.guild, user_id=10)
        cancel_click.edit_original_response = AsyncMock(side_effect=asyncio.CancelledError())
        with self.assertRaises(asyncio.CancelledError):
            await view._scheduled_task(view.next_button, cancel_click)
        with self.assertRaises(asyncio.CancelledError):
            await view.on_error(cancel_click, asyncio.CancelledError(), view.next_button)

    async def test_taste_twins_duel_does_not_double_log_with_shared_view_boundary(self):
        await self.db.link_user(10, "Alpha", None)
        await self.db.link_user(20, "Beta", None)
        lib_a = [
            SimpleNamespace(slug=f"g{i}", title=f"Game {i}", status="played", rating=4.5, cover_url=None, release_year=2020)
            for i in range(5)
        ]
        lib_b = [
            SimpleNamespace(slug=f"g{i}", title=f"Game {i}", status="played", rating=4.0, cover_url=None, release_year=2020)
            for i in range(5)
        ]
        await self.db.replace_user_library(10, "Alpha", lib_a)
        await self.db.replace_user_library(20, "Beta", lib_b)
        cog = SocialCog(
            SimpleNamespace(db=self.db, backloggd=SimpleNamespace()),
            clock=lambda: datetime(2026, 11, 13, tzinfo=timezone.utc),
        )
        self.addAsyncCleanup(cog.close)

        slash = StatefulDiscordInteraction(name="taste-twins", guild=self.guild, user_id=10, display_name="Alpha")
        await SocialCog.taste_twins.callback(cog, slash, None)
        parent = slash.visible_public_messages()[0]
        view = parent["view"]
        self.addCleanup(view.stop)
        duel_btn = _find_button(view, "Duel with ")

        click = _component_interaction(parent, guild=self.guild, user_id=10, display_name="Alpha")
        with patch("bot.cogs.social.calculate_similarity_raw", side_effect=RuntimeError("duel boom")):
            with self.assertLogs("backloggd", level="ERROR") as logs:
                await view._scheduled_task(duel_btn, click)
        self.assertEqual(len(logs.records), 1)
        self.assertEqual(logs.records[0].name, "backloggd.cogs.social")
        self.assertEqual(len(click.visible_ephemeral_messages()), 1)


class PaginationStateConsistencyTests(unittest.IsolatedAsyncioTestCase):
    async def test_click_acknowledges_before_waiting_for_navigation_lock(self):
        rows = [{"backloggd_username": f"User{i}", "rating": 4.5, "status": "played"} for i in range(35)]
        for view in (RatingsView("Hades", "hades", "Guild", rows), FeedListView(10, [discord.Embed(title="A"), discord.Embed(title="B")])):
            with self.subTest(view=type(view).__name__):
                self.addCleanup(view.stop)
                parent = {"id": 1, "kind": "original", "ephemeral": True, "content": None,
                          "embed": discord.Embed(title="Original"), "view": view, "deleted": False}
                click = _component_interaction(parent, user_id=10)
                acknowledged = asyncio.Event()
                original_defer = click.response.defer
                async def observed_defer(**kwargs):
                    await original_defer(**kwargs)
                    acknowledged.set()
                click.response.defer = observed_defer
                await view._nav_lock.acquire()
                task = asyncio.create_task(view._move(click, 1))
                try:
                    await asyncio.wait_for(acknowledged.wait(), timeout=1)
                    self.assertEqual(click.response.type, discord.InteractionResponseType.deferred_message_update)
                    self.assertFalse(task.done())
                    self.assertEqual(parent["embed"].title, "Original")
                finally:
                    view._nav_lock.release()
                    await asyncio.gather(task, return_exceptions=True)

    async def test_ratings_view_failed_edits_preserve_page_and_controls_and_recover(self):
        rows = [{"backloggd_username": f"User{i}", "rating": 4.5, "status": "played"} for i in range(35)]
        view = RatingsView("Hades", "hades", "Guild", rows)
        self.addCleanup(view.stop)
        self.assertEqual(view.total_pages, 3)
        parent = {
            "id": 1, "kind": "original", "ephemeral": True,
            "content": None, "embed": view.embed(), "view": view, "deleted": False,
        }

        # Page 1: Previous disabled, Next enabled. Failed Next keeps page 1 & controls intact.
        fail_next = _component_interaction(
            parent,
            user_id=10,
            edit_original_error=discord.HTTPException(_FakeHTTPResp(500), "edit failed"),
        )
        with self.assertLogs("backloggd.ui.views", level="WARNING"):
            await view._scheduled_task(view.next_button, fail_next)
        self.assertEqual(view.page, 1)
        self.assertTrue(view.previous_button.disabled)
        self.assertFalse(view.next_button.disabled)

        # Subsequent Next reaches page 2
        await view._scheduled_task(view.next_button, _component_interaction(parent, user_id=10))
        self.assertEqual(view.page, 2)
        self.assertFalse(view.previous_button.disabled)
        self.assertFalse(view.next_button.disabled)

        # Advance to page 3, then fail Previous -> stays at page 3 with Next disabled
        await view._scheduled_task(view.next_button, _component_interaction(parent, user_id=10))
        self.assertEqual(view.page, 3)
        self.assertFalse(view.previous_button.disabled)
        self.assertTrue(view.next_button.disabled)

        fail_prev = _component_interaction(
            parent,
            user_id=10,
            edit_original_error=discord.HTTPException(_FakeHTTPResp(500), "edit failed"),
        )
        with self.assertLogs("backloggd.ui.views", level="WARNING"):
            await view._scheduled_task(view.previous_button, fail_prev)
        self.assertEqual(view.page, 3)
        self.assertFalse(view.previous_button.disabled)
        self.assertTrue(view.next_button.disabled)

        # Subsequent Previous reaches page 2
        await view._scheduled_task(view.previous_button, _component_interaction(parent, user_id=10))
        self.assertEqual(view.page, 2)

    async def test_feed_list_view_failed_edits_preserve_page_and_controls_and_recover(self):
        pages = [discord.Embed(title=f"Page {i}") for i in range(3)]
        view = FeedListView(owner_id=10, pages=pages)
        self.addCleanup(view.stop)
        parent = {
            "id": 1, "kind": "original", "ephemeral": True,
            "content": None, "embed": pages[0], "view": view, "deleted": False,
        }

        fail_next = _component_interaction(
            parent,
            user_id=10,
            edit_original_error=discord.HTTPException(_FakeHTTPResp(500), "edit failed"),
        )
        with self.assertLogs("backloggd.ui.views", level="WARNING"):
            await view._scheduled_task(view.next_button, fail_next)
        self.assertEqual(view.page, 0)
        self.assertTrue(view.previous_button.disabled)
        self.assertFalse(view.next_button.disabled)
        self.assertEqual(parent["embed"].title, "Page 0")

        await view._scheduled_task(view.next_button, _component_interaction(parent, user_id=10))
        await view._scheduled_task(view.next_button, _component_interaction(parent, user_id=10))
        self.assertEqual(view.page, 2)
        self.assertFalse(view.previous_button.disabled)
        self.assertTrue(view.next_button.disabled)

        fail_prev = _component_interaction(
            parent,
            user_id=10,
            edit_original_error=discord.HTTPException(_FakeHTTPResp(500), "edit failed"),
        )
        with self.assertLogs("backloggd.ui.views", level="WARNING"):
            await view._scheduled_task(view.previous_button, fail_prev)
        self.assertEqual(view.page, 2)
        self.assertFalse(view.previous_button.disabled)
        self.assertTrue(view.next_button.disabled)

        await view._scheduled_task(view.previous_button, _component_interaction(parent, user_id=10))
        self.assertEqual(view.page, 1)
        self.assertEqual(parent["embed"].title, "Page 1")

    async def test_concurrent_pagination_clicks_serialize_cleanly_without_extra_backend_calls(self):
        rows = [{"backloggd_username": f"User{i}", "rating": 4.5, "status": "played"} for i in range(35)]
        view = RatingsView("Hades", "hades", "Guild", rows)
        self.addCleanup(view.stop)
        parent = {
            "id": 1, "kind": "original", "ephemeral": True,
            "content": None, "embed": view.embed(), "view": view, "deleted": False,
        }

        edit1_started = asyncio.Event()
        release_edit1 = asyncio.Event()
        rendered_pages: list[str] = []

        click1 = _component_interaction(parent, user_id=10)
        orig_edit1 = click1.edit_original_response

        async def held_fail_edit(*, embed=None, view=None, **kwargs):
            rendered_pages.append(f"fail:{view.page}")
            edit1_started.set()
            await release_edit1.wait()
            raise discord.HTTPException(_FakeHTTPResp(500), "transient edit failure")

        click1.edit_original_response = held_fail_edit

        click2 = _component_interaction(parent, user_id=10)
        orig_edit2 = click2.edit_original_response

        async def tracked_edit2(*, embed=None, view=None, **kwargs):
            rendered_pages.append(f"ok:{view.page}")
            return await orig_edit2(embed=embed, view=view, **kwargs)

        click2.edit_original_response = tracked_edit2

        with self.assertLogs("backloggd.ui.views", level="WARNING"):
            t1 = asyncio.create_task(view._scheduled_task(view.next_button, click1))
            await asyncio.wait_for(edit1_started.wait(), timeout=2.0)

            t2 = asyncio.create_task(view._scheduled_task(view.next_button, click2))
            await asyncio.sleep(0)
            # Click 2 must wait on serialization and NOT jump to page 3 while Click 1 is in flight
            self.assertEqual(rendered_pages, ["fail:2"])

            release_edit1.set()
            await asyncio.gather(t1, t2)

        # Since click 1 failed and restored page 1, click 2 lands on page 2 (not 3!)
        self.assertEqual(rendered_pages, ["fail:2", "ok:2"])
        self.assertEqual(view.page, 2)
        self.assertFalse(view.previous_button.disabled)
        self.assertFalse(view.next_button.disabled)

        # Also verify FeedListView serializes concurrent clicks, enforces owner, and does no DB/SDK work
        pages = [discord.Embed(title=f"Feed Page {i}") for i in range(3)]
        feed_view = FeedListView(owner_id=10, pages=pages)
        self.addCleanup(feed_view.stop)
        f_parent = {
            "id": 2, "kind": "original", "ephemeral": True,
            "content": None, "embed": pages[0], "view": feed_view, "deleted": False,
        }
        f_started = asyncio.Event()
        f_release = asyncio.Event()
        f_click1 = _component_interaction(f_parent, user_id=10)
        f_orig1 = f_click1.edit_original_response

        async def held_feed_edit(*, embed=None, view=None, **kwargs):
            f_started.set()
            await f_release.wait()
            return await f_orig1(embed=embed, view=view, **kwargs)

        f_click1.edit_original_response = held_feed_edit
        f_click2 = _component_interaction(f_parent, user_id=10)
        f_unauth = _component_interaction(f_parent, user_id=99)

        ft1 = asyncio.create_task(feed_view._scheduled_task(feed_view.next_button, f_click1))
        await asyncio.wait_for(f_started.wait(), timeout=2.0)
        ft2 = asyncio.create_task(feed_view._scheduled_task(feed_view.next_button, f_click2))
        await feed_view._scheduled_task(feed_view.next_button, f_unauth)
        self.assertEqual(len(f_unauth.visible_ephemeral_messages()), 1)
        f_release.set()
        await asyncio.gather(ft1, ft2)

        self.assertEqual(feed_view.page, 2)
        self.assertFalse(feed_view.previous_button.disabled)
        self.assertTrue(feed_view.next_button.disabled)
        self.assertEqual(f_parent["embed"].title, "Feed Page 2")


if __name__ == "__main__":
    unittest.main()
