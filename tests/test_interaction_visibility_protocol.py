"""Protocol-level regression tests for Task 6: Discord deferred-response visibility semantics."""

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
from unittest.mock import AsyncMock, MagicMock

import discord

from bot.client import BackloggdBot
from bot.cogs.account import AccountCog
from bot.cogs.discovery import DiscoveryCog
from bot.cogs.feed import FeedCog
from bot.cogs.games import GamesCog
from bot.cogs.social import SocialCog
from bot.cogs.stats import StatsCog
from bot.cogs.users import UsersCog
from bot.db import Database
from bot.services.library_sync import LibrarySyncService


class _FakeHTTPResp:
    def __init__(self, status: int = 500, reason: str = "Error"):
        self.status = status
        self.reason = reason


class _ProtocolMessageHandle:
    def __init__(self, record: dict):
        self.record = record
        self.edits: list[dict] = []

    async def edit(self, *, content=None, embed=None, view=None, allowed_mentions=None):
        if content is not None:
            self.record["content"] = content
        if embed is not None:
            self.record["embed"] = embed
        if view is not None:
            self.record["view"] = view
        self.edits.append({"content": content, "embed": embed, "view": view})
        return self


class StatefulDiscordInteraction:
    """Models Discord's actual interaction/@original/followup visibility rules."""

    def __init__(
        self,
        *,
        name: str = "cmd",
        interaction_type=discord.InteractionType.application_command,
        source_message=None,
        user_id: int = 10,
        display_name: str = "Caller",
        guild=None,
        guild_id=None,
        options=None,
        manage_channels: bool = True,
        edit_original_error=None,
        delete_original_error=None,
        followup_send_error=None,
    ):
        self.type = interaction_type
        self.data = {"name": name, "type": 1}
        if options is not None:
            self.data["options"] = options
        self.guild = guild
        self.guild_id = guild_id if guild_id is not None else getattr(guild, "id", None)
        if self.guild_id is not None:
            self.data["guild_id"] = str(self.guild_id)
        self.user = SimpleNamespace(
            id=user_id,
            display_name=display_name,
            name=display_name.lower(),
            display_avatar=None,
        )
        self.channel = SimpleNamespace(id=10)
        perms = discord.Permissions.none()
        perms.manage_channels = manage_channels
        self.permissions = perms
        self.app_permissions = discord.Permissions.all()
        self.extras = {}
        self.command = None
        self.command_failed = False
        self.namespace = None
        self._state = SimpleNamespace()
        self._cs_command = None
        self._cs_namespace = None

        self.responded = False
        self.deferred = False
        self.deferred_ephemeral = None
        self.original_consumed = False
        self.original_message: dict | None = source_message
        self.delivered_messages: list[dict] = []
        self.public_text_history: list[str] = []

        self.edit_original_error = edit_original_error
        self.delete_original_error = delete_original_error
        self.followup_send_error = followup_send_error

        self.response = _StatefulResponse(self)
        self.followup = _StatefulFollowup(self)

    def _record_public_snapshot(self, content, embed):
        if content:
            self.public_text_history.append(str(content))
        if embed is not None:
            payload = embed.to_dict()
            if payload.get("title"):
                self.public_text_history.append(payload["title"])
            if payload.get("description"):
                self.public_text_history.append(payload["description"])

    def visible_public_messages(self) -> list[dict]:
        return [
            m for m in self.delivered_messages
            if not m["ephemeral"] and not m.get("deleted", False)
        ]

    def visible_ephemeral_messages(self) -> list[dict]:
        return [
            m for m in self.delivered_messages
            if m["ephemeral"] and not m.get("deleted", False)
        ]

    async def edit_original_response(self, *, content=None, embed=None, view=None, allowed_mentions=None):
        if self.edit_original_error is not None:
            raise self.edit_original_error
        if not self.responded or self.original_message is None or self.original_message.get("deleted"):
            raise discord.NotFound(_FakeHTTPResp(404, "Not Found"), "Unknown Message")
        self.original_consumed = True
        if content is not None:
            self.original_message["content"] = content
        if embed is not None:
            self.original_message["embed"] = embed
        if view is not None:
            self.original_message["view"] = view
        if not self.original_message["ephemeral"]:
            self._record_public_snapshot(content, embed)
        return _ProtocolMessageHandle(self.original_message)

    async def delete_original_response(self):
        if self.delete_original_error is not None:
            raise self.delete_original_error
        if not self.responded or self.original_message is None or self.original_message.get("deleted"):
            raise discord.NotFound(_FakeHTTPResp(404, "Not Found"), "Unknown Message")
        self.original_message["deleted"] = True
        self.original_consumed = True


class _StatefulResponse:
    def __init__(self, parent: StatefulDiscordInteraction):
        self._parent = parent
        self.type = None

    def is_done(self) -> bool:
        return self._parent.responded

    async def defer(self, *, ephemeral: bool = False, thinking: bool = False):
        if self._parent.responded:
            raise discord.InteractionResponded(self._parent)
        self._parent.responded = True
        self._parent.deferred = True
        self._parent.deferred_ephemeral = bool(ephemeral)
        if self._parent.type == discord.InteractionType.component and not thinking:
            self.type = discord.InteractionResponseType.deferred_message_update
            self._parent.original_consumed = True
            return
        self.type = discord.InteractionResponseType.deferred_channel_message
        record = {
            "id": 1,
            "kind": "original",
            "ephemeral": bool(ephemeral),
            "content": None,
            "embed": None,
            "view": None,
            "deleted": False,
        }
        self._parent.original_message = record
        self._parent.delivered_messages.append(record)

    async def send_message(self, content=None, *, embed=None, view=None, ephemeral: bool = False, allowed_mentions=None):
        if self._parent.responded:
            raise discord.InteractionResponded(self._parent)
        self._parent.responded = True
        self._parent.original_consumed = True
        self.type = discord.InteractionResponseType.channel_message
        record = {
            "id": 1,
            "kind": "original",
            "ephemeral": bool(ephemeral),
            "content": content,
            "embed": embed,
            "view": view,
            "deleted": False,
        }
        self._parent.original_message = record
        self._parent.delivered_messages.append(record)
        if not ephemeral:
            self._parent._record_public_snapshot(content, embed)


class _StatefulFollowup:
    def __init__(self, parent: StatefulDiscordInteraction):
        self._parent = parent
        self._next_id = 2

    async def send(
        self,
        content=None,
        *,
        embed=None,
        view=None,
        ephemeral: bool = False,
        wait: bool = False,
        allowed_mentions=None,
    ):
        if self._parent.followup_send_error is not None:
            raise self._parent.followup_send_error
        if not self._parent.responded or self._parent.original_message is None:
            raise discord.NotFound(_FakeHTTPResp(404, "Not Found"), "Unknown Webhook")

        # Real Discord rule: first followup after a defer edits @original and
        # INHERITS the defer's ephemeral state, ignoring the followup's ephemeral flag!
        if self._parent.deferred and not self._parent.original_consumed:
            self._parent.original_consumed = True
            orig = self._parent.original_message
            orig["content"] = content
            orig["embed"] = embed
            orig["view"] = view
            if not orig["ephemeral"]:
                self._parent._record_public_snapshot(content, embed)
            return _ProtocolMessageHandle(orig)

        record = {
            "id": self._next_id,
            "kind": "followup",
            "ephemeral": bool(ephemeral),
            "content": content,
            "embed": embed,
            "view": view,
            "deleted": False,
        }
        self._next_id += 1
        self._parent.delivered_messages.append(record)
        if not ephemeral:
            self._parent._record_public_snapshot(content, embed)
        return _ProtocolMessageHandle(record)


class InteractionVisibilityProtocolTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmpdir.cleanup)
        self.db = Database(str(Path(self.tmpdir.name) / "vis.db"), turso_url="")
        await self.db.connect()
        self.addAsyncCleanup(self.db.close)

    def _assert_private_error_never_leaked_publicly(
        self,
        interaction: StatefulDiscordInteraction,
        expected_substring: str,
    ):
        ephem = interaction.visible_ephemeral_messages()
        self.assertEqual(len(ephem), 1, f"Expected 1 ephemeral message, got {interaction.delivered_messages}")
        msg_text = ephem[0]["content"] or ""
        self.assertIn(expected_substring.lower(), msg_text.lower())

        for public_text in interaction.public_text_history:
            self.assertNotIn(expected_substring.lower(), public_text.lower())
        self.assertEqual(interaction.visible_public_messages(), [])

    async def test_all_ten_public_commands_keep_errors_and_hints_ephemeral_under_discord_protocol(self):
        guild = SimpleNamespace(
            id=1,
            name="TestGuild",
            chunked=True,
            member_count=1,
            members=[SimpleNamespace(id=10, display_name="Caller")],
            me=SimpleNamespace(id=999),
        )
        broken_guild = SimpleNamespace(
            id=2,
            name="BrokenGuild",
            chunked=False,
            member_count=5,
            members=[],
        )
        api = SimpleNamespace(
            search_games=AsyncMock(return_value=[]),
            get_user_profile=AsyncMock(return_value=None),
        )
        sync_svc = LibrarySyncService()
        self.addAsyncCleanup(sync_svc.close)
        bot = SimpleNamespace(db=self.db, backloggd=api, library_sync=sync_svc)

        games_cog = GamesCog(bot)
        users_cog = UsersCog(bot)
        social_cog = SocialCog(bot)
        discovery_cog = DiscoveryCog(bot)
        stats_cog = StatsCog(bot)
        account_cog = AccountCog(bot)

        # 1. /game
        i_game = StatefulDiscordInteraction(guild=guild)
        await GamesCog.game.callback(games_cog, i_game, "nonexistent-game-xyz")
        self._assert_private_error_never_leaked_publicly(i_game, "No games found matching")

        # 2. /profile
        i_profile = StatefulDiscordInteraction(guild=guild)
        await UsersCog.profile.callback(users_cog, i_profile, None)
        self._assert_private_error_never_leaked_publicly(i_profile, "haven't linked")

        # 3. /compare
        i_compare = StatefulDiscordInteraction(guild=guild)
        await SocialCog.compare.callback(social_cog, i_compare, "Someone", None)
        self._assert_private_error_never_leaked_publicly(i_compare, "haven't linked")

        # 4. /taste-twins
        i_twins = StatefulDiscordInteraction(guild=guild)
        await SocialCog.taste_twins.callback(social_cog, i_twins, None)
        self._assert_private_error_never_leaked_publicly(i_twins, "haven't linked")

        # 5. /roulette
        i_roulette = StatefulDiscordInteraction(guild=guild)
        await DiscoveryCog.roulette.callback(discovery_cog, i_roulette, None)
        self._assert_private_error_never_leaked_publicly(i_roulette, "do not have a linked")

        # 6. /recommend
        i_rec = StatefulDiscordInteraction(guild=guild)
        await DiscoveryCog.recommend.callback(discovery_cog, i_rec, None)
        self._assert_private_error_never_leaked_publicly(i_rec, "do not have a linked")

        # 7. /server-top
        i_top = StatefulDiscordInteraction(guild=broken_guild)
        await StatsCog.server_top.callback(stats_cog, i_top, "Highest Rated")
        self._assert_private_error_never_leaked_publicly(i_top, "membership")

        # 8. /server-stats
        i_stats = StatefulDiscordInteraction(guild=broken_guild)
        await StatsCog.server_stats.callback(stats_cog, i_stats)
        self._assert_private_error_never_leaked_publicly(i_stats, "membership")

        # 9. /link
        i_link = StatefulDiscordInteraction(guild=guild)
        await AccountCog.link.callback(account_cog, i_link, "   ")
        self._assert_private_error_never_leaked_publicly(i_link, "Please enter")

        # 10. /refresh
        i_refresh = StatefulDiscordInteraction(guild=guild)
        await AccountCog.refresh.callback(account_cog, i_refresh, None)
        self._assert_private_error_never_leaked_publicly(i_refresh, "do not have a linked")

    async def test_public_success_results_and_link_refresh_status_handles_stay_public(self):
        guild = SimpleNamespace(
            id=1,
            name="TestGuild",
            chunked=True,
            member_count=2,
            members=[
                SimpleNamespace(id=10, display_name="Alpha", display_avatar=None),
                SimpleNamespace(id=20, display_name="Beta", display_avatar=None),
            ],
            get_member=lambda uid: None,
        )
        await self.db.link_user(10, "Alpha", None)
        await self.db.link_user(20, "Beta", None)
        await self.db.upsert_game("hades", "Hades", None, 2020, 4.7)
        lib_a = [SimpleNamespace(slug=f"g{i}", title=f"Game {i}", status="played", rating=4.5, cover_url=None, release_year=2020) for i in range(5)]
        lib_a.append(SimpleNamespace(slug="hades", title="Hades", status="backlog", rating=None, cover_url=None, release_year=2020))
        lib_b = [SimpleNamespace(slug=f"g{i}", title=f"Game {i}", status="played", rating=4.0, cover_url=None, release_year=2020) for i in range(5)]
        lib_b.append(SimpleNamespace(slug="extra-pick", title="Extra Pick", status="played", rating=5.0, cover_url=None, release_year=2021))
        await self.db.replace_user_library(10, "Alpha", lib_a)
        await self.db.replace_user_library(20, "Beta", lib_b)

        game_obj = SimpleNamespace(
            slug="hades", title="Hades", release_year=2020, community_rating=4.7,
            summary="Escape hell", cover_url=None,
            stats=SimpleNamespace(total_ratings=10, plays_count=5, playing_count=1, backlogs_count=2, wishlists_count=3),
        )
        profile_a = SimpleNamespace(
            username="Alpha", avatar_url=None, bio="Hi", badges=[], favorite_games=[],
            played_count=5, playing_count=0, backlog_count=1, wishlist_count=0,
        )
        api = SimpleNamespace(
            search_games=AsyncMock(return_value=[game_obj]),
            get_game=AsyncMock(return_value=game_obj),
            get_user_profile=AsyncMock(return_value=profile_a),
            get_user_journal=AsyncMock(return_value=[]),
            get_user_reviews=AsyncMock(return_value=[]),
            get_user_library=AsyncMock(return_value=lib_a),
        )
        sync_svc = LibrarySyncService()
        self.addAsyncCleanup(sync_svc.close)
        bot = SimpleNamespace(db=self.db, backloggd=api, library_sync=sync_svc)

        # Public success commands
        for cog_cls, method, kwargs in (
            (GamesCog, GamesCog.game, {"title": "slug:hades"}),
            (UsersCog, UsersCog.profile, {"user": None}),
            (SocialCog, SocialCog.compare, {"user1": "Beta", "user2": None}),
            (SocialCog, SocialCog.taste_twins, {"user": None}),
            (DiscoveryCog, DiscoveryCog.roulette, {"user": None}),
            (DiscoveryCog, DiscoveryCog.recommend, {"user": None}),
            (StatsCog, StatsCog.server_top, {"mode": "Highest Rated"}),
            (StatsCog, StatsCog.server_stats, {}),
        ):
            cog = cog_cls(bot, clock=lambda: datetime(2026, 11, 13, tzinfo=timezone.utc)) if cog_cls is not GamesCog else cog_cls(bot)
            if hasattr(cog, "close"):
                self.addAsyncCleanup(cog.close)
            inter = StatefulDiscordInteraction(guild=guild, user_id=10)
            await method.callback(cog, inter, **kwargs)
            pub = inter.visible_public_messages()
            self.assertEqual(len(pub), 1, f"{method.name} did not produce 1 public message: {inter.delivered_messages}")
            self.assertIsNotNone(pub[0]["embed"])
            self.assertEqual(inter.visible_ephemeral_messages(), [])

        # /link and /refresh public progress message + wait=True handle edit
        sync_started = asyncio.Event()
        release_sync = asyncio.Event()

        async def slow_lib(username, force_refresh=False):
            sync_started.set()
            await release_sync.wait()
            return lib_a

        api.get_user_library = AsyncMock(side_effect=slow_lib)
        acc_cog = AccountCog(bot)

        # /link (new user 30)
        profile_c = SimpleNamespace(username="Charlie", avatar_url=None)
        api.get_user_profile = AsyncMock(return_value=profile_c)
        i_link = StatefulDiscordInteraction(guild=guild, user_id=30, display_name="Charlie")
        await AccountCog.link.callback(acc_cog, i_link, "Charlie")
        await asyncio.wait_for(sync_started.wait(), timeout=2)
        self.assertEqual(len(i_link.visible_public_messages()), 1)
        self.assertEqual(i_link.visible_ephemeral_messages(), [])
        release_sync.set()
        active_tasks = [s.task for s in sync_svc._profiles.values() if s.task is not None]
        if active_tasks:
            await asyncio.gather(*active_tasks)
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        self.assertIn("complete", i_link.visible_public_messages()[0]["embed"].title.lower())

        # /refresh (queued + already running)
        sync_started.clear()
        release_sync.clear()
        sync_svc2 = LibrarySyncService()
        self.addAsyncCleanup(sync_svc2.close)
        bot2 = SimpleNamespace(db=self.db, backloggd=api, library_sync=sync_svc2)
        acc_cog2 = AccountCog(bot2)

        i_ref_q = StatefulDiscordInteraction(guild=guild, user_id=10)
        await AccountCog.refresh.callback(acc_cog2, i_ref_q, None)
        await asyncio.wait_for(sync_started.wait(), timeout=2)
        self.assertEqual(len(i_ref_q.visible_public_messages()), 1)
        self.assertIn("queued", i_ref_q.visible_public_messages()[0]["embed"].title.lower())

        i_ref_run = StatefulDiscordInteraction(guild=guild, user_id=10)
        await AccountCog.refresh.callback(acc_cog2, i_ref_run, None)
        self.assertEqual(len(i_ref_run.visible_public_messages()), 1)
        self.assertIn("already running", i_ref_run.visible_public_messages()[0]["embed"].description.lower())

        release_sync.set()
        active_tasks2 = [s.task for s in sync_svc2._profiles.values() if s.task is not None]
        if active_tasks2:
            await asyncio.gather(*active_tasks2)
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        self.assertIn("complete", i_ref_q.visible_public_messages()[0]["embed"].title.lower())
        self.assertIn("complete", i_ref_run.visible_public_messages()[0]["embed"].title.lower())

    async def test_unlink_and_feed_controls_stay_private(self):
        guild = SimpleNamespace(
            id=1,
            name="G",
            chunked=True,
            member_count=1,
            members=[SimpleNamespace(id=10)],
            me=SimpleNamespace(id=999),
        )
        bot = SimpleNamespace(db=self.db, backloggd=SimpleNamespace(), guilds=[guild])
        acc_cog = AccountCog(bot)
        feed_cog = FeedCog(bot, clock=lambda: datetime(2026, 11, 13, tzinfo=timezone.utc))

        i_unlink = StatefulDiscordInteraction(guild=guild, user_id=10)
        await AccountCog.unlink.callback(acc_cog, i_unlink)
        self.assertEqual(i_unlink.visible_public_messages(), [])
        self.assertEqual(len(i_unlink.visible_ephemeral_messages()), 1)

        i_feed_on = StatefulDiscordInteraction(guild=guild, user_id=10)
        await FeedCog.feed_on.callback(feed_cog, i_feed_on)
        self.assertEqual(i_feed_on.visible_public_messages(), [])
        self.assertEqual(len(i_feed_on.visible_ephemeral_messages()), 1)

        ch = SimpleNamespace(
            id=50,
            mention="#feed",
            permissions_for=lambda _: SimpleNamespace(view_channel=True, send_messages=True, embed_links=True),
        )
        i_setch = StatefulDiscordInteraction(guild=guild, user_id=10)
        await FeedCog.setchannel.callback(feed_cog, i_setch, ch)
        self.assertEqual(i_setch.visible_public_messages(), [])
        self.assertEqual(len(i_setch.visible_ephemeral_messages()), 1)

    async def test_global_error_boundary_keeps_public_deferred_command_error_private(self):
        bot = BackloggdBot()
        await bot.db.close()
        bot.db = self.db
        bot.backloggd = MagicMock()
        bot.backloggd.close = AsyncMock()
        self.addAsyncCleanup(bot.close)

        await bot.load_extension("bot.cogs.discovery")
        bot.db.get_linked_user = AsyncMock(side_effect=RuntimeError("SELECT * FROM secret_table"))
        interaction = StatefulDiscordInteraction(name="roulette", guild_id=1, user_id=10)

        with self.assertLogs("backloggd.bot", level="ERROR") as logs:
            await bot.tree._call(interaction)

        self.assertEqual(len(logs.records), 1)
        ephem = interaction.visible_ephemeral_messages()
        self.assertEqual(len(ephem), 1)
        self.assertIn("something went wrong", ephem[0]["content"].lower())
        self.assertNotIn("secret_table", ephem[0]["content"])
        for public_text in interaction.public_text_history:
            self.assertNotIn("something went wrong", public_text.lower())
            self.assertNotIn("secret_table", public_text.lower())
        self.assertEqual(interaction.visible_public_messages(), [])

    async def test_independent_edit_delete_followup_failures_and_cancellation(self):
        guild = SimpleNamespace(id=1, name="G", chunked=True, member_count=1, members=[SimpleNamespace(id=10)])
        api = SimpleNamespace(search_games=AsyncMock(return_value=[]))
        cog = GamesCog(SimpleNamespace(db=self.db, backloggd=api))

        # 1. edit_original_response fails -> NEVER leaks private message to followup
        i_edit_fail = StatefulDiscordInteraction(
            guild=guild,
            edit_original_error=discord.HTTPException(_FakeHTTPResp(500), "internal error"),
        )
        with self.assertLogs("backloggd.cogs.games", level="WARNING"):
            await GamesCog.game.callback(cog, i_edit_fail, "missing-game")
        self.assertEqual(i_edit_fail.visible_ephemeral_messages(), [])
        self.assertEqual(i_edit_fail.public_text_history, [])

        # 2. edit_original_response succeeds, delete_original_response fails -> ephemeral sent once, generic public only
        i_del_fail = StatefulDiscordInteraction(
            guild=guild,
            delete_original_error=discord.NotFound(_FakeHTTPResp(404), "already deleted"),
        )
        await GamesCog.game.callback(cog, i_del_fail, "missing-game")
        ephem = i_del_fail.visible_ephemeral_messages()
        self.assertEqual(len(ephem), 1)
        self.assertIn("No games found", ephem[0]["content"])
        pub = i_del_fail.visible_public_messages()
        self.assertEqual(len(pub), 1)
        self.assertNotIn("No games found", pub[0]["content"])

        # 3. edit_original_response succeeds, followup.send fails -> generic public kept, no crash, no recursion
        i_send_fail = StatefulDiscordInteraction(
            guild=guild,
            followup_send_error=discord.HTTPException(_FakeHTTPResp(503), "unavailable"),
        )
        with self.assertLogs("backloggd.cogs.games", level="WARNING"):
            await GamesCog.game.callback(cog, i_send_fail, "missing-game")
        self.assertEqual(i_send_fail.visible_ephemeral_messages(), [])
        pub = i_send_fail.visible_public_messages()
        self.assertEqual(len(pub), 1)
        self.assertNotIn("No games found", pub[0]["content"])

        # 4. CancelledError still propagates cleanly
        bot = BackloggdBot()
        await bot.db.close()
        bot.db = self.db
        bot.backloggd = MagicMock()
        bot.backloggd.close = AsyncMock()
        self.addAsyncCleanup(bot.close)

        @bot.tree.command(name="public_cancel", description="test")
        async def public_cancel(inter: discord.Interaction):
            await inter.response.defer()
            raise asyncio.CancelledError()

        i_cancel = StatefulDiscordInteraction(name="public_cancel", guild_id=1)
        with self.assertRaises(asyncio.CancelledError):
            await bot.tree._call(i_cancel)
        self.assertEqual(i_cancel.visible_ephemeral_messages(), [])


if __name__ == "__main__":
    unittest.main()
