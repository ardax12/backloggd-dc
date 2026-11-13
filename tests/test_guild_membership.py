"""Regression tests for complete, coalesced guild membership resolution."""

import asyncio
import os
import tempfile
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

os.environ["PYTHON_DOTENV_DISABLED"] = "1"
os.environ["TURSO_DATABASE_URL"] = ""
os.environ["TURSO_AUTH_TOKEN"] = ""
os.environ["DEV_GUILD_ID"] = ""

import discord

from bot.cogs.discovery import DiscoveryCog
from bot.cogs.games import GamesCog
from bot.cogs.social import SocialCog
from bot.cogs.stats import StatsCog
from bot.db import Database


def _make_interaction(*, guild, user_id=10):
    done = False

    async def defer(*, ephemeral=False, thinking=False):
        nonlocal done
        done = True

    async def send_message(*args, **kwargs):
        nonlocal done
        done = True

    return SimpleNamespace(
        guild=guild,
        guild_id=guild.id if guild is not None else None,
        user=SimpleNamespace(id=user_id, display_name=f"User{user_id}", display_avatar=None),
        response=SimpleNamespace(
            is_done=lambda: done,
            defer=AsyncMock(side_effect=defer),
            send_message=AsyncMock(side_effect=send_message),
        ),
        followup=SimpleNamespace(send=AsyncMock()),
    )


class GuildMembershipServiceTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.db = Database(os.path.join(self.tmpdir.name, "test.db"))
        await self.db.connect()
        self.bot = SimpleNamespace(db=self.db, backloggd=MagicMock())

    async def asyncTearDown(self):
        service = getattr(self.bot, "guild_membership", None)
        if service is not None and hasattr(service, "close"):
            await service.close()
        await self.db.close()
        self.tmpdir.cleanup()

    async def test_complete_cache_does_not_chunk(self):
        from bot.services.guild_membership import GuildMembershipService

        service = GuildMembershipService()
        guild = SimpleNamespace(
            id=1,
            chunked=True,
            member_count=2,
            members=[SimpleNamespace(id=10), SimpleNamespace(id=20)],
            chunk=AsyncMock(),
        )

        ids = await service.get_member_ids(guild)
        self.assertEqual(ids, [10, 20])
        guild.chunk.assert_not_awaited()
        await service.close()

    async def test_incomplete_cache_chunks_and_coalesces_concurrent_requests(self):
        from bot.services.guild_membership import GuildMembershipService

        service = GuildMembershipService(chunk_timeout=1.0)
        started = asyncio.Event()
        release = asyncio.Event()
        calls = 0

        async def do_chunk(*, cache=True):
            nonlocal calls
            calls += 1
            started.set()
            await release.wait()
            guild.members = [SimpleNamespace(id=10), SimpleNamespace(id=20), SimpleNamespace(id=30)]
            guild.chunked = True

        guild = SimpleNamespace(
            id=1,
            chunked=False,
            member_count=3,
            members=[SimpleNamespace(id=10)],
            chunk=do_chunk,
        )

        t1 = asyncio.create_task(service.get_member_ids(guild, exclude_member_id=10))
        t2 = asyncio.create_task(service.get_member_ids(guild, exclude_member_id=20))
        await started.wait()
        release.set()

        r1, r2 = await asyncio.gather(t1, t2)
        self.assertEqual(calls, 1)
        self.assertEqual(r1, [20, 30])
        self.assertEqual(r2, [10, 30])
        self.assertEqual(service._inflight, {})
        await service.close()

    async def test_caller_cancellation_does_not_abort_shared_chunk_and_close_cancels_inflight(self):
        from bot.services.guild_membership import GuildMembershipService

        service = GuildMembershipService(chunk_timeout=5.0)
        started = asyncio.Event()
        release = asyncio.Event()

        async def do_chunk(*, cache=True):
            started.set()
            await release.wait()
            guild.members = [SimpleNamespace(id=10), SimpleNamespace(id=20)]
            guild.chunked = True

        guild = SimpleNamespace(
            id=1, chunked=False, member_count=2, members=[], chunk=do_chunk
        )

        t1 = asyncio.create_task(service.get_member_ids(guild))
        t2 = asyncio.create_task(service.get_member_ids(guild))
        await started.wait()
        t1.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await t1
        release.set()
        self.assertEqual(await t2, [10, 20])
        self.assertEqual(service._inflight, {})

        # Shutdown cancels active chunk tasks cleanly.
        never = asyncio.Event()

        async def hang(*, cache=True):
            started2.set()
            await never.wait()

        started2 = asyncio.Event()
        guild2 = SimpleNamespace(id=2, chunked=False, member_count=2, members=[], chunk=hang)
        pending = asyncio.create_task(service.get_member_ids(guild2))
        await started2.wait()
        await service.close()
        self.assertEqual(service._inflight, {})
        try:
            res = await pending
            self.assertIsNone(res)
        except asyncio.CancelledError:
            pass

    async def test_timeout_failure_and_still_incomplete_cache_return_none_and_clean_up(self):
        from bot.services.guild_membership import GuildMembershipService

        service = GuildMembershipService(chunk_timeout=0.05)

        async def slow_chunk(*, cache=True):
            await asyncio.sleep(1.0)

        hanging_guild = SimpleNamespace(
            id=1, chunked=False, member_count=5, members=[], chunk=slow_chunk
        )
        self.assertIsNone(await service.get_member_ids(hanging_guild))
        self.assertEqual(service._inflight, {})

        still_partial = SimpleNamespace(
            id=2,
            chunked=True,
            member_count=3,
            members=[SimpleNamespace(id=10)],
            chunk=AsyncMock(return_value=[]),
        )
        self.assertIsNone(await service.get_member_ids(still_partial))
        self.assertEqual(service._inflight, {})

        failing = SimpleNamespace(
            id=3,
            chunked=False,
            member_count=2,
            members=[],
            chunk=AsyncMock(side_effect=RuntimeError("gateway down")),
        )
        self.assertIsNone(await service.get_member_ids(failing))
        self.assertEqual(service._inflight, {})
        await service.close()

    async def test_server_commands_wait_for_chunk_and_disclose_membership_failure(self):
        await self.db.link_user(10, "Alice")
        await self.db.link_user(20, "Bob")
        await self.db.link_user(99, "Departed")
        for slug in ("elden-ring", "hades", "celeste", "hollow-knight", "portal", "sekiro"):
            await self.db.upsert_game(slug, slug.replace("-", " ").title(), None, 2022, 4.5)
        for uid, slug, rating in [
            (10, "elden-ring", 5.0),
            (10, "hades", 4.5),
            (10, "celeste", 4.5),
            (10, "hollow-knight", 4.0),
            (10, "portal", 4.0),
            (20, "elden-ring", 4.5),
            (20, "hades", 4.5),
            (20, "celeste", 4.0),
            (20, "hollow-knight", 4.0),
            (20, "portal", 4.5),
            (20, "sekiro", 5.0),
            (99, "elden-ring", 1.0),
        ]:
            await self.db.upsert_user_rating(uid, slug, rating, "played")

        # Failing incomplete guild: must NOT show empty server or partial cached member (only 10).
        broken_guild = SimpleNamespace(
            id=77,
            name="PartialGuild",
            chunked=False,
            member_count=2,
            members=[SimpleNamespace(id=10)],
            chunk=AsyncMock(side_effect=RuntimeError("timeout")),
        )

        self.bot.backloggd.get_game = AsyncMock(
            return_value=SimpleNamespace(
                slug="elden-ring",
                title="Elden Ring",
                release_year=2022,
                community_rating=4.5,
                summary="RPG",
                cover_url=None,
                stats=SimpleNamespace(plays_count=1, backlogs_count=1, wishlists_count=1),
            )
        )
        self.bot.backloggd.get_user_profile = AsyncMock(
            return_value=SimpleNamespace(username="Alice", played_count=5)
        )

        stats_cog = StatsCog(self.bot)
        games_cog = GamesCog(self.bot)
        social_cog = SocialCog(self.bot)
        discovery_cog = DiscoveryCog(self.bot)

        for cog, cmd, kwargs in (
            (stats_cog, StatsCog.server_top, {"mode": "Highest Rated"}),
            (stats_cog, StatsCog.server_stats, {}),
            (games_cog, GamesCog.game, {"title": "elden-ring"}),
            (social_cog, SocialCog.taste_twins, {}),
            (discovery_cog, DiscoveryCog.recommend, {}),
        ):
            if cmd is GamesCog.game:
                games_cog._autocomplete_slugs["elden-ring"] = "elden-ring"
            interaction = _make_interaction(guild=broken_guild, user_id=10)
            await cmd.callback(cog, interaction, **kwargs)
            interaction.followup.send.assert_awaited_once()
            send_kwargs = interaction.followup.send.call_args.kwargs
            msg = interaction.followup.send.call_args.args[0] if interaction.followup.send.call_args.args else send_kwargs.get("content", "")
            self.assertIn("membership", msg.lower(), msg)
            self.assertTrue(send_kwargs.get("ephemeral"), msg)
            self.assertIsNone(send_kwargs.get("embed"), msg)

        # DM behavior for /game must still work without a guild.
        dm_interaction = _make_interaction(guild=None, user_id=10)
        await GamesCog.game.callback(games_cog, dm_interaction, title="elden-ring")
        dm_interaction.followup.send.assert_awaited_once()
        self.assertIsNotNone(dm_interaction.followup.send.call_args.kwargs.get("embed"))

        # Now test an incomplete guild whose chunk() restores complete membership (10 + 20, not 99).
        async def recover_chunk(*, cache=True):
            recovering_guild.members = [SimpleNamespace(id=10), SimpleNamespace(id=20)]
            recovering_guild.chunked = True

        recovering_guild = SimpleNamespace(
            id=88,
            name="RecoveredGuild",
            chunked=False,
            member_count=2,
            members=[SimpleNamespace(id=10)],
            chunk=recover_chunk,
        )
        i_stats = _make_interaction(guild=recovering_guild, user_id=10)
        await StatsCog.server_stats.callback(stats_cog, i_stats)
        embed = i_stats.followup.send.call_args.kwargs["embed"]
        # Departed member 99 is NOT in guild, while 10 and 20 ARE counted (2 members).
        self.assertIn("2", embed.fields[0].value)

        # Caller exclusion still enforced on /recommend after chunking.
        recovering_guild.chunked = False
        recovering_guild.members = [SimpleNamespace(id=10)]
        i_rec = _make_interaction(guild=recovering_guild, user_id=10)
        await DiscoveryCog.recommend.callback(discovery_cog, i_rec)
        rec_embed = i_rec.followup.send.call_args.kwargs["embed"]
        self.assertIn("Sekiro", rec_embed.fields[0].name)


if __name__ == "__main__":
    unittest.main()
