import os
os.environ["PYTHON_DOTENV_DISABLED"] = "1"

import asyncio
from datetime import datetime, timedelta, timezone
from pathlib import Path
import sqlite3
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

import discord

from backloggd import GameEntry, GameStatus
from bot.cogs.account import AccountCog
from bot.cogs.discovery import DiscoveryCog
from bot.cogs.feed import FeedCog
from bot.cogs.social import SocialCog
from bot.cogs.stats import StatsCog
from bot.db import Database
from bot.feed_models import FeedEvent
from bot.services.library_sync import LibrarySyncService


FIXED_NOW = datetime(2026, 11, 13, 12, 0, 0, tzinfo=timezone.utc)


def make_channel(channel_id: int):
    channel = MagicMock(spec=discord.TextChannel)
    channel.id = channel_id
    channel.permissions_for.return_value = SimpleNamespace(send_messages=True, embed_links=True)
    channel.send = AsyncMock(return_value=SimpleNamespace(id=channel_id * 100))
    channel.get_partial_message.return_value.edit = AsyncMock()
    return channel


class LibItem:
    def __init__(self, slug: str, rating=None, status="played", title=None):
        self.slug = slug
        self.rating = rating
        self.status = status
        self.title = title or slug.replace("-", " ").title()
        self.cover_url = None
        self.release_year = 2024


class CombinedIntegrationStoryTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.db_path = Path(self.directory.name) / "stories.db"
        self.db = Database(str(self.db_path), turso_url="")
        await self.db.connect()
        self.now = FIXED_NOW
        self._mono_origin = FIXED_NOW
        self.library_sync = LibrarySyncService(
            clock=lambda: (self.now - self._mono_origin).total_seconds()
        )
        self.api = SimpleNamespace(
            get_user_profile=AsyncMock(),
            get_user_library=AsyncMock(return_value=[]),
            get_user_activity=AsyncMock(return_value=[]),
            get_user_journal=AsyncMock(return_value=[]),
            get_user_reviews=AsyncMock(return_value=[]),
            search_games=AsyncMock(return_value=[]),
            get_game=AsyncMock(),
        )

    async def asyncTearDown(self):
        await self.library_sync.close()
        await self.db.close()
        self.directory.cleanup()

    def _interaction(self, *, user_id=10, guild=None, manage_channels=True):
        return SimpleNamespace(
            user=SimpleNamespace(
                id=user_id,
                display_name=f"Member{user_id}",
                display_avatar=None,
                guild_permissions=SimpleNamespace(manage_channels=manage_channels),
            ),
            guild=guild,
            guild_id=guild.id if guild else None,
            response=SimpleNamespace(defer=AsyncMock(), send_message=AsyncMock()),
            followup=SimpleNamespace(send=AsyncMock(return_value=SimpleNamespace(edit=AsyncMock()))),
        )

    async def _drain_syncs(self):
        tasks = [s.task for s in self.library_sync._profiles.values() if s.task]
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    async def test_duplicate_profiles_local_linking_vs_passive_and_independent_admin_follows(self):
        # Members 10 & 11 connect in Guild A and DM/passive Guild B.
        channel_a = make_channel(101)
        channel_b = make_channel(202)
        members_a = [
            SimpleNamespace(id=10, display_name="Ten"),
            SimpleNamespace(id=11, display_name="Eleven"),
            SimpleNamespace(id=99, display_name="Admin"),
        ]
        members_b = [
            SimpleNamespace(id=10, display_name="Ten"),
            SimpleNamespace(id=99, display_name="Admin"),
        ]
        guild_a = SimpleNamespace(
            id=1, name="Guild A", members=members_a, chunked=True, me=object(),
            get_channel=lambda cid: channel_a if cid == 101 else None,
        )
        guild_b = SimpleNamespace(
            id=2, name="Guild B", members=members_b, chunked=True, me=object(),
            get_channel=lambda cid: channel_b if cid == 202 else None,
        )
        guilds = {1: guild_a, 2: guild_b}

        bot = SimpleNamespace(
            db=self.db,
            backloggd=self.api,
            library_sync=self.library_sync,
            get_guild=lambda gid: guilds.get(gid),
            guilds=[guild_a, guild_b],
        )
        account = AccountCog(bot)
        feed = FeedCog(bot, clock=lambda: self.now)
        stats = StatsCog(bot, clock=lambda: self.now)

        await self.db.set_feed_channel(1, 101)
        await self.db.set_feed_channel(2, 202)

        def profile_for(name):
            return SimpleNamespace(username=name, avatar_url=None)

        self.api.get_user_profile.side_effect = lambda name: profile_for(
            "Millers" if name.lower() == "millers" else "PublicOnly"
        )
        self.api.get_user_library.return_value = [
            LibItem("hades", 5.0, "played", "Hades"),
            LibItem("backlog-pick", None, "backlog", "Backlog Pick"),
        ]

        # Member 10 links in Guild A -> enrolls ONLY in Guild A (passive membership in B stays off).
        await AccountCog.link.callback(account, self._interaction(user_id=10, guild=guild_a), "Millers")
        # Member 11 links in DM -> no server enrollment anywhere.
        await AccountCog.link.callback(account, self._interaction(user_id=11, guild=None), "millers")
        # Admin follows PublicOnly in Guild A (feed-only profile) and Millers in Guild B.
        await FeedCog.feed_add.callback(feed, self._interaction(user_id=99, guild=guild_a), "PublicOnly")
        await FeedCog.feed_add.callback(feed, self._interaction(user_id=99, guild=guild_b), "Millers")
        await self._drain_syncs()

        # Guild A polls Millers (member) + PublicOnly (admin); Guild B polls Millers (admin, even though 10's pref is off in B!).
        self.assertTrue(await self.db.get_guild_feed_preference(1, 10))
        self.assertFalse(await self.db.get_guild_feed_preference(2, 10))
        self.assertFalse(await self.db.get_guild_feed_preference(1, 11))

        # Verify PublicOnly is excluded from gaming stats & Millers is counted once across 2 members in Guild A.
        stats_i = self._interaction(user_id=10, guild=guild_a)
        await StatsCog.server_stats.callback(stats, stats_i)
        server_stats = await self.db.get_server_stats([10, 11, 99])
        self.assertEqual(server_stats["linked_members"], 2)
        self.assertEqual(server_stats["profile_count"], 1)
        self.assertFalse(await self.db.is_profile_library_subscribed("publiconly"))

        # Poll: Millers fetched ONCE for Guild A + B, PublicOnly fetched ONCE; neither posts baseline history.
        ev_old = FeedEvent("e1", "Hades", "hades", None, "completed", "completed", 5.0, None, None, played_date="2026-11-12", occurred_at="2026-11-12T10:00:00Z")
        ev_new = FeedEvent("e2", "Celeste", "celeste", None, "completed", "completed", 5.0, None, None, played_date="2026-11-13", occurred_at="2026-11-13T12:05:00Z")
        fetch_calls = []

        async def fake_fetch(username, defer_boundary_commit=False):
            fetch_calls.append(username.lower())
            if username.lower() == "millers":
                return {"activity": [ev_new, ev_old] if len([x for x in fetch_calls if x == "millers"]) > 1 else [ev_old], "journal": []}
            return {"activity": [], "journal": []}

        feed._fetch_feed_sources = fake_fetch
        with patch("bot.cogs.feed.asyncio.sleep", new=AsyncMock()):
            await feed.journal_feed_loop.coro(feed)
            self.assertEqual(fetch_calls.count("millers"), 1)
            self.assertEqual(fetch_calls.count("publiconly"), 1)
            channel_a.send.assert_not_awaited()
            channel_b.send.assert_not_awaited()

            # Second poll: Millers posts once in Guild A and once in Guild B.
            await feed.journal_feed_loop.coro(feed)
            channel_a.send.assert_awaited_once()
            channel_b.send.assert_awaited_once()

        # Member 10 opts out in Guild B (still kept active by Admin follow) and opts out in Guild A (last source!).
        opt_b = self._interaction(user_id=10, guild=guild_b)
        await FeedCog.feed_off.callback(feed, opt_b)
        self.assertIn("admin follow", opt_b.followup.send.call_args.args[0].lower())
        self.assertTrue(await self.db.has_guild_admin_follow(2, "millers"))

        opt_a = self._interaction(user_id=10, guild=guild_a)
        await FeedCog.feed_off.callback(feed, opt_a)
        # /feed list in Guild B shows Millers via Admin; Guild A shows PublicOnly via Admin.
        list_b = self._interaction(user_id=99, guild=guild_b)
        await FeedCog.feed_list.callback(feed, list_b)
        self.assertIn("Millers", list_b.followup.send.call_args.kwargs["embed"].description)
        self.assertIn("Admin", list_b.followup.send.call_args.kwargs["embed"].description)

    async def test_legacy_grandfathering_migration_and_restart_isolation(self):
        legacy_path = Path(self.directory.name) / "legacy_pre_v1.db"
        conn = sqlite3.connect(legacy_path)
        conn.executescript("""
            CREATE TABLE linked_users (
                discord_id INTEGER PRIMARY KEY,
                backloggd_username TEXT NOT NULL UNIQUE,
                avatar_url TEXT,
                last_synced_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            CREATE TABLE cached_games (
                slug TEXT PRIMARY KEY,
                title TEXT NOT NULL,
                cover_url TEXT,
                release_year INTEGER,
                community_rating REAL
            );
            CREATE TABLE user_ratings (
                discord_id INTEGER NOT NULL,
                game_slug TEXT NOT NULL,
                rating REAL,
                status TEXT NOT NULL,
                is_liked BOOLEAN DEFAULT 0,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (discord_id, game_slug)
            );
            CREATE TABLE feed_sources (
                guild_id INTEGER NOT NULL,
                discord_id INTEGER NOT NULL,
                source TEXT NOT NULL,
                PRIMARY KEY (guild_id, discord_id, source)
            );
            CREATE TABLE feed_messages (
                guild_id INTEGER NOT NULL,
                discord_id INTEGER NOT NULL,
                event_id TEXT NOT NULL,
                payload TEXT NOT NULL,
                message_id INTEGER,
                channel_id INTEGER,
                PRIMARY KEY (guild_id, discord_id, event_id)
            );
            CREATE TABLE seen_journal_deliveries (
                guild_id INTEGER NOT NULL,
                entry_id TEXT NOT NULL,
                discord_id INTEGER NOT NULL,
                posted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (guild_id, entry_id)
            );
            CREATE TABLE seen_journal_logs (
                entry_id TEXT PRIMARY KEY,
                discord_id INTEGER NOT NULL,
                posted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            CREATE TABLE guild_settings (
                guild_id INTEGER PRIMARY KEY,
                feed_channel_id INTEGER NOT NULL
            );
            INSERT INTO linked_users VALUES (10, 'Millers', NULL, '2026-10-01 10:00:00');
            INSERT INTO guild_settings VALUES (1, 101);
        """)
        conn.commit()
        conn.close()

        legacy_db = Database(str(legacy_path), turso_url="")
        try:
            await legacy_db.connect()
            ver = await legacy_db._fetchone("SELECT MAX(version) AS v FROM schema_migrations")
            self.assertEqual(ver["v"], 7)
            pending = await legacy_db.get_legacy_feed_guilds_pending()
            self.assertEqual(pending, [1])

            # Guild 1 starts with incomplete member cache -> migration stays pending!
            incomplete_guild = SimpleNamespace(
                id=1, name="Legacy", members=[], chunked=False, me=object(),
                get_channel=lambda cid: make_channel(101),
            )
            feed = FeedCog(SimpleNamespace(db=legacy_db, guilds=[incomplete_guild]), clock=lambda: self.now)
            await feed._reconcile_legacy_guild(incomplete_guild)
            self.assertEqual(await legacy_db.get_legacy_feed_guilds_pending(), [1])

            # Authoritative member snapshot arrives -> member 10 grandfathered once; later member 20 is NOT.
            ready_guild = SimpleNamespace(
                id=1, name="Legacy", members=[SimpleNamespace(id=10)], chunked=True, me=object(),
                get_channel=lambda cid: make_channel(101),
            )
            await feed._reconcile_legacy_guild(ready_guild)
            self.assertEqual(await legacy_db.get_legacy_feed_guilds_pending(), [])
            self.assertTrue(await legacy_db.get_guild_feed_preference(1, 10))

            # Restart with member 20 also linked: 20 stays OFF (never grandfathered retroactively).
            await legacy_db.link_user(20, "NewJoiner")
            await feed._reconcile_legacy_guild(SimpleNamespace(
                id=1, name="Legacy", members=[SimpleNamespace(id=10), SimpleNamespace(id=20)], chunked=True,
            ))
            self.assertFalse(await legacy_db.get_guild_feed_preference(1, 20))
        finally:
            await legacy_db.close()

    async def test_backlog_recommendations_outage_recovery_old_rating_and_unresolved_edits(self):
        # 1. Recommendation / roulette exclusions with duplicate profile + numeric similarity.
        await self.db.link_user(10, "Millers")
        await self.db.link_user(11, "millers")
        await self.db.link_user(20, "Twin")
        for idx in range(5):
            await self.db.upsert_game(f"shared-{idx}", f"Shared {idx}")
        await self.db.upsert_game("backlog-Pick", "Backlog Pick")
        await self.db.upsert_game("wishlist-pick", "Wishlist Pick")
        await self.db.upsert_game("twin-gem", "Twin Gem")

        millers_items = [LibItem(f"shared-{i}", 4.5, "played") for i in range(5)] + [
            LibItem("backlog-pick", None, "backlog"),
            LibItem("wishlist-pick", None, "wishlist"),
        ]
        twin_items = [LibItem(f"shared-{i}", 4.5, "played") for i in range(5)] + [
            LibItem("twin-gem", 5.0, "played"),
        ]
        await self.db.replace_profile_library("Millers", millers_items, synced_at=self.now)
        await self.db.replace_profile_library("Twin", twin_items, synced_at=self.now)

        members = [SimpleNamespace(id=10), SimpleNamespace(id=11), SimpleNamespace(id=20)]
        guild = SimpleNamespace(id=1, name="Rec Guild", members=members, chunked=True, me=object())
        bot = SimpleNamespace(db=self.db, backloggd=self.api, library_sync=self.library_sync)
        discovery = DiscoveryCog(bot, clock=lambda: self.now)

        # /roulette only picks backlog-pick or wishlist-pick.
        ir = self._interaction(user_id=10, guild=guild)
        await DiscoveryCog.roulette.callback(discovery, ir)
        self.assertIn("Unplayed Game Roulette", ir.followup.send.call_args.kwargs["embed"].title)

        # /recommend excludes shared games (doesn't self-match member 11) and recommends Twin Gem.
        irec = self._interaction(user_id=10, guild=guild)
        await DiscoveryCog.recommend.callback(discovery, irec)
        rec_embed = irec.followup.send.call_args.kwargs["embed"]
        self.assertTrue(any("Twin Gem" in f.name for f in rec_embed.fields))

        # 2. Old session (8 days old) + independent new rating + unresolved identical edits + bounded outage recovery.
        channel = make_channel(101)
        await self.db.set_feed_channel(1, 101)
        await self.db.set_guild_feed_preference(1, 10, True)
        await self.db.reconcile_guild_feed_activity(1, ["millers"], now=self.now)
        feed = FeedCog(bot, clock=lambda: self.now)

        # Baseline includes an existing session on old-game (used to verify later ambiguous edit detection)
        base_session = FeedEvent(
            "base-old-sess", "Old Game", "old-game", None, "playing", "playing",
            None, None, None, played_date="2026-11-10", occurred_at="2026-11-10T10:00:00Z",
        )
        await feed._publish_sources(1, "millers", "Millers", None, channel, {"activity": [base_session], "journal": []})
        channel.send.assert_not_awaited()

        # First live poll returns an 8-day-old milestone AND an independent rating on that same older game.
        old_session = FeedEvent(
            "old-sess", "Older Game", "older-game", None, "completed", "completed",
            None, None, None, played_date="2026-11-05", occurred_at="2026-11-05T10:00:00Z",
        )
        new_rating = FeedEvent(
            "new-rate", "Older Game", "older-game", None, "rated", "rated",
            4.5, None, None, played_date=None, occurred_at="2026-11-13T12:05:00Z", reference="rating-1",
        )
        await feed._publish_sources(1, "millers", "Millers", None, channel, {"activity": [old_session, new_rating], "journal": []})
        channel.send.assert_awaited_once()
        self.assertIn("rated Older Game", channel.send.call_args.kwargs["embed"].title)

        # Unresolved identical-game edit on baselined old-game (same slug, no stable reference) -> suppressed cleanly while clear new game posts!
        channel.send.reset_mock()
        ambiguous = FeedEvent(
            "ambig-sess", "Old Game", "old-game", None, "completed", "completed",
            None, None, None, played_date="2026-11-13", occurred_at="2026-11-13T12:10:00Z",
        )
        clear_new = FeedEvent(
            "clear-new", "Balatro", "balatro", None, "playing", "playing",
            None, None, None, played_date="2026-11-13", occurred_at="2026-11-13T12:10:00Z",
        )
        await feed._publish_sources(1, "millers", "Millers", None, channel, {"activity": [ambiguous, clear_new], "journal": []})
        channel.send.assert_awaited_once()
        self.assertIn("Balatro", channel.send.call_args.kwargs["embed"].title)
        ambig_row = await self.db._fetchone(
            "SELECT decision FROM profile_feed_decisions WHERE guild_id = 1 AND profile_key = 'millers' AND event_id = 'ambig-sess'"
        )
        self.assertEqual(ambig_row["decision"], "held_ambiguous")

        # Outage gap recording & closure on boundary_reached:
        await self.db.record_feed_source_failure("millers", "activity", "503 outage")
        await self.db.record_feed_source_success(
            "millers", "activity", coverage="limited", pages_read=5, gap_until="2026-11-13T10:00:00Z"
        )
        status = await self.db.get_guild_profile_feed_status(1, "millers")
        act_health = next(r for r in status["source_health"] if r["source"] == "activity")
        self.assertIsNotNone(act_health["unresolved_gap_since"])
        await self.db.record_feed_source_success(
            "millers", "activity", coverage="boundary_reached", pages_read=2, boundary_event_id="new-rate"
        )
        status2 = await self.db.get_guild_profile_feed_status(1, "millers")
        act_health2 = next(r for r in status2["source_health"] if r["source"] == "activity")
        self.assertIsNone(act_health2["unresolved_gap_since"])

    async def test_unlink_during_shared_sync_and_relink_association_guard(self):
        # Two members share Millers: Member 10 unlinks mid-sync -> Member 11 keeps subscription alive -> library commits.
        await self.db.link_user(10, "Millers")
        await self.db.link_user(11, "millers")

        started = asyncio.Event()
        release = asyncio.Event()

        async def slow_lib(username, force_refresh=False):
            started.set()
            await release.wait()
            return [LibItem("elden-ring", 5.0, "played", "Elden Ring")]

        self.api.get_user_library.side_effect = slow_lib
        bot = SimpleNamespace(db=self.db, backloggd=self.api, library_sync=self.library_sync)
        account = AccountCog(bot)

        task = account._start_sync_task(10, "Millers")
        await asyncio.wait_for(started.wait(), timeout=1.0)
        await self.db.unlink_user(10)
        release.set()
        await task

        self.assertEqual(await self.db.get_profile_ratings_dict("Millers"), {"elden-ring": 5.0})

        # Single member unlinks last link to SoloA during sync -> stale sync result is discarded cleanly!
        await self.db.link_user(30, "SoloA")
        started2 = asyncio.Event()
        release2 = asyncio.Event()

        async def slow_solo(username, force_refresh=False):
            started2.set()
            await release2.wait()
            return [LibItem("hades", 4.0, "played", "Hades")]

        self.api.get_user_library.side_effect = slow_solo
        task2 = account._start_sync_task(30, "SoloA")
        await asyncio.wait_for(started2.wait(), timeout=1.0)
        await self.db.unlink_user(30)
        await self.db.link_user(30, "SoloB")
        release2.set()
        await task2

        self.assertEqual(await self.db.get_profile_ratings_dict("SoloA"), {})
        self.assertEqual(await self.db.get_profile_ratings_dict("SoloB"), {})


if __name__ == "__main__":
    unittest.main()
