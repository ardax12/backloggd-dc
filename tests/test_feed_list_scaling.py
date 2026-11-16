"""Regression tests for Task 4: bulk feed status retrieval and /feed list query scaling."""

import os

os.environ["PYTHON_DOTENV_DISABLED"] = "1"
os.environ["TURSO_DATABASE_URL"] = ""
os.environ["TURSO_AUTH_TOKEN"] = ""
os.environ["DEV_GUILD_ID"] = ""

import asyncio
from datetime import datetime, timezone
from pathlib import Path
import sqlite3
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, MagicMock

import discord

from bot.cogs.feed import FeedCog
from bot.db import Database
from bot.ui.views import FeedListView
from tests.test_bounded_sql_lookups import BudgetTursoClient
from tests.test_interaction_visibility_protocol import StatefulDiscordInteraction


class _CountingBudgetTursoClient(BudgetTursoClient):
    def __init__(self, sqlite_path: str, *, max_params: int = 800):
        super().__init__(sqlite_path, max_params=max_params)
        self.execute_calls = 0

    async def execute(self, query, params=None):
        self.execute_calls += 1
        return await super().execute(query, params)


def _normalize_status_for_compare(status: dict) -> dict:
    return {
        "active": bool(status["active"]),
        "last_active_at": status["last_active_at"],
        "initialized_sources": list(status["initialized_sources"]),
        "suppression_counts": dict(status["suppression_counts"]),
        "delivery_counts": dict(status["delivery_counts"]),
        "source_health": [
            {k: row[k] for k in row.keys() if k != "profile_key"}
            for row in status["source_health"]
        ],
    }


class FeedListScalingTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmpdir.cleanup)
        self.db_path = Path(self.tmpdir.name) / "feed_status.db"
        self.db = Database(str(self.db_path), turso_url="")
        await self.db.connect()
        await self.db._conn._execute(
            self.db._conn._conn.setlimit, sqlite3.SQLITE_LIMIT_VARIABLE_NUMBER, 999
        )
        self.addAsyncCleanup(self.db.close)

    async def _seed_parity_fixture(self, db: Database):
        # Active profile with all delivery states, policy decisions, and source health gaps
        await db.link_user(10, "Alice", None)
        await db.set_feed_channel(1, 50)
        await db.set_guild_feed_preference(1, 10, True)
        await db.reconcile_guild_feed_activity(1, ["alice"], now=datetime(2026, 11, 10, tzinfo=timezone.utc))
        await db.seed_feed_source(1, "alice", "activity-display-v1", ["base-1"])
        await db.seed_feed_source(1, "alice", "journal-display-v1", ["jbase-1"])
        await db.record_feed_source_success(
            "alice", "activity", coverage="truncated", pages_read=2,
            gap_until=datetime(2026, 11, 12, tzinfo=timezone.utc),
        )
        await db.record_feed_source_failure("alice", "journal", "timeout 504")
        for idx, dec in enumerate(
            ("eligible", "baseline", "suppressed_old", "suppressed_pre_activation",
             "held_uncertain", "held_future", "held_ambiguous")
        ):
            await db.record_feed_event_decision(
                1, "alice", f"ev-{idx}", source="activity", event_date="2026-11-11",
                decision=dec, reason="test", payload="{}",
            )
        await db.seed_feed_message(1, "alice", ["m-base"], "{}")
        await db.begin_feed_delivery(1, "alice", ["m-pend"], "{}", 50)
        await db.begin_feed_delivery(1, "alice", ["m-sent"], "{}", 50)
        await db.store_feed_message(1, "alice", ["m-sent"], "{}", message_id=101, channel_id=50)
        await db.begin_feed_delivery(1, "alice", ["m-unk"], "{}", 50)
        await db.mark_feed_delivery_unknown(1, "alice", ["m-unk"])
        await db.begin_feed_delivery(1, "alice", ["m-del"], "{}", 50)
        await db.mark_feed_message_deleted(1, "alice", ["m-del"], "{}")

        # Inactive profile with no baselines in guild 1, but active in guild 2 (cross-guild isolation!)
        await db.link_user(20, "Bob", None)
        await db.set_feed_channel(2, 60)
        await db.set_guild_feed_preference(2, 20, True)
        await db.reconcile_guild_feed_activity(2, ["bob"], now=datetime(2026, 11, 10, tzinfo=timezone.utc))
        await db.seed_feed_source(2, "bob", "activity-display-v1", ["g2-base"])
        await db.record_feed_event_decision(
            2, "bob", "g2-ev", source="activity", event_date="2026-11-11",
            decision="suppressed_old", reason="test", payload="{}",
        )
        await db.record_feed_source_success("bob", "activity", coverage="exhausted", pages_read=1)

    async def test_bulk_status_matches_singular_and_isolates_guilds_both_backends(self):
        turso_path = Path(self.tmpdir.name) / "parity_turso.db"
        seed = Database(str(turso_path), turso_url="")
        await seed.connect()
        await seed.close()
        client = _CountingBudgetTursoClient(str(turso_path), max_params=800)
        turso_db = Database(str(turso_path), turso_url="https://feed-status-test.turso.io")
        turso_db._client = client
        self.addAsyncCleanup(turso_db.close)

        for label, db in (("sqlite", self.db), ("turso", turso_db)):
            with self.subTest(backend=label):
                await self._seed_parity_fixture(db)
                self.assertEqual(await db.get_guild_feed_statuses(1, []), {})

                keys = ["Alice", "alice", " ALICE ", "", "bob", "absent_user"]
                bulk = await db.get_guild_feed_statuses(1, keys)
                self.assertEqual(set(bulk.keys()), {"alice", "bob", "absent_user"})

                for key in ("alice", "bob", "absent_user"):
                    singular = await db.get_guild_profile_feed_status(1, key)
                    self.assertEqual(
                        _normalize_status_for_compare(bulk[key]),
                        _normalize_status_for_compare(singular),
                    )

                # Guild 1 must not see guild 2's activity/sources/decisions for bob,
                # while source_health remains profile-scoped.
                self.assertFalse(bulk["bob"]["active"])
                self.assertEqual(bulk["bob"]["initialized_sources"], [])
                self.assertEqual(bulk["bob"]["suppression_counts"], {})
                self.assertEqual(len(bulk["bob"]["source_health"]), 1)

                guild2 = await db.get_guild_feed_statuses(2, ["bob"])
                self.assertTrue(guild2["bob"]["active"])
                self.assertEqual(guild2["bob"]["initialized_sources"], ["activity-display-v1"])

    async def test_bulk_status_query_counts_scale_by_chunks_on_sqlite_and_turso(self):
        turso_path = Path(self.tmpdir.name) / "counts_turso.db"
        seed = Database(str(turso_path), turso_url="")
        await seed.connect()
        await seed.close()
        client = _CountingBudgetTursoClient(str(turso_path), max_params=800)
        turso_db = Database(str(turso_path), turso_url="https://feed-counts-test.turso.io")
        turso_db._client = client
        self.addAsyncCleanup(turso_db.close)

        # Seed 850 profiles
        keys = [f"player_{i:04d}" for i in range(850)]
        for db in (self.db, turso_db):
            profiles = [(k, k.title(), None) for k in keys]
            act = [(1, k, 1) for k in keys]
            if db.is_turso:
                conn = db._client.conn
                conn.executemany(
                    "INSERT INTO profiles (profile_key, display_username, avatar_url) VALUES (?, ?, ?)",
                    profiles,
                )
                conn.executemany(
                    "INSERT INTO guild_profile_feed_activity (guild_id, profile_key, active) VALUES (?, ?, ?)",
                    act,
                )
                conn.commit()
            else:
                await db._conn.executemany(
                    "INSERT INTO profiles (profile_key, display_username, avatar_url) VALUES (?, ?, ?)",
                    profiles,
                )
                await db._conn.executemany(
                    "INSERT INTO guild_profile_feed_activity (guild_id, profile_key, active) VALUES (?, ?, ?)",
                    act,
                )
                await db._conn.commit()

        # SQLite query counts via _fetchall wrapper
        sqlite_queries = 0
        orig_sqlite_fetchall = self.db._fetchall
        max_sqlite_params = 0

        async def counted_sqlite_fetchall(query, params=()):
            nonlocal sqlite_queries, max_sqlite_params
            sqlite_queries += 1
            max_sqlite_params = max(max_sqlite_params, len(params))
            return await orig_sqlite_fetchall(query, params)

        self.db._fetchall = counted_sqlite_fetchall
        try:
            for count, expected_max_queries in (
                (0, 0),
                (1, 5),
                (100, 5),
                (800, 10),
                (801, 10),
                (850, 10),
            ):
                sqlite_queries = 0
                res = await self.db.get_guild_feed_statuses(1, keys[:count])
                self.assertEqual(len(res), count)
                self.assertLessEqual(sqlite_queries, expected_max_queries, f"count={count}")
            self.assertLessEqual(max_sqlite_params, 800)
        finally:
            self.db._fetchall = orig_sqlite_fetchall

        # Turso query counts via BudgetTursoClient
        client.max_params_seen = 0
        for count, expected_max_queries in (
            (0, 0),
            (1, 5),
            (100, 5),
            (800, 10),
            (801, 10),
            (850, 10),
        ):
            client.execute_calls = 0
            res = await turso_db.get_guild_feed_statuses(1, keys[:count])
            self.assertEqual(len(res), count)
            self.assertLessEqual(client.execute_calls, expected_max_queries, f"turso count={count}")
        self.assertLessEqual(client.max_params_seen, 800)

    async def test_feed_list_end_to_end_bounded_queries_and_no_lock_held_across_send(self):
        # 100 admin-followed profiles -> 5 pages of 20
        for i in range(100):
            name = f"Gamer{i:03d}"
            await self.db.add_guild_admin_follow(1, name, None, 99)
        await self.db.set_feed_channel(1, 50)
        await self.db.reconcile_guild_feed_activity(1, [f"gamer{i:03d}" for i in range(50)])
        await self.db.seed_feed_source(1, "gamer000", "activity-display-v1", ["a1"])
        await self.db.seed_feed_source(1, "gamer000", "journal-display-v1", ["j1"])
        await self.db.record_feed_source_success("gamer000", "activity", coverage="exhausted", pages_read=1)
        await self.db.record_feed_source_failure("gamer001", "activity", "503 unavailable")

        channel_mock = MagicMock(spec=discord.TextChannel)
        channel_mock.id = 50
        channel_mock.permissions_for = lambda me: discord.Permissions(
            view_channel=True, send_messages=True, embed_links=True
        )
        guild = SimpleNamespace(
            id=1, name="Guild", chunked=True, member_count=1,
            members=[SimpleNamespace(id=10, display_name="Admin")],
            get_channel=lambda cid: channel_mock if cid == 50 else None,
            me=SimpleNamespace(id=999),
        )
        bot = SimpleNamespace(db=self.db, backloggd=MagicMock())
        cog = FeedCog(bot, clock=lambda: datetime(2026, 11, 13, tzinfo=timezone.utc))

        query_count = 0
        orig_fetchall = self.db._fetchall
        orig_fetchone = self.db._fetchone

        async def count_fetchall(*args, **kwargs):
            nonlocal query_count
            query_count += 1
            return await orig_fetchall(*args, **kwargs)

        async def count_fetchone(*args, **kwargs):
            nonlocal query_count
            query_count += 1
            return await orig_fetchone(*args, **kwargs)

        self.db._fetchall = count_fetchall
        self.db._fetchone = count_fetchone

        send_started = asyncio.Event()
        release_send = asyncio.Event()
        interaction = StatefulDiscordInteraction(name="feed", guild=guild, user_id=10, manage_channels=True)
        orig_followup_send = interaction.followup.send

        async def held_followup_send(*args, **kwargs):
            send_started.set()
            await release_send.wait()
            return await orig_followup_send(*args, **kwargs)

        interaction.followup.send = held_followup_send

        task = asyncio.create_task(FeedCog.feed_list.callback(cog, interaction))
        try:
            await asyncio.wait_for(send_started.wait(), timeout=5.0)

            # While Discord send is in flight, DB lock must NOT be held!
            self.assertFalse(self.db._operation_lock.locked())
            self.assertIsNotNone(await self.db.get_feed_channel(1))

            release_send.set()
            await asyncio.wait_for(task, timeout=5.0)
        finally:
            if not task.done():
                task.cancel()
                try:
                    await task
                except BaseException:
                    pass
            self.db._fetchall = orig_fetchall
            self.db._fetchone = orig_fetchone

        # Before Task 4: 100 * 5 = 500+ queries; after Task 4: <= 12 queries (candidates + channel + 5 status + concurrent check)
        self.assertLessEqual(query_count, 15, f"Expected bounded feed_list queries, got {query_count}")

        ephem = interaction.visible_ephemeral_messages()
        self.assertEqual(len(ephem), 1)
        view = ephem[0]["view"]
        self.assertIsInstance(view, FeedListView)
        self.addCleanup(view.stop)
        self.assertEqual(len(view.pages), 5)
        self.assertIn("**Gamer000** — Admin · Active", view.pages[0].description)
        self.assertIn("**Gamer001** — Admin · Degraded (pending baseline)", view.pages[0].description)
        self.assertEqual(view.pages[0].footer.text, "1–20 of 100 profiles")

        # Pagination + owner checks
        unauth = StatefulDiscordInteraction(
            interaction_type=discord.InteractionType.component,
            source_message=ephem[0],
            guild=guild,
            user_id=99,
        )
        await view._scheduled_task(view.next_button, unauth)
        self.assertEqual(view.page, 0)
        self.assertEqual(len(unauth.visible_ephemeral_messages()), 1)

        ok_next = StatefulDiscordInteraction(
            interaction_type=discord.InteractionType.component,
            source_message=ephem[0],
            guild=guild,
            user_id=10,
        )
        await view._scheduled_task(view.next_button, ok_next)
        self.assertEqual(view.page, 1)
        self.assertEqual(ephem[0]["embed"].footer.text, "21–40 of 100 profiles")


if __name__ == "__main__":
    unittest.main()
