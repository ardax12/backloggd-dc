"""Request-scoped sync metadata and bounded subscription lookup regressions."""

import asyncio
import os
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

os.environ["PYTHON_DOTENV_DISABLED"] = "1"
os.environ["TURSO_DATABASE_URL"] = ""
os.environ["TURSO_AUTH_TOKEN"] = ""
os.environ["DEV_GUILD_ID"] = ""

from bot.db import Database
from bot.services.freshness import ensure_profiles_fresh, resolve_guild_aggregate_freshness
from bot.services.library_sync import LibrarySyncService, schedule_profile_refresh


class FreshnessMetadataReuseTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.db = Database(str(Path(self.tmpdir.name) / "freshness-reuse.db"), turso_url="")
        await self.db.connect()
        self.now = datetime(2026, 11, 16, 12, 0, tzinfo=timezone.utc)

    async def asyncTearDown(self):
        await self.db.close()
        self.tmpdir.cleanup()

    async def test_stale_profiles_reuse_sync_rows_and_batch_unknown_subscriptions(self):
        for discord_id, username in enumerate(
            ("Stale0", "Stale1", "Stale2", "Fresh", "Backoff", "Debounce"), start=10,
        ):
            await self.db.link_user(discord_id, username)
        await self.db.replace_profile_library(
            "Fresh", [], synced_at=self.now - timedelta(minutes=10),
        )
        await self.db.replace_profile_library(
            "Debounce", [], synced_at=self.now - timedelta(hours=2),
        )
        await self.db.record_profile_sync_failure(
            "Backoff", "offline", attempted_at=self.now - timedelta(seconds=30),
        )
        await self.db.add_guild_admin_follow(1, "AdminOnly", None, 99)
        await self.db._execute(
            "INSERT INTO profiles (profile_key, display_username) VALUES (?, ?)",
            ("liveunlinked", "LiveUnlinked"),
        )

        started = asyncio.Event()
        release = asyncio.Event()
        requested = []

        async def blocked_library(username, force_refresh=False):
            requested.append(username)
            started.set()
            await release.wait()
            return []

        service = LibrarySyncService(
            max_workers=1, automatic_debounce_seconds=300,
            failure_backoff_seconds=300, clock=lambda: 0.0,
        )
        service.seed_success("debounce", age_seconds=7200, game_count=0)
        service._profiles["debounce"].last_attempt_at = -30.0
        bot = SimpleNamespace(
            db=self.db,
            library_sync=service,
            backloggd=SimpleNamespace(get_user_library=AsyncMock(side_effect=blocked_library)),
        )
        queries = []
        fetchall = self.db._fetchall
        fetchone = self.db._fetchone

        async def tracked_fetchall(query, params=()):
            queries.append(query)
            return await fetchall(query, params)

        async def tracked_fetchone(query, params=()):
            queries.append(query)
            return await fetchone(query, params)

        self.db._fetchall = tracked_fetchall
        self.db._fetchone = tracked_fetchone
        profiles = [
            (key, name) for key, name in (
                ("stale0", "Stale0"),
                ("stale1", "Stale1"),
                ("stale2", "Stale2"),
                ("fresh", "Fresh"),
                ("backoff", "Backoff"),
                ("debounce", "Debounce"),
                ("adminonly", "AdminOnly"),
                ("liveunlinked", "LiveUnlinked"),
            )
        ]
        try:
            statuses = await ensure_profiles_fresh(bot, profiles, now=self.now)
            await asyncio.wait_for(started.wait(), timeout=2.0)

            normalized_queries = [" ".join(query.lower().split()) for query in queries]
            single_sync = [
                query for query in normalized_queries
                if "count(profile_ratings.game_slug)" in query
                and "where profiles.profile_key = lower(trim(?)" in query
            ]
            batch_sync = [
                query for query in normalized_queries
                if "count(profile_ratings.game_slug)" in query
                and "where profiles.profile_key in (" in query
            ]
            single_subscription = [
                query for query in normalized_queries
                if "select 1 from linked_users" in query
            ]
            batch_subscription = [
                query for query in normalized_queries
                if "select distinct profile_key from linked_users" in query
            ]

            self.assertEqual(
                (len(single_sync), len(single_subscription)), (0, 0),
                "expected one request-scoped sync fetch and bounded subscription check, "
                f"got {len(single_sync)} profile sync reads and {len(single_subscription)} per-profile subscription reads",
            )
            self.assertEqual(len(batch_sync), 1)
            self.assertEqual(len(batch_subscription), 1)
            self.assertEqual(statuses["fresh"].stale, False)
            self.assertTrue(statuses["backoff"].failed_last)
            self.assertFalse(statuses["backoff"].refreshing)
            self.assertTrue(statuses["debounce"].stale)
            self.assertFalse(statuses["debounce"].refreshing)
            self.assertFalse(statuses["adminonly"].refreshing)
            self.assertFalse(statuses["liveunlinked"].refreshing)
            self.assertEqual(requested, ["Stale0"])
        finally:
            release.set()
            await service.close()

    async def test_batch_subscription_map_is_bounded_and_marks_only_linked_profiles(self):
        keys = [f"profile-{index:04d}" for index in range(805)]
        await self.db._conn.executemany(
            "INSERT INTO profiles (profile_key, display_username) VALUES (?, ?)",
            [(key, key) for key in keys],
        )
        await self.db._conn.executemany(
            "INSERT INTO linked_users (discord_id, profile_key, backloggd_username) VALUES (?, ?, ?)",
            [(10, keys[0], keys[0]), (11, keys[-1], keys[-1])],
        )
        await self.db._conn.commit()

        queries = []
        fetchall = self.db._fetchall

        async def tracked_fetchall(query, params=()):
            queries.append((query, params))
            return await fetchall(query, params)

        self.db._fetchall = tracked_fetchall
        result = await self.db.get_profiles_library_subscription_map(keys)

        self.assertEqual(len(result), 805)
        self.assertTrue(result[keys[0]])
        self.assertTrue(result[keys[-1]])
        self.assertFalse(result[keys[400]])
        self.assertEqual(len(queries), 2)
        self.assertEqual([len(params) for _, params in queries], [800, 5])

        # The freshness path also scales each lookup by these same bounded chunks.
        await self.db._execute("DELETE FROM linked_users", ())
        queries.clear()
        service = LibrarySyncService(clock=lambda: 0.0)
        try:
            statuses = await ensure_profiles_fresh(
                SimpleNamespace(db=self.db, library_sync=service),
                [(key, key) for key in keys],
                now=datetime(2026, 11, 16, 12, 0, tzinfo=timezone.utc),
            )
            self.assertEqual(len(statuses), len(keys))
            normalized = [" ".join(query.lower().split()) for query, _ in queries]
            self.assertEqual(sum(
                "count(profile_ratings.game_slug)" in query
                and "where profiles.profile_key in (" in query
                for query in normalized
            ), 2)
            self.assertEqual(sum(
                "select distinct profile_key from linked_users" in query
                for query in normalized
            ), 2)
            self.assertFalse(any(
                "count(profile_ratings.game_slug)" in query
                and "where profiles.profile_key = lower(trim(?)" in query
                for query in normalized
            ))
            self.assertFalse(any("select 1 from linked_users" in query for query in normalized))
        finally:
            await service.close()

    async def test_guild_profile_context_reuses_known_linked_subscription_evidence(self):
        await self.db.link_user(10, "GuildLinked")
        profiles = await self.db.get_guild_linked_profiles_with_sync_info([10])
        started = asyncio.Event()
        release = asyncio.Event()

        async def blocked_library(username, force_refresh=False):
            started.set()
            await release.wait()
            return []

        service = LibrarySyncService(clock=lambda: 0.0)
        bot = SimpleNamespace(
            db=self.db,
            library_sync=service,
            backloggd=SimpleNamespace(get_user_library=AsyncMock(side_effect=blocked_library)),
        )
        queries = []
        fetchall = self.db._fetchall
        fetchone = self.db._fetchone

        async def tracked_fetchall(query, params=()):
            queries.append(query)
            return await fetchall(query, params)

        async def tracked_fetchone(query, params=()):
            queries.append(query)
            return await fetchone(query, params)

        self.db._fetchall = tracked_fetchall
        self.db._fetchone = tracked_fetchone
        try:
            await resolve_guild_aggregate_freshness(
                bot, [10], profiles=profiles, profiles_are_linked=True, now=self.now,
            )
            await asyncio.wait_for(started.wait(), timeout=2.0)
            normalized = [" ".join(query.lower().split()) for query in queries]
            self.assertFalse(any("select 1 from linked_users" in query for query in normalized))
            self.assertFalse(any(
                "select distinct profile_key from linked_users" in query
                for query in normalized
            ))
            self.assertFalse(any(
                "count(profile_ratings.game_slug)" in query
                for query in normalized
            ))
        finally:
            release.set()
            await service.close()

    async def test_supplied_none_skips_metadata_lookup_and_unsupplied_remains_compatible(self):
        sync_info_getter = AsyncMock(return_value=None)

        class Service:
            def seed_from_sync_info(self, *args, **kwargs):
                return None

            def request(self, *args, **kwargs):
                return None

        bot = SimpleNamespace(
            db=SimpleNamespace(
                get_profile_sync_info=sync_info_getter,
                is_profile_library_subscribed=AsyncMock(return_value=True),
            ),
            library_sync=Service(),
        )

        await schedule_profile_refresh(
            bot, "Millers", now=self.now, sync_info=None,
        )
        sync_info_getter.assert_not_awaited()
        await schedule_profile_refresh(bot, "Millers", now=self.now)
        sync_info_getter.assert_awaited_once_with("millers")


if __name__ == "__main__":
    unittest.main()
