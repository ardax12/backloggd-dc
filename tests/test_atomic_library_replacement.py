"""Regression tests for atomic, bounded-batch library replacement on SQLite and Turso."""

import asyncio
import json
import os
import sqlite3
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

os.environ["PYTHON_DOTENV_DISABLED"] = "1"
os.environ["TURSO_DATABASE_URL"] = ""
os.environ["TURSO_AUTH_TOKEN"] = ""
os.environ["DEV_GUILD_ID"] = ""

from bot.db import Database


class AtomicTursoClient:
    """Turso client double enforcing atomic batch transactions, bind limits, and SQL size."""

    def __init__(
        self,
        sqlite_path: str,
        *,
        max_params: int = 800,
        max_payload_bytes: int = 65536,
        fail_on_statement_index: int | None = None,
    ):
        self.conn = sqlite3.connect(sqlite_path)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA foreign_keys = ON")
        self.max_params = max_params
        self.max_payload_bytes = max_payload_bytes
        self.fail_on_statement_index = fail_on_statement_index
        self.closed = False
        self.batch_calls = 0
        self.total_statements_seen = 0
        self.max_statements_in_batch = 0
        self.max_params_seen = 0
        self.max_payload_bytes_seen = 0

    def _validate(self, query: str, params) -> list:
        values = list(params or ())
        count = len(values)
        self.max_params_seen = max(self.max_params_seen, count)
        if count > self.max_params:
            raise sqlite3.OperationalError(f"Bind limit exceeded: {count} > {self.max_params}")
        payload_bytes = len(query.encode("utf-8")) + len(json.dumps(values, default=str).encode("utf-8"))
        self.max_payload_bytes_seen = max(self.max_payload_bytes_seen, payload_bytes)
        if payload_bytes > self.max_payload_bytes:
            raise sqlite3.OperationalError(
                f"Statement payload limit exceeded: {payload_bytes} > {self.max_payload_bytes}"
            )
        return values

    def transaction(self):
        client = self

        class Tx:
            async def execute(self, query, params=None):
                values = client._validate(query, params)
                cursor = client.conn.execute(query, values)
                rows = cursor.fetchall() if cursor.description else []
                return SimpleNamespace(rows=rows, rows_affected=cursor.rowcount)

            async def commit(self):
                client.conn.commit()

            async def rollback(self):
                client.conn.rollback()

        return Tx()

    async def execute(self, query, params=None):
        values = self._validate(query, params)
        cursor = self.conn.execute(query, values)
        rows = cursor.fetchall() if cursor.description else []
        self.conn.commit()
        return SimpleNamespace(rows=rows, rows_affected=cursor.rowcount)

    async def batch(self, statements):
        self.batch_calls += 1
        self.max_statements_in_batch = max(self.max_statements_in_batch, len(statements))
        self.total_statements_seen += len(statements)
        results = []
        self.conn.execute("BEGIN IMMEDIATE")
        try:
            for idx, (query, params) in enumerate(statements):
                if self.fail_on_statement_index is not None and idx == self.fail_on_statement_index:
                    raise sqlite3.OperationalError(f"Injected failure on batch statement {idx}")
                values = self._validate(query, params)
                cursor = self.conn.execute(query, values)
                rows = cursor.fetchall() if cursor.description else []
                results.append(SimpleNamespace(rows=rows, rows_affected=cursor.rowcount))
            self.conn.commit()
        except BaseException:
            self.conn.rollback()
            raise
        return results

    async def close(self):
        self.closed = True
        self.conn.close()


def _make_game(
    index: int,
    *,
    rating: float | None = 4.0,
    status: str = "played",
    title: str | None = None,
    cover_url: str | None = "https://images.igdb.com/cover.jpg",
    release_year: int | None = 2022,
    community_rating: float | None = 4.2,
    is_liked: bool = False,
):
    return SimpleNamespace(
        slug=f"game-{index:04d}",
        title=title if title is not None else f"Game {index:04d}",
        cover_url=cover_url,
        release_year=release_year,
        community_rating=community_rating,
        rating=rating,
        status=status,
        is_liked=is_liked,
    )


class AtomicLibraryReplacementTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmpdir.cleanup)
        self.sqlite_path = Path(self.tmpdir.name) / "sqlite.db"
        self.db = Database(str(self.sqlite_path), turso_url="")
        self.addAsyncCleanup(self.db.close)
        await self.db.connect()
        await self.db._conn._execute(
            self.db._conn._conn.setlimit, sqlite3.SQLITE_LIMIT_VARIABLE_NUMBER, 999
        )

    async def _make_turso_db(self, name: str = "turso.db", **client_kwargs):
        path = str(Path(self.tmpdir.name) / name)
        seed_db = Database(path, turso_url="")
        await seed_db.connect()
        await seed_db.close()
        client = AtomicTursoClient(path, **client_kwargs)
        turso_db = Database(path, turso_url="https://atomic-test.turso.io", turso_token="tok")
        turso_db._client = client
        self.addAsyncCleanup(turso_db.close)
        return turso_db, client

    async def test_sqlite_large_library_batches_writes_and_preserves_coalesce_null_zero(self):
        await self.db.link_user(10, "Millers")
        await self.db.upsert_game(
            "game-0000",
            "Old Game 0",
            cover_url="https://images.igdb.com/existing-cover.jpg",
            release_year=2019,
            community_rating=3.9,
        )

        games = [
            _make_game(
                0,
                rating=0.0,
                status="played",
                title="Updated Game 0",
                cover_url=None,
                release_year=None,
                community_rating=None,
                is_liked=True,
            ),
            _make_game(1, rating=None, status="backlog"),
        ] + [_make_game(i, rating=4.5, status="played") for i in range(2, 2100)]

        sql_calls = 0
        orig_execute = self.db._conn.execute
        orig_executemany = self.db._conn.executemany

        def counted_execute(*args, **kwargs):
            nonlocal sql_calls
            sql_calls += 1
            return orig_execute(*args, **kwargs)

        def counted_executemany(*args, **kwargs):
            nonlocal sql_calls
            sql_calls += 1
            return orig_executemany(*args, **kwargs)

        self.db._conn.execute = counted_execute
        self.db._conn.executemany = counted_executemany
        try:
            t1 = datetime(2026, 11, 12, 10, 0, tzinfo=timezone.utc)
            ok = await self.db.replace_profile_library("Millers", games, synced_at=t1)
        finally:
            self.db._conn.execute = orig_execute
            self.db._conn.executemany = orig_executemany

        self.assertTrue(ok)
        # Batched SQLite writes must not issue one statement per game.
        self.assertLessEqual(sql_calls, 25)

        ratings = await self.db.get_profile_ratings_dict("millers")
        self.assertEqual(len(ratings), 2100)
        self.assertEqual(ratings["game-0000"], 0.0)
        self.assertIsNone(ratings["game-0001"])

        cached = await self.db.get_games_by_slugs(["game-0000"])
        self.assertEqual(cached["game-0000"]["title"], "Updated Game 0")
        self.assertEqual(cached["game-0000"]["cover_url"], "https://images.igdb.com/existing-cover.jpg")
        self.assertEqual(cached["game-0000"]["release_year"], 2019)
        self.assertEqual(cached["game-0000"]["community_rating"], 3.9)

        # Empty library clears previous ratings cleanly in one transaction.
        t2 = datetime(2026, 11, 12, 11, 0, tzinfo=timezone.utc)
        self.assertTrue(await self.db.replace_profile_library("Millers", [], synced_at=t2))
        self.assertEqual(await self.db.get_profile_ratings_dict("millers"), {})
        info = await self.db.get_profile_sync_info("millers")
        self.assertIn("11:00:00", info["last_synced_at"])

    async def test_turso_large_and_wide_library_bounds_params_payload_and_statements(self):
        turso_db, client = await self._make_turso_db("turso_bounds.db")
        await turso_db.link_user(10, "Millers")
        client.batch_calls = 0
        client.max_statements_in_batch = 0
        client.max_params_seen = 0
        client.max_payload_bytes_seen = 0

        games = [
            _make_game(i, title=("Wide " + ("X" * 3500)) if i < 40 else f"Game {i:04d}")
            for i in range(2050)
        ]
        ok = await turso_db.replace_profile_library(
            "Millers",
            games,
            synced_at=datetime(2026, 11, 12, 12, 0, tzinfo=timezone.utc),
        )
        self.assertTrue(ok)
        self.assertEqual(client.batch_calls, 1)
        self.assertLessEqual(client.max_params_seen, 800)
        self.assertLessEqual(client.max_payload_bytes_seen, 65536)
        self.assertLessEqual(client.max_statements_in_batch, 80)
        self.assertEqual(len(await turso_db.get_profile_ratings_dict("millers")), 2050)

    async def test_mid_chunk_failure_and_cancellation_roll_back_atomically(self):
        # Baseline on SQLite
        await self.db.link_user(10, "Millers")
        await self.db.set_profile_refresh_pending("millers", "tok-initial")
        t0 = datetime(2026, 11, 12, 9, 0, tzinfo=timezone.utc)
        initial = [_make_game(i, title=f"Old {i}", rating=3.5) for i in range(3)]
        self.assertTrue(await self.db.replace_profile_library("Millers", initial, synced_at=t0))

        # SQLite mid-chunk cancellation after cached_games executemany starts
        orig_executemany = self.db._conn.executemany
        call_num = 0

        async def cancel_on_ratings(*args, **kwargs):
            nonlocal call_num
            call_num += 1
            if call_num == 2:
                raise asyncio.CancelledError()
            return await orig_executemany(*args, **kwargs)

        new_games = [_make_game(i + 100, title=f"New {i}", rating=5.0) for i in range(1000)]
        self.db._conn.executemany = cancel_on_ratings
        try:
            with self.assertRaises(asyncio.CancelledError):
                await self.db.replace_profile_library(
                    "Millers",
                    new_games,
                    synced_at=datetime(2026, 11, 12, 10, 0, tzinfo=timezone.utc),
                    clear_pending_token="tok-initial",
                )
        finally:
            self.db._conn.executemany = orig_executemany

        self.assertEqual(
            await self.db.get_profile_ratings_dict("millers"),
            {"game-0000": 3.5, "game-0001": 3.5, "game-0002": 3.5},
        )
        self.assertEqual(await self.db.get_games_by_slugs(["game-0100"]), {})
        info = await self.db.get_profile_sync_info("millers")
        self.assertIn("09:00:00", info["last_synced_at"])
        self.assertEqual(await self.db.get_profile_refresh_pending_token("millers"), "tok-initial")

        # Turso mid-batch failure (after cached_games & delete, inside profile_ratings chunking)
        turso_db, client = await self._make_turso_db("turso_rollback.db")
        await turso_db.link_user(10, "Millers")
        await turso_db.set_profile_refresh_pending("millers", "tok-initial")
        self.assertTrue(await turso_db.replace_profile_library("Millers", initial, synced_at=t0))

        # Force failure on a late statement inside the batch (e.g. last chunk / clear step).
        client.fail_on_statement_index = 6
        with self.assertRaises(sqlite3.OperationalError):
            await turso_db.replace_profile_library(
                "Millers",
                new_games,
                synced_at=datetime(2026, 11, 12, 10, 0, tzinfo=timezone.utc),
                clear_pending_token="tok-initial",
            )
        self.assertEqual(
            await turso_db.get_profile_ratings_dict("millers"),
            {"game-0000": 3.5, "game-0001": 3.5, "game-0002": 3.5},
        )
        self.assertEqual(await turso_db.get_games_by_slugs(["game-0100"]), {})
        turso_info = await turso_db.get_profile_sync_info("millers")
        self.assertIn("09:00:00", turso_info["last_synced_at"])
        self.assertEqual(await turso_db.get_profile_refresh_pending_token("millers"), "tok-initial")

    async def test_require_linked_and_pending_token_compare_and_clear_on_both_backends(self):
        turso_db, _ = await self._make_turso_db("turso_guards.db")
        for label, db in (("sqlite", self.db), ("turso", turso_db)):
            with self.subTest(backend=label):
                await db.link_user(10, "Millers")
                await db.link_user(20, "millers")
                await db.set_profile_refresh_pending("millers", "tok-1")
                t1 = datetime(2026, 11, 12, 10, 0, tzinfo=timezone.utc)
                self.assertTrue(
                    await db.replace_profile_library(
                        "Millers",
                        [_make_game(1, rating=4.0)],
                        synced_at=t1,
                        clear_pending_token="tok-wrong",
                    )
                )
                # Newer/mismatched pending token was not cleared by stale sync.
                self.assertEqual(await db.get_profile_refresh_pending_token("millers"), "tok-1")

                # One member unlinks; profile remains linked through member 20.
                await db.unlink_user(10)
                t2 = datetime(2026, 11, 12, 11, 0, tzinfo=timezone.utc)
                self.assertTrue(
                    await db.replace_profile_library(
                        "Millers",
                        [_make_game(2, rating=5.0)],
                        synced_at=t2,
                        clear_pending_token="tok-1",
                    )
                )
                self.assertIsNone(await db.get_profile_refresh_pending_token("millers"))
                self.assertEqual(await db.get_profile_ratings_dict("millers"), {"game-0002": 5.0})

                # Last member unlinks -> require_linked=True rejects replacement cleanly.
                await db.unlink_user(20)
                t3 = datetime(2026, 11, 12, 12, 0, tzinfo=timezone.utc)
                self.assertFalse(
                    await db.replace_profile_library(
                        "Millers",
                        [_make_game(3, rating=1.0)],
                        synced_at=t3,
                        require_linked=True,
                    )
                )
                self.assertEqual(await db.get_profile_ratings_dict("millers"), {"game-0002": 5.0})
                self.assertEqual(await db.get_games_by_slugs(["game-0003"]), {})


if __name__ == "__main__":
    unittest.main()
