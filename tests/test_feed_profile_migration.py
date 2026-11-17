import os
os.environ["PYTHON_DOTENV_DISABLED"] = "1"

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from bot.db import Database
from bot.db_migrations import run_migrations


class FeedProfileMigrationTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.db = Database(str(Path(self.directory.name) / "feed-migration.db"), turso_url="")
        await self.db.connect()
        await self.db.link_user(10, "Millers")
        await self.db.link_user(11, "millers")
        await self.db.link_user(20, "PlayerB")

    async def asyncTearDown(self):
        await self.db.close()
        self.directory.cleanup()

    async def add_legacy_feed_rows(self):
        for row in ((1, 10, "activity"), (1, 11, "activity"), (1, 20, "journal"), (1, 99, "activity")):
            await self.db._execute("INSERT INTO feed_sources VALUES (?, ?, ?)", row)
        for row in (
            (1, 10, "event-shared", '{"owner":"older"}', 500, 100),
            (1, 11, "event-shared", '{"owner":"winner"}', 300, 100),
            (1, 10, "event-10", '{"owner":"millers"}', None, None),
            (1, 99, "orphan-event", '{"owner":"unknown"}', 900, 100),
        ):
            await self.db._execute(
                "INSERT INTO feed_messages VALUES (?, ?, ?, ?, ?, ?)", row
            )
        for row in (
            (1, "event-shared", 10, "2026-01-01"),
            (1, "event-second", 11, "2026-01-02"),
            (1, "orphan-entry", 99, "2026-01-03"),
        ):
            await self.db._execute(
                "INSERT INTO seen_journal_deliveries (guild_id, entry_id, discord_id, posted_at) VALUES (?, ?, ?, ?)",
                row,
            )
        await self.db._execute(
            "INSERT INTO seen_journal_logs (entry_id, discord_id, posted_at) VALUES (?, ?, ?)",
            ("global-entry", 10, "2026-01-04"),
        )

    async def migrate_feed_state(self):
        await self.add_legacy_feed_rows()
        await self.db._execute("DELETE FROM schema_migrations WHERE version >= 2")
        await run_migrations(self.db)

    async def test_migration_deduplicates_profile_rows_and_keeps_unmapped_legacy_records(self):
        await self.migrate_feed_state()

        sources = await self.db._fetchall(
            "SELECT guild_id, profile_key, source FROM profile_feed_sources ORDER BY profile_key, source"
        )
        messages = await self.db._fetchall(
            "SELECT profile_key, event_id, payload, message_id, delivery_state "
            "FROM profile_feed_messages ORDER BY profile_key, event_id"
        )
        deliveries = await self.db._fetchall(
            "SELECT profile_key, entry_id FROM profile_seen_journal_deliveries ORDER BY profile_key, entry_id"
        )
        seen_logs = await self.db._fetchall(
            "SELECT profile_key, entry_id FROM profile_seen_journal_logs"
        )

        self.assertEqual(
            [(row["profile_key"], row["source"]) for row in sources],
            [("millers", "activity"), ("playerb", "journal")],
        )
        self.assertEqual(
            [(row["event_id"], row["message_id"], row["payload"], row["delivery_state"]) for row in messages],
            [
                ("event-10", None, '{"owner":"millers"}', "baseline"),
                ("event-shared", 300, '{"owner":"winner"}', "sent"),
            ],
        )
        self.assertEqual(
            [(row["profile_key"], row["entry_id"]) for row in deliveries],
            [("millers", "event-second"), ("millers", "event-shared")],
        )
        self.assertEqual([(row["profile_key"], row["entry_id"]) for row in seen_logs], [
            ("millers", "global-entry"),
        ])
        self.assertIsNotNone(await self.db._fetchone(
            "SELECT 1 FROM feed_messages WHERE discord_id = 99 AND event_id = 'orphan-event'"
        ))
        self.assertIsNone(await self.db._fetchone(
            "SELECT 1 FROM profile_feed_messages WHERE event_id = 'orphan-event'"
        ))

    async def test_migration_rerun_is_idempotent_and_unlink_preserves_archive_rows(self):
        await self.migrate_feed_state()
        await run_migrations(self.db)

        self.assertTrue(await self.db.unlink_user(10))
        self.assertIsNotNone(await self.db._fetchone(
            "SELECT 1 FROM profile_feed_messages WHERE profile_key = 'millers' AND event_id = 'event-shared'"
        ))
        self.assertEqual(
            (await self.db._fetchone("SELECT COUNT(*) AS count FROM profile_feed_messages"))["count"],
            2,
        )
        version = await self.db._fetchone("SELECT MAX(version) AS version FROM schema_migrations")
        self.assertEqual(version["version"], 9)
        self.assertIsNotNone(await self.db._fetchone(
            "SELECT 1 FROM sqlite_master WHERE name = 'guild_profile_feed_recipient_gaps'"
        ))

    async def test_interrupted_feed_migration_rolls_back_and_can_retry(self):
        await self.add_legacy_feed_rows()
        await self.db._execute("DELETE FROM schema_migrations WHERE version >= 2")

        async def fail_after_partial_feed_schema(execute, fetchone):
            await execute("CREATE TABLE migration_partial_feed (value INTEGER)", ())
            raise RuntimeError("injected feed migration interruption")

        with patch("bot.db_migrations._migrate_profile_feed_schema", side_effect=fail_after_partial_feed_schema):
            with self.assertRaisesRegex(RuntimeError, "injected feed migration interruption"):
                await run_migrations(self.db)

        self.assertIsNone(await self.db._fetchone(
            "SELECT 1 FROM sqlite_master WHERE name = 'migration_partial_feed'"
        ))
        version = await self.db._fetchone("SELECT MAX(version) AS version FROM schema_migrations")
        self.assertEqual(version["version"], 1)

        await run_migrations(self.db)
        self.assertIsNotNone(await self.db._fetchone(
            "SELECT 1 FROM profile_feed_messages WHERE profile_key = 'millers' AND event_id = 'event-shared'"
        ))


if __name__ == "__main__":
    unittest.main()
