import os
os.environ["PYTHON_DOTENV_DISABLED"] = "1"

import json
import tempfile
import unittest
from dataclasses import asdict, replace
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import discord

from bot.cogs.feed import FeedCog
from bot.db import Database
from bot.feed_models import FeedEvent


FIXED_NOW = datetime(2026, 11, 12, 12, tzinfo=timezone.utc)


def event(event_id, occurred_at="2026-11-12T10:00:00Z", slug="game"):
    return FeedEvent(
        event_id, "Game", slug, None, "completed", "completed",
        None, None, None, played_date="2026-11-12", occurred_at=occurred_at,
    )


class FeedProfileDedupTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.db = Database(str(Path(self.directory.name) / "feed-profiles.db"), turso_url="")
        await self.db.connect()

    async def asyncTearDown(self):
        await self.db.close()
        self.directory.cleanup()

    def make_channel(self, channel_id):
        channel = MagicMock(spec=discord.TextChannel)
        channel.id = channel_id
        channel.permissions_for.return_value = SimpleNamespace(send_messages=True, embed_links=True)
        channel.send = AsyncMock(return_value=SimpleNamespace(id=channel_id * 10))
        channel.get_partial_message.return_value.edit = AsyncMock()
        return channel

    async def test_two_members_and_two_guilds_fetch_once_and_send_once_per_profile_event_guild(self):
        await self.db.link_user(10, "Millers")
        await self.db.link_user(11, "millers")
        await self.db.link_user(20, "MILLERS")
        await self.db.set_feed_channel(1, 101)
        await self.db.set_feed_channel(2, 202)
        await self.db.set_guild_feed_preference(1, 10, True)
        await self.db.set_guild_feed_preference(1, 11, True)
        await self.db.set_guild_feed_preference(2, 20, True)

        channel1, channel2 = self.make_channel(101), self.make_channel(202)
        guilds = {
            1: SimpleNamespace(
                members=[SimpleNamespace(id=10), SimpleNamespace(id=11)],
                me=object(), get_channel=lambda _: channel1,
            ),
            2: SimpleNamespace(
                members=[SimpleNamespace(id=20)],
                me=object(), get_channel=lambda _: channel2,
            ),
        }
        bot = SimpleNamespace(db=self.db, get_guild=lambda guild_id: guilds.get(guild_id))
        cog = FeedCog(bot, clock=lambda: FIXED_NOW)
        cog._fetch_feed_sources = AsyncMock(return_value={"activity": [event("old")], "journal": []})

        with patch("bot.cogs.feed.asyncio.sleep", new=AsyncMock()):
            await cog.journal_feed_loop.coro(cog)
            self.assertEqual(cog._fetch_feed_sources.await_count, 1)
            channel1.send.assert_not_awaited()
            channel2.send.assert_not_awaited()

            cog._fetch_feed_sources.return_value = {
                "activity": [event("new", "2026-11-12T12:05:00Z", slug="game-2"), event("old")], "journal": [],
            }
            await cog.journal_feed_loop.coro(cog)

        self.assertEqual(cog._fetch_feed_sources.await_count, 2)
        channel1.send.assert_awaited_once()
        channel2.send.assert_awaited_once()
        self.assertEqual(
            len(await self.db.get_feed_messages(1, "millers")), 2
        )
        self.assertEqual(
            len(await self.db.get_feed_messages(2, "millers")), 2
        )

    async def test_unlink_keeps_profile_feed_mappings_and_posted_messages(self):
        await self.db.link_user(10, "Millers")
        await self.db.link_user(11, "millers")
        await self.db.store_feed_message(
            1, "millers", ["event"], '{"event":"saved"}', 123, 456
        )

        await self.db.unlink_user(10)
        await self.db.unlink_user(11)

        rows = await self.db.get_feed_messages(1, "millers")
        self.assertEqual(rows[0]["message_id"], 123)
        self.assertEqual(rows[0]["payload"], '{"event":"saved"}')

    async def test_pending_send_becomes_unknown_after_restart_and_cannot_be_retried(self):
        await self.db.link_user(10, "Millers")
        self.assertTrue(await self.db.begin_feed_delivery(
            1, "millers", ["event"], '{"event":"pending"}', 101
        ))

        await self.db.close()
        await self.db.connect()
        rows = await self.db.get_feed_messages(1, "millers")

        self.assertEqual(rows[0]["delivery_state"], "unknown")
        self.assertFalse(await self.db.begin_feed_delivery(
            1, "millers", ["event"], '{"event":"pending"}', 101
        ))

    async def test_duplicate_legacy_message_aliases_edit_the_lowest_message_id(self):
        await self.db.link_user(10, "Millers")
        await self.db.seed_feed_source(1, "millers", "activity-display-v1", [])
        await self.db.seed_feed_source(1, "millers", "journal-display-v1", [])
        old = FeedEvent(
            "event-a", "Game", "game", None, "rated", "rated", 4.0, None, None,
            event_ids=("event-a", "event-b"),
        )
        payload = json.dumps(asdict(old), sort_keys=True)
        await self.db._execute(
            """INSERT INTO profile_feed_messages
               (guild_id, profile_key, event_id, payload, message_id, channel_id, delivery_state)
               VALUES (?, ?, ?, ?, ?, ?, 'sent')""",
            (1, "millers", "event-a", payload, 500, 100),
        )
        await self.db._execute(
            """INSERT INTO profile_feed_messages
               (guild_id, profile_key, event_id, payload, message_id, channel_id, delivery_state)
               VALUES (?, ?, ?, ?, ?, ?, 'sent')""",
            (1, "millers", "event-b", payload, 300, 100),
        )
        edits = {message_id: AsyncMock() for message_id in (300, 500)}
        channel = SimpleNamespace(
            id=100,
            send=AsyncMock(),
            get_partial_message=lambda message_id: SimpleNamespace(edit=edits[message_id]),
        )
        cog = FeedCog(SimpleNamespace(db=self.db), clock=lambda: FIXED_NOW)

        await cog._publish_sources(
            1, "millers", "Millers", None, channel,
            {"activity": [replace(old, rating=4.5)], "journal": []},
        )

        edits[300].assert_awaited_once()
        edits[500].assert_not_awaited()
        channel.send.assert_not_awaited()


if __name__ == "__main__":
    unittest.main()
