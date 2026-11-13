"""Regressions for the four gaps found during independent cross-validation."""

import asyncio
import os
import sqlite3
import tempfile
import unittest
from dataclasses import asdict
from datetime import datetime, timezone
import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

os.environ['PYTHON_DOTENV_DISABLED'] = '1'

import discord
from backloggd import ActivityEntry
from bot.client import BackloggdBot
from bot.cogs.feed import FeedCog
from bot.cogs.games import GamesCog
from bot.db import Database
from bot.feed_models import FeedEvent


class CommitFailureRegressionTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.db = Database(str(Path(self.directory.name) / 'commit.db'), turso_url='')
        await self.db.connect()
        self.addAsyncCleanup(self.db.close)

    async def test_real_sqlite_commit_failure_rolls_back_and_allows_next_transaction(self):
        await self.db._execute('CREATE TABLE parent(id INTEGER PRIMARY KEY)')
        await self.db._execute('''CREATE TABLE child(parent_id INTEGER REFERENCES parent(id)
                                  DEFERRABLE INITIALLY DEFERRED)''')
        with self.assertRaises(sqlite3.IntegrityError):
            async with self.db._transaction() as tx:
                await tx.execute('INSERT INTO child VALUES (123)')
        self.assertFalse(self.db._conn.in_transaction)
        self.assertFalse(self.db._operation_lock.locked())
        async with self.db._transaction() as tx:
            self.assertEqual((await tx.fetchone('SELECT COUNT(*) AS n FROM child'))['n'], 0)
            await tx.execute('INSERT INTO parent VALUES (123)')

    async def test_sqlite_cancellation_at_commit_rolls_back(self):
        with patch.object(self.db._conn, 'commit', AsyncMock(side_effect=asyncio.CancelledError)):
            with self.assertRaises(asyncio.CancelledError):
                async with self.db._transaction() as tx:
                    await tx.execute("INSERT INTO profiles(profile_key, display_username) VALUES ('cancelled', 'Cancelled')")
        self.assertFalse(self.db._conn.in_transaction)
        self.assertIsNone(await self.db.get_profile('cancelled'))
        async with self.db._transaction() as tx:
            await tx.execute("INSERT INTO profiles(profile_key, display_username) VALUES ('after', 'After')")

    async def test_turso_commit_errors_and_cancellation_attempt_rollback(self):
        for failure in (RuntimeError('commit failed'), asyncio.CancelledError()):
            with self.subTest(failure=type(failure).__name__):
                tx = SimpleNamespace(commit=AsyncMock(side_effect=failure), rollback=AsyncMock(), close=MagicMock())
                with patch.object(type(self.db), 'is_turso', property(lambda _: True)), patch.object(
                    self.db, '_client', SimpleNamespace(transaction=lambda: tx)
                ):
                    with self.assertRaises(type(failure)):
                        async with self.db._transaction():
                            pass
                tx.rollback.assert_awaited_once()
                tx.close.assert_called_once()
                self.assertFalse(self.db._operation_lock.locked())

    async def test_turso_cleanup_failure_does_not_mask_commit_error(self):
        failure = RuntimeError('original commit error')
        tx = SimpleNamespace(commit=AsyncMock(side_effect=failure),
                             rollback=AsyncMock(side_effect=RuntimeError('already closed')), close=MagicMock())
        with patch.object(type(self.db), 'is_turso', property(lambda _: True)), patch.object(
            self.db, '_client', SimpleNamespace(transaction=lambda: tx)
        ):
            with self.assertRaisesRegex(RuntimeError, 'original commit error'):
                async with self.db._transaction():
                    pass
        tx.close.assert_called_once()


class WritableSdkCacheTests(unittest.IsolatedAsyncioTestCase):
    async def test_bot_constructs_and_writes_sdk_cache_with_only_data_directory_writable(self):
        with tempfile.TemporaryDirectory() as folder:
            data = Path(folder) / 'data'
            data.mkdir()
            mkdir = Path.mkdir

            def restricted_mkdir(target, *args, **kwargs):
                if not target.resolve().is_relative_to(data.resolve()):
                    raise PermissionError('application directory is read-only')
                return mkdir(target, *args, **kwargs)

            with patch('bot.client.DATABASE_PATH', str(data / 'bot.db')), patch.object(Path, 'mkdir', restricted_mkdir):
                bot = BackloggdBot()
                try:
                    cache_path = bot.backloggd.cache.cache_path
                    self.assertTrue(cache_path.resolve().is_relative_to(data.resolve()))
                    # Exercise actual SDK initialization and a cache write without network traffic.
                    bot.backloggd.cache.clear()
                    self.assertTrue(cache_path.is_file())
                finally:
                    await bot.close()

    def test_docker_context_excludes_developer_sdk_caches(self):
        ignores = (Path(__file__).parents[1] / '.dockerignore').read_text().splitlines()
        self.assertIn('.cache/', ignores)
        self.assertIn('data/sdk-cache/', ignores)


class AutocompleteRenewalTests(unittest.IsolatedAsyncioTestCase):
    async def test_same_query_regenerates_fallback_after_expiry_or_capacity_eviction(self):
        for reason in ('expiry', 'capacity'):
            with self.subTest(reason=reason):
                now = [0.0]
                game = SimpleNamespace(slug='long-' + 'x' * 100, title='Long Game', release_year=None)
                api = SimpleNamespace(search_games=AsyncMock(return_value=[game]), get_game=AsyncMock(return_value=None))
                cog = GamesCog(SimpleNamespace(backloggd=api), monotonic_clock=lambda: now[0], fallback_ttl_seconds=1)
                first = (await cog.game_autocomplete(None, 'long'))[0].value
                if reason == 'expiry':
                    now[0] = 2
                else:
                    with patch('bot.cogs.games._MAX_FALLBACK_SLUGS', 1):
                        cog._register_fallback_slug('another-' + 'x' * 100, 'another')
                choices = await cog.game_autocomplete(None, 'long')
                renewed = choices[0].value
                self.assertNotEqual(first, renewed)
                self.assertIn(renewed, cog._fallback_entries)
                interaction = SimpleNamespace(response=SimpleNamespace(defer=AsyncMock()),
                                              followup=SimpleNamespace(send=AsyncMock()))
                await GamesCog.game.callback(cog, interaction, renewed)
                api.get_game.assert_awaited_once_with(game.slug)


def activity(event_id, slug):
    return ActivityEntry(activity_id=event_id, game_title=slug, game_slug=slug,
                         cover_url=None, action_type='rated', action_text='rated', rating=4.5,
                         review_notes=None, date_str='11:30', occurred_at='2026-11-13T11:30:00Z')


class SkippedRecipientRegressionTests(unittest.IsolatedAsyncioTestCase):
    async def test_shared_profile_preserves_boundary_and_pending_work_until_skipped_guild_recovers(self):
        for reason in ('permissions', 'channel_cache', 'guild_cache', 'membership'):
            with self.subTest(reason=reason), tempfile.TemporaryDirectory() as folder:
                db = Database(str(Path(folder) / 'feed.db'), turso_url='')
                await db.connect()
                try:
                    await self._exercise_recovery(db, reason)
                finally:
                    await db.close()

    async def _exercise_recovery(self, db, reason):
        now = datetime(2026, 11, 13, 12, tzinfo=timezone.utc)
        await db.link_user(10, 'Millers')
        old = activity('old-1', 'older')
        payload = json.dumps(asdict(FeedEvent.from_activity(old)))
        channels, guilds = {}, {}
        for gid in (1, 2):
            channel = MagicMock(spec=discord.TextChannel)
            channel.id = gid * 100
            channel.permissions_for.return_value = SimpleNamespace(view_channel=True, send_messages=True, embed_links=True)
            channel.send = AsyncMock(return_value=SimpleNamespace(id=gid * 1000))
            channels[gid] = channel
            guilds[gid] = SimpleNamespace(id=gid, me=object(), chunked=True, member_count=1,
                members=[SimpleNamespace(id=10)], get_channel=lambda cid, ch=channel: ch)
            await db.set_feed_channel(gid, channel.id)
            await db.set_guild_feed_preference(gid, 10, True)
            await db.reconcile_guild_feed_activity(gid, ['millers'], now=datetime(2026, 11, 13, 8, tzinfo=timezone.utc))
            await db.seed_feed_source(gid, 'millers', 'activity-display-v1', ['old-1'])
            await db.seed_feed_source(gid, 'millers', 'journal-display-v1', [])
            await db.seed_feed_message(gid, 'millers', ['old-1'], payload)
        await db.record_feed_source_attempt('millers', 'activity')
        await db.record_feed_source_success('millers', 'activity', coverage='exhausted', pages_read=1, boundary_event_id='old-1')
        present = dict(guilds)
        if reason == 'permissions':
            channels[2].permissions_for.return_value.view_channel = False
        elif reason == 'channel_cache':
            guilds[2].get_channel = lambda cid: None
        elif reason == 'guild_cache':
            present.pop(2)
        else:
            guilds[2].chunked = False
            guilds[2].chunk = AsyncMock()  # Discord responds but its cache is still incomplete.
        api = SimpleNamespace(get_user_activity_snapshot=AsyncMock(return_value=SimpleNamespace(
            entries=[activity('new-2', 'new-game'), old], coverage='boundary_reached', pages_read=1)),
            get_user_journal=AsyncMock(return_value=[]))
        bot = SimpleNamespace(db=db, backloggd=api, get_guild=lambda gid: present.get(gid))
        cog = FeedCog(bot, clock=lambda: now)
        cog._observe_profile_activity = AsyncMock()
        await cog.journal_feed_loop.coro(cog)
        channels[1].send.assert_awaited_once()
        channels[2].send.assert_not_awaited()
        health = await db.get_profile_feed_source('millers', 'activity')
        self.assertEqual(health['last_continuous_boundary_event_id'], 'old-1')
        self.assertIsNotNone(health['unresolved_gap_since'])
        pending = await db.get_pending_feed_observations(2, 'millers')
        self.assertIn('new-2', [row['event_id'] for row in pending])
        # A new cog represents restart; the new event has disappeared from the SDK page.
        channels[2].permissions_for.return_value.view_channel = True
        guilds[2].get_channel = lambda cid: channels[2]
        guilds[2].chunked = True
        present[2] = guilds[2]
        api.get_user_activity_snapshot.return_value = SimpleNamespace(entries=[old], coverage='boundary_reached', pages_read=1)
        cog = FeedCog(bot, clock=lambda: now)
        cog._observe_profile_activity = AsyncMock()
        await cog.journal_feed_loop.coro(cog)
        channels[1].send.assert_awaited_once()  # archive is not replayed
        channels[2].send.assert_awaited_once()
        self.assertIn('new-game', channels[2].send.call_args.kwargs['embed'].title)
        health = await db.get_profile_feed_source('millers', 'activity')
        self.assertIsNone(health['unresolved_gap_since'])


if __name__ == '__main__':
    unittest.main()
