"""Framework timeout dispatch and conservative initial activity coverage regressions."""
import asyncio
import os
os.environ["PYTHON_DOTENV_DISABLED"] = "1"
import tempfile
import unittest
from pathlib import Path
from datetime import datetime, timezone, timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock
from discord.ui.view import ViewStore
from backloggd import GameEntry, GameStatus
from bot.ui.views import RouletteView
from bot.cogs.feed import FeedCog
from bot.db import Database
from bot.services.library_sync import LibrarySyncService
from tests.test_feed_policy_batching import _make_event

class TimeoutAndPrecisionTests(unittest.IsolatedAsyncioTestCase):
    async def test_real_timeout_disables_current_controls_but_retired_view_cannot_edit(self):
        edited = asyncio.Event()
        message = SimpleNamespace(edit=AsyncMock(side_effect=lambda **kwargs: edited.set()))
        current = RouletteView(1, "https://example.test/current", AsyncMock())
        self.addCleanup(current.stop)
        current.message = message
        current._start_listening_from_store(ViewStore(SimpleNamespace()))
        current._dispatch_timeout()
        self.assertTrue(current.is_finished())
        await asyncio.wait_for(edited.wait(), timeout=1)
        self.assertTrue(current.reroll_button.disabled)
        message.edit.assert_awaited_once_with(view=current)
        self.assertTrue(all(not item.disabled for item in current.children if item.url))
        retired = RouletteView(1, "https://example.test/old", AsyncMock())
        retired.message = message
        retired._start_listening_from_store(ViewStore(SimpleNamespace()))
        retired.retire()
        retired._dispatch_timeout()
        await retired.on_timeout()
        self.assertEqual(message.edit.await_count, 1)

    async def test_imprecise_activity_keeps_pending_refresh_despite_matching_cache(self):
        for stamp in ("2026-11-12", "2026-11-12T11:30:00"):
            with self.subTest(stamp=stamp), tempfile.TemporaryDirectory() as td:
                db = Database(str(Path(td) / "precision.db"), turso_url="")
                await db.connect()
                t0 = datetime(2026, 11, 12, 12, tzinfo=timezone.utc)
                now = t0 + timedelta(seconds=60)
                service = LibrarySyncService(clock=lambda: (now-t0).total_seconds())
                try:
                    await db.link_user(10, "Millers")
                    await db.replace_profile_library("Millers", [GameEntry(slug="hades", title="Hades", status=GameStatus.PLAYED, rating=5.0)], synced_at=t0)
                    service.seed_from_sync_info(await db.get_profile_sync_info("millers"), now=now, profile_key="millers")
                    api = SimpleNamespace(get_user_library=AsyncMock(return_value=[]))
                    cog = FeedCog(SimpleNamespace(db=db, backloggd=api, library_sync=service), clock=lambda: now)
                    for precise, covered in (("2026-11-12T11:30:00Z", True), ("2026-11-12T13:00:00Z", False)):
                        self.assertEqual(await cog._initial_observations_covered_by_sync("millers", [_make_event("precise", action="rated", rating=5.0, occurred_at=precise)]), covered)
                    event = _make_event("imprecise", action="rated", rating=5.0, occurred_at=stamp)
                    self.assertTrue(await cog._observe_profile_activity("millers", "Millers", {"activity": [event]}))
                    task = service._profiles["millers"].task
                    if task:
                        await task
                    self.assertIsNotNone(await db.get_profile_refresh_pending_token("millers"))
                    api.get_user_library.assert_not_awaited()
                finally:
                    await service.close()
                    await db.close()
