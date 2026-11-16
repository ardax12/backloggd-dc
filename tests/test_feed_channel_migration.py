"""Regression tests for Task 3: prior-channel edits, alias persistence, and permission-safe recovery."""

import json
import os
import tempfile
import unittest
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

os.environ["PYTHON_DOTENV_DISABLED"] = "1"
os.environ["TURSO_DATABASE_URL"] = ""
os.environ["TURSO_AUTH_TOKEN"] = ""
os.environ["DEV_GUILD_ID"] = ""

import discord
from backloggd import ActivityEntry

from bot.cogs.feed import FeedCog
from bot.db import Database
from bot.feed_models import FeedEvent


def _event(
    activity_id: str,
    *,
    action_type: str = "completed",
    action_text: str = "completed",
    slug: str = "hades",
    title: str = "Hades",
    rating: float | None = 3.0,
    review_notes: str | None = None,
    occurred_at: str = "2026-11-12T10:00:00Z",
    reference: str | None = "/u/Millers/log/10/",
) -> FeedEvent:
    return FeedEvent.from_activity(
        ActivityEntry(
            activity_id=activity_id,
            game_title=title,
            game_slug=slug,
            cover_url=None,
            action_type=action_type,
            action_text=action_text,
            rating=rating,
            review_notes=review_notes,
            date_str="10:00",
            occurred_at=occurred_at,
            reference=reference,
        )
    )


def _make_channel(
    channel_id: int,
    *,
    guild_id: int | None = 1,
    view_channel: bool = True,
    send_messages: bool = True,
    embed_links: bool = True,
):
    sent_counter = {"n": channel_id * 10}
    partials = {}

    async def send_impl(**kwargs):
        sent_counter["n"] += 1
        return SimpleNamespace(id=sent_counter["n"], kwargs=kwargs)

    def get_partial(message_id):
        if message_id not in partials:
            partials[message_id] = SimpleNamespace(id=message_id, edit=AsyncMock())
        return partials[message_id]

    channel = MagicMock(spec=discord.TextChannel)
    channel.id = channel_id
    channel.mention = f"<#{channel_id}>"
    if guild_id is not None:
        channel.guild = SimpleNamespace(id=guild_id)
    channel.permissions_for.return_value = SimpleNamespace(
        view_channel=view_channel,
        send_messages=send_messages,
        embed_links=embed_links,
    )
    channel.send = AsyncMock(side_effect=send_impl)
    channel.get_partial_message = get_partial
    channel._partials = partials
    return channel


def _make_guild(guild_id: int, members: list[int], channels: dict[int, object]):
    return SimpleNamespace(
        id=guild_id,
        name=f"Guild {guild_id}",
        me=object(),
        chunked=True,
        members=[SimpleNamespace(id=mid) for mid in members],
        get_channel=lambda cid: channels.get(cid),
    )


class FeedChannelMigrationTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmpdir.cleanup)
        self.db_path = Path(self.tmpdir.name) / "migration.db"
        self.db = Database(str(self.db_path), turso_url="")
        await self.db.connect()
        await self.db.link_user(10, "Millers")
        self.now = datetime(2026, 11, 12, 12, 0, tzinfo=timezone.utc)
        self.sleep = patch("bot.cogs.feed.asyncio.sleep", new=AsyncMock())
        self.sleep.start()
        self.addCleanup(self.sleep.stop)

    async def asyncTearDown(self):
        await self.db.close()

    async def test_move_channel_A_to_B_edits_old_in_A_and_sends_new_in_B(self):
        ch_a = _make_channel(100, guild_id=1)
        ch_b = _make_channel(200, guild_id=1)
        channels = {100: ch_a, 200: ch_b}
        guild = _make_guild(1, [10], channels)
        bot = SimpleNamespace(db=self.db, get_guild=lambda gid: guild if gid == 1 else None)
        cog = FeedCog(bot, clock=lambda: self.now)

        await cog._publish_sources(1, "millers", "Millers", None, ch_a, {"activity": [], "journal": []})
        ev_old = _event("ev-old", rating=3.0, reference="/u/Millers/log/1/")
        self.assertTrue(
            await cog._publish_sources(1, "millers", "Millers", None, ch_a, {"activity": [ev_old], "journal": []})
        )
        ch_a.send.assert_awaited_once()
        old_msg_id = 1001

        # Admin migrates feed channel A -> B.
        await self.db.set_feed_channel(1, 200)
        await self.db.close()
        await self.db.connect()

        ev_old_edited = replace(ev_old, rating=4.5)
        rated_alias = _event(
            "rated-old", action_type="rated", action_text="rated",
            rating=4.5, occurred_at=ev_old.occurred_at, reference=None,
        )
        ev_new = _event(
            "ev-new", slug="celeste", title="Celeste", rating=5.0,
            occurred_at="2026-11-12T12:05:00Z", reference="/u/Millers/log/2/",
        )
        ok = await cog._publish_sources(
            1, "millers", "Millers", None, ch_b, {"activity": [ev_old_edited, rated_alias, ev_new], "journal": []}
        )

        self.assertTrue(ok)
        # Old post edited in A (never reposted in B); new post sent in B.
        ch_a._partials[old_msg_id].edit.assert_awaited_once()
        self.assertIn("4.5", ch_a._partials[old_msg_id].edit.call_args.kwargs["embed"].description)
        ch_a.send.assert_awaited_once()
        ch_b.send.assert_awaited_once()
        self.assertIn("Celeste", ch_b.send.call_args.kwargs["embed"].title)

        rows = {r["event_id"]: r for r in await self.db.get_feed_messages(1, "millers")}
        self.assertEqual(rows["ev-old"]["channel_id"], 100)
        self.assertEqual(rows["ev-old"]["message_id"], old_msg_id)
        self.assertEqual(rows["rated-old"]["channel_id"], 100)
        self.assertEqual(rows["rated-old"]["message_id"], old_msg_id)
        self.assertEqual(rows["ev-new"]["channel_id"], 200)

    async def test_unavailable_and_permission_denied_old_channel_preserves_aliases_and_recovers(self):
        ch_a = _make_channel(100, guild_id=1)
        ch_b = _make_channel(200, guild_id=1)
        channels = {100: ch_a, 200: ch_b}
        guild = _make_guild(1, [10], channels)
        cog = FeedCog(SimpleNamespace(db=self.db, get_guild=lambda gid: guild if gid == 1 else None), clock=lambda: self.now)

        await cog._publish_sources(1, "millers", "Millers", None, ch_a, {"activity": [], "journal": []})
        ev_old = _event("ev-old", rating=3.0, reference="/u/Millers/log/10/")
        await cog._publish_sources(1, "millers", "Millers", None, ch_a, {"activity": [ev_old], "journal": []})
        old_msg_id = 1001
        confirmed_payload = {r["event_id"]: r for r in await self.db.get_feed_messages(1, "millers")}["ev-old"]["payload"]

        # 1) Old channel deleted/unavailable after migration -> no edit attempted, confirmed payload preserved,
        # new alias saved, unrelated ev-new still posts to ch_b, target_ok is False.
        channels.pop(100)
        ev_old_edited = replace(ev_old, rating=4.0)
        rated_alias = _event(
            "rated-old", action_type="rated", action_text="rated",
            rating=4.0, occurred_at=ev_old.occurred_at, reference=None,
        )
        ev_new = _event("ev-new-1", slug="celeste", title="Celeste", reference="/u/Millers/log/20/")
        ok_missing = await cog._publish_sources(
            1, "millers", "Millers", None, ch_b, {"activity": [ev_old_edited, rated_alias, ev_new], "journal": []}
        )
        self.assertFalse(ok_missing)
        self.assertNotIn(old_msg_id, ch_a._partials)
        self.assertNotIn(old_msg_id, ch_b._partials)
        ch_b.send.assert_awaited_once()
        rows_missing = {r["event_id"]: r for r in await self.db.get_feed_messages(1, "millers")}
        self.assertEqual(rows_missing["ev-old"]["payload"], confirmed_payload)
        self.assertEqual(rows_missing["rated-old"]["payload"], confirmed_payload)
        self.assertEqual(rows_missing["rated-old"]["channel_id"], 100)
        self.assertEqual(rows_missing["rated-old"]["message_id"], old_msg_id)

        # 2) Old channel returns but lacks View Channel / Send Messages / Embed Links -> still skipped,
        # still persists newly learned alias without overwriting confirmed payload.
        for missing_perm in ("view_channel", "send_messages", "embed_links"):
            perms = {"view_channel": True, "send_messages": True, "embed_links": True}
            perms[missing_perm] = False
            ch_a.permissions_for.return_value = SimpleNamespace(**perms)
            channels[100] = ch_a
            reviewed_alias = _event(
                f"rev-{missing_perm}", action_type="reviewed", action_text="reviewed",
                rating=None, review_notes=f"Edited {missing_perm}",
                occurred_at=ev_old.occurred_at, reference=None,
            )
            ok_perm = await cog._publish_sources(
                1, "millers", "Millers", None, ch_b, {"activity": [ev_old_edited, reviewed_alias], "journal": []}
            )
            self.assertFalse(ok_perm)
            self.assertNotIn(old_msg_id, ch_a._partials)
            rows_perm = {r["event_id"]: r for r in await self.db.get_feed_messages(1, "millers")}
            self.assertEqual(rows_perm[f"rev-{missing_perm}"]["payload"], confirmed_payload)
            self.assertEqual(rows_perm[f"rev-{missing_perm}"]["channel_id"], 100)
            self.assertEqual(rows_perm[f"rev-{missing_perm}"]["message_id"], old_msg_id)

        # 3) Permissions restored and bot restarts -> edit succeeds in A using alias-only input!
        ch_a.permissions_for.return_value = SimpleNamespace(view_channel=True, send_messages=True, embed_links=True)
        await self.db.close()
        await self.db.connect()
        ok_recovered = await cog._publish_sources(
            1, "millers", "Millers", None, ch_b,
            {"activity": [replace(rated_alias, rating=5.0)], "journal": []},
        )
        self.assertTrue(ok_recovered)
        ch_a._partials[old_msg_id].edit.assert_awaited_once()
        self.assertIn("5.0", ch_a._partials[old_msg_id].edit.call_args.kwargs["embed"].description)
        ch_b.send.assert_awaited_once()

    async def test_deleted_message_in_old_channel_stays_deleted_and_cross_guild_id_rejected(self):
        ch_a = _make_channel(100, guild_id=1)
        ch_b = _make_channel(200, guild_id=1)
        other_guild_ch = _make_channel(999, guild_id=2)
        # Even if a buggy lookup returned other_guild_ch, guild_id guard must still reject it.
        channels = {100: ch_a, 200: ch_b, 999: other_guild_ch}
        guild = _make_guild(1, [10], channels)
        cog = FeedCog(SimpleNamespace(db=self.db, get_guild=lambda gid: guild if gid == 1 else None), clock=lambda: self.now)

        await cog._publish_sources(1, "millers", "Millers", None, ch_a, {"activity": [], "journal": []})
        ev_del = _event("ev-del", rating=3.0, reference="/u/Millers/log/del/")
        await cog._publish_sources(1, "millers", "Millers", None, ch_a, {"activity": [ev_del], "journal": []})

        # Deleted in A after migrating to B -> marked deleted, never reposted, unrelated ev-new sends to B.
        ch_a.get_partial_message(1001).edit.side_effect = discord.NotFound(
            SimpleNamespace(status=404, reason="Not Found"), {"message": "deleted"}
        )
        ev_other = _event("ev-other-guild", rating=4.0, occurred_at="2026-11-12T10:15:00Z", reference="/u/Millers/log/og/")
        await self.db.store_feed_message(
            1, "millers", ("ev-other-guild",), json.dumps(asdict(ev_other) if False else {
                "activity_id": "ev-other-guild", "game_title": "Hades", "game_slug": "hades",
                "cover_url": None, "action_type": "completed", "action_text": "completed",
                "rating": 4.0, "review_notes": None, "date_str": "10:00",
                "occurred_at": "2026-11-12T10:15:00Z", "reference": "/u/Millers/log/og/",
                "played_date": None, "platform": None, "is_replay": False, "session_labels": [],
                "source": "activity", "has_spoilers": False, "previous_rating": None,
                "event_ids": ["ev-other-guild"],
            }, sort_keys=True),
            message_id=777, channel_id=999, delivery_state="sent",
        )
        ev_fresh = _event("ev-fresh", slug="celeste", title="Celeste", rating=5.0, reference="/u/Millers/log/fresh/")

        ok = await cog._publish_sources(
            1, "millers", "Millers", None, ch_b,
            {
                "activity": [
                    replace(ev_del, rating=4.0),
                    replace(ev_other, rating=2.0),
                    ev_fresh,
                ],
                "journal": [],
            },
        )
        self.assertFalse(ok)  # Because ev_other pointed to another guild's channel 999.
        self.assertNotIn(777, other_guild_ch._partials)
        other_guild_ch.send.assert_not_awaited()
        ch_b.send.assert_awaited_once()
        rows = {r["event_id"]: r for r in await self.db.get_feed_messages(1, "millers")}
        self.assertEqual(rows["ev-del"]["delivery_state"], "deleted")
        self.assertEqual(rows["ev-other-guild"]["channel_id"], 999)
        self.assertEqual(rows["ev-other-guild"]["message_id"], 777)

        # Retry poll without other_guild failure never reposts deleted event in A or B.
        ch_a.get_partial_message(1001).edit.side_effect = None
        await cog._publish_sources(
            1, "millers", "Millers", None, ch_b, {"activity": [replace(ev_del, rating=4.5)], "journal": []}
        )
        ch_b.send.assert_awaited_once()
        ch_a.send.assert_awaited_once()

    async def test_http_edit_failure_persists_new_aliases_without_overwriting_confirmed_payload_and_respects_inactive(self):
        ch_a = _make_channel(100, guild_id=1)
        ch_b = _make_channel(200, guild_id=1)
        channels = {100: ch_a, 200: ch_b}
        guild = _make_guild(1, [10], channels)
        cog = FeedCog(SimpleNamespace(db=self.db, get_guild=lambda gid: guild if gid == 1 else None), clock=lambda: self.now)

        await cog._publish_sources(1, "millers", "Millers", None, ch_a, {"activity": [], "journal": []})
        ev_old = _event("ev-old", rating=3.0, reference="/u/Millers/log/1/")
        await cog._publish_sources(1, "millers", "Millers", None, ch_a, {"activity": [ev_old], "journal": []})
        confirmed_payload = {r["event_id"]: r for r in await self.db.get_feed_messages(1, "millers")}["ev-old"]["payload"]

        ch_a.get_partial_message(1001).edit.side_effect = discord.HTTPException(
            SimpleNamespace(status=502, reason="Bad Gateway"), "upstream"
        )
        rated_new = _event(
            "rated-new", action_type="rated", action_text="rated", rating=5.0,
            occurred_at=ev_old.occurred_at, reference=None,
        )
        ok_fail = await cog._publish_sources(
            1, "millers", "Millers", None, ch_b, {"activity": [ev_old, rated_new], "journal": []}
        )
        self.assertFalse(ok_fail)
        rows = {r["event_id"]: r for r in await self.db.get_feed_messages(1, "millers")}
        self.assertEqual(rows["ev-old"]["payload"], confirmed_payload)
        self.assertEqual(rows["rated-new"]["payload"], confirmed_payload)
        self.assertEqual(rows["rated-new"]["message_id"], 1001)
        self.assertEqual(rows["rated-new"]["channel_id"], 100)

        # deliver=False (unavailable target / deferred delivery) never touches Discord edits.
        ch_a.get_partial_message(1001).edit.side_effect = None
        ch_a.get_partial_message(1001).edit.reset_mock()
        ok_nodev = await cog._publish_sources(
            1, "millers", "Millers", None, None, {"activity": [ev_old, rated_new], "journal": []}, deliver=False
        )
        self.assertFalse(ok_nodev)
        ch_a.get_partial_message(1001).edit.assert_not_awaited()

        # Once user preference is removed, journal_feed_loop must NOT reactivate the profile to edit the archive!
        await self.db.set_feed_channel(1, 200)
        await self.db.set_guild_feed_preference(1, 10, False)
        await self.db.reconcile_guild_feed_activity(1, [], now=self.now, present_member_ids=[10])
        api = SimpleNamespace(
            get_user_activity_snapshot=AsyncMock(),
            get_user_journal=AsyncMock(),
        )
        cog.bot = SimpleNamespace(db=self.db, backloggd=api, get_guild=lambda gid: guild if gid == 1 else None)
        await cog.journal_feed_loop.coro(cog)
        api.get_user_activity_snapshot.assert_not_awaited()
        ch_a.get_partial_message(1001).edit.assert_not_awaited()
