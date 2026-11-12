import os
os.environ["PYTHON_DOTENV_DISABLED"] = "1"
import asyncio
import json
import tempfile
import unittest
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch
from bot.db import Database
from bot.cogs.feed import FeedCog
from backloggd import ChallengeResponseError, GameEntry, GameStatus, PageParseError
from feed_samples import completion_event, journal_entries
from bot.feed_rules import journal_events

class PipelineTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.db = Database(str(Path(self.directory.name) / "feed.db"), turso_url="")
        await self.db.connect()
        self.channel = SimpleNamespace(id=100, send=AsyncMock(return_value=SimpleNamespace(id=999)), get_partial_message=MagicMock(return_value=SimpleNamespace(edit=AsyncMock())))
        self.cog = FeedCog(SimpleNamespace(db=self.db))
        self.activity = completion_event()
        self.entries = journal_entries()
        self.log = journal_events(self.entries[:1], "Millers")[0]
        self.sleep = patch("bot.cogs.feed.asyncio.sleep", new=AsyncMock())
        self.sleep.start()

    async def asyncTearDown(self):
        self.sleep.stop()
        await self.db.close()
        self.directory.cleanup()

    async def publish(self, sources, guild=1, member=10):
        await self.cog._publish_sources(guild, member, "Millers", None, self.channel, sources)

    async def baseline(self):
        await self.publish({"activity": [], "journal": []})

    async def test_successful_empty_sources_allow_first_new_event(self):
        await self.baseline()
        await self.publish({"activity": [self.activity], "journal": []})
        self.channel.send.assert_awaited_once()

    async def test_late_journal_updates_existing_activity_message(self):
        await self.baseline()
        await self.publish({"activity": [self.activity], "journal": []})
        await self.publish({"activity": [], "journal": [self.log]})
        self.channel.send.assert_awaited_once()
        edit = self.channel.get_partial_message.return_value.edit
        edit.assert_awaited_once()
        self.assertIn("Played date", edit.call_args.kwargs["embed"].description)
        await self.publish({"activity": [], "journal": [self.log]})
        edit.assert_awaited_once()

    async def test_journal_first_then_activity_updates_in_place(self):
        await self.baseline()
        await self.publish({"activity": [], "journal": [self.log]})
        await self.publish({"activity": [self.activity], "journal": [self.log]})
        self.channel.send.assert_awaited_once()
        self.channel.get_partial_message.return_value.edit.assert_awaited_once()

    async def test_activity_arriving_during_journal_failure_updates_saved_journal_message(self):
        await self.baseline()
        await self.publish({"activity": [], "journal": [self.log]})

        await self.publish({"activity": [self.activity]})

        self.channel.send.assert_awaited_once()
        edit = self.channel.get_partial_message.return_value.edit
        edit.assert_awaited_once()
        self.assertIn("Played date", edit.call_args.kwargs["embed"].description)

    async def test_journal_failure_preserves_previously_observed_enrichment(self):
        await self.baseline()
        await self.publish({"activity": [self.activity], "journal": [self.log]})

        await self.publish({"activity": [self.activity]})

        edit = self.channel.get_partial_message.return_value.edit
        edit.assert_not_awaited()
        rows = await self.db.get_feed_messages(1, 10)
        stored = json.loads(next(row["payload"] for row in rows if row["event_id"] == self.activity.activity_id))
        self.assertEqual(stored["played_date"], self.log.played_date)
        self.assertEqual(stored["session_labels"], list(self.log.session_labels))

    async def test_setchannel_keeps_existing_message_mappings_and_later_edits_them(self):
        await self.baseline()
        await self.publish({"activity": [self.activity], "journal": [self.log]})
        self.cog._fetch_feed_sources = AsyncMock(return_value={"activity": [self.activity], "journal": [self.log]})
        interaction = SimpleNamespace(
            guild=SimpleNamespace(members=[SimpleNamespace(id=10)], me=object()),
            guild_id=1,
            response=SimpleNamespace(defer=AsyncMock()),
            followup=SimpleNamespace(send=AsyncMock()),
        )
        self.channel.mention = "#feed"
        self.channel.permissions_for = lambda _: SimpleNamespace(
            view_channel=True, send_messages=True, embed_links=True
        )

        await FeedCog.setchannel.callback(self.cog, interaction, self.channel)
        updated = replace(self.activity, review_notes="A note added after the channel setup.")
        await self.publish({"activity": [updated], "journal": [self.log]})

        edit = self.channel.get_partial_message.return_value.edit
        edit.assert_awaited_once()
        self.channel.send.assert_awaited_once()
        rows = await self.db.get_feed_messages(1, 10)
        self.assertTrue(all(row["message_id"] == 999 for row in rows))
        self.assertTrue(all(row["channel_id"] == self.channel.id for row in rows))

    async def test_setchannel_rejects_missing_embed_permission(self):
        self.channel.permissions_for = lambda _: SimpleNamespace(
            view_channel=True, send_messages=True, embed_links=False
        )
        interaction = SimpleNamespace(
            guild=SimpleNamespace(members=[], me=object()),
            guild_id=1,
            response=SimpleNamespace(defer=AsyncMock()),
            followup=SimpleNamespace(send=AsyncMock()),
        )

        await FeedCog.setchannel.callback(self.cog, interaction, self.channel)

        self.assertIsNone(await self.db.get_feed_channel(1))
        self.assertIn("embed", interaction.followup.send.call_args.args[0].lower())

    async def test_setchannel_explains_that_only_future_events_are_announced(self):
        self.channel.mention = "#feed"
        interaction = SimpleNamespace(
            guild=SimpleNamespace(members=[], me=object()),
            guild_id=1,
            response=SimpleNamespace(defer=AsyncMock()),
            followup=SimpleNamespace(send=AsyncMock()),
        )
        self.channel.permissions_for = lambda _: SimpleNamespace(
            view_channel=True, send_messages=True, embed_links=True
        )

        await FeedCog.setchannel.callback(self.cog, interaction, self.channel)

        message = interaction.followup.send.call_args.args[0].lower()
        self.assertIn("future", message)
        self.assertIn("existing", message)

    async def test_rating_change_uses_baselined_rating(self):
        old = replace(self.activity, action_type="rated", rating=3.0)
        await self.publish({"activity": [old], "journal": []})
        new = replace(old, activity_id="new-rating", occurred_at="2026-11-12T15:00:00Z", rating=4.0)
        await self.publish({"activity": [new], "journal": []})
        embed = self.channel.send.call_args.kwargs["embed"]
        self.assertIn("changed their rating", embed.title)
        self.assertIn("3.0", embed.description)
        self.assertIn("4.0", embed.description)

    async def test_journal_rating_does_not_replace_previous_activity_rating(self):
        old_rating = replace(
            self.activity,
            activity_id="old-rating",
            action_type="rated",
            occurred_at="2026-11-10T15:00:00Z",
            rating=3.0,
        )
        await self.publish({"activity": [old_rating], "journal": []})
        current_log = replace(self.log, rating=4.0)
        await self.publish({"activity": [old_rating], "journal": [current_log]})
        new_rating = replace(
            old_rating,
            activity_id="new-rating",
            occurred_at="2026-11-12T15:00:00Z",
            rating=4.0,
        )

        await self.publish({"activity": [old_rating, new_rating], "journal": [current_log]})

        embed = self.channel.send.call_args.kwargs["embed"]
        self.assertIn("changed their rating", embed.title)
        self.assertIn("3.0", embed.description)
        self.assertIn("4.0", embed.description)

    async def test_editing_a_sent_rating_shows_previous_value(self):
        await self.baseline()
        rating = replace(self.activity, action_type="rated", rating=3.0)
        await self.publish({"activity": [rating]})
        await self.publish({"activity": [replace(rating, rating=4.5)]})
        embed = self.channel.get_partial_message.return_value.edit.call_args.kwargs["embed"]
        self.assertIn("changed their rating", embed.title)
        self.assertIn("3.0", embed.description)
        self.assertIn("4.5", embed.description)
        self.channel.send.assert_awaited_once()

    async def test_hidden_actions_and_daily_journal_entries_never_send(self):
        await self.baseline()
        hidden = replace(self.activity, action_type="wishlist")
        daily = replace(self.log, action_type="played", session_labels=())
        await self.publish({"activity": [hidden], "journal": [daily]})
        self.channel.send.assert_not_awaited()

    async def test_restart_does_not_repost_and_guilds_are_independent(self):
        await self.baseline()
        await self.publish({"activity": [self.activity], "journal": [self.log]})
        await self.db.close()
        await self.db.connect()
        await self.publish({"activity": [self.activity], "journal": [self.log]})
        self.channel.send.assert_awaited_once()
        await self.publish({"activity": [], "journal": []}, guild=2)
        await self.publish({"activity": [self.activity], "journal": [self.log]}, guild=2)
        self.assertEqual(self.channel.send.await_count, 2)

    async def test_failed_send_does_not_mark_event_and_retries(self):
        await self.baseline()
        self.channel.send.side_effect = RuntimeError("delivery failed")
        with self.assertRaises(RuntimeError):
            await self.publish({"activity": [self.activity]})
        self.assertFalse(await self.db.is_journal_entry_delivered(1, self.activity.activity_id))
        self.channel.send.side_effect = None
        await self.publish({"activity": [self.activity]})
        self.assertTrue(await self.db.is_journal_entry_delivered(1, self.activity.activity_id))

    async def test_partial_baseline_is_not_replayed_after_restart(self):
        await self.db.seed_feed_source(1, 10, "activity-display-v1", [self.activity.activity_id])
        await self.publish({"activity": [self.activity]})
        self.channel.send.assert_not_awaited()

    async def test_failed_sources_do_not_create_baselines(self):
        await self.publish({})
        self.assertTrue(await self.db.seed_feed_source(1, 10, "activity-display-v1", []))

    async def test_failed_edit_retries_without_sending_duplicate(self):
        await self.baseline()
        await self.publish({"activity": [self.activity], "journal": []})
        edit = self.channel.get_partial_message.return_value.edit
        edit.side_effect = RuntimeError("edit failed")
        with self.assertRaises(RuntimeError):
            await self.publish({"activity": [], "journal": [self.log]})
        edit.side_effect = None
        await self.publish({"activity": [], "journal": [self.log]})
        self.assertEqual(edit.await_count, 2)
        self.channel.send.assert_awaited_once()


class FetchTests(unittest.IsolatedAsyncioTestCase):
    async def test_activity_failure_does_not_prevent_journal(self):
        api = SimpleNamespace(get_user_activity=AsyncMock(side_effect=ChallengeResponseError("blocked")),
                              get_user_journal=AsyncMock(return_value=journal_entries()))
        sources = await FeedCog(SimpleNamespace(backloggd=api))._fetch_feed_sources("Millers")
        self.assertNotIn("activity", sources)
        self.assertEqual(len(sources["journal"]), 2)
        api.get_user_journal.assert_awaited_once_with("Millers", max_pages=100, require_complete=True)

    async def test_journal_failure_does_not_prevent_activity(self):
        api = SimpleNamespace(get_user_activity=AsyncMock(return_value=[completion_event()]),
                              get_user_journal=AsyncMock(side_effect=PageParseError("incomplete journal")))
        sources = await FeedCog(SimpleNamespace(backloggd=api))._fetch_feed_sources("Millers")
        self.assertEqual(len(sources["activity"]), 1)
        self.assertNotIn("journal", sources)

    async def test_successful_empty_public_apis_are_preserved(self):
        api = SimpleNamespace(get_user_activity=AsyncMock(return_value=[]), get_user_journal=AsyncMock(return_value=[]))
        sources = await FeedCog(SimpleNamespace(backloggd=api))._fetch_feed_sources("Millers")
        self.assertEqual(sources, {"activity": [], "journal": []})


class LinkedUsernameTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.db = Database(str(Path(self.directory.name) / "links.db"), turso_url="")
        await self.db.connect()

    async def asyncTearDown(self):
        await self.db.close()
        self.directory.cleanup()

    async def test_case_insensitive_profile_links_are_nonexclusive(self):
        results = await asyncio.gather(
            self.db.link_user(10, "Millers"),
            self.db.link_user(11, "millers"),
            return_exceptions=True,
        )

        self.assertEqual(results, [True, True])
        links = await self.db.get_all_linked_users()
        self.assertEqual(len(links), 2)
        self.assertEqual({row["profile_key"] for row in links}, {"millers"})

    async def test_unlinking_one_member_keeps_the_shared_profile_library(self):
        await self.db.link_user(10, "Millers")
        await self.db.link_user(11, "millers")
        await self.db.upsert_game("hades", "Hades")
        await self.db.upsert_user_rating(10, "hades", 4.5, "played")

        self.assertTrue(await self.db.unlink_user(10))

        self.assertIsNone(await self.db.get_linked_user(10))
        self.assertEqual((await self.db.get_linked_user(11))["profile_key"], "millers")
        self.assertEqual(await self.db.get_user_ratings_dict(11), {"hades": 4.5})

    async def test_shared_sync_can_finish_after_its_requesting_member_unlinks(self):
        await self.db.link_user(10, "Millers")
        await self.db.link_user(11, "millers")
        await self.db.unlink_user(10)

        stored = await self.db.replace_user_library(10, "Millers", [
            GameEntry(slug="hades", title="Hades", rating=4.5, status=GameStatus.PLAYED)
        ])

        self.assertTrue(stored)
        self.assertEqual(await self.db.get_user_ratings_dict(11), {"hades": 4.5})

    async def test_stale_sync_cannot_write_into_a_relinked_profile(self):
        await self.db.link_user(10, "OldProfile")
        await self.db.link_user(10, "NewProfile")

        stored = await self.db.replace_user_library(10, "OldProfile", [
            GameEntry(slug="hades", title="Hades", rating=4.5, status=GameStatus.PLAYED)
        ])

        self.assertFalse(stored)
        self.assertEqual(await self.db.get_user_ratings_dict(10), {})
        self.assertEqual(await self.db.get_profile_ratings_dict("OldProfile"), {})

    async def test_profile_sync_info_reports_shared_snapshot_freshness_and_size(self):
        await self.db.link_user(10, "Millers")
        await self.db.replace_user_library(10, "Millers", [
            GameEntry(slug="hades", title="Hades", rating=4.5, status=GameStatus.PLAYED),
            GameEntry(slug="celeste", title="Celeste", rating=4.0, status=GameStatus.PLAYED),
        ])

        info = await self.db.get_profile_sync_info("MILLERS")

        self.assertEqual(info["profile_key"], "millers")
        self.assertEqual(info["game_count"], 2)
        self.assertIsNotNone(info["last_synced_at"])

    async def test_profile_level_rating_lookups_do_not_select_a_discord_owner(self):
        await self.db.link_user(10, "Millers")
        await self.db.link_user(11, "millers")
        await self.db.upsert_game("hades", "Hades")
        await self.db.upsert_user_rating(10, "hades", 4.5, "played")

        profile = await self.db.get_profile("MILLERS")
        stats = await self.db.get_profile_stats("millers")
        ratings = await self.db.get_profile_ratings_dict("Millers")

        self.assertEqual(profile["profile_key"], "millers")
        self.assertEqual(profile["display_username"], "millers")
        self.assertEqual(stats["rated_count"], 1)
        self.assertEqual(ratings, {"hades": 4.5})

    async def test_profile_connections_are_scoped_to_supplied_server_members(self):
        await self.db.link_user(10, "Millers")
        await self.db.link_user(11, "millers")
        await self.db.link_user(12, "MILLERS")

        rows = await self.db.get_profile_connections("MiLLeRs", [10, 11], exclude_discord_id=10)

        self.assertEqual([row["discord_id"] for row in rows], [11])
