import os
os.environ["PYTHON_DOTENV_DISABLED"] = "1"
import asyncio
import json
import tempfile
import unittest
from datetime import datetime, timezone
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
        await self.db.link_user(10, "Millers")
        self.channel = SimpleNamespace(id=100, send=AsyncMock(return_value=SimpleNamespace(id=999)), get_partial_message=MagicMock(return_value=SimpleNamespace(edit=AsyncMock())))
        self.cog = FeedCog(
            SimpleNamespace(db=self.db),
            clock=lambda: datetime(2026, 11, 12, 12, tzinfo=timezone.utc),
        )
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
        await self.cog._publish_sources(guild, "millers", "Millers", None, self.channel, sources)

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
        rows = await self.db.get_feed_messages(1, "millers")
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
        rows = await self.db.get_feed_messages(1, "millers")
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
        import discord
        await self.baseline()
        rejected = discord.Forbidden(SimpleNamespace(status=403, reason="Forbidden"), {"message": "rejected"})
        self.channel.send.side_effect = rejected
        with self.assertRaises(discord.Forbidden):
            await self.publish({"activity": [self.activity]})
        self.assertFalse(await self.db.is_journal_entry_delivered(1, "millers", self.activity.activity_id))
        self.channel.send.side_effect = None
        await self.publish({"activity": [self.activity]})
        self.assertTrue(await self.db.is_journal_entry_delivered(1, "millers", self.activity.activity_id))

    async def test_validated_unsent_observation_retries_after_event_leaves_latest_page(self):
        import discord
        await self.baseline()
        self.channel.send.side_effect = discord.Forbidden(
            SimpleNamespace(status=403, reason="Forbidden"), {"message": "rejected"}
        )
        with self.assertRaises(discord.Forbidden):
            await self.publish({"activity": [self.activity]})
        self.channel.send.side_effect = None

        await self.publish({"activity": [], "journal": []})

        self.assertEqual(self.channel.send.await_count, 2)
        self.assertTrue(await self.db.is_journal_entry_delivered(1, "millers", self.activity.activity_id))
        self.assertEqual(await self.db.get_pending_feed_observations(1, "millers"), [])

    async def test_unknown_send_outcome_is_archived_without_blind_retry(self):
        await self.baseline()
        self.channel.send.side_effect = RuntimeError("connection lost after request")
        with self.assertRaisesRegex(RuntimeError, "connection lost"):
            await self.publish({"activity": [self.activity]})

        self.channel.send.side_effect = None
        await self.publish({"activity": [self.activity]})

        self.channel.send.assert_awaited_once()
        rows = await self.db.get_feed_messages(1, "millers")
        self.assertEqual(rows[0]["delivery_state"], "unknown")
        self.assertFalse(await self.db.is_journal_entry_delivered(
            1, "millers", self.activity.activity_id
        ))

    async def test_older_session_is_suppressed_once_and_decision_survives_clock_advance(self):
        now = datetime(2026, 11, 12, 10, tzinfo=timezone.utc)
        self.cog.clock = lambda: now
        await self.baseline()
        old = replace(
            self.activity, activity_id="eight-days-old", played_date="2026-11-04",
            occurred_at="2026-11-04T10:00:00Z",
        )

        await self.publish({"activity": [old], "journal": []})
        now = datetime(2026, 12, 1, 10, tzinfo=timezone.utc)
        await self.publish({"activity": [old], "journal": []})

        self.channel.send.assert_not_awaited()
        decision = await self.db.get_feed_event_decision(1, "millers", "eight-days-old")
        self.assertEqual(decision["decision"], "suppressed_old")
        self.assertEqual((await self.db.get_feed_decision_counts(1, "millers"))["suppressed_old"], 1)

    async def test_exactly_seven_day_session_is_eligible_but_future_and_uncertain_are_held(self):
        self.cog.clock = lambda: datetime(2026, 11, 12, 10, tzinfo=timezone.utc)
        await self.baseline()
        seven_days = replace(
            self.activity, activity_id="seven-days-old", played_date="2026-11-05",
            occurred_at="2026-11-05T10:00:00Z",
        )
        future = replace(
            self.activity, activity_id="future-session", played_date="2026-11-13",
            occurred_at="2026-11-13T10:00:00Z",
        )
        unknown = replace(
            self.activity, activity_id="uncertain-session", played_date=None, occurred_at=None,
        )

        await self.publish({"activity": [seven_days, future, unknown], "journal": []})

        self.channel.send.assert_awaited_once()
        decisions = {
            key: (await self.db.get_feed_event_decision(1, "millers", key))["decision"]
            for key in ("seven-days-old", "future-session", "uncertain-session")
        }
        self.assertEqual(decisions, {
            "seven-days-old": "eligible",
            "future-session": "held_future",
            "uncertain-session": "held_uncertain",
        })

    async def test_old_milestone_does_not_absorb_a_new_explicit_rating(self):
        self.cog.clock = lambda: datetime(2026, 11, 12, 10, tzinfo=timezone.utc)
        await self.baseline()
        old_session = replace(
            self.activity, activity_id="old-session", played_date="2026-11-04",
            occurred_at="2026-11-04T10:00:00Z", reference="same-action",
        )
        await self.publish({"activity": [old_session], "journal": []})
        rating = replace(
            self.activity, activity_id="new-rating", action_type="rated", rating=4.5,
            occurred_at=old_session.occurred_at, reference="same-action",
        )

        await self.publish({"activity": [old_session, rating], "journal": []})

        self.channel.send.assert_awaited_once()
        self.assertIn("rated", self.channel.send.call_args.kwargs["embed"].title.lower())
        self.assertEqual(
            (await self.db.get_feed_event_decision(1, "millers", "old-session"))["decision"],
            "suppressed_old",
        )
        self.assertEqual(
            (await self.db.get_feed_event_decision(1, "millers", "new-rating"))["decision"],
            "eligible",
        )

    async def test_aged_posted_session_can_still_be_edited(self):
        now = datetime(2026, 11, 12, 10, tzinfo=timezone.utc)
        self.cog.clock = lambda: now
        await self.baseline()
        recent = replace(
            self.activity, activity_id="posted-session", played_date="2026-11-12",
            occurred_at="2026-11-12T10:00:00Z",
        )
        await self.publish({"activity": [recent], "journal": []})
        now = datetime(2026, 11, 30, 10, tzinfo=timezone.utc)

        await self.publish({"activity": [replace(recent, review_notes="Added later")], "journal": []})

        self.channel.send.assert_awaited_once()
        self.channel.get_partial_message.return_value.edit.assert_awaited_once()

    async def test_resume_baseline_suppresses_recent_entries_during_inactive_interval(self):
        self.cog.clock = lambda: datetime(2026, 11, 12, 10, tzinfo=timezone.utc)
        await self.baseline()
        await self.db.reconcile_guild_feed_activity(1, ["millers"])
        await self.db.reconcile_guild_feed_activity(1, [])
        await self.db.reconcile_guild_feed_activity(1, ["millers"])
        recent = replace(self.activity, activity_id="resume-recent", played_date="2026-11-12")

        await self.publish({"activity": [recent], "journal": []})

        self.channel.send.assert_not_awaited()
        self.assertEqual(
            (await self.db.get_feed_event_decision(1, "millers", "resume-recent"))["decision"],
            "baseline",
        )

    async def test_stable_reference_reconciles_changed_raw_id_as_an_edit(self):
        await self.baseline()
        original = replace(
            self.activity, activity_id="session-before-edit", reference="/u/Millers/session/42/",
            played_date="2026-11-11",
        )
        await self.publish({"activity": [original], "journal": []})
        changed = replace(
            original, activity_id="session-after-edit", played_date="2026-11-10",
            occurred_at="2026-11-10T15:00:00Z",
        )

        await self.publish({"activity": [changed], "journal": []})

        self.channel.send.assert_awaited_once()
        self.channel.get_partial_message.return_value.edit.assert_awaited_once()
        aliases = await self.db._fetchall(
            "SELECT event_id, message_id FROM profile_feed_messages WHERE guild_id = 1 AND profile_key = 'millers'"
        )
        self.assertEqual({row["event_id"] for row in aliases}, {
            "session-before-edit", "session-after-edit",
        })
        self.assertEqual({row["message_id"] for row in aliases}, {999})

    async def test_changed_session_without_stable_reference_is_held_across_polls(self):
        await self.baseline()
        original = replace(self.activity, activity_id="known-session", reference=None)
        await self.publish({"activity": [original], "journal": []})
        uncertain = replace(
            original, activity_id="changed-session-id", occurred_at="2026-11-10T15:00:00Z",
            played_date="2026-11-10",
        )

        await self.publish({"activity": [uncertain], "journal": []})
        await self.publish({"activity": [uncertain], "journal": []})

        self.channel.send.assert_awaited_once()
        decision = await self.db.get_feed_event_decision(1, "millers", "changed-session-id")
        self.assertEqual(decision["decision"], "held_ambiguous")

    async def test_late_stable_reference_resolves_held_session_to_existing_post(self):
        await self.baseline()
        reference = "/u/Millers/session/73/"
        original = replace(self.activity, activity_id="old-id", reference=reference)
        await self.publish({"activity": [original], "journal": []})
        changed = replace(
            original, activity_id="new-id", reference=None,
            played_date="2026-11-10", occurred_at="2026-11-10T15:00:00Z",
        )
        await self.publish({"activity": [changed], "journal": []})
        held = await self.db.get_feed_event_decision(1, "millers", "new-id")
        self.assertEqual(held["decision"], "held_ambiguous")

        resolved = replace(changed, reference=reference)
        await self.publish({"activity": [resolved], "journal": []})

        self.channel.send.assert_awaited_once()
        self.channel.get_partial_message.return_value.edit.assert_awaited_once()
        decision = await self.db.get_feed_event_decision(1, "millers", "new-id")
        self.assertEqual(decision["decision"], "eligible")
        self.assertEqual(decision["reason"], "resolved_by_stable_reference")

    async def test_reordered_or_removed_identical_journal_rows_keep_existing_posts(self):
        await self.baseline()
        duplicates = journal_events([self.entries[0], self.entries[0]], "Millers")
        await self.publish({"activity": [], "journal": duplicates})
        self.assertEqual(self.channel.send.await_count, 2)
        await self.publish({"activity": [], "journal": list(reversed(duplicates))})
        await self.publish({"activity": [], "journal": duplicates[:1]})

        self.assertEqual(self.channel.send.await_count, 2)
        rows = await self.db.get_feed_messages(1, "millers")
        self.assertEqual(sum(row["message_id"] is not None for row in rows), 2)

    async def test_not_found_edit_archives_message_and_never_reposts(self):
        import discord
        await self.baseline()
        original = replace(self.activity, activity_id="deleted-on-discord")
        await self.publish({"activity": [original], "journal": []})
        changed = replace(original, review_notes="A later edit")
        edit = self.channel.get_partial_message.return_value.edit
        edit.side_effect = discord.NotFound(
            SimpleNamespace(status=404, reason="Not Found"), {"message": "deleted"}
        )
        await self.publish({"activity": [changed], "journal": []})
        edit.side_effect = None
        await self.publish({"activity": [changed], "journal": []})

        self.channel.send.assert_awaited_once()
        row = await self.db._fetchone(
            "SELECT delivery_state FROM profile_feed_messages WHERE event_id = 'deleted-on-discord'"
        )
        self.assertEqual(row["delivery_state"], "deleted")

    async def test_partial_baseline_is_not_replayed_after_restart(self):
        await self.db.seed_feed_source(1, "millers", "activity-display-v1", [self.activity.activity_id])
        await self.publish({"activity": [self.activity]})
        self.channel.send.assert_not_awaited()

    async def test_failed_sources_do_not_create_baselines(self):
        await self.publish({})
        self.assertTrue(await self.db.seed_feed_source(1, "millers", "activity-display-v1", []))

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

    async def test_started_and_finished_journal_titles_and_played_dates_are_preserved(self):
        await self.baseline()
        finished_ev, started_ev = journal_events(self.entries, "Millers")
        await self.publish({"activity": [], "journal": [finished_ev, started_ev]})
        self.assertEqual(self.channel.send.await_count, 2)
        sent_embeds = [call.kwargs["embed"] for call in self.channel.send.call_args_list]
        titles = [embed.title.lower() for embed in sent_embeds]
        self.assertTrue(any("started" in t for t in titles))
        self.assertTrue(any("finished" in t for t in titles))
        for embed in sent_embeds:
            self.assertIn("Played date", embed.description)

    async def test_pre_activation_suppression_and_post_activation_rating_during_recovery(self):
        import discord
        await self.db.set_feed_channel(1, 100)
        await self.db.set_guild_feed_preference(1, 10, True)
        t_activate = datetime(2026, 11, 12, 12, 0, tzinfo=timezone.utc)
        self.cog.clock = lambda: t_activate
        channel = MagicMock(spec=discord.TextChannel)
        channel.id = 100
        channel.permissions_for.return_value = SimpleNamespace(send_messages=True, embed_links=True)
        channel.send = AsyncMock(return_value=SimpleNamespace(id=501))
        guild = SimpleNamespace(members=[SimpleNamespace(id=10)], me=object(), get_channel=lambda _: channel)
        self.cog.bot = SimpleNamespace(db=self.db, get_guild=lambda _: guild)

        pre_act_session = replace(
            self.activity, activity_id="pre-act-session",
            played_date="2026-11-12", occurred_at="2026-11-12T11:00:00Z",
        )
        post_act_rating = replace(
            self.activity, activity_id="post-act-rating", action_type="rated",
            rating=4.5, occurred_at="2026-11-12T12:05:00Z",
        )
        self.cog._fetch_feed_sources = AsyncMock(return_value={"activity": [], "journal": []})
        with patch("bot.cogs.feed.asyncio.sleep", new=AsyncMock()):
            await self.cog.journal_feed_loop.coro(self.cog)
            self.cog._fetch_feed_sources.return_value = {
                "activity": [post_act_rating, pre_act_session], "journal": [],
            }
            self.cog._activity_snapshot_meta["millers"] = {
                "coverage": "limited", "pages_read": 3, "had_unresolved_gap": True,
                "had_prior_failure": False, "prior_boundary_id": None, "boundary_event_id": None,
            }
            await self.cog.journal_feed_loop.coro(self.cog)

        channel.send.assert_awaited_once()
        self.assertIn("rated", channel.send.call_args.kwargs["embed"].title.lower())
        pre_decision = await self.db.get_feed_event_decision(1, "millers", "pre-act-session")
        self.assertEqual(pre_decision["decision"], "suppressed_pre_activation")

    async def test_unprovable_timing_during_recovery_is_held_without_inventing_dates(self):
        import discord
        await self.db.set_feed_channel(1, 100)
        await self.db.set_guild_feed_preference(1, 10, True)
        self.cog.clock = lambda: datetime(2026, 11, 12, 12, 0, tzinfo=timezone.utc)
        channel = MagicMock(spec=discord.TextChannel)
        channel.id = 100
        channel.permissions_for.return_value = SimpleNamespace(send_messages=True, embed_links=True)
        channel.send = AsyncMock(return_value=SimpleNamespace(id=501))
        guild = SimpleNamespace(members=[SimpleNamespace(id=10)], me=object(), get_channel=lambda _: channel)
        self.cog.bot = SimpleNamespace(db=self.db, get_guild=lambda _: guild)

        undated_rating = replace(
            self.activity, activity_id="undated-recovery-rating", action_type="rated",
            rating=4.0, played_date=None, occurred_at=None,
        )
        await self.db.reconcile_guild_feed_activity(1, ["millers"], now=self.cog.clock())
        await self.db.seed_feed_source(1, "millers", "activity-display-v1", [])
        await self.db.seed_feed_source(1, "millers", "journal-display-v1", [])
        self.cog._activity_snapshot_meta["millers"] = {
            "coverage": "limited", "pages_read": 2, "had_unresolved_gap": False,
            "had_prior_failure": True, "prior_boundary_id": None, "boundary_event_id": None,
        }
        await self.cog._publish_sources(1, "millers", "Millers", None, channel, {"activity": [undated_rating], "journal": []})

        channel.send.assert_not_awaited()
        decision = await self.db.get_feed_event_decision(1, "millers", "undated-recovery-rating")
        self.assertEqual(decision["decision"], "held_uncertain")
        self.assertEqual(decision["reason"], "unprovable_action_time_during_recovery")

    async def test_multi_guild_delivery_failure_defers_boundary_commit_until_all_succeed(self):
        import discord
        await self.db.link_user(20, "Millers")
        await self.db.set_feed_channel(1, 101)
        await self.db.set_feed_channel(2, 202)
        await self.db.set_guild_feed_preference(1, 10, True)
        await self.db.set_guild_feed_preference(2, 20, True)
        await self.db.record_feed_source_attempt("millers", "activity")
        await self.db.record_feed_source_success(
            "millers", "activity", coverage="exhausted", pages_read=1, boundary_event_id="ev-0",
        )
        ch1 = MagicMock(spec=discord.TextChannel)
        ch1.id = 101
        ch1.permissions_for.return_value = SimpleNamespace(send_messages=True, embed_links=True)
        ch1.send = AsyncMock(return_value=SimpleNamespace(id=901))
        ch2 = MagicMock(spec=discord.TextChannel)
        ch2.id = 202
        ch2.permissions_for.return_value = SimpleNamespace(send_messages=True, embed_links=True)
        rejected = discord.Forbidden(SimpleNamespace(status=403, reason="Forbidden"), {"message": "blocked"})
        ch2.send = AsyncMock(side_effect=rejected)
        guilds = {
            1: SimpleNamespace(members=[SimpleNamespace(id=10)], me=object(), get_channel=lambda _: ch1),
            2: SimpleNamespace(members=[SimpleNamespace(id=20)], me=object(), get_channel=lambda _: ch2),
        }
        ev0 = replace(self.activity, activity_id="ev-0", occurred_at="2026-11-12T10:00:00Z")
        ev1 = replace(
            self.activity, activity_id="ev-1", game_slug="hades", game_title="Hades",
            occurred_at="2026-11-12T12:05:00Z",
        )
        api = SimpleNamespace(
            get_user_activity_snapshot=AsyncMock(return_value=SimpleNamespace(
                entries=[ev0], coverage="boundary_reached", pages_read=1,
            )),
            get_user_journal=AsyncMock(return_value=[]),
        )
        cog = FeedCog(
            SimpleNamespace(db=self.db, backloggd=api, get_guild=lambda gid: guilds.get(gid)),
            clock=lambda: datetime(2026, 11, 12, 12, 0, tzinfo=timezone.utc),
        )
        with patch("bot.cogs.feed.asyncio.sleep", new=AsyncMock()):
            await cog.journal_feed_loop.coro(cog)
            api.get_user_activity_snapshot.return_value = SimpleNamespace(
                entries=[ev1, ev0], coverage="boundary_reached", pages_read=1,
            )
            await cog.journal_feed_loop.coro(cog)
            mid_health = await self.db.get_profile_feed_source("millers", "activity")
            self.assertEqual(mid_health["last_continuous_boundary_event_id"], "ev-0")

            ch2.send = AsyncMock(return_value=SimpleNamespace(id=902))
            await cog.journal_feed_loop.coro(cog)
            final_health = await self.db.get_profile_feed_source("millers", "activity")
            self.assertEqual(final_health["last_continuous_boundary_event_id"], "ev-1")
            ch1.send.assert_awaited_once()
            ch2.send.assert_awaited_once()

    async def test_pending_merged_aliases_retry_after_restart_without_duplicate_posts(self):
        import discord
        await self.baseline()
        self.channel.send.side_effect = discord.Forbidden(
            SimpleNamespace(status=403, reason="Forbidden"), {"message": "temporary"}
        )
        act = replace(self.activity, activity_id="act-1")
        with self.assertRaises(discord.Forbidden):
            await self.publish({"activity": [act], "journal": [self.log]})
        self.assertEqual(self.channel.send.await_count, 1)
        await self.db.close()
        await self.db.connect()
        self.channel.send.side_effect = None
        await self.publish({"activity": [], "journal": []})
        self.assertEqual(self.channel.send.await_count, 2)
        await self.publish({"activity": [act], "journal": [self.log]})
        self.assertEqual(self.channel.send.await_count, 2)

    async def test_backdated_activity_completion_today_with_old_journal_entry_is_suppressed(self):
        self.cog.clock = lambda: datetime(2026, 11, 12, 12, 0, tzinfo=timezone.utc)
        await self.baseline()
        today_act = replace(
            self.activity,
            activity_id="backdated-act-today",
            action_type="completed",
            played_date=None,
            occurred_at="2026-11-12T10:00:00Z",
        )
        old_finished_log = replace(
            self.log,
            activity_id="journal_old_finished",
            event_ids=("journal_old_finished",),
            action_type="completed",
            played_date="October, 2026 01",
            session_labels=("Finished",),
        )

        await self.publish({"activity": [today_act], "journal": [old_finished_log]})
        # Even if journal temporarily fails on a subsequent poll, the suppressed activity stays suppressed.
        await self.publish({"activity": [today_act]})

        self.channel.send.assert_not_awaited()
        act_decision = await self.db.get_feed_event_decision(1, "millers", "backdated-act-today")
        log_decision = await self.db.get_feed_event_decision(1, "millers", "journal_old_finished")
        self.assertEqual(act_decision["decision"], "suppressed_old")
        self.assertEqual(log_decision["decision"], "suppressed_old")

    async def test_recent_backdated_session_merges_before_policy_and_posts_once(self):
        self.cog.clock = lambda: datetime(2026, 11, 12, 12, 0, tzinfo=timezone.utc)
        await self.baseline()
        today_act = replace(
            self.activity,
            activity_id="recent-backdate-act",
            action_type="completed",
            played_date=None,
            occurred_at="2026-11-12T10:00:00Z",
        )
        recent_log = replace(
            self.log,
            activity_id="journal_recent_backdate",
            event_ids=("journal_recent_backdate",),
            action_type="completed",
            played_date="November, 2026 09",
            session_labels=("Finished",),
        )

        await self.publish({"activity": [today_act], "journal": [recent_log]})

        self.channel.send.assert_awaited_once()
        self.channel.get_partial_message.return_value.edit.assert_not_awaited()
        embed = self.channel.send.call_args.kwargs["embed"]
        self.assertIn("November, 2026 09", embed.description)
        self.assertIn("Finished", embed.description)

    async def test_first_successful_journal_after_failure_holds_backdated_activity(self):
        await self.publish({"activity": []})
        activity = replace(self.activity, activity_id="delayed-baseline", played_date=None)
        old_log = replace(self.log, played_date="2026-10-01")

        await self.publish({"activity": [activity], "journal": [old_log]})
        await self.publish({"activity": [activity]})

        self.channel.send.assert_not_awaited()
        decision = await self.db.get_feed_event_decision(1, "millers", activity.activity_id)
        self.assertEqual(decision["decision"], "held_uncertain")

    async def test_old_journal_evidence_restricts_rejected_send_across_restart(self):
        import discord
        await self.baseline()
        activity = replace(self.activity, activity_id="rejected-before-journal", played_date=None)
        self.channel.send.side_effect = discord.Forbidden(
            SimpleNamespace(status=403, reason="Forbidden"), {"message": "rejected"}
        )
        with self.assertRaises(discord.Forbidden):
            await self.publish({"activity": [activity], "journal": []})
        self.channel.send.side_effect = None
        old_log = replace(self.log, played_date="2026-10-01")

        await self.publish({"activity": [activity], "journal": [old_log]})
        await self.db.close()
        await self.db.connect()
        await self.publish({"activity": [activity]})

        self.assertEqual(self.channel.send.await_count, 1)  # Only the rejected attempt.
        for alias in (activity.activity_id, old_log.activity_id):
            decision = await self.db.get_feed_event_decision(1, "millers", alias)
            self.assertEqual(decision["decision"], "suppressed_old")

    async def test_delayed_recent_journal_uses_discovery_time_and_survives_retry(self):
        import discord
        await self.baseline()
        activity = replace(self.activity, activity_id="recent-late-evidence", played_date=None)
        rejection = discord.Forbidden(
            SimpleNamespace(status=403, reason="Forbidden"), {"message": "rejected"}
        )
        self.channel.send.side_effect = rejection
        with self.assertRaises(discord.Forbidden):
            await self.publish({"activity": [activity], "journal": []})
        # The played date was exactly seven days old at initial discovery.
        self.cog.clock = lambda: datetime(2026, 11, 20, 12, tzinfo=timezone.utc)
        recent_log = replace(self.log, played_date="2026-11-05")
        with self.assertRaises(discord.Forbidden):
            await self.publish({"activity": [activity], "journal": [recent_log]})
        self.channel.send.side_effect = None

        await self.publish({"activity": [activity]})

        self.assertEqual(self.channel.send.await_count, 3)
        self.assertIn("2026-11-05", self.channel.send.call_args.kwargs["embed"].description)
        decision = await self.db.get_feed_event_decision(1, "millers", activity.activity_id)
        self.assertEqual(decision["decision"], "eligible")

    async def test_uncertain_backdated_journal_linkage_holds_activity_without_announcing(self):
        self.cog.clock = lambda: datetime(2026, 11, 12, 12, 0, tzinfo=timezone.utc)
        await self.baseline()
        today_act = replace(
            self.activity,
            activity_id="ambiguous-backdate-act",
            action_type="completed",
            played_date=None,
            occurred_at="2026-11-12T10:00:00Z",
        )
        old_started = replace(
            self.log,
            activity_id="journal_old_started",
            event_ids=("journal_old_started",),
            action_type="playing",
            played_date="October, 2026 01",
            session_labels=("Started",),
        )
        old_finished = replace(
            self.log,
            activity_id="journal_old_finished",
            event_ids=("journal_old_finished",),
            action_type="completed",
            played_date="October, 2026 05",
            session_labels=("Finished",),
        )

        await self.publish({"activity": [today_act], "journal": [old_started, old_finished]})

        self.channel.send.assert_not_awaited()
        act_decision = await self.db.get_feed_event_decision(1, "millers", "ambiguous-backdate-act")
        self.assertEqual(act_decision["decision"], "held_uncertain")
        self.assertEqual(act_decision["reason"], "uncertain_backdated_journal_linkage")
        self.assertEqual(
            (await self.db.get_feed_event_decision(1, "millers", "journal_old_started"))["decision"],
            "suppressed_old",
        )
        self.assertEqual(
            (await self.db.get_feed_event_decision(1, "millers", "journal_old_finished"))["decision"],
            "suppressed_old",
        )


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
        links = await self.db.get_guild_linked_users([10, 11])
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
        self.assertEqual(await self.db.get_profile_ratings_dict("millers"), {"hades": 4.5})

    async def test_shared_sync_can_finish_after_its_requesting_member_unlinks(self):
        await self.db.link_user(10, "Millers")
        await self.db.link_user(11, "millers")
        await self.db.unlink_user(10)

        stored = await self.db.replace_user_library(10, "Millers", [
            GameEntry(slug="hades", title="Hades", rating=4.5, status=GameStatus.PLAYED)
        ])

        self.assertTrue(stored)
        self.assertEqual(await self.db.get_profile_ratings_dict("millers"), {"hades": 4.5})

    async def test_stale_sync_cannot_write_into_a_relinked_profile(self):
        await self.db.link_user(10, "OldProfile")
        await self.db.link_user(10, "NewProfile")

        stored = await self.db.replace_user_library(10, "OldProfile", [
            GameEntry(slug="hades", title="Hades", rating=4.5, status=GameStatus.PLAYED)
        ])

        self.assertFalse(stored)
        self.assertEqual(await self.db.get_profile_ratings_dict("NewProfile"), {})
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
