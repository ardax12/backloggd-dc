import os
os.environ["PYTHON_DOTENV_DISABLED"] = "1"
import unittest
from dataclasses import replace
from bot.ui.feed_embed import create_journal_feed_embed
from feed_samples import completion_event, journal_entries

class DisplayRuleTests(unittest.TestCase):




    def test_completion_rating_is_not_a_separate_rating_action(self):
        from bot.feed_rules import build_announcements
        activity = [completion_event()]
        journal = journal_entries()
        events = build_announcements(activity, journal, "Millers")
        completion = [e for e in events if e.game_slug == "grand-theft-auto-vice-city" and e.action_type == "completed"]
        self.assertEqual(len(completion), 1)
        self.assertEqual(completion[0].played_date, "November, 2026 11")
        self.assertEqual(completion[0].rating, 3.0)

    def test_hidden_actions_and_daily_sessions_do_not_announce(self):
        from bot.feed_rules import build_announcements
        event = completion_event()
        for action in ("backlog", "wishlist", "liked", "followed", "played", "listed", "badge", "commented"):
            self.assertEqual(build_announcements([replace(event, action_type=action)], [], "Millers"), [])
        entries = journal_entries()
        self.assertEqual(build_announcements([], [replace(entries[0], session_labels=(), is_replay=False)], "Millers"), [])

    def test_rating_only_and_review_actions_are_kept(self):
        from bot.feed_rules import build_announcements
        event = completion_event()
        for action in ("rated", "reviewed"):
            events = build_announcements([replace(event, action_type=action)], [], "Millers")
            self.assertEqual(len(events), 1)
            self.assertEqual(events[0].action_type, action)


class AnnouncementTests(unittest.TestCase):
    def setUp(self):
        self.event = completion_event()



    def test_numeric_permalink_combines_review_with_completion(self):
        from bot.feed_rules import build_announcements
        finish = replace(self.event, reference="/u/Millers/review/123/")
        review = replace(finish, activity_id="review", action_type="reviewed", occurred_at="2026-11-12T00:00:00Z", review_notes="Great game", has_spoilers=True)
        events = build_announcements([finish, review], [], "Millers")
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0].review_notes, "Great game")
        self.assertIn("review", events[0].event_ids)

    def test_separate_rating_timestamp_does_not_merge(self):
        from bot.feed_rules import build_announcements
        rating = replace(self.event, activity_id="new-rating", action_type="rated", occurred_at="2026-11-12T15:48:38Z", rating=4.0)
        self.assertEqual(len(build_announcements([self.event, rating], [], "Millers")), 2)

    def test_ambiguous_same_day_sessions_are_not_collapsed(self):
        from bot.feed_rules import build_announcements, journal_events
        entries = journal_entries()
        events = build_announcements([self.event], [entries[0], entries[0]], "Millers")
        self.assertEqual(len(events), 3)
        logs = journal_events([entries[0], entries[0]], "Millers")
        self.assertNotEqual(logs[0].activity_id, logs[1].activity_id)
        self.assertNotEqual(logs[0].activity_id, journal_events([entries[0]], "AnotherUser")[0].activity_id)

    def test_backdated_embed_does_not_claim_played_today(self):
        event = replace(self.event, played_date="November, 2026 05", session_labels=("Finished",))
        embed = create_journal_feed_embed("Millers", None, event)
        self.assertIn("logged a playthrough", embed.title)
        self.assertIn("**Played date:** November, 2026 05", embed.description)
        self.assertIn("**Session:** Finished", embed.description)

    def test_delayed_poll_does_not_change_an_on_time_completion_into_backdated(self):
        embed = create_journal_feed_embed("Millers", None, replace(self.event, played_date="November, 2026 11"))
        self.assertIn("completed", embed.title)

    def test_rating_change_and_spoiler_review_rendering(self):
        event = replace(self.event, action_type="rated", rating=4.0, previous_rating=3.0, review_notes="Spoiler here", has_spoilers=True)
        embed = create_journal_feed_embed("Millers", None, event)
        self.assertIn("changed their rating", embed.title)
        self.assertIn("3.0", embed.description)
        self.assertIn("4.0", embed.description)
        self.assertIn("||Spoiler here||", embed.fields[0].value)


class MergeHistoryTests(unittest.TestCase):
    def test_a_later_same_day_action_does_not_unmerge_existing_alias(self):
        from bot.feed_rules import build_announcements, merge_announcements, journal_events
        activity = completion_event()
        log = journal_entries()[0]
        combined = build_announcements([activity], [log], "Millers")[0]
        another = replace(activity, activity_id="another-playthrough", occurred_at="2026-11-11T18:00:00Z", event_ids=("another-playthrough",))
        events = merge_announcements([combined, another], journal_events([log], "Millers"))
        self.assertEqual(len(events), 2)
        self.assertEqual(events[0].session_labels, ("Finished",))
