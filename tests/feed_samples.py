"""Typed SDK samples; captured HTML and parser tests live in backloggd-py."""
from backloggd import ActivityEntry, JournalEntry
from bot.feed_models import FeedEvent


def completion_event():
    return FeedEvent.from_activity(ActivityEntry(
        activity_id="millers-vice-city-completed", game_title="Grand Theft Auto: Vice City",
        game_slug="grand-theft-auto-vice-city", cover_url="https://images.igdb.com/igdb/image/upload/t_cover_big/co2lbb.jpg",
        action_type="completed", action_text="completed", rating=3.0, review_notes=None,
        date_str="10:48", occurred_at="2026-11-11T15:48:38Z",
    ))


def journal_entries():
    return [JournalEntry(
        entry_id="733", game_title="Grand Theft Auto: Vice City", game_slug="grand-theft-auto-vice-city",
        played_date=date, rating=3.0, session_labels=(label,),
        cover_url="https://images.igdb.com/igdb/image/upload/t_cover_big/co2lbb.jpg",
    ) for date, label in [("November, 2026 11", "Finished"), ("November, 2026 05", "Started")]]
