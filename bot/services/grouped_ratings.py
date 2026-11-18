"""Resolve rating changes from games named in a newly observed activity group."""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
from datetime import datetime, timedelta
from typing import NamedTuple, TYPE_CHECKING

from bot.feed_models import GROUPED_RATING_ID_PREFIX, FeedEvent
from bot.feed_policy import parse_utc_datetime

if TYPE_CHECKING:
    from backloggd import ActivityGame, GroupedActivityEntry

logger = logging.getLogger("backloggd.services.grouped_ratings")
_SOURCE = "grouped-ratings"
_LOOKUP_LIMIT = 8


class _Candidate(NamedTuple):
    occurred_at: datetime
    group: GroupedActivityEntry
    game: ActivityGame
    event_id: str
    is_latest: bool


async def resolve_grouped_ratings(bot, profile_key, username, entries, previous_poll_at, now):
    groups = [
        (timestamp, entry)
        for entry in entries
        if getattr(entry, "grouped_games", ())
        and (timestamp := parse_utc_datetime(entry.occurred_at)) is not None
        and now - timedelta(days=7) <= timestamp <= now
    ]
    if not groups or not previous_poll_at:
        return []

    candidates = []
    # The per-game page shows one current value. Attribute it only to the
    # newest relevant action for that game on this activity page.
    newest_action = {}
    for entry in entries:
        if entry.action_type in {"rated", "reviewed"} and entry.rating is not None:
            timestamp = parse_utc_datetime(entry.occurred_at)
            if timestamp and now - timedelta(days=7) <= timestamp <= now and (
                entry.game_slug not in newest_action or timestamp > newest_action[entry.game_slug]
            ):
                newest_action[entry.game_slug] = timestamp
    for timestamp, group in sorted(groups, key=lambda row: row[0], reverse=True):
        for game in group.grouped_games:
            identity = f"{group.activity_id}:{game.slug}"
            event_id = GROUPED_RATING_ID_PREFIX + hashlib.sha256(identity.encode()).hexdigest()[:24]
            newest = newest_action.get(game.slug)
            candidates.append(_Candidate(timestamp, group, game, event_id, newest is None or timestamp > newest))
            newest_action[game.slug] = max(timestamp, newest) if newest else timestamp

    observations = await bot.db.get_profile_activity_observations(
        profile_key, _SOURCE,
        [key for row in candidates for key in (row.event_id, f"score:{row.game.slug}")],
    )
    lookups = [
        row for row in candidates
        if row.is_latest and (
            # Pending lookups retain their original eligibility across polls.
            observations.get(row.event_id) == "pending"
            or (row.event_id not in observations and row.occurred_at > previous_poll_at)
        )
    ]
    cached = await bot.db.get_cached_profile_rating_evidence(
        profile_key, [row.game.slug for row in lookups[:_LOOKUP_LIMIT]]
    )
    updates = {}
    for index, (timestamp, _, game, event_id, _) in enumerate(lookups):
        updates[event_id] = "pending"
        if index >= _LOOKUP_LIMIT:
            continue
        try:
            rating = await asyncio.wait_for(bot.backloggd.get_user_game_rating(username, game.slug), timeout=5)
        except Exception as exc:
            logger.debug("Grouped rating lookup failed for %s/%s: %s", profile_key, game.slug, type(exc).__name__)
            continue

        score_key = f"score:{game.slug}"
        prior_values = []
        if score_key in observations:
            observed, score = json.loads(observations[score_key])
            prior_values.append((parse_utc_datetime(observed), score))
        if game.slug in cached:
            observed, score = cached[game.slug]
            prior_values.append((parse_utc_datetime(observed), score))
        prior_values = [value for value in prior_values if value[0] and value[0] < timestamp]
        previous = max(prior_values, key=lambda value: value[0])[1] if prior_values else None
        updates[event_id] = (
            json.dumps([previous, rating])
            if prior_values and rating is not None and previous != rating
            else "null"
        )
        updates[score_key] = json.dumps([now.isoformat(), rating])
    if updates:
        await bot.db.record_profile_activity_observations(profile_key, _SOURCE, updates.items())
        observations.update(updates)

    events = []
    for _, group, game, event_id, _ in candidates:
        stored = observations.get(event_id)
        if stored is None or stored == "pending":
            continue
        change = json.loads(stored)
        if change is not None:
            previous, rating = change
            events.append(FeedEvent(
                event_id, game.title, game.slug, game.cover_url, "rated", "rated",
                rating, None, group.date_str, occurred_at=group.occurred_at,
                event_ids=(event_id,), previous_rating=previous,
            ))
    return events
