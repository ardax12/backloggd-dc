"""Normalize journal milestones and select announcements without collection noise."""
import hashlib
from collections import Counter, defaultdict
from dataclasses import replace
from datetime import datetime

from bot.feed_models import GROUPED_RATING_ID_PREFIX, FeedEvent

VISIBLE_ACTIONS = {"playing", "completed", "rated", "reviewed", "replay"}


def calendar_date(value):
    if not value:
        return None
    for pattern in ("%Y-%m-%d", "%B, %Y %d", "%B %Y %d"):
        try:
            return datetime.strptime(value, pattern).date().isoformat()
        except ValueError:
            pass
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).date().isoformat()
    except ValueError:
        return None


def journal_events(entries, username):
    events = []
    occurrences = Counter()
    for entry in entries:
        labels = getattr(entry, "session_labels", ())
        action = "completed" if "Finished" in labels else "playing" if "Started" in labels else "replay" if entry.is_replay else "played"
        # Public pages have no session ID. Preserve identical rows by count,
        # while excluding editable ratings and review text from identity.
        identity = f"{username.lower()}:{entry.game_slug}:{entry.played_date}:{labels}:{entry.is_replay}"
        occurrences[identity] += 1
        identifier = "journal_" + hashlib.sha256(f"{identity}:{occurrences[identity]}".encode()).hexdigest()[:24]
        events.append(FeedEvent(
            identifier, entry.game_title, entry.game_slug, entry.cover_url,
            action, action, entry.rating, entry.review_notes, None,
            played_date=entry.played_date, platform=entry.platform,
            is_replay=entry.is_replay, session_labels=labels, source="journal",
            event_ids=(identifier,),
        ))
    return events


def build_announcements(activities, journal, username):
    """Merge unique milestones; never infer a rating action from stars alone."""
    activities = [FeedEvent.from_activity(e) for e in activities]
    activity = [replace(e, event_ids=e.event_ids or (e.activity_id,)) for e in activities if e.action_type in VISIBLE_ACTIONS]
    logs = [e for e in journal_events(journal, username) if e.action_type in VISIBLE_ACTIONS]
    return merge_announcements(activity, logs)


def merge_milestones(activities, logs):
    def match_key(event):
        date = calendar_date(event.played_date or event.occurred_at)
        if date and event.action_type in {"playing", "completed", "replay"}:
            return event.game_slug, event.action_type, date
        return None
    activity_groups, log_groups = defaultdict(list), defaultdict(list)
    for event in activities:
        activity_groups[match_key(event)].append(event)
    for event in logs:
        log_groups[match_key(event)].append(event)
    merged, consumed = [], set()
    for event in activities:
        key = match_key(event)
        matches = log_groups.get(key, []) if key else []
        linked = [
            log for log in logs if log.activity_id in event.event_ids
        ] if event.action_type in {"playing", "completed", "replay"} else []
        if len(linked) == 1 or (key is not None and len(activity_groups[key]) == 1 and len(matches) == 1):
            log = linked[0] if linked else matches[0]
            event = replace(event, played_date=log.played_date,
                            platform=log.platform or event.platform,
                            is_replay=log.is_replay or event.is_replay,
                            session_labels=log.session_labels,
                            review_notes=log.review_notes or event.review_notes,
                            event_ids=tuple(dict.fromkeys((*event.event_ids, *log.event_ids))))
            consumed.add(log.activity_id)
        merged.append(event)
    merged.extend(e for e in logs if e.activity_id not in consumed)
    return merged


def link_backdated_milestones(merged, *, baseline_or_saved_ids=(), uncertain_journal_ids=()):
    """Link unmatched milestones, holding newly baselined journal evidence as uncertain.

    Returns (events, uncertain_reasons) where uncertain_reasons maps activity_id
    to the hold reason whenever dated journal evidence cannot be uniquely paired.
    """
    saved_ids = set(baseline_or_saved_ids)
    uncertain_ids = set(uncertain_journal_ids)
    acts_by_game = defaultdict(list)
    logs_by_game = defaultdict(list)

    for event in merged:
        if event.action_type not in {"playing", "completed", "replay"}:
            continue
        if event.source == "activity":
            if (
                event.played_date is None
                and not any(str(eid).startswith("journal_") for eid in event.event_ids)
                and not any(eid in saved_ids for eid in event.event_ids)
            ):
                acts_by_game[event.game_slug].append(event)
        elif event.source == "journal":
            if event.activity_id in uncertain_ids or not any(eid in saved_ids for eid in event.event_ids):
                logs_by_game[event.game_slug].append(event)

    replacements = {}
    consumed_log_ids = set()
    uncertain_reasons = {}

    for game_slug, acts in acts_by_game.items():
        game_logs = logs_by_game.get(game_slug, [])
        if not game_logs:
            continue
        if (
            len(acts) == 1
            and len(game_logs) == 1
            and acts[0].action_type == game_logs[0].action_type
            and game_logs[0].activity_id not in uncertain_ids
        ):
            act, log = acts[0], game_logs[0]
            replacements[act.activity_id] = replace(
                act,
                played_date=log.played_date,
                platform=log.platform or act.platform,
                is_replay=log.is_replay or act.is_replay,
                session_labels=log.session_labels,
                review_notes=log.review_notes or act.review_notes,
                event_ids=tuple(dict.fromkeys((*act.event_ids, *log.event_ids))),
            )
            consumed_log_ids.add(log.activity_id)
        else:
            for act in acts:
                uncertain_reasons[act.activity_id] = "uncertain_backdated_journal_linkage"

    updated = []
    for event in merged:
        if event.activity_id in consumed_log_ids:
            continue
        updated.append(replacements.get(event.activity_id, event))
    return updated, uncertain_reasons


def merge_ratings_and_reviews(merged, *, baseline_ids=frozenset()):
    merged = list(merged)
    grouped_rating_counts = Counter(
        (e.game_slug, calendar_date(e.occurred_at))
        for e in merged if e.activity_id.startswith(GROUPED_RATING_ID_PREFIX)
    )
    result = []
    for event in merged:
        if event.action_type in {"rated", "reviewed"}:
            is_grouped_rating = event.activity_id.startswith(GROUPED_RATING_ID_PREFIX)
            matches = [e for e in merged if e.action_type in {"playing", "completed", "replay"}
                       and e.game_slug == event.game_slug
                       and ((e.reference and e.reference == event.reference)
                            or (e.occurred_at and e.occurred_at == event.occurred_at)
                            or event.activity_id in e.event_ids)]
            if is_grouped_rating:
                matches = [e for e in matches if e.activity_id not in baseline_ids]
            if not matches and is_grouped_rating:
                day = calendar_date(event.occurred_at)
                if day and grouped_rating_counts[event.game_slug, day] == 1:
                    matches = [
                        e for e in merged
                        if e.action_type in {"playing", "completed", "replay"}
                        and e.game_slug == event.game_slug
                        and e.activity_id not in baseline_ids
                        and calendar_date(e.played_date) == day
                        and (e.source == "journal" or any(alias.startswith("journal_") for alias in e.event_ids))
                    ]
                    if any(e.rating is not None and e.rating != event.rating for e in matches):
                        matches = []
            if len(matches) == 1:
                index = merged.index(matches[0])
                combined = replace(matches[0], review_notes=event.review_notes or matches[0].review_notes,
                                   rating=event.rating if event.rating is not None else matches[0].rating,
                                   has_spoilers=event.has_spoilers or matches[0].has_spoilers,
                                   event_ids=tuple(dict.fromkeys((*matches[0].event_ids, *event.event_ids))))
                merged[index] = combined
                continue
        result.append(event)
    # Milestones may have been enriched after they were added to result.
    by_id = {e.activity_id: e for e in merged}
    return [by_id[e.activity_id] for e in result]


def merge_announcements(activities, logs):
    return merge_ratings_and_reviews(merge_milestones(activities, logs))
