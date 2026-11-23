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


_CYCLE_ACTION_PRIORITY = {
    "reviewed": 0,
    "completed": 1,
    "replay": 2,
    "playing": 3,
    "rated": 4,
}


def _inherit_sibling_metadata(winner: FeedEvent, sibling: FeedEvent, *, include_review: bool = False) -> FeedEvent:
    """Copy non-conflicting metadata from an absorbed sibling onto the winning event."""
    labels = tuple(dict.fromkeys((*winner.session_labels, *sibling.session_labels)))
    kwargs = {
        "rating": winner.rating if winner.rating is not None else sibling.rating,
        "platform": winner.platform or sibling.platform,
        "played_date": winner.played_date or sibling.played_date,
        "is_replay": winner.is_replay or sibling.is_replay,
        "session_labels": labels,
    }
    if include_review:
        kwargs["review_notes"] = winner.review_notes or sibling.review_notes
        kwargs["has_spoilers"] = winner.has_spoilers or sibling.has_spoilers
    return replace(winner, **kwargs)


def _should_alias_sibling(winner: FeedEvent, sibling: FeedEvent) -> bool:
    """Return True when sibling belongs to the same Backloggd log/action as winner."""
    if winner.reference and winner.reference == sibling.reference:
        return True
    if winner.occurred_at and sibling.occurred_at and winner.occurred_at == sibling.occurred_at:
        return True
    if sibling.activity_id.startswith(GROUPED_RATING_ID_PREFIX):
        return True
    return False


def merge_ratings_and_reviews(
    merged,
    *,
    baseline_ids=frozenset(),
    sent_ids=None,
    saved_action_types=None,
):
    """Merge ratings/reviews into matching milestones.

    When ``sent_ids`` is provided (pipeline mode), only already-sent milestones
    are merged here so unseen events stay separate for per-guild filtering and
    single-cycle collapsing.
    """
    merged = list(merged)
    grouped_rating_counts = Counter(
        (e.game_slug, calendar_date(e.occurred_at))
        for e in merged if e.activity_id.startswith(GROUPED_RATING_ID_PREFIX)
    )
    saved_actions = saved_action_types or {}
    result_indices: list[int] = []
    consumed_indices: set[int] = set()
    replaced_targets: set[int] = set()
    for idx, event in enumerate(merged):
        if idx not in replaced_targets and event.action_type in {"rated", "reviewed"}:
            event_aliases = event.event_ids or (event.activity_id,)
            if sent_ids is not None and any(alias in baseline_ids for alias in event_aliases):
                result_indices.append(idx)
                continue
            is_grouped_rating = event.activity_id.startswith(GROUPED_RATING_ID_PREFIX)
            target_actions = {"playing", "completed", "replay"}
            candidate_indices = [
                i for i, e in enumerate(merged)
                if i != idx
                and i not in consumed_indices
                and e.action_type in target_actions
                and e.game_slug == event.game_slug
                and (
                    (e.reference and e.reference == event.reference)
                    or (e.occurred_at and e.occurred_at == event.occurred_at)
                    or event.activity_id in e.event_ids
                )
            ]
            if not candidate_indices and event.action_type == "rated":
                candidate_indices = [
                    i for i, e in enumerate(merged)
                    if i != idx
                    and i not in consumed_indices
                    and e.action_type == "reviewed"
                    and e.game_slug == event.game_slug
                    and (
                        (e.reference and e.reference == event.reference)
                        or (e.occurred_at and e.occurred_at == event.occurred_at)
                        or event.activity_id in e.event_ids
                    )
                ]
            if sent_ids is not None:
                candidate_indices = [
                    i for i in candidate_indices
                    if any(alias in sent_ids for alias in (merged[i].event_ids or (merged[i].activity_id,)))
                ]
            if is_grouped_rating:
                candidate_indices = [i for i in candidate_indices if merged[i].activity_id not in baseline_ids]
            if not candidate_indices and is_grouped_rating and sent_ids is None:
                day = calendar_date(event.occurred_at)
                if day and grouped_rating_counts[event.game_slug, day] == 1:
                    candidate_indices = [
                        i for i, e in enumerate(merged)
                        if i != idx
                        and i not in consumed_indices
                        and e.action_type in target_actions
                        and e.game_slug == event.game_slug
                        and e.activity_id not in baseline_ids
                        and calendar_date(e.played_date) == day
                        and (e.source == "journal" or any(alias.startswith("journal_") for alias in e.event_ids))
                    ]
                    if any(merged[i].rating is not None and merged[i].rating != event.rating for i in candidate_indices):
                        candidate_indices = []
            if len(candidate_indices) == 1:
                target_idx = candidate_indices[0]
                target = merged[target_idx]
                target_aliases = target.event_ids or (target.activity_id,)
                saved_action = next(
                    (saved_actions[alias] for alias in target_aliases if alias in saved_actions),
                    None,
                )
                promote_review = (
                    event.action_type == "reviewed"
                    and (sent_ids is None or saved_action == "reviewed")
                )
                if promote_review:
                    combined = _inherit_sibling_metadata(event, target, include_review=False)
                    combined = replace(
                        combined,
                        event_ids=tuple(dict.fromkeys((*event.event_ids, *target.event_ids))),
                    )
                else:
                    combined = replace(
                        target,
                        review_notes=event.review_notes or target.review_notes,
                        rating=event.rating if event.rating is not None else target.rating,
                        has_spoilers=event.has_spoilers or target.has_spoilers,
                        event_ids=tuple(dict.fromkeys((*target.event_ids, *event.event_ids))),
                    )
                merged[target_idx] = combined
                consumed_indices.add(idx)
                if promote_review:
                    replaced_targets.add(target_idx)
                continue
        result_indices.append(idx)
    return [merged[i] for i in result_indices]


def collapse_cycle_announcements(events: list[FeedEvent]) -> list[tuple[list[FeedEvent], list[FeedEvent]]]:
    """Collapse unseen events from a single poll cycle per game.

    Rules within one cycle for a given ``game_slug``:
    - If one or more ``reviewed`` events exist, post every distinct ``reviewed``
      event and absorb non-review siblings (inheriting their metadata).
    - Otherwise collapse to one winner by priority:
      ``completed`` > ``replay`` > ``playing`` > ``rated``.
    - Same-log siblings (matching reference, exact timestamp, or grouped rating)
      are aliased onto the winner; other absorbed siblings are returned in the
      suppressed list to be baselined after delivery succeeds.
    """
    groups: dict[str, list[FeedEvent]] = defaultdict(list)
    for event in events:
        groups[event.game_slug].append(event)

    collapsed: list[tuple[list[FeedEvent], list[FeedEvent]]] = []
    for group in groups.values():
        if len(group) == 1:
            collapsed.append(([group[0]], []))
            continue

        reviews = [e for e in group if e.action_type == "reviewed"]
        if reviews:
            others = sorted(
                (e for e in group if e.action_type != "reviewed"),
                key=lambda e: (
                    _CYCLE_ACTION_PRIORITY.get(e.action_type, 99),
                    0 if e.source == "activity" else 1,
                ),
            )
            winners = list(reviews)
            suppressed: list[FeedEvent] = []
            for other in others:
                if len(winners) == 1:
                    target_idx: int | None = 0
                else:
                    matched = [
                        i for i, rev in enumerate(winners)
                        if (rev.reference and rev.reference == other.reference)
                        or (rev.occurred_at and other.occurred_at and rev.occurred_at == other.occurred_at)
                    ]
                    target_idx = matched[0] if len(matched) == 1 else None

                if target_idx is None:
                    suppressed.append(other)
                    continue

                target = winners[target_idx]
                updated = _inherit_sibling_metadata(target, other, include_review=False)
                if _should_alias_sibling(target, other):
                    updated = replace(
                        updated,
                        event_ids=tuple(dict.fromkeys((*updated.event_ids, *other.event_ids))),
                    )
                else:
                    suppressed.append(other)
                winners[target_idx] = updated
            collapsed.append((winners, suppressed))
            continue

        winner_idx = min(
            range(len(group)),
            key=lambda i: (
                _CYCLE_ACTION_PRIORITY.get(group[i].action_type, 99),
                0 if group[i].source == "activity" else 1,
                0 if group[i].review_notes else 1,
                0 if group[i].rating is not None else 1,
            ),
        )
        winner = group[winner_idx]
        others = sorted(
            (e for i, e in enumerate(group) if i != winner_idx),
            key=lambda e: (
                _CYCLE_ACTION_PRIORITY.get(e.action_type, 99),
                0 if e.source == "activity" else 1,
            ),
        )
        suppressed = []
        for other in others:
            winner = _inherit_sibling_metadata(winner, other, include_review=True)
            if _should_alias_sibling(winner, other):
                winner = replace(
                    winner,
                    event_ids=tuple(dict.fromkeys((*winner.event_ids, *other.event_ids))),
                )
            else:
                suppressed.append(other)
        collapsed.append(([winner], suppressed))

    return collapsed


def merge_announcements(activities, logs):
    return merge_ratings_and_reviews(merge_milestones(activities, logs))

