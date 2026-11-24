"""Feed history loading, source merging, identity reconciliation, and discovery planning."""

from __future__ import annotations

import json
import logging
from dataclasses import asdict, dataclass, replace
from datetime import datetime, timedelta, timezone
from typing import Any

from bot.feed_decisions import (
    accept_feed_decisions,
    feed_decision_aliases,
    plan_feed_decisions,
    prepare_feed_candidates,
)
from bot.feed_models import FeedEvent
from bot.feed_policy import classify_feed_event, parse_utc_datetime
from bot.feed_rules import (
    VISIBLE_ACTIONS,
    calendar_date,
    link_backdated_milestones,
    merge_milestones,
    merge_ratings_and_reviews,
)

logger = logging.getLogger("backloggd.services.feed_pipeline")

SAME_ACTION_SESSION_EDIT_WINDOW = timedelta(minutes=30)


@dataclass(frozen=True)
class FeedTarget:
    guild_id: int
    profile_key: str
    username: str
    avatar_url: str | None
    channel: Any


@dataclass(frozen=True)
class FeedHistory:
    # This recipient's in-memory ledger is updated only as writes are confirmed.
    saved: dict
    persisted_aliases: dict
    canonical_activity: dict
    canonical_journal: dict
    row_events_by_id: dict
    sources: dict
    resolved_event_ids: set[str]
    new_journal_baseline_ids: set[str]


@dataclass(frozen=True)
class PreparedAnnouncements:
    events: list[FeedEvent]
    activity_rating_history: dict
    journal_rating_history: dict
    journal_rating_times_unknown: set[str]
    delivered_alias_ids: set[str]


def valid_alias(value: Any) -> str | None:
    if isinstance(value, str):
        return value if value.strip() else None
    return None


def parse_persisted_feed_history(rows):
    canonical_groups: dict[tuple[str, str], dict[str, Any]] = {}
    row_bindings: list[tuple[str, tuple[str, str]]] = []
    row_events_by_id: dict[str, FeedEvent] = {}
    for row in rows:
        try:
            data = json.loads(row["payload"])
            if not isinstance(data, dict):
                continue
            data["event_ids"] = tuple(data.get("event_ids") or ())
            data["session_labels"] = tuple(data.get("session_labels") or ())
            event = FeedEvent(**data)
        except (TypeError, ValueError):
            logger.warning(
                "Skipping malformed persisted feed payload for event_id=%r",
                dict(row).get("event_id") if hasattr(row, "keys") else None,
            )
            continue
        row_id = valid_alias(row["event_id"])
        if row_id:
            row_events_by_id[row_id] = event
        canonical_key = (event.source, event.activity_id)
        state = dict(row).get("delivery_state")
        message_id = row["message_id"]
        # Like delivery, prefer the oldest saved message among competing
        # sent aliases. The row ID breaks remaining ties across lookup batches.
        rank = (
            0 if message_id is not None else 1,
            0 if state in {"sent", "deleted", "pending", "unknown"} else 1,
            message_id if message_id is not None else 0,
            row_id or "",
        )
        group = canonical_groups.get(canonical_key)
        if group is None:
            group = {"event": event, "rank": rank, "aliases": set()}
            canonical_groups[canonical_key] = group
        elif rank < group["rank"]:
            group["event"] = event
            group["rank"] = rank
        for raw_alias in (row["event_id"], event.activity_id, *event.event_ids):
            alias = valid_alias(raw_alias)
            if alias:
                group["aliases"].add(alias)
        if row_id:
            row_bindings.append((row_id, canonical_key))

    canonical_events: dict[tuple[str, str], FeedEvent] = {}
    canonical_by_source: dict[str, dict[str, FeedEvent]] = {"activity": {}, "journal": {}}
    alias_ranks: dict[str, tuple[int, int, int, str]] = {}
    persisted_aliases: dict[str, FeedEvent] = {}

    for canonical_key, group in canonical_groups.items():
        event = group["event"]
        # Keep the saved payload's alias order and append other known aliases
        # deterministically so archive lookup order cannot change the event.
        saved_order = tuple(dict.fromkeys(
            alias for alias in event.event_ids if valid_alias(alias)
        ))
        aliases = (*saved_order, *sorted(group["aliases"].difference(saved_order)))
        aliases = aliases or (event.activity_id,)
        canonical = replace(event, event_ids=aliases)
        canonical_events[canonical_key] = canonical
        if canonical.source in canonical_by_source:
            canonical_by_source[canonical.source][canonical.activity_id] = canonical
        rank = group["rank"]
        for alias in aliases:
            if alias not in alias_ranks or rank < alias_ranks[alias]:
                alias_ranks[alias] = rank
                persisted_aliases[alias] = canonical

    for row_id, canonical_key in row_bindings:
        persisted_aliases[row_id] = canonical_events[canonical_key]

    return (
        persisted_aliases,
        canonical_by_source["activity"],
        canonical_by_source["journal"],
        row_events_by_id,
    )


def build_persisted_feed_history(rows):
    persisted_aliases, canonical_activity, canonical_journal, _ = (
        parse_persisted_feed_history(rows)
    )
    return persisted_aliases, canonical_activity, canonical_journal


def build_ambiguous_event_decision(event, now):
    _, _, event_date = classify_feed_event(event, now)
    return {
        "kind": "ambiguity",
        "event_id": event.activity_id,
        "source": event.source,
        "event_date": event_date,
        "reason": "raw_session_identity_changed_without_stable_reference",
        "payload": json.dumps(asdict(event), sort_keys=True),
        "observed_at": now,
    }


def keep_old_posted_sessions_and_unlink_old_baselines(events, explicit_actions, saved, now):
    if not explicit_actions:
        return events
    kept = []
    for event in events:
        if event.action_type not in {"playing", "completed", "replay"}:
            kept.append(event)
            continue
        aliases = event.event_ids or (event.activity_id,)
        records = [saved[alias] for alias in aliases if alias in saved]
        is_posted = any(
            row["message_id"] is not None
            or dict(row).get("delivery_state") in {"sent", "deleted", "pending", "unknown"}
            for row in records
        )
        decision, _, _ = classify_feed_event(event, now)
        has_explicit_same_game = any(action.game_slug == event.game_slug for action in explicit_actions)
        if has_explicit_same_game and not is_posted and decision != "eligible":
            continue
        kept.append(event)
    return kept


def previous_rating_from_history(
    event,
    activity_history,
    journal_history,
    *,
    journal_time_unknown: bool = False,
):
    """Use the latest strictly earlier rating observation, never played_date."""
    action_time = parse_utc_datetime(event.occurred_at)
    if action_time is None:
        return None

    activity_candidates = [
        (timestamp, rating)
        for timestamp, rating in activity_history
        if timestamp < action_time
    ]
    if activity_candidates:
        latest_at = max(timestamp for timestamp, _ in activity_candidates)
        latest_ratings = {
            rating for timestamp, rating in activity_candidates if timestamp == latest_at
        }
        return next(iter(latest_ratings)) if len(latest_ratings) == 1 else None

    if journal_time_unknown:
        return None
    journal_candidates = [
        (timestamp, rating)
        for timestamp, rating in journal_history
        if timestamp < action_time
    ]
    if not journal_candidates:
        return None
    latest_at = max(timestamp for timestamp, _ in journal_candidates)
    latest_ratings = {
        rating for timestamp, rating in journal_candidates if timestamp == latest_at
    }
    return next(iter(latest_ratings)) if len(latest_ratings) == 1 else None


async def reconcile_changed_event_identities(
    pipeline: Any,
    guild_id,
    profile_key,
    source,
    events,
    historical,
    *,
    persisted_aliases=None,
    resolved_event_ids=None,
):
    """Reuse aliases only when identity is stable; hold ambiguous sessions."""
    now = pipeline.clock()
    alias_lookup = persisted_aliases if persisted_aliases is not None else historical
    seen_canonical = set()
    history = []
    for old in historical.values():
        key = (old.source, old.activity_id)
        if key not in seen_canonical:
            seen_canonical.add(key)
            history.append(old)
    incoming_reference_counts = {}
    for event in events:
        if event.reference:
            incoming_reference_counts[event.reference] = incoming_reference_counts.get(event.reference, 0) + 1
    reconciled = []
    identity_operations = []
    for event in events:
        previous = alias_lookup.get(event.activity_id)
        if previous:
            aliases = tuple(dict.fromkeys((*(previous.event_ids or (previous.activity_id,)), event.activity_id)))
            reconciled.append(replace(event, event_ids=aliases))
            continue

        if event.reference:
            matches = [old for old in history if old.reference == event.reference]
            if len(matches) == 1 and incoming_reference_counts[event.reference] == 1:
                previous = matches[0]
                aliases = tuple(dict.fromkeys((
                    *(previous.event_ids or (previous.activity_id,)),
                    previous.activity_id,
                    event.activity_id,
                )))
                resolved = replace(event, event_ids=aliases)
                if resolved_event_ids is not None:
                    resolved_event_ids.add(event.activity_id)
                else:
                    _, _, event_date = classify_feed_event(event, now)
                    identity_operations.append({
                        "kind": "resolve",
                        "event_id": event.activity_id,
                        "event_date": event_date,
                        "payload": json.dumps(asdict(resolved), sort_keys=True),
                    })
                reconciled.append(resolved)
                continue
            if len(matches) > 1 or (matches and incoming_reference_counts[event.reference] > 1):
                identity_operations.append(pipeline._build_ambiguous_event_decision(event, now))
                continue
            # A new stable public reference proves this is a distinct event.
            reconciled.append(event)
            continue

        is_session = (
            event.action_type in {"playing", "completed", "replay"}
            or event.is_replay
            or bool(set(event.session_labels) & {"Started", "Finished"})
        )
        if is_session:
            event_at = parse_utc_datetime(event.occurred_at)
            ambiguous_prior = []
            recent_same_action = []
            for old in history:
                if old.game_slug != event.game_slug:
                    continue
                if not (
                    old.action_type in {"playing", "completed", "replay"}
                    or old.is_replay
                    or bool(set(old.session_labels) & {"Started", "Finished"})
                ):
                    continue
                if old.action_type != event.action_type:
                    continue
                if old.reference:
                    ambiguous_prior.append(old)
                    continue
                old_at = parse_utc_datetime(old.occurred_at)
                if event_at is None or old_at is None or event_at <= old_at:
                    ambiguous_prior.append(old)
                    continue
                if (event_at - old_at) <= SAME_ACTION_SESSION_EDIT_WINDOW:
                    recent_same_action.append(old)
            if ambiguous_prior or len(recent_same_action) > 1:
                identity_operations.append(pipeline._build_ambiguous_event_decision(event, now))
                continue
            if len(recent_same_action) == 1:
                previous = recent_same_action[0]
                aliases = tuple(dict.fromkeys((
                    *(previous.event_ids or (previous.activity_id,)),
                    previous.activity_id,
                    event.activity_id,
                )))
                resolved = replace(event, event_ids=aliases)
                if resolved_event_ids is not None:
                    resolved_event_ids.add(event.activity_id)
                reconciled.append(resolved)
                continue
        reconciled.append(event)
    if identity_operations:
        await pipeline.bot.db.persist_feed_identity_decisions(guild_id, profile_key, operations=identity_operations)
    return reconciled


async def load_and_initialize_history(pipeline: Any, target: FeedTarget, sources) -> FeedHistory:
    """Reconcile identities, establish atomic baselines, and restore unsent work."""
    guild_id, profile_key = target.guild_id, target.profile_key
    sources = {source: list(events) for source, events in sources.items()}
    pending = await pipeline.bot.db.get_pending_feed_observations(guild_id, profile_key)
    pending_events: list[FeedEvent] = []
    for row in pending:
        data = json.loads(row["payload"])
        data["event_ids"] = tuple(data.get("event_ids", ()))
        data["session_labels"] = tuple(data.get("session_labels", ()))
        pending_events.append(FeedEvent(**data))

    seed_ids: list[str] = []
    seed_games: list[str] = []
    seed_refs: list[str] = []
    for source_events in sources.values():
        for event in source_events:
            if event.activity_id:
                seed_ids.append(event.activity_id)
            for eid in event.event_ids:
                if eid:
                    seed_ids.append(eid)
            if event.game_slug:
                seed_games.append(event.game_slug)
            if event.reference:
                seed_refs.append(event.reference)
    for event in pending_events:
        if event.activity_id:
            seed_ids.append(event.activity_id)
        for eid in event.event_ids:
            if eid:
                seed_ids.append(eid)
        if event.game_slug:
            seed_games.append(event.game_slug)
        if event.reference:
            seed_refs.append(event.reference)

    rows = list(await pipeline.bot.db.get_relevant_feed_messages(
        guild_id,
        profile_key,
        event_ids=seed_ids,
        game_slugs=seed_games,
        references=seed_refs,
    ))
    saved = {row["event_id"]: row for row in rows}
    persisted_aliases, canonical_activity, canonical_journal, row_events_by_id = (
        pipeline._parse_persisted_feed_history(rows)
    )
    histories = {"activity": canonical_activity, "journal": canonical_journal}
    existing_sources = await pipeline.bot.db.get_feed_sources(guild_id, profile_key)
    initialized_map = {
        source: (source + "-display-v1") in existing_sources
        for source in sources
    }
    resolved_event_ids: set[str] = set()
    for source, events in list(sources.items()):
        if initialized_map[source]:
            sources[source] = await pipeline._reconcile_changed_event_identities(
                guild_id,
                profile_key,
                source,
                events,
                histories.get(source, {}),
                persisted_aliases=persisted_aliases,
                resolved_event_ids=resolved_event_ids,
            )
    new_journal_baseline_ids = set()
    newly_seeded_ids: set[str] = set()
    any_first_baseline = False
    for source, events in sources.items():
        # Empty successful sources are baselined too. A source absent from
        # this mapping failed and must be retried without changing state.
        if initialized_map[source]:
            continue
        ids = [e.activity_id for e in events]
        baseline_payloads = {
            event.activity_id: json.dumps(
                asdict(replace(event, event_ids=(event.activity_id,))), sort_keys=True
            ) for event in events
        }
        now_baseline = pipeline.clock()
        baseline_decisions = []
        for event in events:
            payload = baseline_payloads[event.activity_id]
            _, _, event_date = classify_feed_event(event, now_baseline)
            baseline_decisions.append({
                "event_id": event.activity_id,
                "source": event.source,
                "event_date": event_date,
                "decision": "baseline",
                "reason": "initial_or_resume_source_baseline",
                "payload": payload,
                "observed_at": now_baseline,
            })
        first = await pipeline.bot.db.seed_feed_source_with_messages(
            guild_id,
            profile_key,
            source + "-display-v1",
            baseline_payloads.items(),
            baseline_decisions=baseline_decisions,
        )
        if first:
            any_first_baseline = True
            if source == "journal":
                new_journal_baseline_ids.update(ids)
            for eid, payload in baseline_payloads.items():
                if eid and (not isinstance(eid, str) or eid.strip()) and eid not in saved:
                    newly_seeded_ids.add(eid)
                    row_dict = {
                        "event_id": eid,
                        "payload": payload,
                        "message_id": None,
                        "channel_id": None,
                        "delivery_state": "baseline",
                    }
                    rows.append(row_dict)
                    saved[eid] = row_dict
    if any_first_baseline:
        # Refresh the lookup and canonical maps from the working set with newly seeded rows.
        persisted_aliases, canonical_activity, canonical_journal, row_events_by_id = (
            pipeline._parse_persisted_feed_history(rows)
        )
    for event in pending_events:
        if event.activity_id in newly_seeded_ids:
            continue
        source_events = sources.setdefault(event.source, [])
        if all(candidate.activity_id != event.activity_id for candidate in source_events):
            source_events.append(event)
        else:
            # Keep validated journal enrichment on an unsent retry even
            # when the current poll only returns the activity source.
            for index, current in enumerate(source_events):
                if current.activity_id == event.activity_id:
                    source_events[index] = replace(
                        current,
                        played_date=current.played_date or event.played_date,
                        platform=current.platform or event.platform,
                        session_labels=current.session_labels or event.session_labels,
                        is_replay=current.is_replay or event.is_replay,
                        event_ids=tuple(dict.fromkeys((*current.event_ids, *event.event_ids))),
                    )
                    break
    return FeedHistory(
        saved, persisted_aliases, canonical_activity, canonical_journal,
        row_events_by_id, sources, resolved_event_ids, new_journal_baseline_ids,
    )


def merge_source_events(history: FeedHistory, *, partial_journal: bool = False):
    """Combine current observations and canonical history without losing enrichment."""
    sources = history.sources
    persisted_aliases = history.persisted_aliases
    canonical_activity, canonical_journal = history.canonical_activity, history.canonical_journal
    journal_incomplete = partial_journal or "journal" not in sources
    current_journal_ids = {log.activity_id for log in sources.get("journal", [])}
    activity = {e.activity_id: e for e in canonical_activity.values()}
    current_by_alias_action: dict[tuple[str, str], str] = {}
    min_dt = datetime.min.replace(tzinfo=timezone.utc)
    for event in sources.get("activity", []):
        previous = persisted_aliases.get(event.activity_id)
        if previous:
            preserved = {}
            has_linked_journal = any(
                str(alias).startswith("journal_")
                for alias in (previous.event_ids or ())
            )
            if journal_incomplete:
                preserved = {
                    "played_date": event.played_date or previous.played_date,
                    "platform": event.platform or previous.platform,
                    "session_labels": event.session_labels or previous.session_labels,
                    "is_replay": event.is_replay or previous.is_replay,
                    "rating": event.rating if event.rating is not None else previous.rating,
                    "review_notes": event.review_notes or previous.review_notes,
                    "has_spoilers": event.has_spoilers or previous.has_spoilers,
                    "occurred_at": event.occurred_at or previous.occurred_at,
                    "date_str": event.date_str or previous.date_str,
                    "reference": event.reference or previous.reference,
                    "cover_url": event.cover_url or previous.cover_url,
                    "previous_rating": event.previous_rating if event.previous_rating is not None else previous.previous_rating,
                }
            elif not has_linked_journal or not any(
                alias in current_journal_ids for alias in (previous.event_ids or ())
            ):
                preserved = {
                    "played_date": event.played_date or previous.played_date,
                    "platform": event.platform or previous.platform,
                    "session_labels": event.session_labels or previous.session_labels,
                    "is_replay": event.is_replay or previous.is_replay,
                }
            merged_aliases = tuple(dict.fromkeys((
                *(previous.event_ids or (previous.activity_id,)),
                *(event.event_ids or (event.activity_id,)),
            )))
            candidate = replace(event, event_ids=merged_aliases, **preserved)
        else:
            for alias in event.event_ids:
                activity.pop(alias, None)
            matched_prev = next(
                (persisted_aliases[alias] for alias in event.event_ids if alias in persisted_aliases),
                None,
            )
            if matched_prev and (
                journal_incomplete
                or not any(alias in current_journal_ids for alias in (event.event_ids or ()))
            ):
                candidate = replace(
                    event,
                    played_date=event.played_date or matched_prev.played_date,
                    platform=event.platform or matched_prev.platform,
                    session_labels=event.session_labels or matched_prev.session_labels,
                    is_replay=event.is_replay or matched_prev.is_replay,
                )
            else:
                candidate = event

        cand_aliases = candidate.event_ids or (candidate.activity_id,)
        existing_id = next(
            (
                eid
                for alias in cand_aliases
                if (eid := current_by_alias_action.get((alias, candidate.action_type))) is not None
                and eid in activity
            ),
            None,
        )
        if existing_id is not None and existing_id != candidate.activity_id:
            existing = activity[existing_id]
            existing_at = parse_utc_datetime(existing.occurred_at) or min_dt
            cand_at = parse_utc_datetime(candidate.occurred_at) or min_dt
            combined_aliases = tuple(dict.fromkeys((
                *(existing.event_ids or (existing.activity_id,)),
                *cand_aliases,
            )))
            if cand_at > existing_at:
                activity.pop(existing_id, None)
                winner = replace(
                    candidate,
                    event_ids=combined_aliases,
                    played_date=candidate.played_date or existing.played_date,
                    platform=candidate.platform or existing.platform,
                    session_labels=candidate.session_labels or existing.session_labels,
                    is_replay=candidate.is_replay or existing.is_replay,
                )
                activity[winner.activity_id] = winner
            else:
                winner = replace(existing, event_ids=combined_aliases)
                activity[winner.activity_id] = winner
        else:
            winner = candidate
            activity[winner.activity_id] = winner

        for alias in (winner.event_ids or (winner.activity_id,)):
            current_by_alias_action[(alias, winner.action_type)] = winner.activity_id
    activities = [replace(e, event_ids=e.event_ids or (e.activity_id,)) for e in activity.values() if e.action_type in VISIBLE_ACTIONS]
    journal = {e.activity_id: e for e in canonical_journal.values()}
    for event in sources.get("journal", []):
        for alias in event.event_ids:
            journal.pop(alias, None)
        journal[event.activity_id] = event
    logs = [e for e in journal.values() if e.action_type in VISIBLE_ACTIONS]
    return activities, logs


async def apply_discovery_policy(
    pipeline: Any,
    guild_id,
    profile_key,
    events,
    saved,
    now,
    *,
    activation_at=None,
    recovery_uncertain: bool = False,
    uncertain_reasons=None,
    resolved_event_ids=None,
):
    candidates = prepare_feed_candidates(events, saved, resolved_event_ids=resolved_event_ids)
    aliases = feed_decision_aliases(candidates)
    stored_decisions = (
        await pipeline.bot.db.get_feed_event_decisions(guild_id, profile_key, aliases)
        if aliases else {}
    )
    plan = plan_feed_decisions(
        candidates, saved, stored_decisions, now,
        activation_at=activation_at,
        recovery_uncertain=recovery_uncertain,
        uncertain_reasons=uncertain_reasons,
    )
    authoritative = {}
    if plan.inserts or plan.revisions:
        authoritative = await pipeline.bot.db.persist_feed_event_decisions(
            guild_id, profile_key, inserts=plan.inserts, revisions=plan.revisions,
            alias_groups=plan.alias_groups,
        )
    return accept_feed_decisions(plan, authoritative)


async def filter_discoveries(pipeline: Any, target: FeedTarget, activities, logs, history: FeedHistory, poll_context):
    """Match milestones and apply policy before rating/review merging or delivery."""
    guild_id, profile_key = target.guild_id, target.profile_key
    saved = history.saved
    resolved_event_ids = history.resolved_event_ids
    new_journal_baseline_ids = history.new_journal_baseline_ids
    now = pipeline.clock()
    activation_at = await pipeline.bot.db.get_guild_profile_activation_at(guild_id, profile_key)
    snap_meta = pipeline._activity_snapshot_meta.get(profile_key, {})
    activity_health = None
    if isinstance(poll_context, dict):
        cached_health = poll_context.get("activity_health")
        if isinstance(cached_health, dict) and profile_key in cached_health:
            activity_health = cached_health[profile_key]
    if activity_health is None:
        activity_health = await pipeline.bot.db.get_profile_feed_source(profile_key, "activity")
        if isinstance(poll_context, dict):
            poll_context.setdefault("activity_health", {})[profile_key] = activity_health
    prior_gap_reason = snap_meta.get("prior_gap_reason")
    if prior_gap_reason is None and activity_health and "gap_reason" in (
        activity_health.keys() if hasattr(activity_health, "keys") else ()
    ):
        prior_gap_reason = activity_health["gap_reason"]
    had_source_gap = snap_meta.get("had_source_gap")
    if had_source_gap is None:
        had_source_gap = bool(
            snap_meta.get("had_unresolved_gap")
            and prior_gap_reason not in {"unavailable_feed_target", "delivery_failure"}
        )
    incomplete_coverage_with_boundary = bool(
        snap_meta.get("coverage") in {"limited", "unsupported"}
        and snap_meta.get("prior_boundary_id") is not None
    )
    activity_recovery_uncertain = bool(
        snap_meta.get("had_prior_failure")
        or (
            not snap_meta.get("observed_continuity")
            and (
                (snap_meta.get("pages_read") or 1) > 1
                or incomplete_coverage_with_boundary
                or (
                    had_source_gap
                    and (
                        snap_meta.get("prior_boundary_id") is not None
                        or (activity_health and activity_health["last_error"] is not None)
                    )
                )
            )
        )
    )
    baseline_or_saved_ids = set(saved.keys())
    missing_log_ids = []
    seen_missing_log_ids = set()
    for log in logs:
        if log.activity_id not in baseline_or_saved_ids and log.activity_id not in seen_missing_log_ids:
            seen_missing_log_ids.add(log.activity_id)
            missing_log_ids.append(log.activity_id)
    if missing_log_ids:
        baseline_or_saved_ids.update(
            await pipeline.bot.db.get_delivered_journal_entry_ids(guild_id, profile_key, missing_log_ids)
        )
        remaining_log_ids = [
            log_id for log_id in missing_log_ids if log_id not in baseline_or_saved_ids
        ]
        if remaining_log_ids:
            stored_decisions = await pipeline.bot.db.get_feed_event_decisions(guild_id, profile_key, remaining_log_ids)
            for log_id, stored_decision in stored_decisions.items():
                if stored_decision["decision"] == "baseline":
                    baseline_or_saved_ids.add(log_id)
                else:
                    try:
                        stored_payload = json.loads(stored_decision["payload"] or "{}")
                    except (TypeError, ValueError):
                        stored_payload = {}
                    stored_ids = stored_payload.get("event_ids") or ()
                    if any(
                        eid != log_id and not str(eid).startswith("journal_")
                        for eid in stored_ids
                    ):
                        baseline_or_saved_ids.add(log_id)
    merged_milestones = merge_milestones(activities, logs)
    linked_events, uncertain_reasons = link_backdated_milestones(
        merged_milestones, baseline_or_saved_ids=baseline_or_saved_ids,
        uncertain_journal_ids=new_journal_baseline_ids,
    )
    events = await pipeline._apply_discovery_policy(
        guild_id,
        profile_key,
        linked_events,
        saved,
        now,
        activation_at=activation_at,
        recovery_uncertain=activity_recovery_uncertain,
        uncertain_reasons=uncertain_reasons,
        resolved_event_ids=resolved_event_ids,
    )
    return events, now


async def prepare_announcements(
    pipeline: Any, target: FeedTarget, events, history: FeedHistory, now
) -> PreparedAnnouncements:
    """Merge eligible actions and load bounded rating/delivery evidence once."""
    guild_id, profile_key = target.guild_id, target.profile_key
    saved = history.saved
    canonical_activity, canonical_journal = history.canonical_activity, history.canonical_journal
    explicit_actions = [
        event for event in events
        if event.action_type in {"rated", "reviewed"}
    ]
    events = pipeline._keep_old_posted_sessions_and_unlink_old_baselines(
        events, explicit_actions, saved, now
    )
    baseline_ids = frozenset(event_id for event_id, row in saved.items() if row["delivery_state"] == "baseline")
    sent_ids = frozenset(
        event_id
        for event_id, row in saved.items()
        if row["message_id"] is not None
        or dict(row).get("delivery_state") in {"sent", "deleted", "pending", "unknown"}
    )
    saved_action_types = {
        event_id: row_ev.action_type for event_id, row_ev in history.row_events_by_id.items()
    }
    events = merge_ratings_and_reviews(
        events,
        baseline_ids=baseline_ids,
        sent_ids=sent_ids,
        saved_action_types=saved_action_types,
    )
    events.sort(key=lambda e: e.occurred_at or calendar_date(e.played_date) or "")
    activity_history_sources: dict[str, FeedEvent] = {
        previous.activity_id: previous
        for previous in canonical_activity.values()
        if previous.source == "activity"
    }
    for current in events:
        if current.source != "activity":
            continue
        for alias in current.event_ids:
            activity_history_sources.pop(alias, None)
        activity_history_sources[current.activity_id] = current
    activity_rating_history: dict[str, list[tuple[datetime, float]]] = {}
    for previous in activity_history_sources.values():
        if previous.rating is None:
            continue
        observed_action_at = parse_utc_datetime(previous.occurred_at)
        if observed_action_at is not None:
            activity_rating_history.setdefault(previous.game_slug, []).append(
                (observed_action_at, previous.rating)
            )

    journal_rating_history: dict[str, list[tuple[datetime, float]]] = {}
    journal_rating_times_unknown: set[str] = set()
    rated_game_slugs = {e.game_slug for e in events if e.action_type == "rated"}
    journal_rating_events = [
        event for event in canonical_journal.values()
        if event.rating is not None and event.game_slug in rated_game_slugs
    ]
    if journal_rating_events:
        journal_decisions = await pipeline.bot.db.get_feed_event_decisions(
            guild_id,
            profile_key,
            [event.activity_id for event in journal_rating_events],
        )
        for journal_event in journal_rating_events:
            row = journal_decisions.get(journal_event.activity_id)
            decision = dict(row) if row is not None else None
            if decision is None or decision["decision"] not in {"baseline", "eligible"}:
                journal_rating_times_unknown.add(journal_event.game_slug)
                continue
            observed_at = parse_utc_datetime(decision.get("observed_at"))
            if observed_at is None:
                journal_rating_times_unknown.add(journal_event.game_slug)
                continue
            journal_rating_history.setdefault(journal_event.game_slug, []).append(
                (observed_at, journal_event.rating)
            )
    alias_check_ids = []
    seen_alias_check_ids = set()
    for event in events:
        aliases = event.event_ids or (event.activity_id,)
        if not any(key in saved for key in aliases):
            for alias in aliases:
                if alias not in seen_alias_check_ids:
                    seen_alias_check_ids.add(alias)
                    alias_check_ids.append(alias)
    delivered_alias_ids = set()
    if alias_check_ids:
        delivered_alias_ids = await pipeline.bot.db.get_delivered_journal_entry_ids(guild_id, profile_key, alias_check_ids)
    return PreparedAnnouncements(
        events, activity_rating_history, journal_rating_history,
        journal_rating_times_unknown, delivered_alias_ids,
    )
