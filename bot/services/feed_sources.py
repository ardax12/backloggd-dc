"""Source fetching, snapshot continuity tracking, and activity observation helpers."""

from __future__ import annotations

import hashlib
import json
import logging
import uuid
from datetime import datetime
from typing import Any

from bot.feed_models import FeedEvent
from bot.feed_policy import parse_utc_datetime
from bot.feed_rules import journal_events
from bot.services.grouped_ratings import resolve_grouped_ratings
from bot.services.library_sync import parse_sync_timestamp

logger = logging.getLogger("backloggd.services.feed_pipeline")

OBSERVATION_STATUS_RANK = {
    "played": 4,
    "playing": 3,
    "backlog": 2,
    "wishlist": 1,
    "unknown": 0,
}


def is_library_relevant_event(event: FeedEvent) -> bool:
    return (
        event.action_type != "unknown"
        or event.rating is not None
        or bool(event.review_notes)
        or bool(event.is_replay)
    )


def observation_fingerprint(event: FeedEvent) -> str:
    payload = {
        "action_type": event.action_type,
        "game_slug": event.game_slug,
        "rating": event.rating,
        "review_notes": event.review_notes,
        "played_date": event.played_date,
        "occurred_at": event.occurred_at,
        "is_replay": event.is_replay,
        "session_labels": list(event.session_labels),
        "reference": event.reference,
    }
    return hashlib.sha256(json.dumps(payload, sort_keys=True).encode("utf-8")).hexdigest()


async def initial_observations_covered_by_sync(
    pipeline: Any,
    profile_key: str,
    initial_events: list[FeedEvent],
) -> bool:
    """Return True only when a completed, healthy sync demonstrably covers all initial_events."""
    if not initial_events:
        return True
    library_sync = getattr(pipeline.bot, "library_sync", None)
    if library_sync is not None and library_sync.is_refreshing(profile_key):
        return False
    sync_info = await pipeline.bot.db.get_profile_sync_info(profile_key)
    if not sync_info:
        return False
    keys = sync_info.keys() if hasattr(sync_info, "keys") else ()
    synced_at = parse_sync_timestamp(sync_info["last_synced_at"] if "last_synced_at" in keys else None)
    if synced_at is None:
        return False
    last_error = sync_info["last_sync_error"] if "last_sync_error" in keys else None
    attempt_at = parse_sync_timestamp(
        sync_info["last_sync_attempt_at"] if "last_sync_attempt_at" in keys else None
    )
    if last_error and (attempt_at is None or attempt_at >= synced_at):
        return False

    # Every initial observation must carry an exact UTC timestamp <= synced_at.
    # Date-only (journal / missing occurred_at) or post-sync events stay conservative.
    timestamped_events: list[tuple[datetime, int, FeedEvent]] = []
    for index, event in enumerate(initial_events):
        if event.source == "journal" or not event.occurred_at:
            return False
        # Coverage proof must not inherit the parser's UTC/midnight defaults.
        try:
            precise = (
                event.occurred_at if isinstance(event.occurred_at, datetime)
                else datetime.fromisoformat(str(event.occurred_at).replace("Z", "+00:00"))
            )
        except (TypeError, ValueError):
            return False
        if precise.tzinfo is None or precise.utcoffset() is None:
            return False
        occurred_dt = parse_utc_datetime(precise)
        if occurred_dt is None or occurred_dt > synced_at:
            return False
        timestamped_events.append((occurred_dt, index, event))

    library_entries = await pipeline.bot.db.get_profile_library_entries(profile_key)
    status_map = {
        "completed": "played",
        "replay": "played",
        "playing": "playing",
        "backlog": "backlog",
        "wishlist": "wishlist",
    }
    expected_status_by_slug: dict[str, str] = {}
    expected_rating_by_slug: dict[str, float] = {}
    tracked_slugs: set[str] = set()
    for _, _, event in sorted(timestamped_events, key=lambda item: (item[0], -item[1])):
        slug = event.game_slug
        if event.action_type in status_map:
            tracked_slugs.add(slug)
            expected_status_by_slug[slug] = status_map[event.action_type]
        elif event.is_replay:
            tracked_slugs.add(slug)
            expected_status_by_slug[slug] = "played"
        if event.rating is not None:
            tracked_slugs.add(slug)
            expected_rating_by_slug[slug] = event.rating
    for slug in tracked_slugs:
        cached = library_entries.get(slug)
        if not cached:
            return False
        if slug in expected_status_by_slug:
            cached_status = str(cached.get("status") or "").strip().lower()
            required_rank = pipeline._OBSERVATION_STATUS_RANK.get(expected_status_by_slug[slug], 0)
            if pipeline._OBSERVATION_STATUS_RANK.get(cached_status, 0) < required_rank:
                return False
        if slug in expected_rating_by_slug and cached.get("rating") != expected_rating_by_slug[slug]:
            return False
    return True


async def observe_profile_activity(
    pipeline: Any,
    profile_key: str,
    username: str,
    sources: dict[str, list[FeedEvent]],
) -> bool:
    """Persist profile-level relevant fingerprints and mark subscribed libraries stale without syncing."""
    if not sources:
        return False
    total_changed = 0
    non_initial_changes = False
    initial_relevant_events: list[FeedEvent] = []
    for source_name, events in sources.items():
        relevant = [event for event in events if pipeline._is_library_relevant_event(event)]
        observations = [
            (event.activity_id, pipeline._observation_fingerprint(event))
            for event in relevant
        ]
        result = await pipeline.bot.db.record_profile_activity_observations(profile_key, source_name, observations)
        changed = int(result.get("changed", 0))
        total_changed += changed
        if changed > 0:
            if not result.get("initialized") or int(result.get("updated", 0)) > 0:
                non_initial_changes = True
            else:
                initial_relevant_events.extend(relevant)
    if total_changed == 0:
        return False
    is_subscribed = await pipeline.bot.db.is_profile_library_subscribed(profile_key)
    if not is_subscribed:
        return False
    if (
        not non_initial_changes
        and await pipeline._initial_observations_covered_by_sync(profile_key, initial_relevant_events)
    ):
        return False
    await pipeline.bot.db.set_profile_refresh_pending(profile_key, uuid.uuid4().hex)
    return True


async def fetch_feed_sources(
    pipeline: Any,
    username: str,
    *,
    defer_boundary_commit: bool = False,
    poll_context: dict | None = None,
) -> dict[str, list[FeedEvent]]:
    """Read sources independently; a blocked page is never an empty snapshot."""
    sources: dict[str, list[FeedEvent]] = {}
    profile_key = username.strip().lower()
    database = pipeline.bot.db
    health_now = pipeline.clock()
    previous_snapshot = pipeline._activity_snapshot_meta.pop(profile_key, None)
    upstream_error: BaseException | None = None
    try:
        health = None
        await database.record_feed_source_attempt(
            profile_key, "activity", now=health_now,
        )
        health = await database.get_profile_feed_source(profile_key, "activity")
        snapshot_method = getattr(pipeline.bot.backloggd, "get_user_activity_snapshot", None)
        if snapshot_method:
            stop_at_ids = (
                (health["last_continuous_boundary_event_id"],)
                if health and health["last_continuous_boundary_event_id"] else ()
            )
            snapshot = await snapshot_method(username, stop_at_ids=stop_at_ids, max_pages=5)
            if not hasattr(snapshot, "entries"):
                raise TypeError("activity snapshot returned no entries field")
            entries = snapshot.entries
            coverage = snapshot.coverage
            pages_read = snapshot.pages_read
            if coverage not in {"boundary_reached", "exhausted", "limited", "unsupported"}:
                raise ValueError(f"unknown activity coverage: {coverage!r}")
            if not isinstance(pages_read, int) or pages_read < 1:
                raise ValueError("activity snapshot pages_read must be a positive integer")
        else:
            # Retained list-only fallback for SDK doubles/clients without snapshot coverage metadata.
            entries = await pipeline.bot.backloggd.get_user_activity(username)
            coverage, pages_read = "unsupported", 1
        if not isinstance(entries, (list, tuple)):
            raise TypeError("activity snapshot entries must be a list")
        parsed = [FeedEvent.from_activity(entry) for entry in entries]
        prior_boundary = health["last_continuous_boundary_event_id"] if health else None
        had_unresolved_gap = bool(health and health["unresolved_gap_since"] is not None)
        prior_gap_reason = (
            health["gap_reason"]
            if health and "gap_reason" in (health.keys() if hasattr(health, "keys") else ())
            else None
        )
        had_source_gap = bool(
            had_unresolved_gap
            and prior_gap_reason not in {"unavailable_feed_target", "delivery_failure"}
        )
        prior_attempts = int(health["attempts"] or 0) if health else 0
        prior_successes = int(health["successes"] or 0) if health else 0
        had_prior_failure = bool(
            health and (
                health["last_error"] is not None
                or max(0, prior_attempts - 1) > prior_successes
            )
        )
        boundary_event_id = None
        if coverage == "boundary_reached":
            if not prior_boundary or not any(item.activity_id == prior_boundary for item in parsed):
                raise ValueError("activity snapshot claimed a boundary that was not present")
            boundary_event_id = parsed[0].activity_id if parsed else prior_boundary
        elif coverage == "exhausted" and parsed:
            boundary_event_id = parsed[0].activity_id
        # Source observation continuity is independent of the delivery floor.
        # A recipient outage can pin that floor long after it leaves the page.
        previous_ids = set(previous_snapshot.get("observed_ids", ())) if previous_snapshot else set()
        previous_was_continuous = bool(previous_snapshot and (
            previous_snapshot.get("coverage") in {"boundary_reached", "exhausted"}
            or previous_snapshot.get("observed_continuity")
        ))
        observed_continuity = bool(
            previous_was_continuous
            and not had_prior_failure
            and previous_ids.intersection(event.activity_id for event in parsed)
        )
        sources["activity"] = parsed
        pipeline._activity_snapshot_meta[profile_key] = {
            "observed_ids": tuple(event.activity_id for event in parsed),
            "observed_continuity": observed_continuity,
            "coverage": coverage,
            "pages_read": pages_read,
            "prior_boundary_id": prior_boundary,
            "boundary_event_id": boundary_event_id,
            "had_unresolved_gap": had_unresolved_gap,
            "prior_gap_reason": prior_gap_reason,
            "had_source_gap": had_source_gap,
            "had_prior_failure": had_prior_failure,
            "observed_at": health_now,
        }
        if any(getattr(entry, "grouped_games", ()) for entry in entries):
            previous_poll_at = parse_utc_datetime(health["last_success_at"]) if health else None
            parsed.extend(await resolve_grouped_ratings(
                pipeline.bot, profile_key, username, entries, previous_poll_at, health_now,
            ))
        await database.record_feed_source_success(
            profile_key,
            "activity",
            coverage=coverage,
            pages_read=pages_read,
            boundary_event_id=boundary_event_id,
            defer_boundary_commit=defer_boundary_commit,
            now=health_now,
        )
        if isinstance(poll_context, dict):
            poll_context.setdefault("activity_health", {})[profile_key] = {
                "last_error": None,
                "gap_reason": None if had_source_gap is False and prior_gap_reason in {"unavailable_feed_target", "delivery_failure"} else prior_gap_reason,
            }
    except Exception as error:
        logger.warning("Could not read activity for %s: %s", username, error)
        if isinstance(poll_context, dict) and isinstance(poll_context.get("stats"), dict):
            poll_context["stats"]["source_errors"] += 1
        await database.record_feed_source_failure(profile_key, "activity", str(error))
        upstream_kind = pipeline.session_health.record_failure(error)
        if upstream_kind is not None:
            upstream_error = error
            if isinstance(poll_context, dict):
                poll_context["upstream_aborted"] = upstream_kind
    if upstream_error is None:
        try:
            await database.record_feed_source_attempt(
                profile_key, "journal", now=health_now,
            )
            entries = await pipeline.bot.backloggd.get_user_journal(
                username, max_pages=1, require_complete=False,
            )
            parsed_journal = journal_events(entries, username)
            await database.record_feed_source_success(
                profile_key, "journal", coverage="latest_page", pages_read=1,
                now=health_now,
            )
            sources["journal"] = parsed_journal
            if poll_context is not None:
                poll_context.setdefault("partial_journals", set()).add(profile_key)
        except Exception as error:
            logger.warning("Could not read journal for %s: %s", username, error)
            if isinstance(poll_context, dict) and isinstance(poll_context.get("stats"), dict):
                poll_context["stats"]["source_errors"] += 1
            await database.record_feed_source_failure(profile_key, "journal", str(error))
            upstream_kind = pipeline.session_health.record_failure(error)
            if upstream_kind is not None:
                upstream_error = error
                if isinstance(poll_context, dict):
                    poll_context["upstream_aborted"] = upstream_kind
    if upstream_error is None and "activity" in sources and "journal" in sources:
        pipeline.session_health.record_success()
    return sources
