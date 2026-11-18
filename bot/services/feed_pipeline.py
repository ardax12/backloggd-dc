"""Source polling, reconciliation and Discord delivery for the server feed."""

from __future__ import annotations

import aiohttp
import discord
import hashlib
import logging
import asyncio
import json
import uuid
from datetime import datetime, timezone
from dataclasses import asdict, dataclass, replace
from typing import Any

from bot.feed_rules import (
    VISIBLE_ACTIONS,
    calendar_date,
    journal_events,
    link_backdated_milestones,
    merge_milestones,
    merge_ratings_and_reviews,
)
from bot.feed_policy import classify_feed_event, parse_utc_datetime
from bot.feed_decisions import (
    accept_feed_decisions,
    feed_decision_aliases,
    plan_feed_decisions,
    prepare_feed_candidates,
)
from bot.services.guild_membership import resolve_guild_member_ids
from bot.services.library_sync import parse_sync_timestamp, schedule_profile_refresh
from bot.ui.feed_embed import create_journal_feed_embed
from bot.ui.views import LinkButtonView
from bot.ui.helpers import build_backloggd_game_url
from bot.feed_models import FeedEvent

logger = logging.getLogger("backloggd.services.feed_pipeline")

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


class FeedPipeline:
    def __init__(self, bot, *, clock=None):
        self.bot = bot
        self.clock = clock or (lambda: datetime.now(timezone.utc))
        self._activity_snapshot_meta = {}
        self._unavailable_target_reasons: dict[int, str] = {}
        self._last_publish_errors: dict[tuple[int, str], str] = {}
        self._profiles_with_recipient_gaps: set[str] = set()
        self._delivery_cleanup_tasks: set[asyncio.Task] = set()
        self._closing = False

    async def close(self, *, exclude_task=None) -> None:
        """Drain delivery finalizers after the cog has stopped its polling task."""
        self._closing = True
        current = asyncio.current_task()
        while True:
            pending = [
                task for task in list(self._delivery_cleanup_tasks)
                if not task.done() and task is not current and task is not exclude_task
            ]
            if not pending:
                break
            await asyncio.gather(*pending, return_exceptions=True)


    _OBSERVATION_STATUS_RANK = {
        "played": 4,
        "playing": 3,
        "backlog": 2,
        "wishlist": 1,
        "unknown": 0,
    }


    def _track_delivery_cleanup(self, coro) -> asyncio.Task:
        task = asyncio.create_task(coro)
        self._delivery_cleanup_tasks.add(task)
        task.add_done_callback(self._delivery_cleanup_tasks.discard)
        return task


    def inspect_target_channel(
        self,
        guild_id: int,
        channel_id,
        *,
        guild=None,
        require_channel_lookup: bool = True,
    ):
        """Inspect configured feed channel and permissions without altering persisted state."""
        if channel_id is None:
            return None, None, None
        if guild is None:
            get_guild = getattr(self.bot, "get_guild", None)
            guild = get_guild(guild_id) if callable(get_guild) else None
        if guild is None:
            return None, "guild_unavailable", "Server not in Discord cache"
        get_channel = getattr(guild, "get_channel", None)
        if not callable(get_channel):
            if require_channel_lookup:
                return None, f"channel_lookup_unavailable:{channel_id}", f"Configured channel <#{channel_id}> is unavailable"
            return None, None, None
        channel = get_channel(channel_id)
        if channel is None:
            return None, f"channel_missing:{channel_id}", f"Configured channel <#{channel_id}> is unavailable"
        if not isinstance(channel, discord.TextChannel):
            return None, f"channel_invalid:{channel_id}", f"Configured channel <#{channel_id}> is not a text channel"
        channel_guild_id = getattr(getattr(channel, "guild", None), "id", None)
        if (
            isinstance(channel_guild_id, int)
            and not isinstance(channel_guild_id, bool)
            and channel_guild_id != guild_id
        ):
            return (
                None,
                f"channel_wrong_guild:{channel_id}",
                f"Configured channel <#{channel_id}> belongs to another server",
            )
        me = getattr(guild, "me", None)
        if me is None:
            return None, f"bot_member_missing:{channel_id}", "Bot member permissions are unavailable"
        permissions = channel.permissions_for(me)
        missing_perms = []
        if not getattr(permissions, "view_channel", True):
            missing_perms.append("View Channel")
        if not getattr(permissions, "send_messages", False):
            missing_perms.append("Send Messages")
        if not getattr(permissions, "embed_links", False):
            missing_perms.append("Embed Links")
        if missing_perms:
            perm_list = ", ".join(missing_perms)
            return (
                None,
                f"missing_perms:{channel_id}:{perm_list}",
                f"Missing {perm_list} permission in <#{channel_id}>",
            )
        return channel, None, None


    async def authoritative_member_ids(self, guild):
        """Return a complete cached member snapshot, or None when unavailable."""
        return await resolve_guild_member_ids(self.bot, guild)


    async def reconcile_legacy_guild(self, guild, *, pending_guild_ids: set[int] | None = None):
        guild_id = getattr(guild, "id", None)
        if guild_id is None:
            return False
        if pending_guild_ids is None:
            pending_guild_ids = set(await self.bot.db.get_legacy_feed_guilds_pending())
        if guild_id not in pending_guild_ids:
            return False
        member_ids = await self.authoritative_member_ids(guild)
        if member_ids is None:
            return False
        completed = await self.bot.db.complete_legacy_feed_migration(guild_id, member_ids)
        if completed:
            pending_guild_ids.discard(guild_id)
        return completed


    async def reconcile_legacy_enrollments(self):
        """Apply the one-time legacy link/guild intersection after readiness."""
        pending_guild_ids = set(await self.bot.db.get_legacy_feed_guilds_pending())
        for guild_id in list(pending_guild_ids):
            guild = self.bot.get_guild(guild_id)
            if guild is not None:
                await self.reconcile_legacy_guild(guild, pending_guild_ids=pending_guild_ids)


    @staticmethod
    def _is_library_relevant_event(event: FeedEvent) -> bool:
        return (
            event.action_type != "unknown"
            or event.rating is not None
            or bool(event.review_notes)
            or bool(event.is_replay)
        )


    @staticmethod
    def _observation_fingerprint(event: FeedEvent) -> str:
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


    async def _initial_observations_covered_by_sync(
        self,
        profile_key: str,
        initial_events: list[FeedEvent],
    ) -> bool:
        """Return True only when a completed, healthy sync demonstrably covers all initial_events."""
        if not initial_events:
            return True
        library_sync = getattr(self.bot, "library_sync", None)
        if library_sync is not None and library_sync.is_refreshing(profile_key):
            return False
        sync_info = await self.bot.db.get_profile_sync_info(profile_key)
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

        library_entries = await self.bot.db.get_profile_library_entries(profile_key)
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
                required_rank = self._OBSERVATION_STATUS_RANK.get(expected_status_by_slug[slug], 0)
                if self._OBSERVATION_STATUS_RANK.get(cached_status, 0) < required_rank:
                    return False
            if slug in expected_rating_by_slug and cached.get("rating") != expected_rating_by_slug[slug]:
                return False
        return True


    async def _observe_profile_activity(
        self,
        profile_key: str,
        username: str,
        sources: dict[str, list[FeedEvent]],
    ) -> bool:
        """Persist profile-level relevant fingerprints before per-guild feed filtering."""
        if not sources:
            return False
        total_changed = 0
        non_initial_changes = False
        initial_relevant_events: list[FeedEvent] = []
        for source_name, events in sources.items():
            relevant = [event for event in events if self._is_library_relevant_event(event)]
            observations = [
                (event.activity_id, self._observation_fingerprint(event))
                for event in relevant
            ]
            result = await self.bot.db.record_profile_activity_observations(profile_key, source_name, observations)
            changed = int(result.get("changed", 0))
            total_changed += changed
            if changed > 0:
                if not result.get("initialized") or int(result.get("updated", 0)) > 0:
                    non_initial_changes = True
                else:
                    initial_relevant_events.extend(relevant)
        has_pending = (await self.bot.db.get_profile_refresh_pending_token(profile_key)) is not None
        if total_changed > 0:
            is_subscribed = await self.bot.db.is_profile_library_subscribed(profile_key)
            if (
                is_subscribed
                and not has_pending
                and not non_initial_changes
                and await self._initial_observations_covered_by_sync(profile_key, initial_relevant_events)
            ):
                return False
            if is_subscribed:
                await self.bot.db.set_profile_refresh_pending(profile_key, uuid.uuid4().hex)
        else:
            if not has_pending:
                return False
            is_subscribed = await self.bot.db.is_profile_library_subscribed(profile_key)
            if not is_subscribed:
                await self.bot.db.clear_profile_refresh_pending(profile_key)
                return False
        await schedule_profile_refresh(
            self.bot,
            username,
            dirty=total_changed > 0,
            now=self.clock(),
            require_subscribed=True,
            subscribed=is_subscribed,
        )
        return True


    async def _fetch_feed_sources(
        self,
        username: str,
        *,
        defer_boundary_commit: bool = False,
        poll_context: dict | None = None,
    ) -> dict[str, list[FeedEvent]]:
        """Read sources independently; a blocked page is never an empty snapshot."""
        sources = {}
        profile_key = username.strip().lower()
        database = self.bot.db
        health_now = self.clock()
        previous_snapshot = self._activity_snapshot_meta.pop(profile_key, None)
        try:
            health = None
            await database.record_feed_source_attempt(
                profile_key, "activity", now=health_now,
            )
            health = await database.get_profile_feed_source(profile_key, "activity")
            snapshot_method = getattr(self.bot.backloggd, "get_user_activity_snapshot", None)
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
                # Published backloggd-py 0.2.0 exposes only the latest-page list API.
                entries = await self.bot.backloggd.get_user_activity(username)
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
            self._activity_snapshot_meta[profile_key] = {
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
            await database.record_feed_source_failure(profile_key, "activity", str(error))
        try:
            await database.record_feed_source_attempt(
                profile_key, "journal", now=health_now,
            )
            entries = await self.bot.backloggd.get_user_journal(
                username, max_pages=100, require_complete=True
            )
            sources["journal"] = journal_events(entries, username)
            await database.record_feed_source_success(
                profile_key, "journal", coverage="exhausted", pages_read=None,
                now=health_now,
            )
        except Exception as error:
            logger.warning("Could not read journal for %s: %s", username, error)
            await database.record_feed_source_failure(profile_key, "journal", str(error))
        return sources


    async def publish_sources(
        self, guild_id, profile_key, username, avatar_url, channel, sources,
        *, deliver: bool = True, poll_context: dict | None = None,
    ) -> bool:
        self._last_publish_errors.pop((guild_id, profile_key), None)
        target = FeedTarget(guild_id, profile_key, username, avatar_url, channel)
        history = await self._load_and_initialize_history(target, sources)
        activities, logs = self._merge_source_events(history)
        events, now = await self._filter_discoveries(target, activities, logs, history, poll_context)
        if not deliver:
            # Observations stay durable without modifying archived Discord messages.
            return False
        prepared = await self._prepare_announcements(target, events, history, now)
        return await self._deliver_announcements(target, history, prepared, now)


    async def _load_and_initialize_history(self, target, sources) -> FeedHistory:
        """Reconcile identities, establish atomic baselines, and restore unsent work."""
        guild_id, profile_key = target.guild_id, target.profile_key
        rows = await self.bot.db.get_feed_messages(guild_id, profile_key)
        saved = {row["event_id"]: row for row in rows}
        sources = {source: list(events) for source, events in sources.items()}
        persisted_aliases, canonical_activity, canonical_journal, row_events_by_id = (
            self._parse_persisted_feed_history(rows)
        )
        histories = {"activity": canonical_activity, "journal": canonical_journal}
        existing_sources = await self.bot.db.get_feed_sources(guild_id, profile_key)
        initialized_map = {
            source: (source + "-display-v1") in existing_sources
            for source in sources
        }
        resolved_event_ids: set[str] = set()
        for source, events in list(sources.items()):
            if initialized_map[source]:
                sources[source] = await self._reconcile_changed_event_identities(
                    guild_id,
                    profile_key,
                    source,
                    events,
                    histories.get(source, {}),
                    persisted_aliases=persisted_aliases,
                    resolved_event_ids=resolved_event_ids,
                )
        new_journal_baseline_ids = set()
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
            now_baseline = self.clock()
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
            first = await self.bot.db.seed_feed_source_with_messages(
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
        if any_first_baseline:
            # Refresh the lookup and canonical maps to include the baseline just written.
            rows = await self.bot.db.get_feed_messages(guild_id, profile_key)
            saved = {row["event_id"]: row for row in rows}
            persisted_aliases, canonical_activity, canonical_journal, row_events_by_id = (
                self._parse_persisted_feed_history(rows)
            )
        pending = await self.bot.db.get_pending_feed_observations(guild_id, profile_key)
        for row in pending:
            data = json.loads(row["payload"])
            data["event_ids"] = tuple(data.get("event_ids", ()))
            data["session_labels"] = tuple(data.get("session_labels", ()))
            event = FeedEvent(**data)
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


    @staticmethod
    def _merge_source_events(history):
        """Combine current observations and canonical history without losing enrichment."""
        sources = history.sources
        persisted_aliases = history.persisted_aliases
        canonical_activity, canonical_journal = history.canonical_activity, history.canonical_journal
        activity = {e.activity_id: e for e in canonical_activity.values()}
        for event in sources.get("activity", []):
            previous = persisted_aliases.get(event.activity_id)
            if previous:
                preserved = {}
                if "journal" not in sources:
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
                merged_aliases = tuple(dict.fromkeys((
                    *(previous.event_ids or (previous.activity_id,)),
                    *(event.event_ids or (event.activity_id,)),
                )))
                activity[event.activity_id] = replace(event, event_ids=merged_aliases, **preserved)
            else:
                for alias in event.event_ids:
                    activity.pop(alias, None)
                matched_prev = next(
                    (persisted_aliases[alias] for alias in event.event_ids if alias in persisted_aliases),
                    None,
                )
                if matched_prev and "journal" not in sources:
                    activity[event.activity_id] = replace(
                        event,
                        played_date=event.played_date or matched_prev.played_date,
                        platform=event.platform or matched_prev.platform,
                        session_labels=event.session_labels or matched_prev.session_labels,
                        is_replay=event.is_replay or matched_prev.is_replay,
                    )
                else:
                    activity[event.activity_id] = event
        activities = [replace(e, event_ids=e.event_ids or (e.activity_id,)) for e in activity.values() if e.action_type in VISIBLE_ACTIONS]
        journal = {e.activity_id: e for e in canonical_journal.values()}
        for event in sources.get("journal", []):
            for alias in event.event_ids:
                journal.pop(alias, None)
            journal[event.activity_id] = event
        logs = [e for e in journal.values() if e.action_type in VISIBLE_ACTIONS]
        return activities, logs


    async def _filter_discoveries(self, target, activities, logs, history, poll_context):
        """Match milestones and apply policy before rating/review merging or delivery."""
        guild_id, profile_key = target.guild_id, target.profile_key
        saved = history.saved
        resolved_event_ids = history.resolved_event_ids
        new_journal_baseline_ids = history.new_journal_baseline_ids
        now = self.clock()
        activation_at = await self.bot.db.get_guild_profile_activation_at(guild_id, profile_key)
        snap_meta = self._activity_snapshot_meta.get(profile_key, {})
        activity_health = None
        if isinstance(poll_context, dict):
            cached_health = poll_context.get("activity_health")
            if isinstance(cached_health, dict) and profile_key in cached_health:
                activity_health = cached_health[profile_key]
        if activity_health is None:
            activity_health = await self.bot.db.get_profile_feed_source(profile_key, "activity")
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
                await self.bot.db.get_delivered_journal_entry_ids(guild_id, profile_key, missing_log_ids)
            )
            remaining_log_ids = [
                log_id for log_id in missing_log_ids if log_id not in baseline_or_saved_ids
            ]
            if remaining_log_ids:
                stored_decisions = await self.bot.db.get_feed_event_decisions(guild_id, profile_key, remaining_log_ids)
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
        events = await self._apply_discovery_policy(
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


    async def _prepare_announcements(self, target, events, history, now) -> PreparedAnnouncements:
        """Merge eligible actions and load bounded rating/delivery evidence once."""
        guild_id, profile_key = target.guild_id, target.profile_key
        saved = history.saved
        canonical_activity, canonical_journal = history.canonical_activity, history.canonical_journal
        explicit_actions = [
            event for event in events
            if event.action_type in {"rated", "reviewed"}
        ]
        events = self._keep_old_posted_sessions_and_unlink_old_baselines(
            events, explicit_actions, saved, now
        )
        events = merge_ratings_and_reviews(events)
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
        journal_rating_events = [
            event for event in canonical_journal.values() if event.rating is not None
        ]
        if journal_rating_events:
            journal_decisions = await self.bot.db.get_feed_event_decisions(
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
            delivered_alias_ids = await self.bot.db.get_delivered_journal_entry_ids(guild_id, profile_key, alias_check_ids)
        return PreparedAnnouncements(
            events, activity_rating_history, journal_rating_history,
            journal_rating_times_unknown, delivered_alias_ids,
        )


    async def _deliver_announcements(self, target, history, prepared, now) -> bool:
        target_ok = True
        for event in prepared.events:
            if not await self._deliver_event(target, event, history, prepared, now):
                target_ok = False
        return target_ok


    async def _deliver_event(self, target, event, history, prepared, now) -> bool:
        """Keep archive edits, send ownership, persistence and cleanup in one scope."""
        guild_id, profile_key = target.guild_id, target.profile_key
        username, avatar_url, channel = target.username, target.avatar_url, target.channel
        saved, row_events_by_id = history.saved, history.row_events_by_id
        activity_rating_history = prepared.activity_rating_history
        journal_rating_history = prepared.journal_rating_history
        journal_rating_times_unknown = prepared.journal_rating_times_unknown
        delivered_alias_ids = prepared.delivered_alias_ids
        target_ok = True
        aliases = event.event_ids or (event.activity_id,)
        records = [saved[key] for key in aliases if key in saved]
        sent_records = [r for r in records if r["message_id"] is not None]
        if sent_records:
            deleted_sent = [r for r in sent_records if dict(r).get("delivery_state") == "deleted"]
            pool = deleted_sent or sent_records
            record = min(pool, key=lambda row: row["message_id"])
        elif records:
            protected = [
                r for r in records
                if dict(r).get("delivery_state") in {"deleted", "unknown", "pending"}
            ]
            record = protected[0] if protected else records[0]
        else:
            record = None
        if record is None and any(alias in delivered_alias_ids for alias in aliases):
            record = {
                "payload": json.dumps(asdict(event), sort_keys=True),
                "message_id": None,
                "channel_id": None,
                "delivery_state": "baseline",
                "_synthetic_event": event,
            }
        unchanged = False
        if record:
            record_keys = record.keys() if hasattr(record, "keys") else ()
            stored_event = (
                row_events_by_id.get(record["event_id"])
                if "event_id" in record_keys
                else record.get("_synthetic_event")
            )
            if stored_event is not None:
                previous_rating = stored_event.previous_rating
                if event.action_type == "rated" and stored_event.rating != event.rating:
                    previous_rating = stored_event.rating
                event = replace(event, previous_rating=previous_rating)
                unchanged = (
                    event == stored_event
                    and (event.rating is None or type(event.rating) is type(stored_event.rating))
                    and (
                        event.previous_rating is None
                        or type(event.previous_rating) is type(stored_event.previous_rating)
                    )
                )
            else:
                old = json.loads(record["payload"])
                previous_rating = old.get("previous_rating")
                if event.action_type == "rated" and old.get("rating") != event.rating:
                    previous_rating = old.get("rating")
                event = replace(event, previous_rating=previous_rating)
        elif event.action_type == "rated":
            old_rating = self._previous_rating_from_history(
                event,
                activity_rating_history.get(event.game_slug, ()),
                journal_rating_history.get(event.game_slug, ()),
                journal_time_unknown=event.game_slug in journal_rating_times_unknown,
            )
            if old_rating != event.rating:
                event = replace(event, previous_rating=old_rating)
        message_id = record["message_id"] if record else None
        old_channel = record["channel_id"] if record else None
        state = record["delivery_state"] if record and "delivery_state" in record.keys() else None
        missing_aliases = [alias for alias in aliases if alias not in saved] if record else list(aliases)
        if not unchanged:
            payload = json.dumps(asdict(event), sort_keys=True)
            if record and payload == record["payload"]:
                unchanged = True
        if unchanged:
            if missing_aliases:
                alias_state = state or ("sent" if message_id is not None else "baseline")
                await self.bot.db.store_feed_message(
                    guild_id, profile_key, missing_aliases, record["payload"],
                    message_id, old_channel, delivery_state=alias_state,
                )
                for alias in missing_aliases:
                    saved[alias] = {
                        "payload": record["payload"],
                        "message_id": message_id,
                        "channel_id": old_channel,
                        "delivery_state": alias_state,
                    }
            return target_ok
        if state in {"pending", "unknown", "deleted"}:
            await self.bot.db.store_feed_message(
                guild_id, profile_key, aliases, payload, message_id, old_channel,
                delivery_state=state,
            )
            for alias in aliases:
                saved[alias] = {
                    "payload": payload,
                    "message_id": message_id,
                    "channel_id": old_channel,
                    "delivery_state": state,
                }
            return target_ok
        if record and message_id is None:
            # Historic baseline: remember aliases but never publish history.
            pass
        else:
            embed = create_journal_feed_embed(username, avatar_url, event, now=now)
            view = LinkButtonView(label="View on Backloggd", url=build_backloggd_game_url(event.game_slug))
            if message_id is not None:
                if old_channel is None:
                    edit_channel, edit_issue = None, "Original channel ID missing"
                elif old_channel == channel.id:
                    channel_guild_id = getattr(getattr(channel, "guild", None), "id", None)
                    if (
                        isinstance(channel_guild_id, int)
                        and not isinstance(channel_guild_id, bool)
                        and channel_guild_id != guild_id
                    ):
                        edit_channel, edit_issue = None, f"Configured channel <#{old_channel}> belongs to another server"
                    else:
                        edit_channel, edit_issue = channel, None
                else:
                    edit_channel, _, edit_issue = self.inspect_target_channel(
                        guild_id, old_channel, require_channel_lookup=True
                    )

                if edit_channel is None:
                    if missing_aliases:
                        alias_state = state or "sent"
                        await self.bot.db.store_feed_message(
                            guild_id, profile_key, missing_aliases, record["payload"],
                            message_id, old_channel, delivery_state=alias_state,
                        )
                        for alias in missing_aliases:
                            saved[alias] = {
                                "payload": record["payload"],
                                "message_id": message_id,
                                "channel_id": old_channel,
                                "delivery_state": alias_state,
                            }
                    target_ok = False
                    self._last_publish_errors[(guild_id, profile_key)] = (
                        edit_issue or "channel unavailable"
                    )[:500]
                    logger.warning(
                        "Could not edit feed message %s in prior channel %s for %s in guild %d (%s); keeping confirmed payload for retry",
                        message_id,
                        old_channel,
                        username,
                        guild_id,
                        edit_issue or "channel unavailable",
                    )
                    return target_ok

                try:
                    await edit_channel.get_partial_message(message_id).edit(embed=embed, view=view)
                except discord.NotFound:
                    # A removed announcement stays removed; do not repost it.
                    await self.bot.db.mark_feed_message_deleted(
                        guild_id, profile_key, aliases, payload
                    )
                    if missing_aliases:
                        await self.bot.db.store_feed_message(
                            guild_id, profile_key, missing_aliases, payload,
                            message_id, old_channel, delivery_state="deleted",
                        )
                    for alias in aliases:
                        saved[alias] = {
                            "payload": payload,
                            "message_id": message_id,
                            "channel_id": old_channel,
                            "delivery_state": "deleted",
                        }
                    return target_ok
                except asyncio.CancelledError:
                    raise
                except (discord.HTTPException, discord.RateLimited, aiohttp.ClientError, TimeoutError, ConnectionError) as error:
                    if missing_aliases:
                        alias_state = state or "sent"
                        await self.bot.db.store_feed_message(
                            guild_id, profile_key, missing_aliases, record["payload"],
                            message_id, old_channel, delivery_state=alias_state,
                        )
                        for alias in missing_aliases:
                            saved[alias] = {
                                "payload": record["payload"],
                                "message_id": message_id,
                                "channel_id": old_channel,
                                "delivery_state": alias_state,
                            }
                    target_ok = False
                    err_status = getattr(error, "status", "rate_limited" if isinstance(error, discord.RateLimited) else type(error).__name__)
                    self._last_publish_errors[(guild_id, profile_key)] = f"edit_failed:{err_status}: {error}"[:500]
                    logger.warning(
                        "Could not edit feed message %s for %s in guild %d (status %s); keeping previous payload for retry",
                        message_id,
                        username,
                        guild_id,
                        err_status,
                    )
                    return target_ok
            else:
                owner_token = uuid.uuid4().hex
                if not await self.bot.db.begin_feed_delivery(
                    guild_id, profile_key, aliases, payload, channel.id,
                    owner_token=owner_token,
                ):
                    return target_ok
                durable_finalized = False
                cleanup_launched = False
                sent_message = None

                async def _finalize_delivery_state(msg=None, *, release_unsent: bool = False) -> None:
                    nonlocal durable_finalized
                    try:
                        if release_unsent:
                            await self.bot.db.release_feed_delivery(guild_id, profile_key, aliases)
                            durable_finalized = True
                        elif msg is not None and getattr(msg, "id", None) is not None:
                            await self.bot.db.store_feed_message(
                                guild_id, profile_key, aliases, payload, msg.id, channel.id
                            )
                            durable_finalized = True
                            for alias in aliases:
                                saved[alias] = {
                                    "payload": payload,
                                    "message_id": msg.id,
                                    "channel_id": channel.id,
                                    "delivery_state": "sent",
                                }
                        else:
                            await self.bot.db.mark_feed_delivery_unknown(guild_id, profile_key, aliases)
                            durable_finalized = True
                            for alias in aliases:
                                saved[alias] = {
                                    "payload": payload,
                                    "message_id": None,
                                    "channel_id": channel.id,
                                    "delivery_state": "unknown",
                                }
                    except Exception:
                        logger.warning(
                            "Could not finalize delivery state for %s (%s) in guild %d; leaving intent recoverable",
                            event.activity_id,
                            username,
                            guild_id,
                            exc_info=True,
                        )
                    finally:
                        self.bot.db.release_feed_delivery_ownership(
                            guild_id,
                            profile_key,
                            aliases,
                            owner_token,
                            recheck_recovery=not durable_finalized,
                        )

                try:
                    try:
                        sent_message = await channel.send(embed=embed, view=view)
                    except asyncio.CancelledError:
                        cleanup_launched = True
                        cleanup_task = self._track_delivery_cleanup(_finalize_delivery_state(None))
                        await asyncio.shield(cleanup_task)
                        raise
                    except discord.RateLimited as error:
                        try:
                            await self.bot.db.release_feed_delivery(guild_id, profile_key, aliases)
                            durable_finalized = True
                        except asyncio.CancelledError:
                            cleanup_launched = True
                            cleanup_task = self._track_delivery_cleanup(
                                _finalize_delivery_state(None, release_unsent=True)
                            )
                            await asyncio.shield(cleanup_task)
                            raise
                        target_ok = False
                        self._last_publish_errors[(guild_id, profile_key)] = f"rate_limited:{error.retry_after:.2f}s"[:500]
                        logger.warning(
                            "Rate limited sending feed event %s for %s in guild %d (retry_after=%.2fs); released pending intent",
                            event.activity_id,
                            username,
                            guild_id,
                            error.retry_after,
                        )
                        return target_ok
                    except discord.HTTPException as error:
                        status = getattr(error, "status", None)
                        is_client_rejection = isinstance(status, int) and 400 <= status < 500
                        try:
                            if is_client_rejection:
                                await self.bot.db.release_feed_delivery(guild_id, profile_key, aliases)
                            else:
                                await self.bot.db.mark_feed_delivery_unknown(guild_id, profile_key, aliases)
                            durable_finalized = True
                        except asyncio.CancelledError:
                            cleanup_launched = True
                            cleanup_task = self._track_delivery_cleanup(
                                _finalize_delivery_state(None, release_unsent=is_client_rejection)
                            )
                            await asyncio.shield(cleanup_task)
                            raise
                        target_ok = False
                        self._last_publish_errors[(guild_id, profile_key)] = (
                            f"http_{status if status is not None else 'error'}: {error}"[:500]
                        )
                        logger.warning(
                            "Could not send feed event %s for %s in guild %d (status %s); %s",
                            event.activity_id,
                            username,
                            guild_id,
                            status if status is not None else "unknown",
                            "released pending intent" if is_client_rejection else "marked delivery unknown",
                        )
                        return target_ok
                    except (aiohttp.ClientError, TimeoutError, ConnectionError) as error:
                        try:
                            await self.bot.db.mark_feed_delivery_unknown(guild_id, profile_key, aliases)
                            durable_finalized = True
                        except asyncio.CancelledError:
                            cleanup_launched = True
                            cleanup_task = self._track_delivery_cleanup(_finalize_delivery_state(None))
                            await asyncio.shield(cleanup_task)
                            raise
                        target_ok = False
                        self._last_publish_errors[(guild_id, profile_key)] = f"{type(error).__name__}: {error}"[:500]
                        logger.warning(
                            "Uncertain transport failure sending feed event %s for %s in guild %d (%s); marked delivery unknown",
                            event.activity_id,
                            username,
                            guild_id,
                            error,
                        )
                        return target_ok
                    except Exception:
                        try:
                            await self.bot.db.mark_feed_delivery_unknown(guild_id, profile_key, aliases)
                            durable_finalized = True
                        except Exception:
                            logger.warning(
                                "Could not mark delivery unknown after error sending %s in guild %d",
                                event.activity_id,
                                guild_id,
                                exc_info=True,
                            )
                        raise
                    message_id, old_channel = sent_message.id, channel.id
                    try:
                        await self.bot.db.store_feed_message(
                            guild_id, profile_key, aliases, payload, message_id, old_channel
                        )
                        durable_finalized = True
                    except asyncio.CancelledError:
                        cleanup_launched = True
                        cleanup_task = self._track_delivery_cleanup(_finalize_delivery_state(sent_message))
                        await asyncio.shield(cleanup_task)
                        raise
                    except Exception:
                        try:
                            await self.bot.db.mark_feed_delivery_unknown(guild_id, profile_key, aliases)
                            durable_finalized = True
                        except Exception:
                            logger.warning(
                                "Could not record unknown state after post-send write error for message %s (%s) in guild %d",
                                message_id,
                                event.activity_id,
                                guild_id,
                                exc_info=True,
                            )
                        raise
                finally:
                    if not cleanup_launched:
                        self.bot.db.release_feed_delivery_ownership(
                            guild_id,
                            profile_key,
                            aliases,
                            owner_token,
                            recheck_recovery=not durable_finalized,
                        )
                saved_state = "sent" if message_id is not None else "baseline"
                for alias in aliases:
                    saved[alias] = {
                        "payload": payload,
                        "message_id": message_id,
                        "channel_id": old_channel,
                        "delivery_state": saved_state,
                    }
                if not record:
                    await asyncio.sleep(1)
                return target_ok
        await self.bot.db.store_feed_message(guild_id, profile_key, aliases, payload, message_id, old_channel)
        saved_state = "sent" if message_id is not None else "baseline"
        for alias in aliases:
            saved[alias] = {
                "payload": payload,
                "message_id": message_id,
                "channel_id": old_channel,
                "delivery_state": saved_state,
            }
        if not record:
            await asyncio.sleep(1)
        return target_ok


    @staticmethod
    def _previous_rating_from_history(
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


    async def _apply_discovery_policy(
        self,
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
            await self.bot.db.get_feed_event_decisions(guild_id, profile_key, aliases)
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
            authoritative = await self.bot.db.persist_feed_event_decisions(
                guild_id, profile_key, inserts=plan.inserts, revisions=plan.revisions,
                alias_groups=plan.alias_groups,
            )
        return accept_feed_decisions(plan, authoritative)


    @staticmethod
    def _valid_alias(value: Any) -> str | None:
        if isinstance(value, str):
            return value if value.strip() else None
        return None


    @classmethod
    def _parse_persisted_feed_history(cls, rows):
        canonical_groups: dict[tuple[str, str], dict[str, Any]] = {}
        row_bindings: list[tuple[str, tuple[str, str]]] = []
        row_events_by_id: dict[str, FeedEvent] = {}
        for row in rows:
            data = json.loads(row["payload"])
            data["event_ids"] = tuple(data.get("event_ids") or ())
            data["session_labels"] = tuple(data.get("session_labels") or ())
            event = FeedEvent(**data)
            row_id = cls._valid_alias(row["event_id"])
            if row_id:
                row_events_by_id[row_id] = event
            canonical_key = (event.source, event.activity_id)
            state = dict(row).get("delivery_state")
            rank = (
                0 if row["message_id"] is not None else 1,
                0 if state in {"sent", "deleted", "pending", "unknown"} else 1,
            )
            group = canonical_groups.get(canonical_key)
            if group is None:
                group = {"event": event, "rank": rank, "aliases": []}
                canonical_groups[canonical_key] = group
            elif rank < group["rank"]:
                group["event"] = event
                group["rank"] = rank
            for raw_alias in (row["event_id"], event.activity_id, *event.event_ids):
                alias = cls._valid_alias(raw_alias)
                if alias and alias not in group["aliases"]:
                    group["aliases"].append(alias)
            if row_id:
                row_bindings.append((row_id, canonical_key))

        canonical_events: dict[tuple[str, str], FeedEvent] = {}
        canonical_by_source: dict[str, dict[str, FeedEvent]] = {"activity": {}, "journal": {}}
        alias_ranks: dict[str, tuple[int, int]] = {}
        persisted_aliases: dict[str, FeedEvent] = {}

        for canonical_key, group in canonical_groups.items():
            event = group["event"]
            aliases = tuple(group["aliases"]) or (event.activity_id,)
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


    @classmethod
    def _build_persisted_feed_history(cls, rows):
        persisted_aliases, canonical_activity, canonical_journal, _ = (
            cls._parse_persisted_feed_history(rows)
        )
        return persisted_aliases, canonical_activity, canonical_journal


    async def _reconcile_changed_event_identities(
        self, guild_id, profile_key, source, events, historical, *, persisted_aliases=None,
        resolved_event_ids=None,
    ):
        """Reuse aliases only when identity is stable; hold ambiguous sessions."""
        now = self.clock()
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
                    identity_operations.append(self._build_ambiguous_event_decision(event, now))
                    continue
                # A new stable public reference proves this is a distinct event.
                reconciled.append(event)
                continue

            is_session = (
                event.action_type in {"playing", "completed", "replay"}
                or event.is_replay
                or bool(set(event.session_labels) & {"Started", "Finished"})
            )
            prior_sessions = [
                old for old in history
                if old.game_slug == event.game_slug
                and (
                    old.action_type in {"playing", "completed", "replay"}
                    or old.is_replay
                    or bool(set(old.session_labels) & {"Started", "Finished"})
                )
            ]
            if is_session and prior_sessions:
                identity_operations.append(self._build_ambiguous_event_decision(event, now))
                continue
            reconciled.append(event)
        if identity_operations:
            await self.bot.db.persist_feed_identity_decisions(guild_id, profile_key, operations=identity_operations)
        return reconciled


    @staticmethod
    def _build_ambiguous_event_decision(event, now):
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


    @staticmethod
    def _keep_old_posted_sessions_and_unlink_old_baselines(events, explicit_actions, saved, now):
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


    async def poll(self):
        """Periodically check linked users' journals and broadcast new logs."""
        try:
            if self._closing:
                return
            if not self.bot.db or not self.bot.db.is_connected:
                return

            guild_settings = await self.bot.db.get_all_guild_settings()
            if not guild_settings:
                self._unavailable_target_reasons.clear()
                return

            active_guild_ids = {setting["guild_id"] for setting in guild_settings}
            for stale_guild_id in list(self._unavailable_target_reasons):
                if stale_guild_id not in active_guild_ids:
                    self._unavailable_target_reasons.pop(stale_guild_id, None)

            profile_entries: dict[str, dict] = {}
            skipped_targets: dict[str, list[tuple]] = {}
            poll_context = {"activity_health": {}}
            pending_legacy_guilds: set[int] | None = None

            async def retain_skipped_targets(guild_id):
                for row in await self.bot.db.get_guild_feed_recovery_profiles(guild_id):
                    skipped_targets.setdefault(row['profile_key'], []).append(
                        (guild_id, row['display_username'], row['avatar_url'], None)
                    )

            for setting in guild_settings:
                guild_id = setting["guild_id"]
                channel_id = setting["feed_channel_id"]

                guild = self.bot.get_guild(guild_id)
                channel, reason_key, reason_note = self.inspect_target_channel(
                    guild_id,
                    channel_id,
                    guild=guild,
                    require_channel_lookup=True,
                )
                if channel is None:
                    if self._unavailable_target_reasons.get(guild_id) != reason_key:
                        self._unavailable_target_reasons[guild_id] = reason_key
                        logger.warning(
                            "Feed target unavailable for guild %d channel %s: %s",
                            guild_id,
                            channel_id,
                            reason_note,
                        )
                    await retain_skipped_targets(guild_id)
                    continue
                if guild_id in self._unavailable_target_reasons:
                    self._unavailable_target_reasons.pop(guild_id, None)
                    logger.info("Feed target recovered for guild %d channel %d", guild_id, channel_id)

                if pending_legacy_guilds is None:
                    pending_legacy_guilds = set(await self.bot.db.get_legacy_feed_guilds_pending())
                await self.reconcile_legacy_guild(guild, pending_guild_ids=pending_legacy_guilds)
                member_ids = await self.authoritative_member_ids(guild)
                if member_ids is None:
                    logger.warning("Skipping feed polling for guild %d without a complete member snapshot", guild_id)
                    await retain_skipped_targets(guild_id)
                    continue
                linked_profiles = await self.bot.db.get_guild_feed_candidates(guild_id, member_ids)
                await self.bot.db.reconcile_guild_feed_activity(
                    guild_id,
                    [row["profile_key"] for row in linked_profiles],
                    now=self.clock(),
                    present_member_ids=member_ids,
                )

                for profile_row in linked_profiles:
                    profile_key = profile_row["profile_key"]
                    if profile_key not in profile_entries:
                        profile_entries[profile_key] = {
                            "username": profile_row["display_username"],
                            "targets": [],
                        }
                    profile_entries[profile_key]["targets"].append(
                        (guild_id, profile_row["display_username"], profile_row["avatar_url"], channel)
                    )

            for profile_key in sorted(set(profile_entries) | set(skipped_targets)):
                entry = profile_entries.get(profile_key)
                active_targets = entry["targets"] if entry else []
                skipped = skipped_targets.get(profile_key, [])
                username = entry["username"] if entry else skipped[0][1]
                try:
                    sources = await self._fetch_feed_sources(
                        username, defer_boundary_commit=True, poll_context=poll_context
                    )
                    meta = self._activity_snapshot_meta.get(profile_key)
                    health_now = meta.get("observed_at") if meta else None
                    if health_now is None:
                        health_now = self.clock()
                    await self._observe_profile_activity(profile_key, username, sources)
                    all_guilds_ok = bool(active_targets) and not skipped
                    skipped_guild_ids = [item[0] for item in skipped]
                    if skipped_guild_ids:
                        await self.bot.db.record_feed_target_gap(
                            profile_key, now=health_now, guild_ids=skipped_guild_ids,
                            reason="unavailable_feed_target",
                        )
                    succeeded_guild_ids: list[int] = []
                    failed_active: list[tuple[int, str | None]] = []
                    for guild_id, target_username, target_avatar, channel in [*active_targets, *skipped]:
                        try:
                            published_ok = await self.publish_sources(
                                guild_id,
                                profile_key,
                                target_username,
                                target_avatar,
                                channel,
                                sources,
                                deliver=channel is not None,
                                poll_context=poll_context,
                            )
                            if published_ok is False:
                                all_guilds_ok = False
                                if channel is not None:
                                    failed_active.append((
                                        guild_id,
                                        self._last_publish_errors.pop((guild_id, profile_key), None),
                                    ))
                            elif channel is not None:
                                succeeded_guild_ids.append(guild_id)
                                self._last_publish_errors.pop((guild_id, profile_key), None)
                        except Exception as e:
                            all_guilds_ok = False
                            if channel is not None:
                                failed_active.append((
                                    guild_id,
                                    self._last_publish_errors.pop((guild_id, profile_key), str(e)),
                                ))
                            logger.warning("Error publishing activity for %s in guild %d: %s", target_username, guild_id, e)
                    had_known_gap = (
                        profile_key in self._profiles_with_recipient_gaps
                        or bool(meta and meta.get("had_unresolved_gap"))
                        or not all_guilds_ok
                        or "activity" not in sources
                    )
                    if succeeded_guild_ids and had_known_gap:
                        await self.bot.db.clear_guild_feed_recipient_gaps(profile_key, succeeded_guild_ids)
                    if skipped_guild_ids or failed_active:
                        self._profiles_with_recipient_gaps.add(profile_key)
                    elif succeeded_guild_ids and "activity" in sources:
                        self._profiles_with_recipient_gaps.discard(profile_key)
                    for failed_gid, failed_err in failed_active:
                        await self.bot.db.record_feed_target_gap(
                            profile_key, now=health_now, guild_id=failed_gid,
                            reason="delivery_failure", last_error=failed_err,
                        )
                    if all_guilds_ok and meta and "activity" in sources:
                        await self.bot.db.commit_feed_source_boundary(
                            profile_key,
                            "activity",
                            coverage=meta["coverage"],
                            boundary_event_id=meta.get("boundary_event_id"),
                            now=health_now,
                        )
                except Exception as e:
                    logger.warning("Error polling activity for %s: %s", username, e)

                # Pause between users to stay respectful to Backloggd servers
                await asyncio.sleep(1.5)

        except Exception as e:
            logger.error("Error in journal_feed_loop: %s", e, exc_info=True)
