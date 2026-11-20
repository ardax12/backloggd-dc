"""Source polling, reconciliation and Discord delivery façade for the server feed."""

from __future__ import annotations

import asyncio
import json as json
import logging
import time
import zlib
from datetime import datetime, timedelta, timezone
from typing import Any, Callable

from bot.config import (
    FEED_ADAPTIVE_POLL_THRESHOLD,
    FEED_DELIVERY_CONCURRENCY,
    HISTORY_RETENTION_DAYS,
    MISSING_GUILD_PAUSE_HOURS,
)
from bot.feed_models import FeedEvent
from bot.feed_policy import parse_utc_datetime
from bot.services import feed_delivery, feed_history, feed_sources
from bot.services.guild_membership import resolve_guild_member_ids
from bot.services.session_health import UpstreamSessionHealth

logger = logging.getLogger("backloggd.services.feed_pipeline")

FeedTarget = feed_history.FeedTarget
FeedHistory = feed_history.FeedHistory
PreparedAnnouncements = feed_history.PreparedAnnouncements


class FeedPipeline:
    def __init__(
        self,
        bot,
        *,
        clock=None,
        monotonic_clock=None,
        missing_guild_pause_seconds: float | None = None,
        retention_interval_seconds: float = 86400.0,
        history_retention_days: int | None = None,
        adaptive_poll_threshold: int | None = None,
        dormant_after_days: int = 7,
        dormant_tick_modulus: int = 3,
        force_adaptive: bool = False,
        delivery_concurrency: int | None = None,
        scrape_delay_seconds: float = 1.5,
        poll_interval_seconds: float = 300.0,
    ):
        self.bot = bot
        self.clock = clock or (lambda: datetime.now(timezone.utc))
        self._monotonic_clock = monotonic_clock or time.monotonic
        existing_health = getattr(bot, "session_health", None)
        self.session_health = (
            existing_health
            if isinstance(existing_health, UpstreamSessionHealth)
            else UpstreamSessionHealth(clock=self.clock)
        )
        self._activity_snapshot_meta = {}
        self._unavailable_target_reasons: dict[int, str | None] = {}
        self._missing_guild_since: dict[int, float] = {}
        self._missing_guild_pause_seconds = (
            float(missing_guild_pause_seconds)
            if missing_guild_pause_seconds is not None
            else float(MISSING_GUILD_PAUSE_HOURS * 3600)
        )
        self._retention_interval_seconds = float(retention_interval_seconds)
        self._history_retention_days = (
            int(history_retention_days)
            if history_retention_days is not None
            else int(HISTORY_RETENTION_DAYS)
        )
        self._adaptive_poll_threshold = (
            int(adaptive_poll_threshold)
            if adaptive_poll_threshold is not None
            else int(FEED_ADAPTIVE_POLL_THRESHOLD)
        )
        self._dormant_after_days = max(1, int(dormant_after_days))
        self._dormant_tick_modulus = max(1, int(dormant_tick_modulus))
        self._force_adaptive = bool(force_adaptive)
        raw_concurrency = (
            FEED_DELIVERY_CONCURRENCY
            if delivery_concurrency is None
            else int(delivery_concurrency)
        )
        self._delivery_concurrency = max(1, min(5, int(raw_concurrency)))
        self._scrape_delay_seconds = max(0.0, float(scrape_delay_seconds))
        self._poll_interval_seconds = max(1.0, float(poll_interval_seconds))
        self._poll_tick_index = 0
        self._last_retention_prune_at: float | None = None
        self._last_publish_errors: dict[tuple[int, str], str] = {}
        self._profiles_with_recipient_gaps: set[str] = set()
        self._channel_send_locks: dict[int, asyncio.Lock] = {}
        self._delivery_cleanup_tasks: set[asyncio.Task] = set()
        self.last_poll_completed_at: datetime | None = None
        self.last_poll_duration_seconds: float | None = None
        self._closing = False

    _OBSERVATION_STATUS_RANK = feed_sources.OBSERVATION_STATUS_RANK

    def _get_channel_lock(self, channel_id: int) -> asyncio.Lock:
        lock = self._channel_send_locks.get(channel_id)
        if lock is None:
            lock = asyncio.Lock()
            self._channel_send_locks[channel_id] = lock
        return lock

    def forget_guild(self, guild_id: int) -> None:
        """Drop in-memory outage tracking for a guild the bot has left or paused."""
        self._unavailable_target_reasons.pop(guild_id, None)
        self._missing_guild_since.pop(guild_id, None)

    async def _select_profiles_for_tick(
        self,
        all_profile_keys: list[str],
        profile_entries: dict[str, dict],
        skipped_targets: dict[str, list[tuple]],
        now: datetime,
        *,
        probe_only: bool = False,
    ) -> tuple[list[str], int]:
        """Partition profiles into active/priority (every tick) and dormant (staggered buckets)."""
        if not all_profile_keys or self._dormant_tick_modulus <= 1:
            return all_profile_keys, 0
        adaptive_enabled = self._force_adaptive or (
            self._adaptive_poll_threshold > 0 and len(all_profile_keys) >= self._adaptive_poll_threshold
        )
        if not adaptive_enabled:
            return all_profile_keys, 0

        cadence_loader: Callable[..., Any] | None = getattr(self.bot.db, "get_profiles_poll_cadence_metadata", None)
        if not callable(cadence_loader):
            return all_profile_keys, 0

        metadata_by_profile = await cadence_loader(all_profile_keys)
        cutoff = now - timedelta(days=self._dormant_after_days)
        tick_bucket = self._poll_tick_index % self._dormant_tick_modulus
        required_display_sources = {"activity-display-v1", "journal-display-v1"}
        selected: list[str] = []
        skipped_dormant = 0

        for profile_key in all_profile_keys:
            if profile_key in skipped_targets or profile_key in self._profiles_with_recipient_gaps:
                selected.append(profile_key)
                continue
            meta = metadata_by_profile.get(profile_key)
            if not meta:
                selected.append(profile_key)
                continue
            if (
                not {"activity", "journal"}.issubset(meta["healthy_sources"])
                or meta["has_source_gap"]
                or meta["has_recipient_gap"]
                or meta["has_pending_reconciliation"]
                or meta["has_pending_delivery"]
            ):
                selected.append(profile_key)
                continue

            entry = profile_entries.get(profile_key)
            active_targets = entry["targets"] if entry else []
            init_sources = meta["initialized_guild_sources"]
            if any(
                not required_display_sources.issubset(init_sources.get(guild_id, ()))
                for guild_id, *_ in active_targets
            ):
                selected.append(profile_key)
                continue

            last_activity_dt = parse_utc_datetime(meta.get("last_activity_at"))
            if last_activity_dt is not None and last_activity_dt >= cutoff:
                selected.append(profile_key)
                continue

            # Dormant healthy profile: poll deterministically once every N ticks.
            bucket = zlib.crc32(profile_key.encode("utf-8")) % self._dormant_tick_modulus
            if bucket == tick_bucket:
                selected.append(profile_key)
            else:
                skipped_dormant += 1

        if probe_only and not selected and all_profile_keys:
            selected = [all_profile_keys[0]]
            skipped_dormant = max(0, skipped_dormant - 1)

        return selected, skipped_dormant

    async def _maybe_prune_stale_history(self, now_mono: float) -> None:
        """Run safe history retention pruning at most once per retention interval."""
        if self._closing:
            return
        prune_fn: Callable[..., Any] | None = getattr(self.bot.db, "prune_stale_history", None)
        if not callable(prune_fn):
            return
        if (
            self._last_retention_prune_at is not None
            and (now_mono - self._last_retention_prune_at) < self._retention_interval_seconds
        ):
            return
        self._last_retention_prune_at = now_mono
        try:
            pruned = await prune_fn(
                self.clock(),
                retention_days=self._history_retention_days,
            )
        except Exception:
            logger.warning("Periodic history retention pruning failed", exc_info=True)
            return
        if isinstance(pruned, dict):
            total = sum(int(v or 0) for v in pruned.values())
            if total > 0:
                logger.info(
                    "Pruned stale history: observations=%d journal_deliveries=%d journal_logs=%d feed_decisions=%d",
                    int(pruned.get("observations", 0)),
                    int(pruned.get("journal_deliveries", 0)),
                    int(pruned.get("journal_logs", 0)),
                    int(pruned.get("feed_decisions", 0)),
                )

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
        return feed_delivery.inspect_target_channel(
            self,
            guild_id,
            channel_id,
            guild=guild,
            require_channel_lookup=require_channel_lookup,
        )

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

    # ── Source fetching & activity observation delegates ──────────────

    @staticmethod
    def _is_library_relevant_event(event: FeedEvent) -> bool:
        return feed_sources.is_library_relevant_event(event)

    @staticmethod
    def _observation_fingerprint(event: FeedEvent) -> str:
        return feed_sources.observation_fingerprint(event)

    async def _initial_observations_covered_by_sync(
        self,
        profile_key: str,
        initial_events: list[FeedEvent],
    ) -> bool:
        """Return True only when a completed, healthy sync demonstrably covers all initial_events."""
        return await feed_sources.initial_observations_covered_by_sync(
            self, profile_key, initial_events
        )

    async def _observe_profile_activity(
        self,
        profile_key: str,
        username: str,
        sources: dict[str, list[FeedEvent]],
    ) -> bool:
        """Persist profile-level relevant fingerprints and mark subscribed libraries stale without syncing."""
        return await feed_sources.observe_profile_activity(self, profile_key, username, sources)

    async def _fetch_feed_sources(
        self,
        username: str,
        *,
        defer_boundary_commit: bool = False,
        poll_context: dict | None = None,
    ) -> dict[str, list[FeedEvent]]:
        """Read sources independently; a blocked page is never an empty snapshot."""
        return await feed_sources.fetch_feed_sources(
            self,
            username,
            defer_boundary_commit=defer_boundary_commit,
            poll_context=poll_context,
        )

    # ── Publish orchestration ─────────────────────────────────────────

    async def publish_sources(
        self, guild_id, profile_key, username, avatar_url, channel, sources,
        *, deliver: bool = True, poll_context: dict | None = None,
    ) -> bool:
        self._last_publish_errors.pop((guild_id, profile_key), None)
        target = FeedTarget(guild_id, profile_key, username, avatar_url, channel)
        partial_journal = profile_key in (poll_context or {}).get("partial_journals", ())
        history = await self._load_and_initialize_history(target, sources)
        activities, logs = self._merge_source_events(history, partial_journal=partial_journal)
        events, now = await self._filter_discoveries(target, activities, logs, history, poll_context)
        if not deliver:
            # Observations stay durable without modifying archived Discord messages.
            return False
        prepared = await self._prepare_announcements(target, events, history, now)
        return await self._deliver_announcements(
            target, history, prepared, now, poll_context=poll_context,
        )

    # ── History, identity reconciliation & announcement prep delegates ──

    async def _load_and_initialize_history(self, target, sources) -> FeedHistory:
        """Reconcile identities, establish atomic baselines, and restore unsent work."""
        return await feed_history.load_and_initialize_history(self, target, sources)

    @staticmethod
    def _merge_source_events(history, *, partial_journal=False):
        """Combine current observations and canonical history without losing enrichment."""
        return feed_history.merge_source_events(history, partial_journal=partial_journal)

    async def _filter_discoveries(self, target, activities, logs, history, poll_context):
        """Match milestones and apply policy before rating/review merging or delivery."""
        return await feed_history.filter_discoveries(
            self, target, activities, logs, history, poll_context
        )

    async def _prepare_announcements(self, target, events, history, now) -> PreparedAnnouncements:
        """Merge eligible actions and load bounded rating/delivery evidence once."""
        return await feed_history.prepare_announcements(self, target, events, history, now)

    @staticmethod
    def _previous_rating_from_history(
        event,
        activity_history,
        journal_history,
        *,
        journal_time_unknown: bool = False,
    ):
        """Use the latest strictly earlier rating observation, never played_date."""
        return feed_history.previous_rating_from_history(
            event,
            activity_history,
            journal_history,
            journal_time_unknown=journal_time_unknown,
        )

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
        return await feed_history.apply_discovery_policy(
            self,
            guild_id,
            profile_key,
            events,
            saved,
            now,
            activation_at=activation_at,
            recovery_uncertain=recovery_uncertain,
            uncertain_reasons=uncertain_reasons,
            resolved_event_ids=resolved_event_ids,
        )

    @staticmethod
    def _valid_alias(value: Any) -> str | None:
        return feed_history.valid_alias(value)

    @classmethod
    def _parse_persisted_feed_history(cls, rows):
        return feed_history.parse_persisted_feed_history(rows)

    @classmethod
    def _build_persisted_feed_history(cls, rows):
        return feed_history.build_persisted_feed_history(rows)

    async def _reconcile_changed_event_identities(
        self, guild_id, profile_key, source, events, historical, *, persisted_aliases=None,
        resolved_event_ids=None,
    ):
        """Reuse aliases only when identity is stable; hold ambiguous sessions."""
        return await feed_history.reconcile_changed_event_identities(
            self,
            guild_id,
            profile_key,
            source,
            events,
            historical,
            persisted_aliases=persisted_aliases,
            resolved_event_ids=resolved_event_ids,
        )

    @staticmethod
    def _build_ambiguous_event_decision(event, now):
        return feed_history.build_ambiguous_event_decision(event, now)

    @staticmethod
    def _keep_old_posted_sessions_and_unlink_old_baselines(events, explicit_actions, saved, now):
        return feed_history.keep_old_posted_sessions_and_unlink_old_baselines(
            events, explicit_actions, saved, now
        )

    # ── Discord delivery delegates ────────────────────────────────────

    async def _deliver_announcements(
        self, target, history, prepared, now, *, poll_context: dict | None = None
    ) -> bool:
        return await feed_delivery.deliver_announcements(
            self, target, history, prepared, now, poll_context=poll_context
        )

    async def _get_guild_feed_filters(
        self, guild_id: int, poll_context: dict | None = None
    ) -> dict[str, bool]:
        return await feed_delivery.get_guild_feed_filters(self, guild_id, poll_context)

    async def _record_guild_filtered_event(
        self,
        guild_id: int,
        profile_key: str,
        event: FeedEvent,
        aliases: tuple[str, ...],
        payload: str,
        now: datetime,
        saved: dict,
    ) -> None:
        """Record a guild-filtered event as suppressed and baseline it against future backfill."""
        await feed_delivery.record_guild_filtered_event(
            self, guild_id, profile_key, event, aliases, payload, now, saved
        )

    @staticmethod
    def _select_delivery_record(saved, delivered_alias_ids, event, aliases):
        """Select the canonical saved delivery record (or synthetic baseline) for an event's aliases."""
        return feed_delivery.select_delivery_record(saved, delivered_alias_ids, event, aliases)

    def _resolve_delivery_event(self, event, record, row_events_by_id, prepared):
        """Annotate previous_rating from saved state or poll history and detect unchanged events."""
        return feed_delivery.resolve_delivery_event(self, event, record, row_events_by_id, prepared)

    async def _store_missing_aliases(
        self,
        guild_id: int,
        profile_key: str,
        missing_aliases,
        payload: str,
        message_id,
        channel_id,
        delivery_state: str,
        saved: dict,
    ) -> None:
        """Persist newly observed aliases for an existing feed message record and update local cache."""
        await feed_delivery.store_missing_aliases(
            self,
            guild_id,
            profile_key,
            missing_aliases,
            payload,
            message_id,
            channel_id,
            delivery_state,
            saved,
        )

    def _resolve_edit_channel(self, guild_id: int, old_channel, current_channel):
        """Resolve and validate the target channel for editing an existing announcement."""
        return feed_delivery.resolve_edit_channel(self, guild_id, old_channel, current_channel)

    async def _edit_existing_announcement(
        self,
        target,
        *,
        old_channel,
        message_id,
        state,
        record,
        aliases,
        missing_aliases,
        payload: str,
        embed,
        view,
        saved: dict,
        stats,
    ) -> bool | None:
        """Edit an existing announcement message."""
        return await feed_delivery.edit_existing_announcement(
            self,
            target,
            old_channel=old_channel,
            message_id=message_id,
            state=state,
            record=record,
            aliases=aliases,
            missing_aliases=missing_aliases,
            payload=payload,
            embed=embed,
            view=view,
            saved=saved,
            stats=stats,
        )

    async def _send_new_announcement(
        self,
        target,
        *,
        event,
        record,
        aliases,
        payload: str,
        embed,
        view,
        saved: dict,
        stats,
    ) -> bool:
        """Publish a new announcement inside one send-ownership and cancellation-shielded scope."""
        return await feed_delivery.send_new_announcement(
            self,
            target,
            event=event,
            record=record,
            aliases=aliases,
            payload=payload,
            embed=embed,
            view=view,
            saved=saved,
            stats=stats,
        )

    async def _deliver_event(
        self, target, event, history, prepared, now, *, poll_context: dict | None = None
    ) -> bool:
        """Orchestrate record lookup, filtering, archive edits, new sends, and persistence."""
        return await feed_delivery.deliver_event(
            self, target, event, history, prepared, now, poll_context=poll_context
        )

    async def _deliver_profile_item(
        self,
        profile_key: str,
        username: str,
        sources: dict[str, list[FeedEvent]],
        meta: dict[str, Any] | None,
        health_now: datetime,
        active_targets: list[tuple],
        skipped: list[tuple],
        *,
        poll_context: dict[str, Any],
        stats: dict[str, int],
    ) -> None:
        """Persist observations, deliver announcements across target guilds, and update gaps/boundaries."""
        try:
            await self._observe_profile_activity(profile_key, username, sources)
            all_guilds_ok = bool(active_targets) and not skipped
            skipped_guild_ids = [item[0] for item in skipped]
            if skipped_guild_ids:
                await self.bot.db.record_feed_target_gap(
                    profile_key,
                    now=health_now,
                    guild_ids=skipped_guild_ids,
                    reason="unavailable_feed_target",
                )
            succeeded_guild_ids: list[int] = []
            failed_active: list[tuple[int, str | None]] = []
            for guild_id, target_username, target_avatar, channel in [*active_targets, *skipped]:
                try:
                    channel_id = getattr(channel, "id", None) if channel is not None else None
                    if channel is not None and isinstance(channel_id, int):
                        async with self._get_channel_lock(channel_id):
                            published_ok = await self.publish_sources(
                                guild_id,
                                profile_key,
                                target_username,
                                target_avatar,
                                channel,
                                sources,
                                deliver=True,
                                poll_context=poll_context,
                            )
                    else:
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
                            failed_active.append(
                                (
                                    guild_id,
                                    self._last_publish_errors.pop((guild_id, profile_key), None),
                                )
                            )
                    elif channel is not None:
                        succeeded_guild_ids.append(guild_id)
                        self._last_publish_errors.pop((guild_id, profile_key), None)
                except Exception as e:
                    all_guilds_ok = False
                    if channel is not None:
                        stats["delivery_errors"] += 1
                        failed_active.append(
                            (
                                guild_id,
                                self._last_publish_errors.pop((guild_id, profile_key), str(e)),
                            )
                        )
                    logger.warning(
                        "Error publishing activity for %s in guild %d: %s",
                        target_username,
                        guild_id,
                        e,
                    )
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
                    profile_key,
                    now=health_now,
                    guild_id=failed_gid,
                    reason="delivery_failure",
                    last_error=failed_err,
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
            stats["delivery_errors"] += 1
            logger.warning("Error delivering activity for %s: %s", username, e)

    @staticmethod
    async def _drain_workers(workers: list[asyncio.Task[Any]]) -> None:
        """Cancel unfinished delivery workers and wait until all have unwound."""
        for worker in workers:
            if not worker.done():
                worker.cancel()
        while True:
            pending = [worker for worker in workers if not worker.done()]
            if not pending:
                break
            try:
                await asyncio.gather(*pending, return_exceptions=True)
            except asyncio.CancelledError:
                continue

    # ── Periodic poll loop ────────────────────────────────────────────

    async def poll(self):
        """Periodically check linked users' journals and broadcast new logs."""
        if self._closing:
            return
        if not self.bot.db or not self.bot.db.is_connected:
            return

        poll_start = self._monotonic_clock()
        should_run, probe_only = self.session_health.get_poll_gate(self.clock())
        if not should_run:
            logger.info(
                "Feed poll skipped: upstream session health in backoff (kind=%s, next_retry_at=%s).",
                self.session_health.kind or "degraded",
                self.session_health.next_retry_at.isoformat() if self.session_health.next_retry_at else "unknown",
            )
            await self._maybe_prune_stale_history(poll_start)
            return
        if probe_only:
            logger.info(
                "Feed poll probing upstream session health after backoff (kind=%s).",
                self.session_health.kind or "degraded",
            )

        stats = {
            "guilds": 0,
            "profiles": 0,
            "sent": 0,
            "edited": 0,
            "source_errors": 0,
            "delivery_errors": 0,
            "skipped_dormant": 0,
        }
        try:
            guild_settings = await self.bot.db.get_all_guild_settings()
            if not guild_settings:
                self._unavailable_target_reasons.clear()
                self._missing_guild_since.clear()
                await self._maybe_prune_stale_history(poll_start)
                return

            active_guild_ids = {setting["guild_id"] for setting in guild_settings}
            stats["guilds"] = len(active_guild_ids)
            for stale_guild_id in list(self._unavailable_target_reasons):
                if stale_guild_id not in active_guild_ids:
                    self._unavailable_target_reasons.pop(stale_guild_id, None)
            for stale_guild_id in list(self._missing_guild_since):
                if stale_guild_id not in active_guild_ids:
                    self._missing_guild_since.pop(stale_guild_id, None)

            profile_entries: dict[str, dict] = {}
            skipped_targets: dict[str, list[tuple]] = {}
            guild_filters: dict[int, dict[str, bool]] = {}
            if hasattr(self.bot.db, "get_all_guild_feed_filters"):
                guild_filters = await self.bot.db.get_all_guild_feed_filters(active_guild_ids)
            poll_context = {
                "activity_health": {},
                "stats": stats,
                "guild_filters": guild_filters,
            }
            pending_legacy_guilds: set[int] | None = None

            async def retain_skipped_targets(guild_id):
                for row in await self.bot.db.get_guild_feed_recovery_profiles(guild_id):
                    skipped_targets.setdefault(row["profile_key"], []).append(
                        (guild_id, row["display_username"], row["avatar_url"], None)
                    )

            bot_disconnected = getattr(self.bot, "_disconnected_since", None) is not None
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
                    if reason_key == "guild_unavailable" and not bot_disconnected:
                        first_missing = self._missing_guild_since.setdefault(guild_id, poll_start)
                        elapsed_missing = max(0.0, poll_start - first_missing)
                        if (
                            self._missing_guild_pause_seconds > 0
                            and elapsed_missing >= self._missing_guild_pause_seconds
                        ):
                            unset_fn: Callable[..., Any] | None = getattr(self.bot.db, "unset_feed_channel", None)
                            if callable(unset_fn):
                                try:
                                    await unset_fn(guild_id)
                                except Exception:
                                    logger.warning(
                                        "Failed to auto-pause feed for absent guild %d",
                                        guild_id,
                                        exc_info=True,
                                    )
                                else:
                                    self.forget_guild(guild_id)
                                    logger.warning(
                                        "Paused feed for guild %d after %.1fh continuous absence from Discord cache.",
                                        guild_id,
                                        elapsed_missing / 3600.0,
                                    )
                                    continue
                    elif reason_key != "guild_unavailable":
                        self._missing_guild_since.pop(guild_id, None)
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
                self._missing_guild_since.pop(guild_id, None)
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

            all_profile_keys = sorted(set(profile_entries) | set(skipped_targets))
            selected_profile_keys, skipped_dormant = await self._select_profiles_for_tick(
                all_profile_keys,
                profile_entries,
                skipped_targets,
                self.clock(),
                probe_only=probe_only,
            )
            stats["skipped_dormant"] = skipped_dormant

            if selected_profile_keys:
                worker_count = min(self._delivery_concurrency, len(selected_profile_keys))
                delivery_queue: asyncio.Queue[tuple[Any, ...] | None] = asyncio.Queue(
                    maxsize=max(1, self._delivery_concurrency * 2)
                )

                async def _delivery_worker() -> None:
                    while True:
                        item = await delivery_queue.get()
                        if item is None:
                            return
                        await self._deliver_profile_item(
                            *item,
                            poll_context=poll_context,
                            stats=stats,
                        )

                workers = [asyncio.create_task(_delivery_worker()) for _ in range(worker_count)]
                try:
                    last_scrape_finished: float | None = None
                    for profile_key in selected_profile_keys:
                        if self._closing:
                            break
                        if last_scrape_finished is not None and self._scrape_delay_seconds > 0:
                            elapsed_since_prev = max(0.0, time.monotonic() - last_scrape_finished)
                            remaining_delay = self._scrape_delay_seconds - elapsed_since_prev
                            if remaining_delay > 0:
                                # Pause between users to stay respectful to Backloggd servers
                                await asyncio.sleep(remaining_delay)
                        if self._closing:
                            break

                        stats["profiles"] += 1
                        entry = profile_entries.get(profile_key)
                        active_targets = entry["targets"] if entry else []
                        skipped = skipped_targets.get(profile_key, [])
                        username = entry["username"] if entry else skipped[0][1]
                        try:
                            sources = await self._fetch_feed_sources(
                                username, defer_boundary_commit=True, poll_context=poll_context
                            )
                            raw_meta = self._activity_snapshot_meta.get(profile_key)
                            meta = dict(raw_meta) if isinstance(raw_meta, dict) else raw_meta
                            health_now = meta.get("observed_at") if isinstance(meta, dict) else None
                            if health_now is None:
                                health_now = self.clock()
                        except Exception as e:
                            stats["source_errors"] += 1
                            logger.warning("Error polling activity for %s: %s", username, e)
                            last_scrape_finished = time.monotonic()
                            if poll_context.get("upstream_aborted"):
                                break
                            continue

                        last_scrape_finished = time.monotonic()
                        await delivery_queue.put(
                            (profile_key, username, sources, meta, health_now, active_targets, skipped)
                        )
                        if poll_context.get("upstream_aborted"):
                            break

                    for _ in workers:
                        await delivery_queue.put(None)
                    await asyncio.gather(*workers)
                except BaseException:
                    await self._drain_workers(workers)
                    raise
                finally:
                    await self._drain_workers(workers)
                    for cid in [
                        cid
                        for cid, lock in list(self._channel_send_locks.items())
                        if not lock.locked()
                    ]:
                        self._channel_send_locks.pop(cid, None)

            if all_profile_keys and not poll_context.get("upstream_aborted") and not self._closing:
                self._poll_tick_index += 1

            await self._maybe_prune_stale_history(poll_start)

        except Exception as e:
            logger.error("Error in journal_feed_loop: %s", e, exc_info=True)
        finally:
            elapsed_s = max(0.0, self._monotonic_clock() - poll_start)
            self.last_poll_completed_at = self.clock()
            self.last_poll_duration_seconds = elapsed_s
            logger.info(
                "Feed poll complete: guilds=%d profiles=%d sent=%d edited=%d source_errors=%d delivery_errors=%d skipped_dormant=%d elapsed_s=%.2f",
                stats["guilds"],
                stats["profiles"],
                stats["sent"],
                stats["edited"],
                stats["source_errors"],
                stats["delivery_errors"],
                stats["skipped_dormant"],
                elapsed_s,
            )
            if elapsed_s > self._poll_interval_seconds:
                logger.warning(
                    "Feed poll overran its %.0fs interval (elapsed_s=%.2f, profiles=%d, guilds=%d).",
                    self._poll_interval_seconds,
                    elapsed_s,
                    stats["profiles"],
                    stats["guilds"],
                )
            elif elapsed_s >= self._poll_interval_seconds * 0.8:
                logger.warning(
                    "Feed poll approached its %.0fs interval (elapsed_s=%.2f, profiles=%d, guilds=%d).",
                    self._poll_interval_seconds,
                    elapsed_s,
                    stats["profiles"],
                    stats["guilds"],
                )
