"""FeedCog: Handles /setchannel, /unsetchannel, and the periodic playthrough journal feed loop."""

import aiohttp
import discord
from discord import app_commands
from discord.ext import commands, tasks
import hashlib
import inspect
import logging
import asyncio
import json
import uuid
from datetime import datetime, timezone
from dataclasses import asdict, replace
from backloggd.exceptions import ResourceNotFoundError, UserNotFoundError
from bot.feed_rules import (
    VISIBLE_ACTIONS,
    calendar_date,
    journal_events,
    link_backdated_milestones,
    merge_announcements,
    merge_milestones,
    merge_ratings_and_reviews,
)
from bot.feed_policy import classify_feed_event, parse_utc_datetime
from bot.command_errors import (
    defer_interaction,
    mark_interaction_error_handled,
    send_ephemeral_error,
    send_private_followup,
)
from bot.input_validation import (
    InvalidUsernameError,
    escape_display_text,
    normalize_backloggd_username,
)
from bot.services.guild_membership import resolve_guild_member_ids
from bot.services.library_sync import parse_sync_timestamp, schedule_profile_refresh
from bot.ui.feed_embed import create_journal_feed_embed
from bot.ui.views import FeedListView, LinkButtonView
from bot.ui.helpers import (
    build_backloggd_game_url,
    build_backloggd_user_url,
    clamp_escaped_text,
    format_health_error_excerpt,
)
from bot.feed_models import FeedEvent

logger = logging.getLogger("backloggd.cogs.feed")

class FeedCog(commands.Cog, name="Feed"):
    feed = app_commands.Group(name="feed", description="Manage your server's Backloggd activity feed")

    def __init__(self, bot, *, clock=None):
        self.bot = bot
        self.clock = clock or (lambda: datetime.now(timezone.utc))
        self._activity_snapshot_meta = {}
        self._unavailable_target_reasons: dict[int, str] = {}
        self._closing = False
        self._draining_task: asyncio.Task | None = None

    def _inspect_target_channel(
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

    async def cog_load(self):
        # Start loop when cog is loaded and bot event loop is active
        if not self._closing and not self.journal_feed_loop.is_running():
            self.journal_feed_loop.start()

    async def close(self) -> None:
        """Stop accepting polls, cancel the loop and wait for its work to unwind."""
        self._closing = True
        current = asyncio.current_task()
        if self._draining_task is not None:
            if self._draining_task is not current and not self._draining_task.done():
                await asyncio.shield(self._draining_task)
            return

        async def _drain() -> None:
            task = self.journal_feed_loop.get_task()
            if task is None or task is current:
                return
            if not task.done() and not task.cancelling():
                self.journal_feed_loop.cancel()
            await asyncio.gather(task, return_exceptions=True)

        self._draining_task = asyncio.create_task(_drain())
        try:
            await asyncio.shield(self._draining_task)
        except asyncio.CancelledError:
            await asyncio.shield(self._draining_task)
            raise
        finally:
            if self._draining_task.done():
                self._draining_task = None

    async def cog_unload(self) -> None:
        await self.close()

    async def _authoritative_member_ids(self, guild):
        """Return a complete cached member snapshot, or None when unavailable."""
        return await resolve_guild_member_ids(self.bot, guild)

    @staticmethod
    def _feed_enrollment_state_copy(status: dict) -> str:
        """Describe baseline state without treating missing membership as proof of a gap."""
        if status.get("pending_reconciliation"):
            return (
                "Source continuity is still being verified. No baseline reset is assumed; "
                "a complete member snapshot is required to resolve it."
            )
        if status.get("active"):
            initialized = set(status.get("initialized_sources") or ())
            expected = {"activity-display-v1", "journal-display-v1"}
            if expected.issubset(initialized):
                return "This profile is already active with initialized baselines; this command does not reset them."
            if initialized:
                return "This profile is already active; any uninitialized source will be baselined without resetting existing sources."
            return "This profile is active and its source baseline is pending the first successful check."
        if status.get("last_active_at") is not None:
            return "This profile is resuming after a proven inactive period; existing history will be skipped as a fresh baseline is established."
        return "This is the first enrollment; existing history will be skipped as the initial baseline is established."

    async def _reconcile_legacy_guild(self, guild, *, pending_guild_ids: set[int] | None = None):
        guild_id = getattr(guild, "id", None)
        if guild_id is None:
            return False
        if pending_guild_ids is None:
            pending_guild_ids = set(await self.bot.db.get_legacy_feed_guilds_pending())
        if guild_id not in pending_guild_ids:
            return False
        member_ids = await self._authoritative_member_ids(guild)
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
                await self._reconcile_legacy_guild(guild, pending_guild_ids=pending_guild_ids)

    # =========================================================================
    # Channel Configuration Slash Commands (Admin Only)
    # =========================================================================

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

    _OBSERVATION_STATUS_RANK = {
        "played": 4,
        "playing": 3,
        "backlog": 2,
        "wishlist": 1,
        "unknown": 0,
    }

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
        sync_getter = getattr(self.bot.db, "get_profile_sync_info", None)
        if sync_getter is None:
            return False
        sync_info = await sync_getter(profile_key)
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

        entries_getter = getattr(self.bot.db, "get_profile_library_entries", None)
        if entries_getter is not None:
            library_entries = await entries_getter(profile_key)
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
        recorder = getattr(self.bot.db, "record_profile_activity_observations", None)
        if recorder is None or not sources:
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
            result = await recorder(profile_key, source_name, observations)
            changed = int(result.get("changed", 0))
            total_changed += changed
            if changed > 0:
                if not result.get("initialized") or int(result.get("updated", 0)) > 0:
                    non_initial_changes = True
                else:
                    initial_relevant_events.extend(relevant)
        sub_checker = getattr(self.bot.db, "is_profile_library_subscribed", None)
        pending_setter = getattr(self.bot.db, "set_profile_refresh_pending", None)
        pending_getter = getattr(self.bot.db, "get_profile_refresh_pending_token", None)
        pending_clearer = getattr(self.bot.db, "clear_profile_refresh_pending", None)
        has_pending = (
            (await pending_getter(profile_key)) is not None
            if pending_getter is not None else False
        )
        if total_changed > 0:
            is_subscribed = await sub_checker(profile_key) if sub_checker is not None else True
            if (
                is_subscribed
                and not has_pending
                and not non_initial_changes
                and await self._initial_observations_covered_by_sync(profile_key, initial_relevant_events)
            ):
                return False
            if is_subscribed and pending_setter is not None:
                await pending_setter(profile_key, uuid.uuid4().hex)
        else:
            if not has_pending:
                return False
            is_subscribed = await sub_checker(profile_key) if sub_checker is not None else True
            if not is_subscribed:
                if pending_clearer is not None:
                    await pending_clearer(profile_key)
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
        database = getattr(self.bot, "db", None)
        health_now = self.clock()
        self._activity_snapshot_meta.pop(profile_key, None)
        try:
            health = None
            if database and hasattr(database, "record_feed_source_attempt"):
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
            sources["activity"] = parsed
            self._activity_snapshot_meta[profile_key] = {
                "coverage": coverage,
                "pages_read": pages_read,
                "prior_boundary_id": prior_boundary,
                "boundary_event_id": boundary_event_id,
                "had_unresolved_gap": had_unresolved_gap,
                "had_prior_failure": had_prior_failure,
                "observed_at": health_now,
            }
            if database and hasattr(database, "record_feed_source_success"):
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
                poll_context.setdefault("activity_health", {})[profile_key] = {"last_error": None}
        except Exception as error:
            logger.warning("Could not read activity for %s: %s", username, error)
            if database and hasattr(database, "record_feed_source_failure"):
                await database.record_feed_source_failure(profile_key, "activity", str(error))
        try:
            if database and hasattr(database, "record_feed_source_attempt"):
                await database.record_feed_source_attempt(
                    profile_key, "journal", now=health_now,
                )
            entries = await self.bot.backloggd.get_user_journal(
                username, max_pages=100, require_complete=True
            )
            sources["journal"] = journal_events(entries, username)
            if database and hasattr(database, "record_feed_source_success"):
                await database.record_feed_source_success(
                    profile_key, "journal", coverage="exhausted", pages_read=None,
                    now=health_now,
                )
        except Exception as error:
            logger.warning("Could not read journal for %s: %s", username, error)
            if database and hasattr(database, "record_feed_source_failure"):
                await database.record_feed_source_failure(profile_key, "journal", str(error))
        return sources

    async def _publish_sources(
        self, guild_id, profile_key, username, avatar_url, channel, sources,
        *, deliver: bool = True, poll_context: dict | None = None,
    ) -> bool:
        target_ok = True
        rows = await self.bot.db.get_feed_messages(guild_id, profile_key)
        saved = {row["event_id"]: row for row in rows}
        sources = {source: list(events) for source, events in sources.items()}
        persisted_aliases, canonical_activity, canonical_journal, row_events_by_id = (
            self._parse_persisted_feed_history(rows)
        )
        histories = {"activity": canonical_activity, "journal": canonical_journal}
        bulk_sources_getter = getattr(self.bot.db, "get_feed_sources", None)
        if callable(bulk_sources_getter):
            existing_sources = await bulk_sources_getter(guild_id, profile_key)
            initialized_map = {
                source: (source + "-display-v1") in existing_sources
                for source in sources
            }
        else:
            initialized_map = {
                source: await self.bot.db.has_feed_source(
                    guild_id, profile_key, source + "-display-v1"
                )
                for source in sources
            }
        for source, events in list(sources.items()):
            if initialized_map[source]:
                sources[source] = await self._reconcile_changed_event_identities(
                    guild_id,
                    profile_key,
                    source,
                    events,
                    histories.get(source, {}),
                    persisted_aliases=persisted_aliases,
                )
        new_journal_baseline_ids = set()
        any_first_baseline = False
        for source, events in sources.items():
            # Empty successful sources are baselined too. A source absent from
            # this mapping failed and must be retried without changing state.
            if initialized_map[source]:
                continue
            ids = [e.activity_id for e in events]
            bulk_baseline = getattr(self.bot.db, "seed_feed_source_with_messages", None)
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
            if callable(bulk_baseline):
                first = await bulk_baseline(
                    guild_id,
                    profile_key,
                    source + "-display-v1",
                    baseline_payloads.items(),
                    baseline_decisions=baseline_decisions,
                )
            else:
                first = await self.bot.db.seed_feed_source(
                    guild_id, profile_key, source + "-display-v1", ids
                )
                if first and events:
                    bulk_persister = getattr(self.bot.db, "persist_feed_event_decisions", None)
                    for event in events:
                        await self.bot.db.seed_feed_message(
                            guild_id, profile_key, (event.activity_id,), baseline_payloads[event.activity_id]
                        )
                    if callable(bulk_persister):
                        await bulk_persister(guild_id, profile_key, inserts=baseline_decisions)
                    else:
                        for item in baseline_decisions:
                            await self.bot.db.record_feed_event_decision(
                                guild_id,
                                profile_key,
                                item["event_id"],
                                source=item["source"],
                                event_date=item["event_date"],
                                decision=item["decision"],
                                reason=item["reason"],
                                payload=item["payload"],
                                observed_at=item["observed_at"],
                            )
            if first:
                any_first_baseline = True
                if source == "journal":
                    new_journal_baseline_ids.update(ids)
        if any_first_baseline:
            # Refresh the lookup to include the baseline just written.
            rows = await self.bot.db.get_feed_messages(guild_id, profile_key)
            saved = {row["event_id"]: row for row in rows}
            _, _, _, row_events_by_id = self._parse_persisted_feed_history(rows)
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
        now = self.clock()
        activation_at = None
        if hasattr(self.bot.db, "get_guild_profile_activation_at"):
            activation_at = await self.bot.db.get_guild_profile_activation_at(guild_id, profile_key)
        snap_meta = self._activity_snapshot_meta.get(profile_key, {})
        activity_health = None
        if isinstance(poll_context, dict):
            cached_health = poll_context.get("activity_health")
            if isinstance(cached_health, dict) and profile_key in cached_health:
                activity_health = cached_health[profile_key]
        if activity_health is None and hasattr(self.bot.db, "get_profile_feed_source"):
            activity_health = await self.bot.db.get_profile_feed_source(profile_key, "activity")
            if isinstance(poll_context, dict):
                poll_context.setdefault("activity_health", {})[profile_key] = activity_health
        activity_recovery_uncertain = bool(
            (snap_meta.get("pages_read") or 1) > 1
            or snap_meta.get("had_prior_failure")
            or (
                snap_meta.get("had_unresolved_gap")
                and (
                    snap_meta.get("prior_boundary_id") is not None
                    or (activity_health and activity_health["last_error"] is not None)
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
            bulk_delivered = getattr(self.bot.db, "get_delivered_journal_entry_ids", None)
            if callable(bulk_delivered):
                baseline_or_saved_ids.update(
                    await bulk_delivered(guild_id, profile_key, missing_log_ids)
                )
            else:
                for log_id in missing_log_ids:
                    if await self.bot.db.is_journal_entry_delivered(guild_id, profile_key, log_id):
                        baseline_or_saved_ids.add(log_id)
            remaining_log_ids = [
                log_id for log_id in missing_log_ids if log_id not in baseline_or_saved_ids
            ]
            if remaining_log_ids:
                bulk_decisions = getattr(self.bot.db, "get_feed_event_decisions", None)
                if callable(bulk_decisions):
                    stored_decisions = await bulk_decisions(guild_id, profile_key, remaining_log_ids)
                else:
                    stored_decisions = {}
                    for log_id in remaining_log_ids:
                        stored = await self.bot.db.get_feed_event_decision(
                            guild_id, profile_key, log_id
                        )
                        if stored:
                            stored_decisions[log_id] = stored
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
        )
        if not deliver:
            # Keep validated observations durable for a known active recipient,
            # without touching Discord or rewriting archived messages. On recovery,
            # get_pending_feed_observations replays eligible work through policy.
            return False
        explicit_actions = [
            event for event in events
            if event.action_type in {"rated", "reviewed"}
        ]
        events = self._keep_old_posted_sessions_and_unlink_old_baselines(
            events, explicit_actions, saved, now
        )
        events = merge_ratings_and_reviews(events)
        events.sort(key=lambda e: e.occurred_at or calendar_date(e.played_date) or "")
        activity_rating_history: dict[str, list[tuple[datetime, float]]] = {}
        for previous in (*canonical_activity.values(), *events):
            if previous.source != "activity" or previous.rating is None:
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
            decision_getter = getattr(self.bot.db, "get_feed_event_decisions", None)
            journal_decisions = (
                await decision_getter(
                    guild_id,
                    profile_key,
                    [event.activity_id for event in journal_rating_events],
                )
                if callable(decision_getter)
                else {}
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
            bulk_delivered = getattr(self.bot.db, "get_delivered_journal_entry_ids", None)
            if callable(bulk_delivered):
                delivered_alias_ids = await bulk_delivered(guild_id, profile_key, alias_check_ids)
            else:
                for alias in alias_check_ids:
                    if await self.bot.db.is_journal_entry_delivered(guild_id, profile_key, alias):
                        delivered_alias_ids.add(alias)
        for event in events:
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
                continue
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
                continue
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
                        edit_channel, _, edit_issue = self._inspect_target_channel(
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
                        logger.warning(
                            "Could not edit feed message %s in prior channel %s for %s in guild %d (%s); keeping confirmed payload for retry",
                            message_id,
                            old_channel,
                            username,
                            guild_id,
                            edit_issue or "channel unavailable",
                        )
                        continue

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
                        continue
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
                        logger.warning(
                            "Could not edit feed message %s for %s in guild %d (status %s); keeping previous payload for retry",
                            message_id,
                            username,
                            guild_id,
                            getattr(error, "status", "rate_limited" if isinstance(error, discord.RateLimited) else type(error).__name__),
                        )
                        continue
                else:
                    if not await self.bot.db.begin_feed_delivery(
                        guild_id, profile_key, aliases, payload, channel.id
                    ):
                        continue
                    try:
                        message = await channel.send(embed=embed, view=view)
                    except asyncio.CancelledError:
                        raise
                    except discord.RateLimited as error:
                        await self.bot.db.release_feed_delivery(guild_id, profile_key, aliases)
                        target_ok = False
                        logger.warning(
                            "Rate limited sending feed event %s for %s in guild %d (retry_after=%.2fs); released pending intent",
                            event.activity_id,
                            username,
                            guild_id,
                            error.retry_after,
                        )
                        continue
                    except discord.HTTPException as error:
                        status = getattr(error, "status", None)
                        is_client_rejection = isinstance(status, int) and 400 <= status < 500
                        if is_client_rejection:
                            await self.bot.db.release_feed_delivery(guild_id, profile_key, aliases)
                        else:
                            await self.bot.db.mark_feed_delivery_unknown(guild_id, profile_key, aliases)
                        target_ok = False
                        logger.warning(
                            "Could not send feed event %s for %s in guild %d (status %s); %s",
                            event.activity_id,
                            username,
                            guild_id,
                            status if status is not None else "unknown",
                            "released pending intent" if is_client_rejection else "marked delivery unknown",
                        )
                        continue
                    except (aiohttp.ClientError, TimeoutError, ConnectionError) as error:
                        await self.bot.db.mark_feed_delivery_unknown(guild_id, profile_key, aliases)
                        target_ok = False
                        logger.warning(
                            "Uncertain transport failure sending feed event %s for %s in guild %d (%s); marked delivery unknown",
                            event.activity_id,
                            username,
                            guild_id,
                            error,
                        )
                        continue
                    except Exception:
                        await self.bot.db.mark_feed_delivery_unknown(guild_id, profile_key, aliases)
                        raise
                    message_id, old_channel = message.id, channel.id
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
    ):
        uncertain_reasons = uncertain_reasons or {}
        decision_priority = {
            "suppressed_pre_activation": 0,
            "suppressed_old": 1,
            "held_ambiguous": 2,
            "held_future": 3,
            "held_uncertain": 4,
            "baseline": 5,
            "eligible": 6,
        }
        event_entries = []
        unique_aliases = []
        seen_aliases = set()
        for event in events:
            aliases = tuple(dict.fromkeys((event.activity_id, *(event.event_ids or ()))))
            rows = [saved[alias] for alias in aliases if alias in saved]
            already_tracked = any(
                row["message_id"] is not None
                or dict(row).get("delivery_state") in {"sent", "deleted", "pending", "unknown"}
                for row in rows
            )
            event_entries.append((event, aliases, already_tracked))
            if not already_tracked:
                for alias in aliases:
                    if alias not in seen_aliases:
                        seen_aliases.add(alias)
                        unique_aliases.append(alias)

        decision_map = {}
        if unique_aliases:
            bulk_getter = getattr(self.bot.db, "get_feed_event_decisions", None)
            if callable(bulk_getter):
                decision_map = dict(await bulk_getter(guild_id, profile_key, unique_aliases))
            else:
                for alias in unique_aliases:
                    stored = await self.bot.db.get_feed_event_decision(guild_id, profile_key, alias)
                    if stored:
                        decision_map[alias] = stored

        bulk_persister = getattr(self.bot.db, "persist_feed_event_decisions", None)
        if not callable(bulk_persister):
            accepted = []
            for event, aliases, already_tracked in event_entries:
                if already_tracked:
                    accepted.append(event)
                    continue
                decisions = [(alias, decision_map[alias]) for alias in aliases if alias in decision_map]
                if decisions:
                    chosen_alias, chosen = min(
                        decisions, key=lambda item: decision_priority.get(item[1]["decision"], 99)
                    )
                    decision = chosen["decision"]
                    # New dated evidence can tighten an unsent decision. Use its
                    # original discovery time so retries do not age into suppression.
                    reason, event_date = chosen["reason"], chosen["event_date"]
                    if decision == "eligible":
                        previous_payload = json.loads(chosen["payload"])
                        if event.activity_id in uncertain_reasons:
                            decision, reason = "held_uncertain", uncertain_reasons[event.activity_id]
                        elif event.played_date and event.played_date != previous_payload.get("played_date"):
                            discovery_time = parse_utc_datetime(chosen["observed_at"]) or now
                            decision, reason, event_date = classify_feed_event(
                                event, discovery_time,
                                activation_at=activation_at if event.source == "activity" else None,
                                recovery_uncertain=recovery_uncertain if event.source == "activity" else False,
                            )
                    eligible_revisions = [
                        (alias, stored) for alias, stored in decisions if stored["decision"] == "eligible"
                    ]
                    missing_decided_aliases = (
                        [alias for alias in aliases if alias not in {a for a, _ in decisions}]
                        if len(decisions) < len(aliases)
                        else []
                    )
                    if eligible_revisions or missing_decided_aliases:
                        payload = json.dumps(asdict(event), sort_keys=True)
                        for alias, stored in eligible_revisions:
                            await self.bot.db.revise_unsent_eligible_feed_decision(
                                guild_id, profile_key, alias, decision=decision,
                                reason=reason, event_date=event_date, payload=payload,
                            )
                            decision_map[alias] = {
                                "source": stored["source"],
                                "event_date": event_date,
                                "decision": decision,
                                "reason": reason,
                                "payload": payload,
                                "observed_at": stored["observed_at"],
                            }
                        for alias in missing_decided_aliases:
                            alias_source = "journal" if str(alias).startswith("journal_") else event.source
                            recorded = await self.bot.db.record_feed_event_decision(
                                guild_id, profile_key, alias, source=alias_source,
                                event_date=event_date, decision=decision,
                                reason=reason, payload=payload, observed_at=chosen["observed_at"],
                            )
                            if recorded is not None:
                                decision_map[alias] = recorded
                else:
                    decision, reason, event_date = classify_feed_event(
                        event,
                        now,
                        activation_at=activation_at if event.source == "activity" else None,
                        recovery_uncertain=recovery_uncertain if event.source == "activity" else False,
                    )
                    if event.activity_id in uncertain_reasons and decision != "suppressed_pre_activation":
                        decision = "held_uncertain"
                        reason = uncertain_reasons[event.activity_id]
                    payload = json.dumps(asdict(event), sort_keys=True)
                    stored = await self.bot.db.record_feed_event_decision(
                        guild_id, profile_key, event.activity_id, source=event.source,
                        event_date=event_date, decision=decision, reason=reason, payload=payload,
                        observed_at=now,
                    )
                    if stored is not None:
                        decision_map[event.activity_id] = stored
                    decision = stored["decision"]
                    for alias in aliases:
                        if alias != event.activity_id:
                            alias_source = "journal" if str(alias).startswith("journal_") else event.source
                            recorded_alias = await self.bot.db.record_feed_event_decision(
                                guild_id, profile_key, alias, source=alias_source,
                                event_date=event_date, decision=decision, reason=reason, payload=payload,
                                observed_at=now,
                            )
                            if recorded_alias is not None:
                                decision_map[alias] = recorded_alias
                if decision in {"eligible", "baseline"}:
                    accepted.append(event)
            return accepted

        initially_stored = set(decision_map.keys())
        planned_inserts: dict[str, dict] = {}
        planned_revisions: dict[str, dict] = {}
        event_outcomes = []
        for event, aliases, already_tracked in event_entries:
            if already_tracked:
                event_outcomes.append((event, aliases, True, "eligible"))
                continue
            decisions = [(alias, decision_map[alias]) for alias in aliases if alias in decision_map]
            if decisions:
                chosen_alias, chosen = min(
                    decisions, key=lambda item: decision_priority.get(item[1]["decision"], 99)
                )
                decision = chosen["decision"]
                # New dated evidence can tighten an unsent decision. Use its
                # original discovery time so retries do not age into suppression.
                reason, event_date = chosen["reason"], chosen["event_date"]
                if decision == "eligible":
                    previous_payload = json.loads(chosen["payload"])
                    if event.activity_id in uncertain_reasons:
                        decision, reason = "held_uncertain", uncertain_reasons[event.activity_id]
                    elif event.played_date and event.played_date != previous_payload.get("played_date"):
                        discovery_time = parse_utc_datetime(chosen["observed_at"]) or now
                        decision, reason, event_date = classify_feed_event(
                            event, discovery_time,
                            activation_at=activation_at if event.source == "activity" else None,
                            recovery_uncertain=recovery_uncertain if event.source == "activity" else False,
                        )
                eligible_revisions = [
                    (alias, stored) for alias, stored in decisions if stored["decision"] == "eligible"
                ]
                missing_decided_aliases = (
                    [alias for alias in aliases if alias not in {a for a, _ in decisions}]
                    if len(decisions) < len(aliases)
                    else []
                )
                if eligible_revisions or missing_decided_aliases:
                    payload = json.dumps(asdict(event), sort_keys=True)
                    for alias, stored in eligible_revisions:
                        decision_map[alias] = {
                            "source": stored["source"],
                            "event_date": event_date,
                            "decision": decision,
                            "reason": reason,
                            "payload": payload,
                            "observed_at": stored["observed_at"],
                        }
                        if alias in initially_stored:
                            planned_revisions[alias] = {
                                "event_id": alias,
                                "decision": decision,
                                "reason": reason,
                                "event_date": event_date,
                                "payload": payload,
                            }
                        elif alias in planned_inserts:
                            planned_inserts[alias]["decision"] = decision
                            planned_inserts[alias]["reason"] = reason
                            planned_inserts[alias]["event_date"] = event_date
                            planned_inserts[alias]["payload"] = payload
                    for alias in missing_decided_aliases:
                        alias_source = "journal" if str(alias).startswith("journal_") else event.source
                        item = {
                            "event_id": alias,
                            "source": alias_source,
                            "event_date": event_date,
                            "decision": decision,
                            "reason": reason,
                            "payload": payload,
                            "observed_at": chosen["observed_at"],
                        }
                        planned_inserts[alias] = item
                        decision_map[alias] = item
            else:
                decision, reason, event_date = classify_feed_event(
                    event,
                    now,
                    activation_at=activation_at if event.source == "activity" else None,
                    recovery_uncertain=recovery_uncertain if event.source == "activity" else False,
                )
                if event.activity_id in uncertain_reasons and decision != "suppressed_pre_activation":
                    decision = "held_uncertain"
                    reason = uncertain_reasons[event.activity_id]
                payload = json.dumps(asdict(event), sort_keys=True)
                for alias in aliases:
                    alias_source = (
                        event.source
                        if alias == event.activity_id
                        else ("journal" if str(alias).startswith("journal_") else event.source)
                    )
                    item = {
                        "event_id": alias,
                        "source": alias_source,
                        "event_date": event_date,
                        "decision": decision,
                        "reason": reason,
                        "payload": payload,
                        "observed_at": now,
                    }
                    planned_inserts[alias] = item
                    decision_map[alias] = item
            event_outcomes.append((event, aliases, False, decision))

        authoritative = {}
        if planned_inserts or planned_revisions:
            authoritative = await bulk_persister(
                guild_id,
                profile_key,
                inserts=list(planned_inserts.values()),
                revisions=list(planned_revisions.values()),
                alias_groups=[aliases for _, aliases, tracked in event_entries if not tracked],
            )

        accepted = []
        for event, aliases, already_tracked, local_decision in event_outcomes:
            if already_tracked:
                accepted.append(event)
                continue
            final_decision = self._final_alias_decision(
                aliases,
                authoritative,
                decision_map,
                fallback=local_decision,
            )
            if final_decision in {"eligible", "baseline"}:
                accepted.append(event)
        return accepted

    @staticmethod
    def _final_alias_decision(aliases, authoritative, request_decisions, *, fallback=None):
        """Choose the strictest persisted alias decision, using request state for missing rows."""
        priority = {
            "suppressed_pre_activation": 0,
            "suppressed_old": 1,
            "held_ambiguous": 2,
            "held_future": 3,
            "held_uncertain": 4,
            "baseline": 5,
            "eligible": 6,
        }
        decisions = []
        for alias in aliases:
            row = authoritative.get(alias)
            if row is None:
                row = request_decisions.get(alias)
            if row is not None:
                decisions.append(row["decision"])
        if not decisions:
            return fallback
        return min(decisions, key=lambda decision: priority.get(decision, 99))

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
            persist_identity = getattr(self.bot.db, "persist_feed_identity_decisions", None)
            if callable(persist_identity):
                await persist_identity(guild_id, profile_key, operations=identity_operations)
            else:
                # Retain compatibility with lightweight DB adapters while the
                # published Database path uses one bounded transaction.
                for operation in identity_operations:
                    if operation["kind"] == "resolve":
                        await self.bot.db.resolve_feed_event_decision(
                            guild_id, profile_key, operation["event_id"],
                            operation["payload"], operation.get("event_date"),
                        )
                    else:
                        await self.bot.db.record_feed_event_decision(
                            guild_id, profile_key, operation["event_id"],
                            source=operation["source"], event_date=operation["event_date"],
                            decision="held_ambiguous", reason=operation["reason"],
                            payload=operation["payload"], observed_at=operation["observed_at"],
                        )
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

    @staticmethod
    def _format_rel_timestamp(value) -> str:
        dt = parse_sync_timestamp(value)
        if dt is None:
            return "Never"
        return f"<t:{int(dt.timestamp())}:R>"

    @classmethod
    def _describe_profile_feed_state(
        cls,
        *,
        channel_id,
        status: dict,
        is_candidate: bool,
        target_issue: str | None = None,
    ) -> str:
        if channel_id is None:
            return "Pending channel"
        if not is_candidate:
            return "Inactive"
        if target_issue:
            return f"Unavailable ({target_issue})"
        health_rows = status.get("source_health") or []
        has_error = any(row["last_error"] for row in health_rows)
        has_gap = any(
            row["unresolved_gap_since"] is not None
            or row["coverage"] in {"limited", "unsupported"}
            for row in health_rows
        )
        initialized = status.get("initialized_sources") or []
        if not status.get("active") or not initialized:
            return "Degraded (pending baseline)" if has_error else "Pending baseline"
        if len(initialized) < 2:
            return "Degraded (partially checked)" if has_error else "Partially checked"
        if has_error:
            return "Degraded"
        if has_gap:
            return "Active (incomplete coverage)"
        return "Active"

    @app_commands.command(name="setchannel", description="Set the channel where automatic Backloggd playthrough logs are posted")
    @app_commands.describe(channel="The text channel for game log announcements")
    @app_commands.guild_only()
    @app_commands.checks.has_permissions(manage_channels=True)
    async def setchannel(self, interaction: discord.Interaction, channel: discord.TextChannel):
        if interaction.guild is None:
            await interaction.response.send_message("❌ Choose a text channel in a server to set up the activity feed.", ephemeral=True)
            return

        await defer_interaction(interaction, ephemeral=True)

        me = getattr(interaction.guild, "me", None)
        if me is None:
            await send_private_followup(
                interaction,
                "❌ I could not verify my permissions in that channel right now because Discord's member cache is unavailable. Please try `/setchannel` again shortly.",
                log=logger,
            )
            return

        permissions = channel.permissions_for(me)
        missing = [
            name for attribute, name in (
                ("view_channel", "View Channel"),
                ("send_messages", "Send Messages"),
                ("embed_links", "Embed Links"),
            )
            if not getattr(permissions, attribute, False)
        ]
        if missing:
            await send_private_followup(
                interaction,
                "❌ I need **" + "**, **".join(missing) + "** permission in that channel before I can post activity.",
                log=logger,
            )
            return

        await self.bot.db.set_feed_channel(interaction.guild_id, channel.id)
        await self._reconcile_legacy_guild(interaction.guild)
        member_ids = await self._authoritative_member_ids(interaction.guild)
        if member_ids is not None:
            linked_profiles = await self.bot.db.get_guild_feed_candidates(
                interaction.guild_id, member_ids
            )
            await self.bot.db.reconcile_guild_feed_activity(
                interaction.guild_id,
                [row["profile_key"] for row in linked_profiles],
                now=self.clock(),
                present_member_ids=member_ids,
            )
            note = (
                "Each active profile source will initialize on its first successful poll and "
                "skip existing history so only future activity is announced."
            )
        else:
            note = (
                "Discord membership could not be verified right now; active profiles will "
                "enroll and initialize on their first successful poll (skipping existing history, "
                "so only future activity is announced) once a complete member snapshot is available."
            )

        await send_private_followup(
            interaction,
            f"✅ Set {channel.mention} as the Backloggd activity feed channel. {note}",
            log=logger,
        )

    @app_commands.command(name="unsetchannel", description="Disable the automatic Backloggd activity feed for this server")
    @app_commands.guild_only()
    @app_commands.checks.has_permissions(manage_channels=True)
    async def unsetchannel(self, interaction: discord.Interaction):
        if interaction.guild is None:
            await interaction.response.send_message("This command can only be used in a server.", ephemeral=True)
            return
        await defer_interaction(interaction, ephemeral=True)
        removed = await self.bot.db.unset_feed_channel(interaction.guild_id)
        if removed:
            await send_private_followup(
                interaction,
                "✅ Automatic activity feed has been disabled for this server.",
                log=logger,
            )
        else:
            await send_private_followup(
                interaction,
                "ℹ️ No activity feed channel was currently set for this server.",
                log=logger,
            )

    @feed.command(name="on", description="Include your linked Backloggd profile in this server's feed")
    async def feed_on(self, interaction: discord.Interaction):
        if interaction.guild is None:
            await interaction.response.send_message("Use `/feed on` in each server where you want activity announcements.", ephemeral=True)
            return
        await defer_interaction(interaction, ephemeral=True)
        linked = await self.bot.db.get_linked_user(interaction.user.id)
        if not linked:
            await send_private_followup(
                interaction,
                "Link your Backloggd profile with `/link` first. This command does not ask for or change your username.",
                log=logger,
            )
            return
        member_ids = await self._authoritative_member_ids(interaction.guild)
        await self.bot.db.set_guild_feed_preference(
            interaction.guild_id, interaction.user.id, True, member_ids=member_ids,
        )
        status = await self.bot.db.get_guild_profile_feed_status(
            interaction.guild_id, linked["profile_key"],
        )
        channel_id = await self.bot.db.get_feed_channel(interaction.guild_id)
        message = f"Your linked profile **{escape_display_text(linked['backloggd_username'])}** is enabled for this server."
        if channel_id is None:
            message += " The feed is pending until an admin runs `/setchannel`; no Discord delivery can happen before a channel is configured."
        if member_ids is None:
            message += " No Discord delivery will happen until a complete member snapshot is available."
        message += " " + self._feed_enrollment_state_copy(status)
        await send_private_followup(interaction, message, log=logger)

    @feed.command(name="off", description="Stop your linked profile from participating in this server's feed")
    async def feed_off(self, interaction: discord.Interaction):
        if interaction.guild is None:
            await interaction.response.send_message("This command can only be used in a server.", ephemeral=True)
            return
        await defer_interaction(interaction, ephemeral=True)
        linked = await self.bot.db.get_linked_user(interaction.user.id)
        if not linked:
            await send_private_followup(
                interaction,
                "You do not have a linked Backloggd profile. Your server preference is unchanged.",
                log=logger,
            )
            return
        member_ids = await self._authoritative_member_ids(interaction.guild)
        result = await self.bot.db.disable_guild_feed_preference_and_reconcile(
            interaction.guild_id,
            interaction.user.id,
            member_ids=member_ids,
        )
        if result is None:
            await send_private_followup(
                interaction,
                "You do not have a linked Backloggd profile. Your server preference is unchanged.",
                log=logger,
            )
            return
        if result["deactivated"]:
            message = "Your participation is off. Existing announcements stay archived. Use `/feed on` or `/link` in this server to participate again."
        elif result["pending_reconciliation"]:
            message = "Your opt-out is saved. I could not verify whether another current member keeps this profile active."
        elif result["has_admin"] and not result["remaining_members"]:
            message = "Your participation is off, but this profile remains in this server's feed through an admin follow."
        else:
            message = "Your participation is off, but this profile remains in this server's feed through another linked member."
        await send_private_followup(interaction, message, log=logger)

    @feed.command(name="status", description="Show your profile's feed participation in this server")
    async def feed_status(self, interaction: discord.Interaction):
        if interaction.guild is None:
            await interaction.response.send_message("This command can only be used in a server.", ephemeral=True)
            return
        await defer_interaction(interaction, ephemeral=True)
        linked = await self.bot.db.get_linked_user(interaction.user.id)
        if not linked:
            await send_private_followup(
                interaction,
                "No Backloggd profile is linked to your Discord account. Use `/link` first.",
                log=logger,
            )
            return
        profile_key = linked["profile_key"]
        username = linked["backloggd_username"]
        enabled = await self.bot.db.get_guild_feed_preference(interaction.guild_id, interaction.user.id)
        channel_id = await self.bot.db.get_feed_channel(interaction.guild_id)
        member_ids = await self._authoritative_member_ids(interaction.guild)
        sources = (
            await self.bot.db.get_guild_profile_member_sources(
                interaction.guild_id, profile_key, member_ids
            ) if member_ids is not None else []
        )
        admin_source = await self.bot.db.has_guild_admin_follow(
            interaction.guild_id, profile_key
        )
        status = await self.bot.db.get_guild_profile_feed_status(interaction.guild_id, profile_key)
        is_candidate = bool(sources or admin_source) if member_ids is not None else bool(enabled or admin_source)
        _, _, target_issue = self._inspect_target_channel(
            interaction.guild_id,
            channel_id,
            guild=interaction.guild,
            require_channel_lookup=False,
        )
        effective_state = self._describe_profile_feed_state(
            channel_id=channel_id,
            status=status,
            is_candidate=is_candidate,
            target_issue=target_issue,
        )

        if channel_id is None:
            channel_text = "Not configured"
        elif target_issue:
            channel_text = f"<#{channel_id}> (Unavailable — {target_issue})"
        else:
            channel_text = f"<#{channel_id}>"
        if sources and admin_source:
            source_summary = "Member + Admin follow"
        elif sources:
            source_summary = "Member (you)" if enabled else "Another linked member"
        elif admin_source:
            source_summary = "Admin follow"
        else:
            source_summary = "None"

        desc_lines = [
            f"**Profile:** [{escape_display_text(username)}]({build_backloggd_user_url(username)})",
            f"**Your participation:** {'On' if enabled else 'Off'}",
            f"**In this server's feed:** {'Yes' if is_candidate else 'No'} ({source_summary})",
            f"**Feed channel:** {channel_text}",
            f"**Effective state:** {effective_state}",
        ]
        if member_ids is None:
            desc_lines.append("⚠️ Current member participation could not be verified from Discord's member cache.")
        elif admin_source and not sources and not enabled:
            desc_lines.append("An admin follow keeps this public profile active in this server's feed.")
        elif sources and not enabled:
            desc_lines.append("Another linked server member keeps this public profile active in this server's feed.")

        embed = discord.Embed(
            title="Server Feed Status",
            description="\n".join(desc_lines),
            color=discord.Color.blurple(),
        )

        initialized = set(status.get("initialized_sources") or [])
        health_by_source = {row["source"]: row for row in (status.get("source_health") or [])}
        source_lines = []
        for source_name, display_name in (("activity", "Activity"), ("journal", "Journal")):
            marker = f"{source_name}-display-v1"
            baselined = "Initialized" if marker in initialized else "Pending baseline"
            row = health_by_source.get(source_name)
            if not row:
                source_lines.append(f"**{display_name}:** {baselined} · Last check: Never")
                continue
            last_ok = self._format_rel_timestamp(row["last_success_at"])
            cov = str(row["coverage"] or "unknown").replace("`", "")
            line = f"**{display_name}:** {baselined} · Last OK: {last_ok} · Coverage: `{cov}`"
            health_keys = row.keys()
            gap_since_raw = row["unresolved_gap_since"]
            gap_until_raw = (
                row["unresolved_gap_until"]
                if "unresolved_gap_until" in health_keys else None
            )
            gap_since_at = parse_sync_timestamp(gap_since_raw)
            gap_until_at = parse_sync_timestamp(gap_until_raw)
            gap_reason = row["gap_reason"] if "gap_reason" in health_keys else None
            last_boundary_at = (
                row["last_continuous_boundary_at"]
                if "last_continuous_boundary_at" in health_keys else None
            )
            if gap_since_at and gap_until_at and gap_until_at < gap_since_at:
                line += " · Gap bounds conflict; extent unknown"
            elif gap_since_raw:
                gap_label = {
                    "unavailable_feed_target": "Recipient gap",
                    "source_failure": "Continuity gap",
                }.get(gap_reason, "Coverage gap")
                gap_since = self._format_rel_timestamp(gap_since_raw)
                gap_until = self._format_rel_timestamp(gap_until_raw) if gap_until_raw else "present"
                line += f" · {gap_label}: {gap_since} \u2192 {gap_until}"
            elif gap_reason or cov in {"limited", "unsupported"}:
                line += " · Coverage extent unknown (no continuous boundary)"
            if (
                cov in {"limited", "unsupported"}
                and last_boundary_at is None
                and gap_since_raw
            ):
                line += " · Source extent unknown (no continuous boundary)"
            if row["last_error"]:
                err = format_health_error_excerpt(row["last_error"], max_length=120)
                line += f" · Error: {err}"
            source_lines.append(line)
        embed.add_field(
            name="Source Health & Coverage",
            value=clamp_escaped_text("\n".join(source_lines), 1024),
            inline=False,
        )

        suppressions = status.get("suppression_counts") or {}
        deliveries = status.get("delivery_counts") or {}
        policy_parts = [
            f"Old (>7d): {suppressions.get('suppressed_old', 0)}",
            f"Pre-activation: {suppressions.get('suppressed_pre_activation', 0)}",
            f"Uncertain time: {suppressions.get('held_uncertain', 0)}",
            f"Future-dated: {suppressions.get('held_future', 0)}",
            f"Ambiguous edits: {suppressions.get('held_ambiguous', 0)}",
        ]
        unknown_deliveries = deliveries.get("unknown", 0)
        pending_deliveries = deliveries.get("pending", 0)
        if unknown_deliveries or pending_deliveries:
            policy_parts.append(f"Unknown send outcomes: {unknown_deliveries}")
            policy_parts.append(f"Pending sends: {pending_deliveries}")
        embed.add_field(
            name="Suppressed & Held Items",
            value=clamp_escaped_text(" · ".join(policy_parts), 1024),
            inline=False,
        )
        embed.set_footer(text="Feed source health is separate from library cache freshness (/profile, /refresh).")
        await send_private_followup(interaction, embed=embed, log=logger)

    def _feed_list_pages(self, candidates, channel_id):
        pages = []
        page_size = 20
        for start in range(0, len(candidates), page_size):
            embed = discord.Embed(
                title="Backloggd feed profiles",
                color=discord.Color.blurple(),
            )
            lines = []
            for candidate in candidates[start:start + page_size]:
                username = clamp_escaped_text(escape_display_text(candidate["display_username"]), 120)
                source = candidate["sources"].title()
                state = candidate.get("feed_state", "Pending baseline")
                lines.append(f"**{username}** — {source} · {state}")
            embed.description = clamp_escaped_text("\n".join(lines), 4000)
            embed.set_footer(text=f"{start + 1}–{min(start + page_size, len(candidates))} of {len(candidates)} profiles")
            pages.append(embed)
        return pages

    @feed.command(name="add", description="Follow a public Backloggd profile in this server's feed")
    @app_commands.describe(username="A public Backloggd username")
    @app_commands.guild_only()
    @app_commands.checks.has_permissions(manage_channels=True)
    async def feed_add(self, interaction: discord.Interaction, username: str):
        if interaction.guild is None:
            await interaction.response.send_message("This command can only be used in a server.", ephemeral=True)
            return
        await defer_interaction(interaction, ephemeral=True)
        try:
            requested = normalize_backloggd_username(username)
        except InvalidUsernameError as error:
            await send_private_followup(interaction, f"❌ {error}", log=logger)
            return
        safe_requested = escape_display_text(requested)
        try:
            profile = await self.bot.backloggd.get_user_profile(requested)
        except (UserNotFoundError, ResourceNotFoundError):
            await send_private_followup(
                interaction,
                f"No public Backloggd profile was found for **{safe_requested}**.",
                log=logger,
            )
            return
        except Exception as error:
            logger.warning("Could not validate feed profile %s: %s", requested, error)
            await send_private_followup(
                interaction,
                "Backloggd is temporarily unavailable. No feed subscription was added; try again shortly.",
                log=logger,
            )
            return
        if not profile or not profile.username:
            await send_private_followup(
                interaction,
                f"No public Backloggd profile was found for **{safe_requested}**.",
                log=logger,
            )
            return
        member_ids = await self._authoritative_member_ids(interaction.guild)
        created = await self.bot.db.add_guild_admin_follow(
            interaction.guild_id,
            profile.username,
            profile.avatar_url,
            interaction.user.id,
            member_ids=member_ids,
        )
        channel_id = await self.bot.db.get_feed_channel(interaction.guild_id)
        if member_ids is not None and channel_id is not None:
            candidates = await self.bot.db.get_guild_feed_candidates(interaction.guild_id, member_ids)
            await self.bot.db.reconcile_guild_feed_activity(
                interaction.guild_id,
                [row["profile_key"] for row in candidates],
                now=self.clock(),
                present_member_ids=member_ids,
            )
        status = await self.bot.db.get_guild_profile_feed_status(
            interaction.guild_id, profile.username.lower(),
        )
        safe_profile = escape_display_text(profile.username)
        if not created:
            message = f"**{safe_profile}** is already followed in this server."
        else:
            message = f"Added **{safe_profile}** to this server's activity feed."
        if channel_id is None:
            message += " The follow is pending until an admin runs `/setchannel`; no Discord delivery can happen before a channel is configured."
        if member_ids is None:
            message += " No Discord delivery will happen until Discord provides a complete member snapshot."
        message += " " + self._feed_enrollment_state_copy(status)
        await send_private_followup(interaction, message, log=logger)

    @feed.command(name="remove", description="Remove a public profile follow from this server's feed")
    @app_commands.describe(username="The Backloggd username to stop following")
    @app_commands.guild_only()
    @app_commands.checks.has_permissions(manage_channels=True)
    async def feed_remove(self, interaction: discord.Interaction, username: str):
        if interaction.guild is None:
            await interaction.response.send_message("This command can only be used in a server.", ephemeral=True)
            return
        await defer_interaction(interaction, ephemeral=True)
        try:
            requested = normalize_backloggd_username(username)
        except InvalidUsernameError as error:
            await send_private_followup(interaction, f"❌ {error}", log=logger)
            return
        safe_requested = escape_display_text(requested)
        if not await self.bot.db.has_guild_admin_follow(interaction.guild_id, requested):
            await send_private_followup(
                interaction,
                f"**{safe_requested}** does not have an admin follow in this server.",
                log=logger,
            )
            return
        member_ids = await self._authoritative_member_ids(interaction.guild)
        result = await self.bot.db.remove_guild_admin_follow_and_reconcile(
            interaction.guild_id, requested, member_ids=member_ids
        )
        if not result["removed"]:
            await send_private_followup(
                interaction,
                f"**{safe_requested}** does not have an admin follow in this server.",
                log=logger,
            )
            return
        message = f"Removed the admin follow for **{safe_requested}**."
        if result["pending_reconciliation"]:
            message += " Current member participation could not be verified."
        elif not result["deactivated"] and result["remaining_members"]:
            message += " The profile remains active through member participation."
        else:
            message += " Existing announcements remain archived; a later re-add will skip inactive history."
        await send_private_followup(interaction, message, log=logger)

    @feed.command(name="list", description="List profiles included in this server's activity feed")
    @app_commands.guild_only()
    @app_commands.checks.has_permissions(manage_channels=True)
    async def feed_list(self, interaction: discord.Interaction):
        if interaction.guild is None:
            await interaction.response.send_message("This command can only be used in a server.", ephemeral=True)
            return
        await defer_interaction(interaction, ephemeral=True)
        member_ids = await self._authoritative_member_ids(interaction.guild)
        if member_ids is None:
            await send_private_followup(
                interaction,
                "Discord has not provided a complete member snapshot yet. Try again shortly.",
                log=logger,
            )
            return
        candidates = await self.bot.db.get_guild_feed_candidates(interaction.guild_id, member_ids)
        if not candidates:
            await send_private_followup(
                interaction,
                "No member profiles are enabled and no admin profiles are followed in this server.",
                log=logger,
            )
            return
        channel_id = await self.bot.db.get_feed_channel(interaction.guild_id)
        _, _, target_issue = self._inspect_target_channel(
            interaction.guild_id,
            channel_id,
            guild=interaction.guild,
            require_channel_lookup=False,
        )
        statuses = await self.bot.db.get_guild_feed_statuses(
            interaction.guild_id,
            [candidate["profile_key"] for candidate in candidates],
        )
        for candidate in candidates:
            status = statuses.get(candidate["profile_key"], {})
            candidate["feed_state"] = self._describe_profile_feed_state(
                channel_id=channel_id,
                status=status,
                is_candidate=True,
                target_issue=target_issue,
            )
        pages = self._feed_list_pages(candidates, channel_id)
        view = FeedListView(interaction.user.id, pages)
        message = await interaction.followup.send(
            embed=pages[0], view=view, ephemeral=True, wait=True
        )
        view.message = message

    @setchannel.error
    @unsetchannel.error
    @feed_add.error
    @feed_remove.error
    @feed_list.error
    async def channel_command_error(self, interaction: discord.Interaction, error: app_commands.AppCommandError):
        if isinstance(error, app_commands.MissingPermissions):
            reply = "❌ You need the **Manage Channels** permission to configure or manage the server activity feed."
        elif isinstance(error, app_commands.NoPrivateMessage):
            reply = "This command can only be used in a server."
        else:
            return
        mark_interaction_error_handled(interaction)
        await send_ephemeral_error(interaction, reply, log=logger)

    # =========================================================================
    # Background Polling Task Loop (Filmlinkd-style)
    # =========================================================================

    @staticmethod
    def _supports_kwarg(fn, name: str) -> bool:
        try:
            params = inspect.signature(fn).parameters
        except (TypeError, ValueError):
            return False
        if name in params:
            return params[name].kind != inspect.Parameter.POSITIONAL_ONLY
        return any(p.kind == inspect.Parameter.VAR_KEYWORD for p in params.values())

    @tasks.loop(minutes=5.0)
    async def journal_feed_loop(self):
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
                channel, reason_key, reason_note = self._inspect_target_channel(
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
                await self._reconcile_legacy_guild(guild, pending_guild_ids=pending_legacy_guilds)
                member_ids = await self._authoritative_member_ids(guild)
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
                    fetch_options = {}
                    if self._supports_kwarg(self._fetch_feed_sources, "defer_boundary_commit"):
                        fetch_options["defer_boundary_commit"] = True
                    if self._supports_kwarg(self._fetch_feed_sources, "poll_context"):
                        fetch_options["poll_context"] = poll_context
                    sources = await self._fetch_feed_sources(
                        username, **fetch_options
                    )
                    meta = self._activity_snapshot_meta.get(profile_key)
                    health_now = meta.get("observed_at") if meta else None
                    if health_now is None:
                        health_now = self.clock()
                    await self._observe_profile_activity(profile_key, username, sources)
                    all_guilds_ok = bool(active_targets) and not skipped
                    if skipped:
                        await self.bot.db.record_feed_target_gap(
                            profile_key, now=health_now,
                        )
                    for guild_id, target_username, target_avatar, channel in [*active_targets, *skipped]:
                        try:
                            delivery_options = {'deliver': False} if channel is None else {}
                            if self._supports_kwarg(self._publish_sources, "poll_context"):
                                delivery_options["poll_context"] = poll_context
                            published_ok = await self._publish_sources(
                                guild_id,
                                profile_key,
                                target_username,
                                target_avatar,
                                channel,
                                sources,
                                **delivery_options,
                            )
                            if published_ok is False:
                                all_guilds_ok = False
                        except Exception as e:
                            all_guilds_ok = False
                            logger.warning("Error publishing activity for %s in guild %d: %s", target_username, guild_id, e)
                    if all_guilds_ok and meta and "activity" in sources and hasattr(self.bot.db, "commit_feed_source_boundary"):
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

    @journal_feed_loop.before_loop
    async def before_feed_loop(self):
        """Wait until bot is fully logged in before running the background feed."""
        try:
            await self.bot.wait_until_ready()
        except RuntimeError:
            # Raised if testing offline without bot.start()
            pass

async def setup(bot):
    await bot.add_cog(FeedCog(bot))
