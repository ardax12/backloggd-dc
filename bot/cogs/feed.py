"""FeedCog: Handles /setchannel, /unsetchannel, and the periodic playthrough journal feed loop."""

import discord
from discord import app_commands
from discord.ext import commands, tasks
import hashlib
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
from bot.ui.helpers import build_backloggd_game_url, build_backloggd_user_url
from bot.feed_models import FeedEvent

logger = logging.getLogger("backloggd.cogs.feed")

class FeedCog(commands.Cog, name="Feed"):
    feed = app_commands.Group(name="feed", description="Manage your server's Backloggd activity feed")

    def __init__(self, bot, *, clock=None):
        self.bot = bot
        self.clock = clock or (lambda: datetime.now(timezone.utc))
        self._activity_snapshot_meta = {}
        self._unavailable_target_reasons: dict[int, str] = {}

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
        if not self.journal_feed_loop.is_running():
            self.journal_feed_loop.start()

    def cog_unload(self):
        if self.journal_feed_loop.is_running():
            self.journal_feed_loop.cancel()

    async def _authoritative_member_ids(self, guild):
        """Return a complete cached member snapshot, or None when unavailable."""
        return await resolve_guild_member_ids(self.bot, guild)

    async def _reconcile_legacy_guild(self, guild):
        guild_id = getattr(guild, "id", None)
        if guild_id is None or guild_id not in await self.bot.db.get_legacy_feed_guilds_pending():
            return False
        member_ids = await self._authoritative_member_ids(guild)
        if member_ids is None:
            return False
        return await self.bot.db.complete_legacy_feed_migration(guild_id, member_ids)

    async def reconcile_legacy_enrollments(self):
        """Apply the one-time legacy link/guild intersection after readiness."""
        for guild_id in await self.bot.db.get_legacy_feed_guilds_pending():
            guild = self.bot.get_guild(guild_id)
            if guild is not None:
                await self._reconcile_legacy_guild(guild)

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
        for source_name, events in sources.items():
            observations = [
                (event.activity_id, self._observation_fingerprint(event))
                for event in events
                if self._is_library_relevant_event(event)
            ]
            result = await recorder(profile_key, source_name, observations)
            total_changed += int(result.get("changed", 0))
        sub_checker = getattr(self.bot.db, "is_profile_library_subscribed", None)
        is_subscribed = await sub_checker(profile_key) if sub_checker is not None else True
        pending_setter = getattr(self.bot.db, "set_profile_refresh_pending", None)
        pending_getter = getattr(self.bot.db, "get_profile_refresh_pending_token", None)
        pending_clearer = getattr(self.bot.db, "clear_profile_refresh_pending", None)
        has_pending = (
            (await pending_getter(profile_key)) is not None
            if pending_getter is not None else False
        )
        if total_changed > 0:
            if is_subscribed and pending_setter is not None:
                await pending_setter(profile_key, uuid.uuid4().hex)
        else:
            if not is_subscribed:
                if has_pending and pending_clearer is not None:
                    await pending_clearer(profile_key)
                return False
            if not has_pending:
                return False
        await schedule_profile_refresh(
            self.bot,
            username,
            dirty=total_changed > 0,
            now=self.clock(),
            require_subscribed=True,
        )
        return True

    async def _fetch_feed_sources(
        self, username: str, *, defer_boundary_commit: bool = False
    ) -> dict[str, list[FeedEvent]]:
        """Read sources independently; a blocked page is never an empty snapshot."""
        sources = {}
        profile_key = username.strip().lower()
        database = getattr(self.bot, "db", None)
        self._activity_snapshot_meta.pop(profile_key, None)
        try:
            health = None
            if database and hasattr(database, "record_feed_source_attempt"):
                await database.record_feed_source_attempt(profile_key, "activity")
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
            gap_until = None
            if coverage in {"limited", "unsupported"}:
                entry_times = [parse_utc_datetime(item.occurred_at) for item in parsed]
                valid_times = [dt for dt in entry_times if dt is not None]
                gap_until = min(valid_times) if valid_times else self.clock()
            sources["activity"] = parsed
            self._activity_snapshot_meta[profile_key] = {
                "coverage": coverage,
                "pages_read": pages_read,
                "prior_boundary_id": prior_boundary,
                "boundary_event_id": boundary_event_id,
                "had_unresolved_gap": had_unresolved_gap,
                "had_prior_failure": had_prior_failure,
            }
            if database and hasattr(database, "record_feed_source_success"):
                await database.record_feed_source_success(
                    profile_key,
                    "activity",
                    coverage=coverage,
                    pages_read=pages_read,
                    boundary_event_id=boundary_event_id,
                    defer_boundary_commit=defer_boundary_commit,
                    gap_until=gap_until,
                )
        except Exception as error:
            logger.warning("Could not read activity for %s: %s", username, error)
            if database and hasattr(database, "record_feed_source_failure"):
                await database.record_feed_source_failure(profile_key, "activity", str(error))
        try:
            if database and hasattr(database, "record_feed_source_attempt"):
                await database.record_feed_source_attempt(profile_key, "journal")
            entries = await self.bot.backloggd.get_user_journal(
                username, max_pages=100, require_complete=True
            )
            sources["journal"] = journal_events(entries, username)
            if database and hasattr(database, "record_feed_source_success"):
                await database.record_feed_source_success(
                    profile_key, "journal", coverage="exhausted", pages_read=None,
                )
        except Exception as error:
            logger.warning("Could not read journal for %s: %s", username, error)
            if database and hasattr(database, "record_feed_source_failure"):
                await database.record_feed_source_failure(profile_key, "journal", str(error))
        return sources

    async def _publish_sources(
        self, guild_id, profile_key, username, avatar_url, channel, sources,
        *, deliver: bool = True,
    ) -> bool:
        target_ok = True
        rows = await self.bot.db.get_feed_messages(guild_id, profile_key)
        saved = {row["event_id"]: row for row in rows}
        sources = {source: list(events) for source, events in sources.items()}
        historical = {}
        historical_journal = {}
        for row in rows:
            data = json.loads(row["payload"])
            data["event_ids"] = tuple(data["event_ids"])
            data["session_labels"] = tuple(data["session_labels"])
            event = FeedEvent(**data)
            if event.source == "activity":
                historical[event.activity_id] = event
            elif event.source == "journal":
                historical_journal[event.activity_id] = event
        histories = {"activity": historical, "journal": historical_journal}
        for source, events in list(sources.items()):
            initialized = await self.bot.db.has_feed_source(
                guild_id, profile_key, source + "-display-v1"
            )
            if initialized:
                sources[source] = await self._reconcile_changed_event_identities(
                    guild_id, profile_key, source, events, histories.get(source, {}),
                )
        new_journal_baseline_ids = set()
        for source, events in sources.items():
            # Empty successful sources are baselined too. A source absent from
            # this mapping failed and must be retried without changing state.
            ids = [e.activity_id for e in events]
            first = await self.bot.db.seed_feed_source(guild_id, profile_key, source + "-display-v1", ids)
            if first:
                if source == "journal":
                    new_journal_baseline_ids.update(ids)
                for event in events:
                    payload = json.dumps(asdict(replace(event, event_ids=(event.activity_id,))), sort_keys=True)
                    await self.bot.db.seed_feed_message(guild_id, profile_key, (event.activity_id,), payload)
                    _, _, event_date = classify_feed_event(event, self.clock())
                    await self.bot.db.record_feed_event_decision(
                        guild_id, profile_key, event.activity_id, source=event.source,
                        event_date=event_date, decision="baseline",
                        reason="initial_or_resume_source_baseline", payload=payload,
                    )
                # Refresh the lookup to include the baseline just written.
                saved = {r["event_id"]: r for r in await self.bot.db.get_feed_messages(guild_id, profile_key)}
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
        activity = {e.activity_id: e for e in historical.values()}
        for event in sources.get("activity", []):
            previous = historical.get(event.activity_id)
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
                activity[event.activity_id] = replace(event, event_ids=previous.event_ids, **preserved)
            else:
                for alias in event.event_ids:
                    activity.pop(alias, None)
                activity[event.activity_id] = event
        activities = [replace(e, event_ids=e.event_ids or (e.activity_id,)) for e in activity.values() if e.action_type in VISIBLE_ACTIONS]
        journal = {e.activity_id: e for e in historical_journal.values()}
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
        activity_health = (
            await self.bot.db.get_profile_feed_source(profile_key, "activity")
            if hasattr(self.bot.db, "get_profile_feed_source")
            else None
        )
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
        for log in logs:
            if log.activity_id in baseline_or_saved_ids:
                continue
            if await self.bot.db.is_journal_entry_delivered(guild_id, profile_key, log.activity_id):
                baseline_or_saved_ids.add(log.activity_id)
                continue
            stored_decision = await self.bot.db.get_feed_event_decision(
                guild_id, profile_key, log.activity_id
            )
            if stored_decision:
                if stored_decision["decision"] == "baseline":
                    baseline_or_saved_ids.add(log.activity_id)
                else:
                    try:
                        stored_payload = json.loads(stored_decision["payload"] or "{}")
                    except (TypeError, ValueError):
                        stored_payload = {}
                    stored_ids = stored_payload.get("event_ids") or ()
                    if any(
                        eid != log.activity_id and not str(eid).startswith("journal_")
                        for eid in stored_ids
                    ):
                        baseline_or_saved_ids.add(log.activity_id)
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
        known_ratings = {}
        for previous in sorted(historical.values(), key=lambda e: e.occurred_at or ""):
            if previous.source == "activity" and previous.rating is not None:
                known_ratings[previous.game_slug] = previous.rating
        for event in events:
            aliases = event.event_ids or (event.activity_id,)
            records = [saved[key] for key in aliases if key in saved]
            sent_records = [r for r in records if r["message_id"] is not None]
            record = min(sent_records, key=lambda row: row["message_id"]) if sent_records else (records[0] if records else None)
            if record is None:
                for alias in aliases:
                    if await self.bot.db.is_journal_entry_delivered(guild_id, profile_key, alias):
                        record = {
                            "payload": json.dumps(asdict(event)), "message_id": None,
                            "channel_id": None, "delivery_state": "baseline",
                        }
                        break
            if record:
                old = json.loads(record["payload"])
                previous_rating = old.get("previous_rating")
                if event.action_type == "rated" and old.get("rating") != event.rating:
                    previous_rating = old.get("rating")
                event = replace(event, previous_rating=previous_rating)
            elif event.action_type == "rated":
                old_rating = known_ratings.get(event.game_slug)
                if old_rating != event.rating:
                    event = replace(event, previous_rating=old_rating)
            payload = json.dumps(asdict(event), sort_keys=True)
            if record and payload == record["payload"] and all(alias in saved for alias in aliases):
                if event.source == "activity" and event.rating is not None:
                    known_ratings[event.game_slug] = event.rating
                continue
            message_id = record["message_id"] if record else None
            old_channel = record["channel_id"] if record else None
            state = record["delivery_state"] if record and "delivery_state" in record.keys() else None
            if state in {"pending", "unknown", "deleted"}:
                await self.bot.db.store_feed_message(
                    guild_id, profile_key, aliases, payload, message_id, old_channel,
                    delivery_state=state,
                )
                continue
            if record and message_id is None:
                # Historic baseline: remember aliases but never publish history.
                pass
            else:
                embed = create_journal_feed_embed(username, avatar_url, event, now=now)
                view = LinkButtonView(label="View on Backloggd", url=build_backloggd_game_url(event.game_slug))
                if message_id is not None:
                    if old_channel != channel.id or payload == record["payload"]:
                        continue
                    try:
                        await channel.get_partial_message(message_id).edit(embed=embed, view=view)
                    except discord.NotFound:
                        # A removed announcement stays removed; do not repost it.
                        await self.bot.db.mark_feed_message_deleted(
                            guild_id, profile_key, aliases, payload
                        )
                        continue
                    except asyncio.CancelledError:
                        raise
                    except discord.HTTPException as error:
                        target_ok = False
                        logger.warning(
                            "Could not edit feed message %s for %s in guild %d (status %s); keeping previous payload for retry",
                            message_id,
                            username,
                            guild_id,
                            getattr(error, "status", "unknown"),
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
                    except discord.HTTPException as error:
                        if 400 <= getattr(error, "status", 500) < 500:
                            await self.bot.db.release_feed_delivery(guild_id, profile_key, aliases)
                        else:
                            await self.bot.db.mark_feed_delivery_unknown(guild_id, profile_key, aliases)
                        raise
                    except Exception:
                        await self.bot.db.mark_feed_delivery_unknown(guild_id, profile_key, aliases)
                        raise
                    message_id, old_channel = message.id, channel.id
            await self.bot.db.store_feed_message(guild_id, profile_key, aliases, payload, message_id, old_channel)
            for alias in aliases:
                saved[alias] = {"payload": payload, "message_id": message_id, "channel_id": old_channel}
            if not record:
                await asyncio.sleep(1)
            if event.source == "activity" and event.rating is not None:
                known_ratings[event.game_slug] = event.rating
        return target_ok

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
        accepted = []
        for event in events:
            aliases = tuple(dict.fromkeys((event.activity_id, *(event.event_ids or ()))))
            rows = [saved[alias] for alias in aliases if alias in saved]
            if any(
                row["message_id"] is not None
                or dict(row).get("delivery_state") in {"sent", "deleted", "pending", "unknown"}
                for row in rows
            ):
                accepted.append(event)
                continue
            decisions = []
            for alias in aliases:
                stored = await self.bot.db.get_feed_event_decision(guild_id, profile_key, alias)
                if stored:
                    decisions.append((alias, stored))
            if decisions:
                chosen_alias, chosen = min(
                    decisions, key=lambda item: decision_priority.get(item[1]["decision"], 99)
                )
                decision = chosen["decision"]
                # New dated evidence can tighten an unsent decision. Use its
                # original discovery time so retries do not age into suppression.
                reason, event_date = chosen["reason"], chosen["event_date"]
                payload = json.dumps(asdict(event), sort_keys=True)
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
                for alias, stored in decisions:
                    if stored["decision"] == "eligible":
                        await self.bot.db.revise_unsent_eligible_feed_decision(
                            guild_id, profile_key, alias, decision=decision,
                            reason=reason, event_date=event_date, payload=payload,
                        )
                if len(decisions) < len(aliases):
                    payload = json.dumps(asdict(event), sort_keys=True)
                    decided_ids = {alias for alias, _ in decisions}
                    for alias in aliases:
                        if alias not in decided_ids:
                            alias_source = "journal" if str(alias).startswith("journal_") else event.source
                            await self.bot.db.record_feed_event_decision(
                                guild_id, profile_key, alias, source=alias_source,
                                event_date=event_date, decision=decision,
                                reason=reason, payload=payload, observed_at=chosen["observed_at"],
                            )
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
                decision = stored["decision"]
                for alias in aliases:
                    if alias != event.activity_id:
                        alias_source = "journal" if str(alias).startswith("journal_") else event.source
                        await self.bot.db.record_feed_event_decision(
                            guild_id, profile_key, alias, source=alias_source,
                            event_date=event_date, decision=decision, reason=reason, payload=payload,
                            observed_at=now,
                        )
            if decision in {"eligible", "baseline"}:
                accepted.append(event)
        return accepted

    async def _reconcile_changed_event_identities(
        self, guild_id, profile_key, source, events, historical
    ):
        """Reuse aliases only when identity is stable; hold ambiguous sessions."""
        now = self.clock()
        history = list(historical.values())
        incoming_reference_counts = {}
        for event in events:
            if event.reference:
                incoming_reference_counts[event.reference] = incoming_reference_counts.get(event.reference, 0) + 1
        reconciled = []
        for event in events:
            previous = historical.get(event.activity_id)
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
                    await self.bot.db.resolve_feed_event_decision(
                        guild_id, profile_key, event.activity_id,
                        json.dumps(asdict(resolved), sort_keys=True), event_date,
                    )
                    reconciled.append(resolved)
                    continue
                if len(matches) > 1 or (matches and incoming_reference_counts[event.reference] > 1):
                    await self._record_ambiguous_event(guild_id, profile_key, event, now)
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
                await self._record_ambiguous_event(guild_id, profile_key, event, now)
                continue
            reconciled.append(event)
        return reconciled

    async def _record_ambiguous_event(self, guild_id, profile_key, event, now):
        _, _, event_date = classify_feed_event(event, now)
        await self.bot.db.record_feed_event_decision(
            guild_id, profile_key, event.activity_id, source=event.source,
            event_date=event_date, decision="held_ambiguous",
            reason="raw_session_identity_changed_without_stable_reference",
            payload=json.dumps(asdict(event), sort_keys=True),
        )

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
        removed = await self.bot.db.unset_feed_channel(interaction.guild_id)
        if removed:
            await interaction.response.send_message(
                "✅ Automatic activity feed has been disabled for this server.",
                ephemeral=True
            )
        else:
            await interaction.response.send_message(
                "ℹ️ No activity feed channel was currently set for this server.",
                ephemeral=True
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
        await self.bot.db.set_guild_feed_preference(interaction.guild_id, interaction.user.id, True)
        channel_id = await self.bot.db.get_feed_channel(interaction.guild_id)
        message = f"Your linked profile **{escape_display_text(linked['backloggd_username'])}** is enabled for this server."
        if channel_id is None:
            message += " The feed is pending until an admin runs `/setchannel`."
        else:
            message += " Existing history will be skipped when the profile is first checked."
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
            cov = row["coverage"] or "unknown"
            line = f"**{display_name}:** {baselined} · Last OK: {last_ok} · Coverage: `{cov}`"
            if row["unresolved_gap_since"]:
                gap_since = self._format_rel_timestamp(row["unresolved_gap_since"])
                gap_until = (
                    self._format_rel_timestamp(row["unresolved_gap_until"])
                    if "unresolved_gap_until" in row.keys() and row["unresolved_gap_until"]
                    else "present"
                )
                line += f" · Gap: {gap_since} \u2192 {gap_until}"
            if row["last_error"]:
                err = escape_display_text(str(row["last_error"])[:80])
                line += f" · Error: `{err}`"
            source_lines.append(line)
        embed.add_field(
            name="Source Health & Coverage",
            value="\n".join(source_lines)[:1024],
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
            value=" · ".join(policy_parts)[:1024],
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
                username = escape_display_text(candidate["display_username"][:100])
                source = candidate["sources"].title()
                state = candidate.get("feed_state", "Pending baseline")
                lines.append(f"**{username}** — {source} · {state}")
            embed.description = "\n".join(lines)[:4000]
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
        created = await self.bot.db.add_guild_admin_follow(
            interaction.guild_id, profile.username, profile.avatar_url, interaction.user.id
        )
        channel_id = await self.bot.db.get_feed_channel(interaction.guild_id)
        member_ids = await self._authoritative_member_ids(interaction.guild)
        if member_ids is not None and channel_id is not None:
            candidates = await self.bot.db.get_guild_feed_candidates(interaction.guild_id, member_ids)
            await self.bot.db.reconcile_guild_feed_activity(
                interaction.guild_id,
                [row["profile_key"] for row in candidates],
                now=self.clock(),
                present_member_ids=member_ids,
            )
        safe_profile = escape_display_text(profile.username)
        if not created:
            message = f"**{safe_profile}** is already followed in this server."
        else:
            message = f"Added **{safe_profile}** to this server's activity feed."
        if channel_id is None:
            message += " The follow is pending until an admin runs `/setchannel`."
        elif member_ids is None:
            message += " The feed will pick it up after Discord provides a complete member snapshot."
        else:
            message += " Existing history will be skipped on its first check."
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
        for candidate in candidates:
            status = await self.bot.db.get_guild_profile_feed_status(
                interaction.guild_id, candidate["profile_key"]
            )
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

    @tasks.loop(minutes=5.0)
    async def journal_feed_loop(self):
        """Periodically check linked users' journals and broadcast new logs."""
        try:
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

                await self._reconcile_legacy_guild(guild)
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
                    sources = await self._fetch_feed_sources(username, defer_boundary_commit=True)
                    await self._observe_profile_activity(profile_key, username, sources)
                    all_guilds_ok = bool(active_targets) and not skipped
                    if skipped:
                        await self.bot.db.record_feed_target_gap(profile_key)
                    for guild_id, target_username, target_avatar, channel in [*active_targets, *skipped]:
                        try:
                            delivery_options = {'deliver': False} if channel is None else {}
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
                    meta = self._activity_snapshot_meta.get(profile_key)
                    if all_guilds_ok and meta and "activity" in sources and hasattr(self.bot.db, "commit_feed_source_boundary"):
                        await self.bot.db.commit_feed_source_boundary(
                            profile_key,
                            "activity",
                            coverage=meta["coverage"],
                            boundary_event_id=meta.get("boundary_event_id"),
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
