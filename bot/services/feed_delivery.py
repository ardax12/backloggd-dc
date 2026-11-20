"""Channel inspection, per-guild filter suppression, archive edits, and shielded sends."""

from __future__ import annotations

import asyncio
import json
import logging
import uuid
from dataclasses import asdict, replace
from datetime import datetime
from typing import Any

import aiohttp
import discord

from bot.feed_models import GROUPED_RATING_ID_PREFIX, FeedEvent
from bot.feed_policy import (
    DEFAULT_GUILD_FEED_SETTINGS,
    classify_feed_event,
    feed_event_allowed_by_filters,
)
from bot.ui.feed_embed import create_journal_feed_embed
from bot.ui.helpers import build_backloggd_game_url
from bot.ui.views import LinkButtonView

logger = logging.getLogger("backloggd.services.feed_pipeline")


def inspect_target_channel(
    pipeline: Any,
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
        get_guild = getattr(pipeline.bot, "get_guild", None)
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


async def get_guild_feed_filters(
    pipeline: Any, guild_id: int, poll_context: dict | None = None
) -> dict[str, bool]:
    cached = poll_context.get("guild_filters") if isinstance(poll_context, dict) else None
    if isinstance(cached, dict) and guild_id in cached:
        return cached[guild_id]
    if hasattr(pipeline.bot.db, "get_guild_feed_filters"):
        filters = await pipeline.bot.db.get_guild_feed_filters(guild_id)
    else:
        filters = dict(DEFAULT_GUILD_FEED_SETTINGS)
    if isinstance(cached, dict):
        cached[guild_id] = filters
    return filters


async def record_guild_filtered_event(
    pipeline: Any,
    guild_id: int,
    profile_key: str,
    event: FeedEvent,
    aliases: tuple[str, ...],
    payload: str,
    now: datetime,
    saved: dict,
) -> None:
    """Record a guild-filtered event as suppressed and baseline it against future backfill."""
    _, _, event_date = classify_feed_event(event, now)
    if hasattr(pipeline.bot.db, "persist_feed_event_decisions"):
        inserts = [
            {
                "event_id": alias,
                "source": "journal" if str(alias).startswith("journal_") else event.source,
                "event_date": event_date,
                "decision": "filtered",
                "reason": "guild_filter",
                "payload": payload,
                "observed_at": now,
            }
            for alias in aliases
        ]
        await pipeline.bot.db.persist_feed_event_decisions(
            guild_id,
            profile_key,
            inserts=inserts,
            alias_groups=(aliases,),
        )
    await pipeline.bot.db.store_feed_message(
        guild_id,
        profile_key,
        aliases,
        payload,
        None,
        None,
        delivery_state="baseline",
    )
    for alias in aliases:
        saved[alias] = {
            "payload": payload,
            "message_id": None,
            "channel_id": None,
            "delivery_state": "baseline",
        }


def select_delivery_record(saved, delivered_alias_ids, event, aliases):
    """Select the canonical saved delivery record (or synthetic baseline) for an event's aliases."""
    records = [saved[key] for key in aliases if key in saved]
    sent_records = [r for r in records if r["message_id"] is not None]
    if sent_records:
        deleted_sent = [r for r in sent_records if dict(r).get("delivery_state") == "deleted"]
        pool = deleted_sent or sent_records
        return min(pool, key=lambda row: row["message_id"])
    if records:
        protected = [
            r for r in records
            if dict(r).get("delivery_state") in {"deleted", "unknown", "pending"}
        ]
        return protected[0] if protected else records[0]
    if any(alias in delivered_alias_ids for alias in aliases):
        return {
            "payload": json.dumps(asdict(event), sort_keys=True),
            "message_id": None,
            "channel_id": None,
            "delivery_state": "baseline",
            "_synthetic_event": event,
        }
    return None


def resolve_delivery_event(pipeline: Any, event, record, row_events_by_id, prepared):
    """Annotate previous_rating from saved state or poll history and detect unchanged events."""
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
            try:
                old = json.loads(record["payload"])
                if not isinstance(old, dict):
                    old = {}
            except (TypeError, ValueError):
                old = {}
            previous_rating = old.get("previous_rating")
            if event.action_type == "rated" and old.get("rating") != event.rating:
                previous_rating = old.get("rating")
            event = replace(event, previous_rating=previous_rating)
    elif event.action_type == "rated" and not event.activity_id.startswith(GROUPED_RATING_ID_PREFIX):
        old_rating = pipeline._previous_rating_from_history(
            event,
            prepared.activity_rating_history.get(event.game_slug, ()),
            prepared.journal_rating_history.get(event.game_slug, ()),
            journal_time_unknown=event.game_slug in prepared.journal_rating_times_unknown,
        )
        if old_rating != event.rating:
            event = replace(event, previous_rating=old_rating)
    return event, unchanged


async def store_missing_aliases(
    pipeline: Any,
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
    await pipeline.bot.db.store_feed_message(
        guild_id,
        profile_key,
        missing_aliases,
        payload,
        message_id,
        channel_id,
        delivery_state=delivery_state,
    )
    for alias in missing_aliases:
        saved[alias] = {
            "payload": payload,
            "message_id": message_id,
            "channel_id": channel_id,
            "delivery_state": delivery_state,
        }


def resolve_edit_channel(pipeline: Any, guild_id: int, old_channel, current_channel):
    """Resolve and validate the target channel for editing an existing announcement."""
    if old_channel is None:
        return None, "Original channel ID missing"
    if old_channel == current_channel.id:
        channel_guild_id = getattr(getattr(current_channel, "guild", None), "id", None)
        if (
            isinstance(channel_guild_id, int)
            and not isinstance(channel_guild_id, bool)
            and channel_guild_id != guild_id
        ):
            return None, f"Configured channel <#{old_channel}> belongs to another server"
        return current_channel, None
    edit_channel, _, edit_issue = pipeline.inspect_target_channel(
        guild_id, old_channel, require_channel_lookup=True
    )
    return edit_channel, edit_issue


async def edit_existing_announcement(
    pipeline: Any,
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
    """Edit an existing announcement message.

    Returns None when the edit succeeded and the caller should persist the updated payload,
    True when handled terminally (e.g. message deleted), or False on a retryable error.
    """
    guild_id, profile_key = target.guild_id, target.profile_key
    username, channel = target.username, target.channel
    edit_channel, edit_issue = pipeline._resolve_edit_channel(guild_id, old_channel, channel)
    if edit_channel is None:
        if missing_aliases:
            await pipeline._store_missing_aliases(
                guild_id, profile_key, missing_aliases, record["payload"],
                message_id, old_channel, state or "sent", saved,
            )
        if isinstance(stats, dict):
            stats["delivery_errors"] += 1
        pipeline._last_publish_errors[(guild_id, profile_key)] = (
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
        return False

    try:
        await edit_channel.get_partial_message(message_id).edit(embed=embed, view=view)
        if isinstance(stats, dict):
            stats["edited"] += 1
        return None
    except discord.NotFound:
        # A removed announcement stays removed; do not repost it.
        await pipeline.bot.db.mark_feed_message_deleted(
            guild_id, profile_key, aliases, payload
        )
        if missing_aliases:
            await pipeline.bot.db.store_feed_message(
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
        return True
    except asyncio.CancelledError:
        raise
    except (discord.HTTPException, discord.RateLimited, aiohttp.ClientError, TimeoutError, ConnectionError) as error:
        if missing_aliases:
            await pipeline._store_missing_aliases(
                guild_id, profile_key, missing_aliases, record["payload"],
                message_id, old_channel, state or "sent", saved,
            )
        if isinstance(stats, dict):
            stats["delivery_errors"] += 1
        err_status = getattr(error, "status", "rate_limited" if isinstance(error, discord.RateLimited) else type(error).__name__)
        pipeline._last_publish_errors[(guild_id, profile_key)] = f"edit_failed:{err_status}: {error}"[:500]
        logger.warning(
            "Could not edit feed message %s for %s in guild %d (status %s); keeping previous payload for retry",
            message_id,
            username,
            guild_id,
            err_status,
        )
        return False


async def send_new_announcement(
    pipeline: Any,
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
    guild_id, profile_key = target.guild_id, target.profile_key
    username, channel = target.username, target.channel
    owner_token = uuid.uuid4().hex
    if not await pipeline.bot.db.begin_feed_delivery(
        guild_id, profile_key, aliases, payload, channel.id,
        owner_token=owner_token,
    ):
        return True
    durable_finalized = False
    cleanup_launched = False
    sent_message = None

    async def _write_delivery_state(outcome: str, *, msg: Any = None) -> None:
        nonlocal durable_finalized
        if outcome == "release":
            await pipeline.bot.db.release_feed_delivery(guild_id, profile_key, aliases)
            durable_finalized = True
            return
        if outcome == "sent" and msg is not None and getattr(msg, "id", None) is not None:
            await pipeline.bot.db.store_feed_message(
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
            return
        await pipeline.bot.db.mark_feed_delivery_unknown(guild_id, profile_key, aliases)
        durable_finalized = True
        for alias in aliases:
            saved[alias] = {
                "payload": payload,
                "message_id": None,
                "channel_id": channel.id,
                "delivery_state": "unknown",
            }

    async def _finalize_delivery_state(outcome: str, *, msg: Any = None) -> None:
        try:
            await _write_delivery_state(outcome, msg=msg)
        except Exception:
            logger.warning(
                "Could not finalize delivery state for %s (%s) in guild %d; leaving intent recoverable",
                event.activity_id,
                username,
                guild_id,
                exc_info=True,
            )
        finally:
            pipeline.bot.db.release_feed_delivery_ownership(
                guild_id,
                profile_key,
                aliases,
                owner_token,
                recheck_recovery=not durable_finalized,
            )

    async def _shield_delivery_cleanup(outcome: str, *, msg: Any = None) -> None:
        nonlocal cleanup_launched
        cleanup_launched = True
        cleanup_task = pipeline._track_delivery_cleanup(
            _finalize_delivery_state(outcome, msg=msg)
        )
        await asyncio.shield(cleanup_task)

    async def _transition_delivery(outcome: str, *, msg: Any = None) -> None:
        try:
            await _write_delivery_state(outcome, msg=msg)
        except asyncio.CancelledError:
            await _shield_delivery_cleanup(outcome, msg=msg)
            raise

    async def _mark_unknown_fallback(warning_msg: str, *warning_args: Any) -> None:
        try:
            await _transition_delivery("unknown")
        except Exception:
            logger.warning(warning_msg, *warning_args, exc_info=True)

    try:
        try:
            sent_message = await channel.send(embed=embed, view=view)
        except asyncio.CancelledError:
            await _shield_delivery_cleanup("unknown")
            raise
        except discord.RateLimited as error:
            await _transition_delivery("release")
            if isinstance(stats, dict):
                stats["delivery_errors"] += 1
            pipeline._last_publish_errors[(guild_id, profile_key)] = f"rate_limited:{error.retry_after:.2f}s"[:500]
            logger.warning(
                "Rate limited sending feed event %s for %s in guild %d (retry_after=%.2fs); released pending intent",
                event.activity_id,
                username,
                guild_id,
                error.retry_after,
            )
            return False
        except discord.HTTPException as error:
            status = getattr(error, "status", None)
            is_client_rejection = isinstance(status, int) and 400 <= status < 500
            await _transition_delivery("release" if is_client_rejection else "unknown")
            if isinstance(stats, dict):
                stats["delivery_errors"] += 1
            pipeline._last_publish_errors[(guild_id, profile_key)] = (
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
            return False
        except (aiohttp.ClientError, TimeoutError, ConnectionError) as error:
            await _transition_delivery("unknown")
            if isinstance(stats, dict):
                stats["delivery_errors"] += 1
            pipeline._last_publish_errors[(guild_id, profile_key)] = f"{type(error).__name__}: {error}"[:500]
            logger.warning(
                "Uncertain transport failure sending feed event %s for %s in guild %d (%s); marked delivery unknown",
                event.activity_id,
                username,
                guild_id,
                error,
            )
            return False
        except Exception:
            await _mark_unknown_fallback(
                "Could not mark delivery unknown after error sending %s in guild %d",
                event.activity_id,
                guild_id,
            )
            raise
        message_id = sent_message.id
        try:
            await _transition_delivery("sent", msg=sent_message)
            if isinstance(stats, dict):
                stats["sent"] += 1
        except Exception:
            await _mark_unknown_fallback(
                "Could not record unknown state after post-send write error for message %s (%s) in guild %d",
                message_id,
                event.activity_id,
                guild_id,
            )
            raise
    finally:
        if not cleanup_launched:
            pipeline.bot.db.release_feed_delivery_ownership(
                guild_id,
                profile_key,
                aliases,
                owner_token,
                recheck_recovery=not durable_finalized,
            )
    if not record:
        await asyncio.sleep(1)
    return True


async def deliver_event(
    pipeline: Any, target, event, history, prepared, now, *, poll_context: dict | None = None
) -> bool:
    """Orchestrate record lookup, filtering, archive edits, new sends, and persistence."""
    stats = poll_context.get("stats") if isinstance(poll_context, dict) else None
    guild_id, profile_key = target.guild_id, target.profile_key
    saved = history.saved
    aliases = event.event_ids or (event.activity_id,)

    record = pipeline._select_delivery_record(saved, prepared.delivered_alias_ids, event, aliases)
    event, unchanged = pipeline._resolve_delivery_event(event, record, history.row_events_by_id, prepared)
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
            await pipeline._store_missing_aliases(
                guild_id, profile_key, missing_aliases, record["payload"],
                message_id, old_channel, alias_state, saved,
            )
        return True
    if state in {"pending", "unknown", "deleted"}:
        await pipeline._store_missing_aliases(
            guild_id, profile_key, aliases, payload, message_id, old_channel, state, saved
        )
        return True

    if record and message_id is None:
        # Historic baseline: remember aliases but never publish history.
        pass
    else:
        guild_filters = await pipeline._get_guild_feed_filters(guild_id, poll_context)
        if message_id is None and not feed_event_allowed_by_filters(event, guild_filters):
            # Suppress newly discovered event blocked by this server's category filters
            # and baseline it so re-enabling the category later never backfills history.
            await pipeline._record_guild_filtered_event(
                guild_id, profile_key, event, aliases, payload, now, saved
            )
            return True
        embed = create_journal_feed_embed(
            target.username,
            target.avatar_url,
            event,
            now=now,
            force_spoiler_reviews=bool(guild_filters.get("spoiler_reviews", False)),
        )
        view = LinkButtonView(label="View on Backloggd", url=build_backloggd_game_url(event.game_slug))
        if message_id is not None:
            edit_outcome = await pipeline._edit_existing_announcement(
                target,
                old_channel=old_channel, message_id=message_id, state=state,
                record=record, aliases=aliases, missing_aliases=missing_aliases,
                payload=payload, embed=embed, view=view, saved=saved, stats=stats,
            )
            if edit_outcome is not None:
                return edit_outcome
        else:
            return await pipeline._send_new_announcement(
                target,
                event=event, record=record, aliases=aliases,
                payload=payload, embed=embed, view=view, saved=saved, stats=stats,
            )

    await pipeline.bot.db.store_feed_message(guild_id, profile_key, aliases, payload, message_id, old_channel)
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
    return True


async def deliver_announcements(
    pipeline: Any, target, history, prepared, now, *, poll_context: dict | None = None
) -> bool:
    target_ok = True
    for event in prepared.events:
        if not await pipeline._deliver_event(target, event, history, prepared, now, poll_context=poll_context):
            target_ok = False
    return target_ok
