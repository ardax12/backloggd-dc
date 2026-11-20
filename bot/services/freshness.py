"""Shared library cache freshness evaluation, scheduling, and Discord disclosures."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Callable, Iterable, Optional

from bot.input_validation import escape_display_text
from bot.services.library_sync import (
    LibrarySyncService,
    parse_sync_timestamp,
    schedule_profile_refresh,
)

FRESHNESS_WINDOW_SECONDS = 3600


def valid_played_count(value: Any) -> Optional[int]:
    """Return non-negative integer Played-tab count or None for missing/invalid values."""
    if isinstance(value, int) and not isinstance(value, bool) and value >= 0:
        return value
    return None


@dataclass(frozen=True, slots=True)
class ProfileFreshness:
    profile_key: str
    username: str
    synced_at: Optional[datetime]
    attempt_at: Optional[datetime]
    last_error: Optional[str]
    missing: bool
    stale: bool
    refreshing: bool
    failed_last: bool
    dirty: bool = False


def _ensure_utc(now: Optional[datetime]) -> datetime:
    if now is None:
        return datetime.now(timezone.utc)
    if now.tzinfo is None:
        return now.replace(tzinfo=timezone.utc)
    return now.astimezone(timezone.utc)


def _row_get(row: Any, key: str, default: Any = None) -> Any:
    if row is None:
        return default
    if isinstance(row, dict):
        return row.get(key, default)
    if hasattr(row, "keys") and key in row.keys():
        return row[key]
    return getattr(row, key, default)


def evaluate_profile_freshness(
    sync_info: Any,
    *,
    profile_key: str,
    username: Optional[str] = None,
    now: Optional[datetime] = None,
    library_sync: Optional[LibrarySyncService] = None,
    freshness_window_seconds: float = FRESHNESS_WINDOW_SECONDS,
) -> ProfileFreshness:
    """Compute cache age, staleness, active refresh, and failure state for one profile."""
    normalized_key = profile_key.strip().lower()
    display_name = (
        str(_row_get(sync_info, "display_username") or _row_get(sync_info, "backloggd_username") or username or normalized_key)
    ).strip()
    current_now = _ensure_utc(now)

    synced_at = parse_sync_timestamp(_row_get(sync_info, "last_synced_at"))
    attempt_at = parse_sync_timestamp(_row_get(sync_info, "last_sync_attempt_at"))
    db_error = _row_get(sync_info, "last_sync_error")
    dirty = bool(_row_get(sync_info, "pending_refresh_token"))

    svc_state = (
        library_sync.get_profile_state(normalized_key)
        if library_sync is not None and hasattr(library_sync, "get_profile_state")
        else {}
    )
    refreshing = bool(svc_state.get("refreshing"))
    db_failed_last = bool(
        db_error
        and (
            synced_at is None
            or (attempt_at is not None and attempt_at >= synced_at)
        )
    )
    failed_last = bool(svc_state.get("failed_last")) or db_failed_last
    last_error = svc_state.get("last_error") or (str(db_error) if db_failed_last and db_error else None)

    missing = synced_at is None
    stale = missing or dirty or (current_now - synced_at).total_seconds() >= freshness_window_seconds
    return ProfileFreshness(
        profile_key=normalized_key,
        username=display_name,
        synced_at=synced_at,
        attempt_at=attempt_at,
        last_error=last_error,
        missing=missing,
        stale=stale,
        refreshing=refreshing,
        failed_last=failed_last,
        dirty=dirty,
    )


def _has_sync_columns(row: Any) -> bool:
    if row is None:
        return False
    if hasattr(row, "keys"):
        keys = set(row.keys())
        return {"last_synced_at", "last_sync_attempt_at", "last_sync_error"}.issubset(keys)
    return (
        hasattr(row, "last_synced_at")
        and hasattr(row, "last_sync_attempt_at")
        and hasattr(row, "last_sync_error")
    )


def _has_pending_token_field(row: Any) -> bool:
    if row is None:
        return False
    if isinstance(row, dict):
        return "pending_refresh_token" in row
    if hasattr(row, "keys"):
        return "pending_refresh_token" in row.keys()
    return hasattr(row, "pending_refresh_token")


async def _fetch_sync_info_map(db: Any, profiles: dict[str, tuple[str, Any]]) -> dict[str, Any]:
    if not profiles or db is None:
        return {}
    result: dict[str, Any] = {}
    missing_keys: list[str] = []
    missing_token_keys: list[str] = []
    for key, (_, raw_row) in profiles.items():
        if _has_sync_columns(raw_row):
            result[key] = raw_row
            if not _has_pending_token_field(raw_row):
                missing_token_keys.append(key)
        else:
            missing_keys.append(key)
    if missing_keys:
        result.update(await db.get_profiles_sync_info(missing_keys))
    if missing_token_keys:
        token_getter: Optional[Callable[..., Any]] = getattr(db, "get_profiles_refresh_pending_tokens", None)
        if callable(token_getter):
            token_map = await token_getter(missing_token_keys)
            for key in missing_token_keys:
                raw_row = result[key]
                merged = dict(raw_row) if hasattr(raw_row, "keys") else {
                    "profile_key": getattr(raw_row, "profile_key", key),
                    "display_username": getattr(raw_row, "display_username", None),
                    "backloggd_username": getattr(raw_row, "backloggd_username", None),
                    "last_synced_at": getattr(raw_row, "last_synced_at", None),
                    "last_sync_attempt_at": getattr(raw_row, "last_sync_attempt_at", None),
                    "last_sync_error": getattr(raw_row, "last_sync_error", None),
                    "game_count": getattr(raw_row, "game_count", 0),
                }
                merged["pending_refresh_token"] = token_map.get(key)
                result[key] = merged
    return result


async def ensure_profiles_fresh(
    bot: Any,
    profiles: Iterable[Any],
    *,
    now: Optional[datetime] = None,
    freshness_window_seconds: float = FRESHNESS_WINDOW_SECONDS,
    require_subscribed: bool = True,
    subscribed: Optional[bool] = None,
) -> dict[str, ProfileFreshness]:
    """Deduplicate profiles, schedule background refreshes when stale/absent, and return freshness state."""
    current_now = _ensure_utc(now)
    deduped: dict[str, tuple[str, Any]] = {}
    for item in profiles:
        if isinstance(item, tuple) and len(item) >= 2:
            raw_key, raw_name = item[0], item[1]
            raw_row = None
        else:
            raw_key = _row_get(item, "profile_key") or _row_get(item, "display_username") or _row_get(item, "backloggd_username")
            raw_name = _row_get(item, "display_username") or _row_get(item, "backloggd_username") or raw_key
            raw_row = item
        if not raw_key:
            continue
        key = str(raw_key).strip().lower()
        name = str(raw_name or key).strip()
        if key and key not in deduped:
            deduped[key] = (name, raw_row)

    if not deduped:
        return {}

    db = getattr(bot, "db", None)
    service: Optional[LibrarySyncService] = getattr(bot, "library_sync", None)
    sync_map = await _fetch_sync_info_map(db, deduped)

    statuses = {
        key: evaluate_profile_freshness(
            sync_map.get(key),
            profile_key=key,
            username=name,
            now=current_now,
            library_sync=service,
            freshness_window_seconds=freshness_window_seconds,
        )
        for key, (name, _) in deduped.items()
    }
    stale_keys = [key for key, status in statuses.items() if status.stale]
    subscription_evidence: dict[str, bool] = {}
    if db is not None and service is not None and require_subscribed and subscribed is None and stale_keys:
        fetched_subscriptions = await db.get_profiles_library_subscription_map(stale_keys)
        subscription_evidence = {
            key: bool(fetched_subscriptions[key])
            for key in stale_keys
            if key in fetched_subscriptions
        }

    for key, (_name, _) in deduped.items():
        status = statuses[key]
        info = sync_map.get(key)
        if status.stale and service is not None:
            await schedule_profile_refresh(
                bot,
                status.username,
                bypass_freshness=status.dirty,
                freshness_window_seconds=freshness_window_seconds,
                now=current_now,
                require_subscribed=require_subscribed,
                subscribed=(
                    subscribed if subscribed is not None
                    else subscription_evidence.get(key)
                ),
                sync_info=info,
            )
            statuses[key] = evaluate_profile_freshness(
                info,
                profile_key=key,
                username=status.username,
                now=current_now,
                library_sync=service,
                freshness_window_seconds=freshness_window_seconds,
            )
    return statuses


def format_discord_rel(dt: Optional[datetime]) -> str:
    if dt is None:
        return "Not synced yet"
    return f"<t:{int(_ensure_utc(dt).timestamp())}:R>"


def format_profile_freshness_line(
    status: Optional[ProfileFreshness],
    *,
    label: str = "Library",
    unlinked_live: bool = False,
) -> Optional[str]:
    if status is None:
        return None
    if unlinked_live and status.missing and not status.refreshing and not status.failed_last:
        return f"{label} fetched live"
    age = f"updated {format_discord_rel(status.synced_at)}" if status.synced_at else "not synced yet"
    line = f"{label} {age}"
    if status.refreshing:
        return f"{line} (updating)"
    if status.failed_last:
        return f"{line} (last update failed)"
    if status.stale and not status.missing:
        return f"{line} (update pending)" if status.dirty else f"{line} (out of date)"
    return line


def summarize_aggregate_freshness(statuses: Iterable[ProfileFreshness]) -> dict[str, Any]:
    unique = {s.profile_key: s for s in statuses}
    values = list(unique.values())
    synced_times = [s.synced_at for s in values if s.synced_at is not None]
    missing_count = sum(1 for s in values if s.missing)
    stale_count = sum(1 for s in values if s.stale and not s.missing)
    dirty_count = sum(1 for s in values if s.dirty and not s.missing)
    fresh_count = sum(1 for s in values if not s.stale)
    refreshing_count = sum(1 for s in values if s.refreshing)
    failed_count = sum(1 for s in values if s.failed_last)
    return {
        "total": len(values),
        "fresh": fresh_count,
        "stale": stale_count,
        "dirty": dirty_count,
        "missing": missing_count,
        "refreshing": refreshing_count,
        "failed": failed_count,
        "oldest_synced_at": min(synced_times) if synced_times else None,
    }


def format_aggregate_freshness(summary: Optional[dict[str, Any]]) -> Optional[str]:
    if not summary or summary.get("total", 0) <= 0:
        return None
    total = summary["total"]
    oldest = summary.get("oldest_synced_at")
    library = "library" if total == 1 else "libraries"
    updated = (
        f"{'updated' if total == 1 else 'oldest update'} {format_discord_rel(oldest)}"
        if oldest is not None
        else "no updates yet"
    )
    parts = [f"{total} {library} · {updated}"]
    for key, label in (
        ("stale", "out of date"),
        ("missing", "missing"),
        ("refreshing", "updating"),
        ("failed", "failed to update"),
    ):
        if n := summary.get(key):
            parts.append(f"{n} {label}")
    return " · ".join(parts)


def format_fetching_library_message(
    username: str,
    status: Optional[ProfileFreshness],
) -> str:
    """Format the missing-cache disclosure message while safely escaping display names once."""
    safe_username = escape_display_text(username)
    if status is not None and status.failed_last and not status.refreshing:
        return (
            f"⏳ **{safe_username}**'s cached library data is not available yet because the last background refresh failed. "
            "The bot will retry automatically, or you can run `/refresh`."
        )
    return (
        f"⏳ **{safe_username}**'s cached library data is currently being fetched in the background. "
        "Try again shortly or use `/refresh`."
    )


async def resolve_guild_aggregate_freshness(
    bot: Any,
    member_ids: Iterable[int],
    *,
    now: Optional[datetime] = None,
    profiles: Optional[Iterable[Any]] = None,
    profiles_are_linked: bool = False,
    require_subscribed: bool = True,
) -> Optional[str]:
    """Evaluate guild freshness; skip subscription reads only with linked-member evidence."""
    linked_evidence = profiles is None or profiles_are_linked
    if profiles is None:
        db = getattr(bot, "db", None)
        if db is None:
            return None
        profile_list = await db.get_guild_linked_profiles_with_sync_info(list(member_ids))
    else:
        profile_list = list(profiles)

    if not profile_list:
        return "No linked Backloggd profiles"

    statuses = await ensure_profiles_fresh(
        bot,
        profile_list,
        now=now,
        require_subscribed=require_subscribed,
        subscribed=True if require_subscribed and linked_evidence else None,
    )
    return format_aggregate_freshness(summarize_aggregate_freshness(statuses.values()))
