"""Shared 1-hour library cache freshness evaluation, scheduling, and Discord disclosures."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Iterable, Optional

from bot.services.library_sync import (
    LibrarySyncService,
    parse_sync_timestamp,
    schedule_profile_refresh,
)

FRESHNESS_WINDOW_SECONDS = 3600


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
    stale = missing or (current_now - synced_at).total_seconds() >= freshness_window_seconds
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
    )


async def _fetch_sync_info_map(db: Any, profiles: dict[str, tuple[str, Any]]) -> dict[str, Any]:
    if not profiles or db is None:
        return {}
    batch_getter = getattr(db, "get_profiles_sync_info", None)
    if batch_getter is not None:
        result = await batch_getter(list(profiles))
        if isinstance(result, dict):
            return result
    result: dict[str, Any] = {}
    single_getter = getattr(db, "get_profile_sync_info", None)
    for key, (display_name, raw_row) in profiles.items():
        if raw_row is not None and (
            _row_get(raw_row, "last_synced_at") is not None
            or _row_get(raw_row, "last_sync_attempt_at") is not None
            or _row_get(raw_row, "last_sync_error") is not None
        ):
            result[key] = raw_row
        elif single_getter is not None:
            info = await single_getter(display_name)
            if info is not None:
                result[key] = info
    return result


async def ensure_profiles_fresh(
    bot: Any,
    profiles: Iterable[Any],
    *,
    now: Optional[datetime] = None,
    freshness_window_seconds: float = FRESHNESS_WINDOW_SECONDS,
    require_subscribed: bool = True,
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

    statuses: dict[str, ProfileFreshness] = {}
    for key, (name, _) in deduped.items():
        info = sync_map.get(key)
        status = evaluate_profile_freshness(
            info,
            profile_key=key,
            username=name,
            now=current_now,
            library_sync=service,
            freshness_window_seconds=freshness_window_seconds,
        )
        if status.stale and service is not None:
            await schedule_profile_refresh(
                bot,
                status.username,
                freshness_window_seconds=freshness_window_seconds,
                now=current_now,
                require_subscribed=require_subscribed,
            )
            status = evaluate_profile_freshness(
                info,
                profile_key=key,
                username=status.username,
                now=current_now,
                library_sync=service,
                freshness_window_seconds=freshness_window_seconds,
            )
        statuses[key] = status
    return statuses


def format_discord_rel(dt: Optional[datetime]) -> str:
    if dt is None:
        return "Not synced yet"
    return f"<t:{int(_ensure_utc(dt).timestamp())}:R>"


def format_profile_freshness_line(
    status: Optional[ProfileFreshness],
    *,
    label: str = "Library cache",
    unlinked_live: bool = False,
) -> Optional[str]:
    if status is None:
        return None
    if unlinked_live and status.missing and not status.refreshing and not status.failed_last:
        return f"{label}: Fetched live from Backloggd"
    parts = [f"{label}: {format_discord_rel(status.synced_at)}"]
    notes = []
    if status.refreshing:
        notes.append("refreshing in background")
    elif status.failed_last:
        notes.append("last refresh failed (keeping cached data)" if not status.missing else "last refresh failed")
    elif status.stale and not status.missing:
        notes.append("stale (>1h)")
    if notes:
        parts.append(f"({', '.join(notes)})")
    return " ".join(parts)


def summarize_aggregate_freshness(statuses: Iterable[ProfileFreshness]) -> dict[str, Any]:
    unique = {s.profile_key: s for s in statuses}
    values = list(unique.values())
    synced_times = [s.synced_at for s in values if s.synced_at is not None]
    missing_count = sum(1 for s in values if s.missing)
    stale_count = sum(1 for s in values if s.stale and not s.missing)
    fresh_count = sum(1 for s in values if not s.stale)
    refreshing_count = sum(1 for s in values if s.refreshing)
    failed_count = sum(1 for s in values if s.failed_last)
    return {
        "total": len(values),
        "fresh": fresh_count,
        "stale": stale_count,
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
    oldest_text = format_discord_rel(oldest) if oldest is not None else "None synced yet"
    parts = [
        f"Oldest contributing sync: {oldest_text}",
        f"Profiles: {total} ({summary['fresh']} fresh, {summary['stale']} stale (>1h), {summary['missing']} missing)",
    ]
    if summary.get("refreshing"):
        parts.append(f"Refreshing now: {summary['refreshing']}")
    if summary.get("failed"):
        parts.append(f"Last refresh failed: {summary['failed']}")
    return " · ".join(parts)
