"""Coalesce and bound full-library refreshes by Backloggd profile."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field, is_dataclass, replace
import logging
import time
from types import SimpleNamespace
from typing import Any, Awaitable, Callable, Optional

logger = logging.getLogger("backloggd.library_sync")

SyncOperation = Callable[[], Awaitable[Optional[int]]]


@dataclass(frozen=True, slots=True)
class SyncOutcome:
    status: str
    game_count: Optional[int] = None
    reason: Optional[str] = None


@dataclass(slots=True)
class _ProfileState:
    task: Optional[asyncio.Task] = None
    last_attempt_at: Optional[float] = None
    last_success_at: Optional[float] = None
    last_failure_at: Optional[float] = None
    game_count: Optional[int] = None
    last_error: Optional[str] = None
    dirty_operation: Optional[SyncOperation] = None


def parse_sync_timestamp(value) -> Optional[datetime]:
    """Parse a DB SQLite/ISO UTC timestamp into a timezone-aware UTC datetime."""
    from datetime import datetime, timezone
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)
    text = str(value).strip()
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        try:
            parsed = datetime.strptime(text, "%Y-%m-%d %H:%M:%S")
        except ValueError:
            return None
    return parsed.replace(tzinfo=timezone.utc) if parsed.tzinfo is None else parsed.astimezone(timezone.utc)


class LibrarySyncService:
    """Run at most one library refresh per profile and two profiles at once."""

    def __init__(
        self,
        *,
        max_workers: int = 2,
        automatic_debounce_seconds: float = 300,
        failure_backoff_seconds: float = 300,
        clock: Callable[[], float] | None = None,
    ) -> None:
        self.automatic_debounce_seconds = automatic_debounce_seconds
        self.failure_backoff_seconds = failure_backoff_seconds
        self._clock = clock or time.monotonic
        self._semaphore = asyncio.Semaphore(max(1, max_workers))
        self._profiles: dict[str, _ProfileState] = {}
        self._closed = False

    def seed_success(self, profile_key: str, *, age_seconds: float, game_count: int) -> None:
        """Seed cooldown state from a persisted successful snapshot."""
        state = self._profiles.setdefault(profile_key, _ProfileState())
        synced_at = self._clock() - max(0.0, age_seconds)
        if state.task and not state.task.done():
            return
        if state.last_attempt_at is not None and state.last_attempt_at > synced_at:
            return
        if state.last_success_at is not None and state.last_success_at > synced_at:
            return
        state.last_attempt_at = synced_at
        state.last_success_at = synced_at
        state.last_failure_at = None
        state.last_error = None
        state.game_count = game_count

    def seed_failure(
        self, profile_key: str, *, age_seconds: float, error: Optional[str] = None
    ) -> None:
        """Seed failure backoff state from a persisted failed attempt."""
        state = self._profiles.setdefault(profile_key, _ProfileState())
        failed_at = self._clock() - max(0.0, age_seconds)
        if state.task and not state.task.done():
            return
        if state.last_attempt_at is not None and state.last_attempt_at > failed_at:
            return
        state.last_attempt_at = failed_at
        state.last_failure_at = failed_at
        if error:
            state.last_error = error

    def seed_from_sync_info(self, info, *, now=None, profile_key: Optional[str] = None) -> None:
        """Seed in-memory coordinator state from a DB profile sync row."""
        from datetime import datetime, timezone
        if not info:
            return
        keys = info.keys() if hasattr(info, "keys") else ()
        resolved_key = (
            info["profile_key"] if "profile_key" in keys and info["profile_key"]
            else (profile_key or "")
        ).strip().lower()
        if not resolved_key:
            return
        current_now = now or datetime.now(timezone.utc)
        if current_now.tzinfo is None:
            current_now = current_now.replace(tzinfo=timezone.utc)
        synced_at = parse_sync_timestamp(info["last_synced_at"] if "last_synced_at" in keys else None)
        if synced_at is not None:
            age_seconds = max(0.0, (current_now - synced_at).total_seconds())
            game_count = int((info["game_count"] if "game_count" in keys else 0) or 0)
            self.seed_success(resolved_key, age_seconds=age_seconds, game_count=game_count)
        last_error = info["last_sync_error"] if "last_sync_error" in keys else None
        attempt_at = parse_sync_timestamp(info["last_sync_attempt_at"] if "last_sync_attempt_at" in keys else None)
        if last_error and attempt_at is not None and (synced_at is None or attempt_at >= synced_at):
            age_seconds = max(0.0, (current_now - attempt_at).total_seconds())
            self.seed_failure(resolved_key, age_seconds=age_seconds, error=str(last_error))

    def is_refreshing(self, profile_key: str) -> bool:
        state = self._profiles.get(profile_key.strip().lower())
        return bool(state and state.task and not state.task.done())

    def get_profile_state(self, profile_key: str) -> dict:
        state = self._profiles.get(profile_key.strip().lower())
        if not state:
            return {"refreshing": False, "last_error": None, "failed_last": False}
        refreshing = bool(state.task and not state.task.done())
        failed_last = (
            state.last_failure_at is not None
            and (state.last_success_at is None or state.last_failure_at >= state.last_success_at)
        )
        return {
            "refreshing": refreshing,
            "last_error": state.last_error,
            "failed_last": failed_last,
            "last_attempt_at": state.last_attempt_at,
            "last_success_at": state.last_success_at,
            "last_failure_at": state.last_failure_at,
        }

    def request(
        self,
        profile_key: str,
        operation: SyncOperation,
        *,
        manual: bool = False,
        dirty: bool = False,
        freshness_window_seconds: Optional[float] = None,
    ) -> asyncio.Task:
        """Request a refresh, joining active work and coalescing dirty updates."""
        if self._closed:
            raise RuntimeError("Library sync service is closed")
        state = self._profiles.setdefault(profile_key, _ProfileState())
        if state.task and not state.task.done():
            if dirty:
                state.dirty_operation = operation
            return state.task

        if (
            not manual
            and freshness_window_seconds is not None
            and state.last_success_at is not None
            and self._clock() - state.last_success_at < freshness_window_seconds
        ):
            return asyncio.create_task(self._skipped(state, "fresh"))

        if not manual and state.last_attempt_at is not None:
            failed_last = (
                state.last_failure_at is not None
                and (state.last_success_at is None or state.last_failure_at >= state.last_success_at)
            )
            interval = self.failure_backoff_seconds if failed_last else self.automatic_debounce_seconds
            if self._clock() - state.last_attempt_at < interval:
                reason = "backoff" if failed_last else "debounce"
                return asyncio.create_task(self._skipped(state, reason))

        task = asyncio.create_task(self._run(profile_key, state, operation))
        state.task = task
        return task

    async def _skipped(self, state: _ProfileState, reason: str) -> SyncOutcome:
        return SyncOutcome(status="skipped", game_count=state.game_count, reason=reason)

    async def _run(
        self,
        profile_key: str,
        state: _ProfileState,
        operation: SyncOperation,
    ) -> SyncOutcome:
        current_operation = operation
        try:
            while True:
                async with self._semaphore:
                    state.last_attempt_at = self._clock()
                    try:
                        result = await current_operation()
                        if isinstance(result, SyncOutcome):
                            outcome = result
                        elif result is None:
                            outcome = SyncOutcome(status="failed", game_count=state.game_count)
                        else:
                            outcome = SyncOutcome(status="success", game_count=result)
                    except asyncio.CancelledError:
                        raise
                    except Exception as error:
                        logger.exception("Library refresh failed for profile %s", profile_key)
                        outcome = SyncOutcome(status="failed", game_count=state.game_count, reason=str(error))

                    if outcome.status == "success":
                        state.last_success_at = self._clock()
                        state.last_failure_at = None
                        state.last_error = None
                        state.game_count = outcome.game_count
                    elif outcome.status == "failed":
                        state.last_failure_at = self._clock()
                        if outcome.reason:
                            state.last_error = outcome.reason

                if state.dirty_operation is None:
                    return outcome
                current_operation = state.dirty_operation
                state.dirty_operation = None
        finally:
            if state.task is asyncio.current_task():
                state.task = None
                state.dirty_operation = None

    async def close(self) -> None:
        """Cancel and wait for all active work before the database closes."""
        self._closed = True
        tasks = [state.task for state in self._profiles.values() if state.task and not state.task.done()]
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)


async def seed_profile_sync_state(
    bot,
    username: str,
    *,
    service: Optional[LibrarySyncService] = None,
    now=None,
) -> None:
    """Seed coordinator cooldown/backoff state from persisted profile sync metadata."""
    target_service = service if service is not None else getattr(bot, "library_sync", None)
    db = getattr(bot, "db", None)
    if target_service is None or db is None:
        return
    getter = getattr(db, "get_profile_sync_info", None)
    if getter is None:
        return
    profile_key = username.strip().lower()
    info = await getter(profile_key)
    if not info:
        return
    if hasattr(target_service, "seed_from_sync_info"):
        try:
            target_service.seed_from_sync_info(info, now=now, profile_key=profile_key)
            return
        except (TypeError, ValueError, OverflowError):
            logger.info("Could not seed library refresh age for %s", username, exc_info=True)
            return
    keys = info.keys() if hasattr(info, "keys") else ()
    synced_dt = parse_sync_timestamp(info["last_synced_at"] if "last_synced_at" in keys else None)
    if synced_dt is None:
        return
    from datetime import datetime, timezone

    current_now = now or datetime.now(timezone.utc)
    if current_now.tzinfo is None:
        current_now = current_now.replace(tzinfo=timezone.utc)
    age_seconds = max(0.0, (current_now - synced_dt).total_seconds())
    game_count = int((info["game_count"] if "game_count" in keys else 0) or 0)
    target_service.seed_success(profile_key, age_seconds=age_seconds, game_count=game_count)


_COLLECTION_STATUS_PRECEDENCE = {
    "played": 4,
    "playing": 3,
    "backlog": 2,
    "wishlist": 1,
    "unknown": 0,
}
_PLAYING_SINGLE_PAGE_LIMIT = 70


def _entry_status_key(status: Any) -> str:
    raw = status.value if hasattr(status, "value") else str(status or "unknown")
    return raw.strip().lower() or "unknown"


def _merge_entry(existing: Any, incoming: Any) -> Any:
    cur_status = getattr(existing, "status", "unknown")
    new_status = getattr(incoming, "status", "unknown")
    cur_rank = _COLLECTION_STATUS_PRECEDENCE.get(_entry_status_key(cur_status), 0)
    new_rank = _COLLECTION_STATUS_PRECEDENCE.get(_entry_status_key(new_status), 0)
    primary, secondary = (incoming, existing) if new_rank > cur_rank else (existing, incoming)

    rating = getattr(primary, "rating", None)
    if rating is None:
        rating = getattr(secondary, "rating", None)
    is_liked = bool(getattr(primary, "is_liked", False) or getattr(secondary, "is_liked", False))
    cover_url = getattr(primary, "cover_url", None) or getattr(secondary, "cover_url", None)
    title = getattr(primary, "title", None) or getattr(secondary, "title", None) or getattr(primary, "slug", "")
    release_year = getattr(primary, "release_year", None)
    if release_year is None:
        release_year = getattr(secondary, "release_year", None)
    community_rating = getattr(primary, "community_rating", None)
    if community_rating is None:
        community_rating = getattr(secondary, "community_rating", None)

    if is_dataclass(primary):
        updates: dict[str, Any] = {
            "rating": rating,
            "is_liked": is_liked,
            "cover_url": cover_url,
            "title": title,
        }
        return replace(primary, **updates)
    return SimpleNamespace(
        slug=primary.slug,
        title=title,
        rating=rating,
        status=getattr(primary, "status", "unknown"),
        is_liked=is_liked,
        cover_url=cover_url,
        release_year=release_year,
        community_rating=community_rating,
    )


def _with_fallback_status(entry: Any, fallback_status: Optional[str]) -> Any:
    if not fallback_status:
        return entry
    if _entry_status_key(getattr(entry, "status", "unknown")) != "unknown":
        return entry
    if is_dataclass(entry):
        from backloggd.models.game import GameStatus
        try:
            return replace(entry, status=GameStatus(fallback_status))
        except ValueError:
            return entry
    return SimpleNamespace(
        slug=entry.slug,
        title=getattr(entry, "title", entry.slug),
        rating=getattr(entry, "rating", None),
        status=fallback_status,
        is_liked=bool(getattr(entry, "is_liked", False)),
        cover_url=getattr(entry, "cover_url", None),
        release_year=getattr(entry, "release_year", None),
        community_rating=getattr(entry, "community_rating", None),
    )


def compose_library_entries(
    played_entries,
    *,
    playing_entries=(),
    backlog_entries=(),
    wishlist_entries=(),
) -> list[Any]:
    """Merge Played/Playing/Backlog/Wishlist collections by slug deterministically."""
    merged: dict[str, Any] = {}
    for entries, default_status in (
        (played_entries, None),
        (playing_entries, "playing"),
        (backlog_entries, "backlog"),
        (wishlist_entries, "wishlist"),
    ):
        for raw_entry in entries:
            slug = getattr(raw_entry, "slug", None)
            if not slug:
                continue
            entry = _with_fallback_status(raw_entry, default_status)
            existing = merged.get(slug)
            merged[slug] = entry if existing is None else _merge_entry(existing, entry)
    return list(merged.values())


async def _fetch_complete_library(api: Any, username: str):
    """Fetch Played plus any exposed Playing/Backlog/Wishlist collections atomically."""
    get_playing = getattr(api, "get_user_playing", None)
    get_backlog = getattr(api, "get_user_backlog", None)
    get_wishlist = getattr(api, "get_user_wishlist", None)

    has_extra_collections = any(callable(fn) for fn in (get_playing, get_backlog, get_wishlist))
    if not has_extra_collections:
        return await api.get_user_library(username, force_refresh=True)

    names = ["played"]
    coros = [api.get_user_library(username, force_refresh=True)]
    if callable(get_playing):
        names.append("playing")
        coros.append(get_playing(username))
    if callable(get_backlog):
        names.append("backlog")
        coros.append(get_backlog(username))
    if callable(get_wishlist):
        names.append("wishlist")
        coros.append(get_wishlist(username))

    tasks = [asyncio.create_task(coro) for coro in coros]
    try:
        results = await asyncio.gather(*tasks)
    except BaseException:
        for task in tasks:
            if not task.done():
                task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        raise

    collected = dict(zip(names, results))
    playing_entries = collected.get("playing", ())
    if callable(get_playing) and len(playing_entries) >= _PLAYING_SINGLE_PAGE_LIMIT:
        raise RuntimeError(
            f"SDK get_user_playing returned {len(playing_entries)} entries "
            f"(single-page limit {_PLAYING_SINGLE_PAGE_LIMIT}) without pagination confirmation; "
            "refusing to mark potentially partial library complete"
        )

    return compose_library_entries(
        collected["played"],
        playing_entries=playing_entries,
        backlog_entries=collected.get("backlog", ()),
        wishlist_entries=collected.get("wishlist", ()),
    )


async def sync_profile_library(
    bot,
    username: str,
    *,
    discord_id: int = 0,
    synced_at=None,
) -> Optional[int]:
    """Fetch and atomically replace a linked profile's cached library."""
    profile_key = username.strip().lower()
    try:
        pending_getter = getattr(bot.db, "get_profile_refresh_pending_token", None)
        pending_token = await pending_getter(profile_key) if pending_getter is not None else None
        library = await _fetch_complete_library(bot.backloggd, username)
        replacer = getattr(bot.db, "replace_profile_library", None)
        if replacer is not None:
            kwargs = {"require_linked": True}
            if synced_at is not None:
                kwargs["synced_at"] = synced_at
            if pending_token is not None:
                kwargs["clear_pending_token"] = pending_token
            replaced = await replacer(username, library, **kwargs)
        else:
            replace_kwargs = {}
            if pending_token is not None:
                replace_kwargs["clear_pending_token"] = pending_token
            replaced = await bot.db.replace_user_library(discord_id, username, library, **replace_kwargs)
        if not replaced:
            logger.info("Discarded stale library sync for %s", username)
            return None
        logger.info("Synced library for %s (%d games)", username, len(library))
        return len(library)
    except asyncio.CancelledError:
        raise
    except Exception as error:
        logger.error("Error syncing library for %s: %s", username, error, exc_info=True)
        recorder = getattr(bot.db, "record_profile_sync_failure", None)
        if recorder is not None:
            try:
                kwargs = {"attempted_at": synced_at} if synced_at is not None else {}
                await recorder(username, str(error), **kwargs)
            except Exception:
                logger.warning("Could not persist library sync failure for %s", username, exc_info=True)
        return None


async def schedule_profile_refresh(
    bot,
    username: str,
    *,
    manual: bool = False,
    dirty: bool = False,
    freshness_window_seconds: Optional[float] = None,
    now=None,
    require_subscribed: bool = True,
) -> Optional[asyncio.Task]:
    """Coalesce a background full-library refresh through the shared LibrarySyncService."""
    service: Optional[LibrarySyncService] = getattr(bot, "library_sync", None)
    if service is None:
        return None
    profile_key = username.strip().lower()
    if require_subscribed and hasattr(bot.db, "is_profile_library_subscribed"):
        if not await bot.db.is_profile_library_subscribed(profile_key):
            return None
    await seed_profile_sync_state(bot, username, service=service, now=now)
    return service.request(
        profile_key,
        lambda: sync_profile_library(bot, username, synced_at=now),
        manual=manual,
        dirty=dirty,
        freshness_window_seconds=freshness_window_seconds,
    )
