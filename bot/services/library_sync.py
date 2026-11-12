"""Coalesce and bound full-library refreshes by Backloggd profile."""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass, field
from typing import Awaitable, Callable, Optional

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
    dirty_operation: Optional[SyncOperation] = None


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
        state.game_count = game_count

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
                    except Exception:
                        logger.exception("Library refresh failed for profile %s", profile_key)
                        outcome = SyncOutcome(status="failed", game_count=state.game_count)

                    if outcome.status == "success":
                        state.last_success_at = self._clock()
                        state.last_failure_at = None
                        state.game_count = outcome.game_count
                    elif outcome.status == "failed":
                        state.last_failure_at = self._clock()

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
