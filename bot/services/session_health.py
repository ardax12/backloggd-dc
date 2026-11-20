"""Upstream Backloggd session and challenge health tracking."""

from __future__ import annotations

import asyncio
import logging
import math
from datetime import datetime, timedelta, timezone
from typing import Any, Awaitable, Callable, Optional

from backloggd.exceptions import (
    ChallengeResponseError,
    RateLimitError,
    ResourceNotFoundError,
)

logger = logging.getLogger("backloggd.services.session_health")

DEFAULT_AUTH_BACKOFF_SCHEDULE: tuple[float, ...] = (300.0, 600.0, 1200.0, 1800.0)
DEFAULT_RATE_LIMIT_BACKOFF_SECONDS: float = 300.0
DEFAULT_MAX_BACKOFF_SECONDS: float = 1800.0

_RATE_LIMIT_MARKERS = (
    "rate limit",
    "rate-limit",
    "too many requests",
    "http 429",
    "status 429",
)
_CHALLENGE_MARKERS = (
    "challenge",
    "anubis",
    "cloudflare",
    "proof of work",
    "captcha",
)
_AUTH_COOKIE_MARKERS = (
    "unauthorized",
    "forbidden",
    "invalid cookie",
    "expired cookie",
    "cookie expired",
    "session cookie",
    "session expired",
    "not logged in",
    "login required",
    "sign in required",
    "authentication required",
    "http 401",
    "http 403",
    "status 401",
    "status 403",
    "_backloggd_session",
)


def _retry_after_seconds(error: BaseException) -> Optional[float]:
    value = getattr(error, "retry_after", None)
    if value is None:
        response = getattr(error, "response", None)
        headers = getattr(response, "headers", None) or getattr(error, "headers", None) or {}
        if hasattr(headers, "get"):
            value = headers.get("Retry-After", headers.get("retry-after"))
    if value is None or isinstance(value, bool):
        return None
    try:
        seconds = float(value)
    except (ValueError, TypeError):
        return None
    return seconds if math.isfinite(seconds) and seconds >= 0 else None


def classify_upstream_error(error: BaseException) -> Optional[str]:
    """Classify Backloggd challenge, rate-limit, or auth/cookie failures.

    Returns ``"challenge"``, ``"rate_limit"``, ``"auth"``, or ``None``.
    """
    if isinstance(error, ResourceNotFoundError):
        return None
    if isinstance(error, ChallengeResponseError):
        return "challenge"
    if isinstance(error, RateLimitError):
        return "rate_limit"

    status = getattr(error, "status", None)
    if status is None:
        status = getattr(getattr(error, "response", None), "status_code", None)
    if status == 429:
        return "rate_limit"
    if status in (401, 403):
        text_lower = str(error).lower()
        if any(marker in text_lower for marker in _CHALLENGE_MARKERS):
            return "challenge"
        return "auth"

    text_lower = " ".join(str(error).split()).lower()
    if any(marker in text_lower for marker in _RATE_LIMIT_MARKERS):
        return "rate_limit"
    if any(marker in text_lower for marker in _CHALLENGE_MARKERS):
        return "challenge"
    if any(marker in text_lower for marker in _AUTH_COOKIE_MARKERS):
        return "auth"
    return None


class UpstreamSessionHealth:
    """Tracks degraded upstream Backloggd session/challenge/rate-limit episodes."""

    def __init__(
        self,
        *,
        clock: Optional[Callable[[], datetime]] = None,
        log: Optional[logging.Logger] = None,
        auth_backoff_schedule: tuple[float, ...] = DEFAULT_AUTH_BACKOFF_SCHEDULE,
        rate_limit_backoff_seconds: float = DEFAULT_RATE_LIMIT_BACKOFF_SECONDS,
        max_backoff_seconds: float = DEFAULT_MAX_BACKOFF_SECONDS,
        alert_callback: Optional[Callable[[str, dict[str, Any]], Awaitable[None]]] = None,
    ):
        self._clock = clock or (lambda: datetime.now(timezone.utc))
        self._log = log or logger
        self._auth_backoff_schedule = tuple(max(0.0, float(step)) for step in auth_backoff_schedule)
        self._rate_limit_backoff_seconds = max(0.0, float(rate_limit_backoff_seconds))
        self._max_backoff_seconds = max(0.0, float(max_backoff_seconds))
        self._alert_callback = alert_callback
        self._alert_tasks: set[asyncio.Task] = set()
        self._degraded: bool = False
        self._kind: Optional[str] = None
        self._since: Optional[datetime] = None
        self._last_error_at: Optional[datetime] = None
        self._failure_count: int = 0
        self._next_retry_at: Optional[datetime] = None
        self._backoff_step: int = 0

    @property
    def degraded(self) -> bool:
        return self._degraded

    @property
    def kind(self) -> Optional[str]:
        return self._kind

    @property
    def next_retry_at(self) -> Optional[datetime]:
        return self._next_retry_at

    def _compute_backoff_seconds(self, kind: str, error: BaseException, step: int) -> float:
        if kind == "rate_limit":
            retry_after = _retry_after_seconds(error)
            delay = max(self._rate_limit_backoff_seconds, retry_after or 0.0)
            return min(self._max_backoff_seconds, max(0.0, delay))
        if not self._auth_backoff_schedule:
            return 0.0
        index = min(max(0, step - 1), len(self._auth_backoff_schedule) - 1)
        return min(self._max_backoff_seconds, self._auth_backoff_schedule[index])

    def _schedule_alert(self, event: str, details: dict[str, Any]) -> None:
        callback = self._alert_callback
        if callback is None:
            return
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return

        async def _run() -> None:
            try:
                await callback(event, details)
            except asyncio.CancelledError:
                raise
            except Exception:
                self._log.warning(
                    "Failed to dispatch upstream session health %s alert",
                    event,
                    exc_info=True,
                )

        task = loop.create_task(_run())
        self._alert_tasks.add(task)
        task.add_done_callback(self._alert_tasks.discard)

    async def close(self) -> None:
        """Cancel and drain any pending alert delivery tasks during shutdown."""
        pending = [task for task in list(self._alert_tasks) if not task.done()]
        for task in pending:
            task.cancel()
        if pending:
            await asyncio.gather(*pending, return_exceptions=True)
        self._alert_tasks.clear()

    def get_poll_gate(self, now: Optional[datetime] = None) -> tuple[bool, bool]:
        """Return ``(should_run, probe_only)`` for the next feed poll tick."""
        if not self._degraded:
            return True, False
        current = now or self._clock()
        if self._next_retry_at is not None and current < self._next_retry_at:
            return False, False
        return True, True

    def record_failure(self, error: BaseException) -> Optional[str]:
        """Classify and record an upstream error; returns the episode kind if tripped."""
        kind = classify_upstream_error(error)
        if kind is None:
            return None

        now = self._clock()
        was_degraded = self._degraded
        first_in_episode = (not was_degraded) or (self._kind != kind)
        self._degraded = True
        self._kind = kind
        if self._since is None:
            self._since = now
        self._last_error_at = now
        self._failure_count += 1

        if not was_degraded:
            self._backoff_step = 1
            delay = self._compute_backoff_seconds(kind, error, self._backoff_step)
            self._next_retry_at = now + timedelta(seconds=delay)
        elif self._next_retry_at is None or now >= self._next_retry_at:
            self._backoff_step += 1
            delay = self._compute_backoff_seconds(kind, error, self._backoff_step)
            self._next_retry_at = now + timedelta(seconds=delay)
        elif kind == "rate_limit":
            delay = self._compute_backoff_seconds(kind, error, self._backoff_step)
            candidate_retry = now + timedelta(seconds=delay)
            if candidate_retry > self._next_retry_at:
                self._next_retry_at = candidate_retry

        if first_in_episode:
            if kind == "rate_limit":
                self._log.warning(
                    "Backloggd rate-limited feed requests (%s); halting remaining profile fetches for this poll tick.",
                    type(error).__name__,
                )
            elif kind == "challenge":
                self._log.warning(
                    "Backloggd returned an anti-bot challenge (%s); halting remaining profile fetches for this poll tick. "
                    "Check or update BACKLOGGD_SESSION if this persists.",
                    type(error).__name__,
                )
            else:
                self._log.warning(
                    "Backloggd rejected session/auth credentials (%s); halting remaining profile fetches for this poll tick. "
                    "Verify or rotate BACKLOGGD_SESSION if this persists.",
                    type(error).__name__,
                )

        if not was_degraded:
            self._schedule_alert(
                "degraded",
                {
                    "kind": kind,
                    "error_type": type(error).__name__,
                    "since": self._since,
                    "next_retry_at": self._next_retry_at,
                    "failure_count": self._failure_count,
                    "is_cookie_issue": kind in {"challenge", "auth"},
                },
            )
        return kind

    def record_success(self) -> bool:
        """Clear active degradation after a healthy fetch; returns True on recovery."""
        if not self._degraded:
            return False
        prior_kind = self._kind
        failures = self._failure_count
        since = self._since
        self._degraded = False
        self._kind = None
        self._since = None
        self._last_error_at = None
        self._failure_count = 0
        self._next_retry_at = None
        self._backoff_step = 0
        self._log.info(
            "Upstream Backloggd session health recovered (was %s after %d error(s)).",
            prior_kind or "degraded",
            failures,
        )
        self._schedule_alert(
            "recovered",
            {
                "kind": prior_kind,
                "since": since,
                "failure_count": failures,
                "is_cookie_issue": prior_kind in {"challenge", "auth"},
            },
        )
        return True

    def get_status(self) -> dict[str, Any]:
        backoff_active = bool(
            self._degraded
            and self._next_retry_at is not None
            and self._clock() < self._next_retry_at
        )
        return {
            "degraded": self._degraded,
            "kind": self._kind,
            "since": self._since,
            "last_error_at": self._last_error_at,
            "failure_count": self._failure_count,
            "is_cookie_issue": self._kind in {"challenge", "auth"},
            "backoff_active": backoff_active,
            "next_retry_at": self._next_retry_at,
        }

    def get_health_metadata(self) -> dict[str, str]:
        """Return extra /health JSON fields when upstream scraping is degraded."""
        if not self._degraded:
            return {}
        metadata = {"upstream": "degraded"}
        if self._kind:
            metadata["upstream_kind"] = self._kind
        return metadata

    def format_feed_status_warning(self) -> Optional[str]:
        """Return a safe, redacted warning banner for /feed status when degraded."""
        if not self._degraded:
            return None
        retry_suffix = ""
        if self._next_retry_at is not None and self._clock() < self._next_retry_at:
            retry_suffix = f" Next automatic check <t:{int(self._next_retry_at.timestamp())}:R>."
        if self._kind == "rate_limit":
            return (
                "⚠️ **Upstream warning:** Backloggd temporarily rate-limited recent feed requests. "
                "Remaining checks paused for this cycle and will resume automatically."
                f"{retry_suffix}"
            )
        if self._kind == "challenge":
            return (
                "⚠️ **Upstream warning:** Backloggd challenged recent scraping requests "
                "(anti-bot check or stale `BACKLOGGD_SESSION`). Feed polling will resume automatically once access recovers."
                f"{retry_suffix}"
            )
        return (
            "⚠️ **Upstream warning:** Backloggd rejected the active session credentials "
            "(`BACKLOGGD_SESSION` may need to be refreshed). Feed polling will resume once credentials recover."
            f"{retry_suffix}"
        )


def report_upstream_failure(bot: Any, error: BaseException) -> Optional[str]:
    """Record an upstream scraping failure on ``bot.session_health`` if present."""
    health = getattr(bot, "session_health", None)
    if health is None or not callable(getattr(health, "record_failure", None)):
        return None
    return health.record_failure(error)


def report_upstream_success(bot: Any) -> bool:
    """Record a healthy upstream scraping response on ``bot.session_health`` if present."""
    health = getattr(bot, "session_health", None)
    if health is None or not callable(getattr(health, "record_success", None)):
        return False
    return bool(health.record_success())
