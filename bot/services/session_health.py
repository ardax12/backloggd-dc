"""Upstream Backloggd session and challenge health tracking."""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any, Callable, Optional

from backloggd.exceptions import (
    ChallengeResponseError,
    RateLimitError,
    ResourceNotFoundError,
)

logger = logging.getLogger("backloggd.services.session_health")

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
    ):
        self._clock = clock or (lambda: datetime.now(timezone.utc))
        self._log = log or logger
        self._degraded: bool = False
        self._kind: Optional[str] = None
        self._since: Optional[datetime] = None
        self._last_error_at: Optional[datetime] = None
        self._failure_count: int = 0

    @property
    def degraded(self) -> bool:
        return self._degraded

    @property
    def kind(self) -> Optional[str]:
        return self._kind

    def record_failure(self, error: BaseException) -> Optional[str]:
        """Classify and record an upstream error; returns the episode kind if tripped."""
        kind = classify_upstream_error(error)
        if kind is None:
            return None

        now = self._clock()
        first_in_episode = (not self._degraded) or (self._kind != kind)
        self._degraded = True
        self._kind = kind
        if self._since is None:
            self._since = now
        self._last_error_at = now
        self._failure_count += 1

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
        return kind

    def record_success(self) -> bool:
        """Clear active degradation after a healthy fetch; returns True on recovery."""
        if not self._degraded:
            return False
        prior_kind = self._kind
        failures = self._failure_count
        self._degraded = False
        self._kind = None
        self._since = None
        self._last_error_at = None
        self._failure_count = 0
        self._log.info(
            "Upstream Backloggd session health recovered (was %s after %d error(s)).",
            prior_kind or "degraded",
            failures,
        )
        return True

    def get_status(self) -> dict[str, Any]:
        return {
            "degraded": self._degraded,
            "kind": self._kind,
            "since": self._since,
            "last_error_at": self._last_error_at,
            "failure_count": self._failure_count,
            "is_cookie_issue": self._kind in {"challenge", "auth"},
        }

    def get_health_metadata(self) -> dict[str, str]:
        """Return extra /health JSON fields when upstream scraping is degraded."""
        if not self._degraded:
            return {}
        return {"upstream": "degraded"}

    def format_feed_status_warning(self) -> Optional[str]:
        """Return a safe, redacted warning banner for /feed status when degraded."""
        if not self._degraded:
            return None
        if self._kind == "rate_limit":
            return (
                "⚠️ **Upstream warning:** Backloggd temporarily rate-limited recent feed requests. "
                "Remaining checks paused for this cycle and will resume automatically."
            )
        if self._kind == "challenge":
            return (
                "⚠️ **Upstream warning:** Backloggd challenged recent scraping requests "
                "(anti-bot check or stale `BACKLOGGD_SESSION`). Feed polling will resume automatically once access recovers."
            )
        return (
            "⚠️ **Upstream warning:** Backloggd rejected the active session credentials "
            "(`BACKLOGGD_SESSION` may need to be refreshed). Feed polling will resume once credentials recover."
        )
