"""Conservative first-discovery policy for activity feed announcements."""

from datetime import date, datetime, timezone
from collections.abc import Iterable


SESSION_ACTIONS = {"playing", "completed", "replay"}

FEED_DECISION_PRIORITY = {
    "suppressed_pre_activation": 0,
    "suppressed_old": 1,
    "held_ambiguous": 2,
    "held_future": 3,
    "held_uncertain": 4,
    "baseline": 5,
    "eligible": 6,
}


def group_event_aliases(alias_groups: Iterable[Iterable[str]]) -> list[list[str]]:
    """Group overlapping aliases transitively, retaining first-seen group order."""
    parents = {}

    def root(key):
        parents.setdefault(key, key)
        while parents[key] != key:
            parents[key] = parents[parents[key]]
            key = parents[key]
        return key

    for aliases in alias_groups:
        group = [eid for eid in aliases if eid]
        for eid in group:
            parents[root(eid)] = root(group[0])
    groups = {}
    for eid in parents:
        groups.setdefault(root(eid), []).append(eid)
    return list(groups.values())


def parse_utc_datetime(value: str | datetime | None) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        parsed = value
    else:
        raw = str(value).strip()
        if not raw:
            return None
        try:
            parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        except ValueError:
            return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _parse_utc_date(value: str | None) -> date | None:
    if not value:
        return None
    raw = str(value).strip()
    for pattern in ("%Y-%m-%d", "%B, %Y %d", "%B %Y %d"):
        try:
            return datetime.strptime(raw, pattern).date()
        except ValueError:
            continue
    parsed = parse_utc_datetime(raw)
    return parsed.date() if parsed is not None else None


def classify_feed_event(
    event,
    now: datetime,
    *,
    max_age_days: int = 7,
    activation_at: datetime | str | None = None,
    recovery_uncertain: bool = False,
) -> tuple[str, str, str | None]:
    """Return a stable decision, explanation, and UTC event date.

    Rating and review actions stay independent of a game's session date.
    Session timestamps compare by UTC calendar date, with an inclusive
    seven-day window. Missing, malformed, and future session evidence is held.
    Pre-activation actions discovered later (or actions with unprovable timing
    during incomplete recovery scans) are suppressed before delivery.
    """
    if now.tzinfo is None:
        now = now.replace(tzinfo=timezone.utc)
    today = now.astimezone(timezone.utc).date()

    parsed_activation = parse_utc_datetime(activation_at)
    occurred_at = parse_utc_datetime(getattr(event, "occurred_at", None))
    played_date = getattr(event, "played_date", None)
    evidence_date = _parse_utc_date(played_date if played_date else getattr(event, "occurred_at", None))
    evidence_iso = evidence_date.isoformat() if evidence_date is not None else None

    if parsed_activation is not None:
        if occurred_at is not None:
            if occurred_at < parsed_activation:
                return "suppressed_pre_activation", "action_occurred_before_feed_activation", evidence_iso
        elif recovery_uncertain:
            return "held_uncertain", "unprovable_action_time_during_recovery", evidence_iso

    action = getattr(event, "action_type", "")
    if action in {"rated", "reviewed"}:
        return "eligible", "explicit_rating_or_review", None

    labels = tuple(getattr(event, "session_labels", ()) or ())
    is_session = (
        action in SESSION_ACTIONS
        or bool(getattr(event, "is_replay", False))
        or any(label in {"Started", "Finished"} for label in labels)
    )
    if not is_session:
        return "eligible", "not_a_play_session", None

    if evidence_date is None:
        return "held_uncertain", "session_date_missing_or_unparseable", None
    if evidence_date > today:
        return "held_future", "session_date_is_in_the_future", evidence_iso
    age_days = (today - evidence_date).days
    if age_days > max_age_days:
        return "suppressed_old", "session_is_older_than_seven_utc_calendar_days", evidence_iso
    return "eligible", "session_is_within_seven_utc_calendar_days", evidence_iso
