"""Pure feed decision planning, independent of database and Discord operations."""

from __future__ import annotations

from collections.abc import Collection, Iterable, Mapping, Sequence
from dataclasses import asdict, dataclass
from datetime import datetime
import json
from typing import TYPE_CHECKING, Any

from bot.feed_policy import FEED_DECISION_PRIORITY, classify_feed_event, parse_utc_datetime

if TYPE_CHECKING:
    from bot.feed_models import FeedEvent


@dataclass(frozen=True)
class FeedCandidate:
    event: FeedEvent
    aliases: tuple[str, ...]
    already_tracked: bool
    already_baselined: bool
    needs_resolved_check: bool


@dataclass(frozen=True)
class FeedDecisionPlan:
    inserts: list[dict[str, Any]]
    revisions: list[dict[str, Any]]
    alias_groups: list[tuple[str, ...]]
    outcomes: list[tuple[FeedEvent, tuple[str, ...], bool, str]]
    decisions: dict[str, Any]


def prepare_feed_candidates(
    events: Iterable[FeedEvent],
    saved: Mapping[str, Any],
    *,
    resolved_event_ids: Collection[str] | None = None,
) -> list[FeedCandidate]:
    """Attach history/identity evidence without fetching or changing stored state."""
    event_entries = []
    for event in events:
        aliases = tuple(dict.fromkeys((event.activity_id, *(event.event_ids or ()))))
        rows = [saved[alias] for alias in aliases if alias in saved]
        already_tracked = any(
            row["message_id"] is not None
            or dict(row).get("delivery_state") in {"sent", "deleted", "pending", "unknown"}
            for row in rows
        )
        already_baselined = (not already_tracked) and any(
            row["message_id"] is None
            and dict(row).get("delivery_state", "baseline") == "baseline"
            for row in rows
        )
        needs_resolved_check = bool(
            resolved_event_ids and any(alias in resolved_event_ids for alias in aliases)
        )
        event_entries.append(FeedCandidate(
            event, aliases, already_tracked, already_baselined, needs_resolved_check,
        ))
    return event_entries


def feed_decision_aliases(candidates: Iterable[FeedCandidate]) -> list[str]:
    """Return the deduplicated aliases requiring stored decisions, in input order."""
    return list(dict.fromkeys(
        alias for candidate in candidates
        if not candidate.already_tracked or candidate.needs_resolved_check
        for alias in candidate.aliases
    ))


def _is_protected_saved_row(saved: dict, alias: str) -> bool:
    row = saved.get(alias)
    if row is None:
        return False
    return row["message_id"] is not None or dict(row).get("delivery_state") in {
        "sent", "deleted", "pending", "unknown"
    }


def _classify_resolved_candidate(
    event,
    discovery_time,
    *,
    activation_at=None,
    recovery_uncertain: bool = False,
    uncertain_reasons=None,
):
    uncertain_reasons = uncertain_reasons or {}
    decision, reason, event_date = classify_feed_event(
        event,
        discovery_time,
        activation_at=activation_at if event.source == "activity" else None,
        recovery_uncertain=recovery_uncertain if event.source == "activity" else False,
    )
    if event.activity_id in uncertain_reasons and decision != "suppressed_pre_activation":
        decision = "held_uncertain"
        reason = uncertain_reasons[event.activity_id]
    return decision, reason, event_date


def _plan_decision_for_entry(
    candidate, saved, now, *, activation_at=None, recovery_uncertain=False,
    uncertain_reasons=None, existing_decisions=(),
):
    event, aliases = candidate.event, candidate.aliases
    already_tracked = candidate.already_tracked
    already_baselined = candidate.already_baselined
    needs_resolved_check = candidate.needs_resolved_check
    if already_tracked:
        if not needs_resolved_check or not existing_decisions:
            return None
        held_to_resolve = [
            (alias, stored)
            for alias, stored in existing_decisions
            if stored["decision"] == "held_ambiguous"
            and not _is_protected_saved_row(saved, alias)
        ]
        if not held_to_resolve:
            return None
        discovery_time = parse_utc_datetime(held_to_resolve[0][1]["observed_at"]) or now
        decision, reason, event_date = _classify_resolved_candidate(
            event,
            discovery_time,
            activation_at=activation_at,
            recovery_uncertain=recovery_uncertain,
            uncertain_reasons=uncertain_reasons,
        )
        if decision == "eligible":
            reason = "resolved_by_stable_reference"
        non_ambig = [
            (alias, stored)
            for alias, stored in existing_decisions
            if stored["decision"] != "held_ambiguous"
        ]
        if non_ambig:
            _, stricter = min(
                non_ambig, key=lambda item: FEED_DECISION_PRIORITY.get(item[1]["decision"], 99)
            )
            if FEED_DECISION_PRIORITY.get(stricter["decision"], 99) < FEED_DECISION_PRIORITY.get(decision, 99):
                decision, reason, event_date = (
                    stricter["decision"], stricter["reason"], stricter["event_date"]
                )
        return {
            "decision": decision,
            "reason": reason,
            "event_date": event_date,
            "revisions": held_to_resolve,
            "allowed_from": ("eligible", "held_ambiguous"),
            "missing": [],
            "observed_at": held_to_resolve[0][1]["observed_at"],
        }

    if already_baselined:
        non_ambig = [
            (alias, stored)
            for alias, stored in existing_decisions
            if stored["decision"] != "held_ambiguous"
        ]
        strict_row = (
            min(non_ambig, key=lambda item: FEED_DECISION_PRIORITY.get(item[1]["decision"], 99))[1]
            if non_ambig
            else None
        )
        if (
            strict_row is not None
            and FEED_DECISION_PRIORITY.get(strict_row["decision"], 99) < FEED_DECISION_PRIORITY["baseline"]
        ):
            decision, reason, event_date = (
                strict_row["decision"], strict_row["reason"], strict_row["event_date"]
            )
        else:
            _, _, event_date = classify_feed_event(event, now)
            if existing_decisions:
                _, first_row = min(
                    existing_decisions,
                    key=lambda item: FEED_DECISION_PRIORITY.get(item[1]["decision"], 99),
                )
                event_date = first_row["event_date"] or event_date
            decision, reason = "baseline", "existing_source_baseline"
        revs = [
            (alias, stored)
            for alias, stored in existing_decisions
            if stored["decision"] in {"eligible", "held_ambiguous"}
            and not _is_protected_saved_row(saved, alias)
        ]
        missing = (
            [alias for alias in aliases if alias not in {a for a, _ in existing_decisions}]
            if len(existing_decisions) < len(aliases)
            else []
        )
        obs_at = (
            min(
                existing_decisions,
                key=lambda item: FEED_DECISION_PRIORITY.get(item[1]["decision"], 99),
            )[1]["observed_at"]
            if existing_decisions
            else now
        )
        return {
            "decision": decision,
            "reason": reason,
            "event_date": event_date,
            "revisions": revs,
            "allowed_from": ("eligible", "held_ambiguous"),
            "missing": missing,
            "observed_at": obs_at,
        }

    if existing_decisions:
        _, chosen = min(
            existing_decisions,
            key=lambda item: FEED_DECISION_PRIORITY.get(item[1]["decision"], 99),
        )
        decision = chosen["decision"]
        reason, event_date = chosen["reason"], chosen["event_date"]
        allowed_from = ("eligible",)
        if decision == "held_ambiguous":
            can_resolve = needs_resolved_check
            if can_resolve:
                discovery_time = parse_utc_datetime(chosen["observed_at"]) or now
                decision, reason, event_date = _classify_resolved_candidate(
                    event,
                    discovery_time,
                    activation_at=activation_at,
                    recovery_uncertain=recovery_uncertain,
                    uncertain_reasons=uncertain_reasons,
                )
                if decision == "eligible":
                    reason = "resolved_by_stable_reference"
                non_ambig = [
                    (alias, stored)
                    for alias, stored in existing_decisions
                    if stored["decision"] != "held_ambiguous"
                ]
                if non_ambig:
                    _, stricter = min(
                        non_ambig,
                        key=lambda item: FEED_DECISION_PRIORITY.get(item[1]["decision"], 99),
                    )
                    if FEED_DECISION_PRIORITY.get(stricter["decision"], 99) < FEED_DECISION_PRIORITY.get(decision, 99):
                        decision, reason, event_date = (
                            stricter["decision"], stricter["reason"], stricter["event_date"]
                        )
                allowed_from = ("eligible", "held_ambiguous")
        elif decision == "eligible":
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
        revs = [
            (alias, stored)
            for alias, stored in existing_decisions
            if stored["decision"] in allowed_from
            and not (
                stored["decision"] == "held_ambiguous"
                and _is_protected_saved_row(saved, alias)
            )
        ]
        missing = (
            [alias for alias in aliases if alias not in {a for a, _ in existing_decisions}]
            if len(existing_decisions) < len(aliases)
            else []
        )
        return {
            "decision": decision,
            "reason": reason,
            "event_date": event_date,
            "revisions": revs,
            "allowed_from": allowed_from,
            "missing": missing,
            "observed_at": chosen["observed_at"],
        }

    decision, reason, event_date = _classify_resolved_candidate(
        event,
        now,
        activation_at=activation_at,
        recovery_uncertain=recovery_uncertain,
        uncertain_reasons=uncertain_reasons,
    )
    return {
        "decision": decision,
        "reason": reason,
        "event_date": event_date,
        "revisions": [],
        "allowed_from": ("eligible",),
        "missing": list(aliases),
        "observed_at": now,
        "is_new": True,
    }


def plan_feed_decisions(
    candidates: Sequence[FeedCandidate],
    saved: Mapping[str, Any],
    stored_decisions: Mapping[str, Any],
    now: datetime,
    *,
    activation_at: datetime | str | None = None,
    recovery_uncertain: bool = False,
    uncertain_reasons: Mapping[str, str] | None = None,
) -> FeedDecisionPlan:
    """Plan inserts/revisions from supplied evidence; never mutate input rows.

    Request-local alias updates are ordered. The database must reconcile these
    plans against current rows atomically before new announcements are accepted.
    """
    uncertain_reasons = uncertain_reasons or {}
    decision_map = dict(stored_decisions)
    initially_stored = set(decision_map.keys())
    planned_inserts: dict[str, dict] = {}
    planned_revisions: dict[str, dict] = {}
    event_outcomes = []
    for candidate in candidates:
        event, aliases = candidate.event, candidate.aliases
        already_tracked = candidate.already_tracked
        decisions = [(alias, decision_map[alias]) for alias in aliases if alias in decision_map]
        plan = _plan_decision_for_entry(
            candidate, saved, now, activation_at=activation_at,
            recovery_uncertain=recovery_uncertain, uncertain_reasons=uncertain_reasons,
            existing_decisions=decisions,
        )
        if already_tracked:
            if plan and plan["revisions"]:
                payload = json.dumps(asdict(event), sort_keys=True)
                for alias, stored in plan["revisions"]:
                    decision_map[alias] = {
                        "source": stored["source"],
                        "event_date": plan["event_date"],
                        "decision": plan["decision"],
                        "reason": plan["reason"],
                        "payload": payload,
                        "observed_at": stored["observed_at"],
                    }
                    if alias in initially_stored:
                        planned_revisions[alias] = {
                            "event_id": alias,
                            "decision": plan["decision"],
                            "reason": plan["reason"],
                            "event_date": plan["event_date"],
                            "payload": payload,
                            "allowed_from": plan["allowed_from"],
                        }
            event_outcomes.append((event, aliases, True, "eligible"))
            continue

        decision = plan["decision"]
        reason = plan["reason"]
        event_date = plan["event_date"]
        if plan["revisions"] or plan["missing"]:
            payload = json.dumps(asdict(event), sort_keys=True)
            for alias, stored in plan["revisions"]:
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
                        "allowed_from": plan["allowed_from"],
                    }
                elif alias in planned_inserts:
                    planned_inserts[alias]["decision"] = decision
                    planned_inserts[alias]["reason"] = reason
                    planned_inserts[alias]["event_date"] = event_date
                    planned_inserts[alias]["payload"] = payload
            for alias in plan["missing"]:
                alias_source = (
                    event.source
                    if plan.get("is_new") and alias == event.activity_id
                    else ("journal" if str(alias).startswith("journal_") else event.source)
                )
                item = {
                    "event_id": alias,
                    "source": alias_source,
                    "event_date": event_date,
                    "decision": decision,
                    "reason": reason,
                    "payload": payload,
                    "observed_at": plan["observed_at"],
                }
                planned_inserts[alias] = item
                decision_map[alias] = item
        event_outcomes.append((event, aliases, False, decision))

    alias_groups = [
        candidate.aliases for candidate in candidates
        if not candidate.already_tracked or candidate.needs_resolved_check
    ]

    return FeedDecisionPlan(
        inserts=list(planned_inserts.values()),
        revisions=list(planned_revisions.values()),
        alias_groups=alias_groups,
        outcomes=event_outcomes,
        decisions=decision_map,
    )


def _final_alias_decision(aliases, authoritative, request_decisions, *, fallback=None):
    """Choose the strictest persisted alias decision, using request state for missing rows."""
    decisions = []
    for alias in aliases:
        row = authoritative.get(alias)
        if row is None:
            row = request_decisions.get(alias)
        if row is not None:
            decisions.append(row["decision"])
    if not decisions:
        return fallback
    return min(decisions, key=lambda decision: FEED_DECISION_PRIORITY.get(decision, 99))


def accept_feed_decisions(
    plan: FeedDecisionPlan, authoritative: Mapping[str, Any],
) -> list[FeedEvent]:
    """Use persisted alias decisions to filter new events, preserving archive edits.

    Baselines may pass for alias maintenance; they do not authorize a new post.
    Discord delivery retains its existing baseline/protected-state checks.
    """
    accepted = []
    for event, aliases, already_tracked, local_decision in plan.outcomes:
        if already_tracked:
            accepted.append(event)
            continue
        final_decision = _final_alias_decision(
            aliases,
            authoritative,
            plan.decisions,
            fallback=local_decision,
        )
        if final_decision in {"eligible", "baseline"}:
            accepted.append(event)
    return accepted
