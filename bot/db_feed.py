"""Feed reconciliation, source health, decision, delivery, and observation helpers."""

import json
from datetime import datetime, timedelta, timezone
from typing import Any, Iterable, List, Optional

from bot.db_library import iter_bounded_row_batches as _iter_bounded_row_batches
from bot.db_stats import iter_sql_chunks as _iter_sql_chunks
from bot.feed_policy import FEED_DECISION_PRIORITY, group_event_aliases

CLEAR_UNSENT_FEED_DECISIONS_SQL = """
    DELETE FROM profile_feed_decisions
    WHERE guild_id = ? AND profile_key = ?
      AND decision IN ('baseline', 'eligible')
      AND event_id NOT IN (
          SELECT event_id FROM profile_feed_messages
          WHERE guild_id = ? AND profile_key = ?
            AND delivery_state IN ('sent', 'deleted', 'pending', 'unknown')
      )
"""


async def deactivate_guild_profile_tx(db: Any, tx: Any, guild_id: int, profile_key: str) -> None:
    await tx.execute(
        """UPDATE guild_profile_feed_activity SET active = 0
           WHERE guild_id = ? AND profile_key = ?""",
        (guild_id, profile_key),
    )
    await tx.execute(
        "DELETE FROM profile_feed_sources WHERE guild_id = ? AND profile_key = ?",
        (guild_id, profile_key),
    )
    await tx.execute(
        "DELETE FROM guild_profile_feed_recipient_gaps WHERE guild_id = ? AND profile_key = ?",
        (guild_id, profile_key),
    )
    await tx.execute(
        db._CLEAR_UNSENT_FEED_DECISIONS_SQL,
        (guild_id, profile_key, guild_id, profile_key),
    )


async def prepare_profile_source_addition_tx(
    db: Any,
    tx: Any,
    guild_id: int,
    profile_key: str,
    *,
    present_member_ids: Optional[Iterable[int]] = None,
) -> None:
    normalized_key = profile_key.strip().lower()
    act_row = await tx.fetchone(
        "SELECT active FROM guild_profile_feed_activity WHERE guild_id = ? AND profile_key = ?",
        (guild_id, normalized_key),
    )
    was_active = bool(act_row and act_row["active"])
    if not was_active:
        await tx.execute(
            "DELETE FROM guild_profile_feed_reconciliation WHERE guild_id = ? AND profile_key = ?",
            (guild_id, normalized_key),
        )
        return

    pending_row = await tx.fetchone(
        """SELECT 1 FROM guild_profile_feed_reconciliation
           WHERE guild_id = ? AND profile_key = ?""",
        (guild_id, normalized_key),
    )
    if pending_row is not None:
        if present_member_ids is not None:
            await db._resolve_pending_reconciliation_tx(
                tx, guild_id, present_member_ids, profile_key=normalized_key,
            )
        return

    has_admin = (
        await tx.fetchone(
            "SELECT 1 FROM guild_admin_follows WHERE guild_id = ? AND profile_key = ? LIMIT 1",
            (guild_id, normalized_key),
        )
    ) is not None
    if has_admin:
        return

    prior_rows = await tx.fetchall(
        """SELECT u.discord_id FROM linked_users AS u
           JOIN guild_feed_preferences AS pref ON pref.discord_id = u.discord_id
           WHERE u.profile_key = ? AND pref.guild_id = ? AND pref.enabled = 1
           ORDER BY u.discord_id""",
        (normalized_key, guild_id),
    )
    prior_member_ids = [row["discord_id"] for row in prior_rows]

    if present_member_ids is not None:
        present_set = {int(value) for value in present_member_ids}
        if not any(mid in present_set for mid in prior_member_ids):
            await db._deactivate_guild_profile_tx(tx, guild_id, normalized_key)
        return

    if not prior_member_ids:
        await db._deactivate_guild_profile_tx(tx, guild_id, normalized_key)
        return

    await tx.execute(
        """INSERT INTO guild_profile_feed_reconciliation
           (guild_id, profile_key, had_admin_source, candidate_member_ids)
           VALUES (?, ?, 0, ?)
           ON CONFLICT(guild_id, profile_key) DO NOTHING""",
        (guild_id, normalized_key, json.dumps(prior_member_ids)),
    )


async def prepare_relinked_user_preferences_tx(
    db: Any, tx: Any, discord_id: int, profile_key: str
) -> None:
    normalized_key = profile_key.strip().lower()
    rows = await tx.fetchall(
        """SELECT pref.guild_id
           FROM guild_feed_preferences AS pref
           JOIN guild_profile_feed_activity AS act
             ON act.guild_id = pref.guild_id AND act.profile_key = ?
           WHERE pref.discord_id = ? AND pref.enabled = 1 AND act.active = 1
           ORDER BY pref.guild_id""",
        (normalized_key, discord_id),
    )
    for row in rows:
        await db._prepare_profile_source_addition_tx(
            tx, row["guild_id"], normalized_key, present_member_ids=None,
        )


async def resolve_pending_reconciliation_tx(
    db: Any,
    tx: Any,
    guild_id: int,
    present_member_ids: Iterable[int],
    *,
    profile_key: Optional[str] = None,
) -> list[str]:
    present_set = {int(value) for value in present_member_ids}
    if profile_key is not None:
        rows = await tx.fetchall(
            """SELECT profile_key, had_admin_source, candidate_member_ids
               FROM guild_profile_feed_reconciliation
               WHERE guild_id = ? AND profile_key = ?""",
            (guild_id, profile_key),
        )
    else:
        rows = await tx.fetchall(
            """SELECT profile_key, had_admin_source, candidate_member_ids
               FROM guild_profile_feed_reconciliation
               WHERE guild_id = ? ORDER BY profile_key""",
            (guild_id,),
        )
    deactivated: list[str] = []
    for row in rows:
        pkey = row["profile_key"]
        had_admin = bool(row["had_admin_source"])
        candidate_ids = [int(value) for value in json.loads(row["candidate_member_ids"] or "[]")]
        has_continuous_admin = False
        if had_admin:
            has_continuous_admin = (
                await tx.fetchone(
                    "SELECT 1 FROM guild_admin_follows WHERE guild_id = ? AND profile_key = ? LIMIT 1",
                    (guild_id, pkey),
                )
            ) is not None
        has_continuous_member = False
        present_candidates = [cid for cid in candidate_ids if cid in present_set]
        if present_candidates and not has_continuous_admin:
            for chunk in _iter_sql_chunks(present_candidates, fixed_params=2):
                placeholders = ",".join("?" for _ in chunk)
                found = await tx.fetchone(
                    f"""SELECT 1 FROM linked_users AS u
                        JOIN guild_feed_preferences AS pref ON pref.discord_id = u.discord_id
                        WHERE pref.guild_id = ? AND pref.enabled = 1
                          AND u.profile_key = ?
                          AND u.discord_id IN ({placeholders})
                        LIMIT 1""",
                    [guild_id, pkey, *chunk],
                )
                if found is not None:
                    has_continuous_member = True
                    break
        if not has_continuous_admin and not has_continuous_member:
            await db._deactivate_guild_profile_tx(tx, guild_id, pkey)
            deactivated.append(pkey)
        await tx.execute(
            "DELETE FROM guild_profile_feed_reconciliation WHERE guild_id = ? AND profile_key = ?",
            (guild_id, pkey),
        )
    return deactivated


async def reconcile_removed_profile_source_tx(
    db: Any,
    tx: Any,
    guild_id: int,
    profile_key: str,
    *,
    present_member_ids: Optional[Iterable[int]] = None,
    removed_discord_id: Optional[int] = None,
    removed_admin: bool = False,
) -> dict[str, Any]:
    act_row = await tx.fetchone(
        "SELECT active FROM guild_profile_feed_activity WHERE guild_id = ? AND profile_key = ?",
        (guild_id, profile_key),
    )
    was_active = bool(act_row and act_row["active"])
    has_admin = (
        await tx.fetchone(
            "SELECT 1 FROM guild_admin_follows WHERE guild_id = ? AND profile_key = ? LIMIT 1",
            (guild_id, profile_key),
        )
    ) is not None
    stored_member_rows = await tx.fetchall(
        """SELECT u.discord_id FROM linked_users AS u
           JOIN guild_feed_preferences AS pref ON pref.discord_id = u.discord_id
           WHERE u.profile_key = ? AND pref.guild_id = ? AND pref.enabled = 1
           ORDER BY u.discord_id""",
        (profile_key, guild_id),
    )
    stored_member_ids = [row["discord_id"] for row in stored_member_rows]
    stored_member_set = set(stored_member_ids)

    if present_member_ids is not None:
        present_set = {int(value) for value in present_member_ids}
        present_members = [mid for mid in stored_member_ids if mid in present_set]
        if has_admin or present_members:
            return {
                "profile_key": profile_key,
                "deactivated": False,
                "pending_reconciliation": False,
                "has_admin": has_admin,
                "remaining_members": present_members,
            }
        if was_active:
            await db._deactivate_guild_profile_tx(tx, guild_id, profile_key)
        return {
            "profile_key": profile_key,
            "deactivated": True,
            "pending_reconciliation": False,
            "has_admin": False,
            "remaining_members": [],
        }

    pending_row = await tx.fetchone(
        """SELECT had_admin_source, candidate_member_ids
           FROM guild_profile_feed_reconciliation
           WHERE guild_id = ? AND profile_key = ?""",
        (guild_id, profile_key),
    )
    if not was_active:
        await tx.execute(
            "DELETE FROM guild_profile_feed_reconciliation WHERE guild_id = ? AND profile_key = ?",
            (guild_id, profile_key),
        )
        if not has_admin and not stored_member_ids:
            return {
                "profile_key": profile_key,
                "deactivated": True,
                "pending_reconciliation": False,
                "has_admin": False,
                "remaining_members": [],
            }
        return {
            "profile_key": profile_key,
            "deactivated": False,
            "pending_reconciliation": not has_admin and bool(stored_member_ids),
            "has_admin": has_admin,
            "remaining_members": stored_member_ids,
        }

    if pending_row is not None:
        still_prior_admin = bool(pending_row["had_admin_source"]) and not removed_admin and has_admin
        narrowed_candidates = [
            int(cid)
            for cid in json.loads(pending_row["candidate_member_ids"] or "[]")
            if int(cid) != removed_discord_id and int(cid) in stored_member_set
        ]
        if not still_prior_admin and not narrowed_candidates:
            if present_member_ids is not None or (not stored_member_ids and not has_admin):
                await db._deactivate_guild_profile_tx(tx, guild_id, profile_key)
                await tx.execute(
                    "DELETE FROM guild_profile_feed_reconciliation WHERE guild_id = ? AND profile_key = ?",
                    (guild_id, profile_key),
                )
                return {
                    "profile_key": profile_key,
                    "deactivated": True,
                    "pending_reconciliation": False,
                    "has_admin": False,
                    "remaining_members": [],
                }
            await tx.execute(
                """UPDATE guild_profile_feed_reconciliation
                   SET had_admin_source = 0, candidate_member_ids = '[]'
                   WHERE guild_id = ? AND profile_key = ?""",
                (guild_id, profile_key),
            )
            return {
                "profile_key": profile_key,
                "deactivated": False,
                "pending_reconciliation": True,
                "has_admin": False,
                "remaining_members": [],
            }
        await tx.execute(
            """UPDATE guild_profile_feed_reconciliation
               SET had_admin_source = ?, candidate_member_ids = ?
               WHERE guild_id = ? AND profile_key = ?""",
            (still_prior_admin, json.dumps(narrowed_candidates), guild_id, profile_key),
        )
        return {
            "profile_key": profile_key,
            "deactivated": False,
            "pending_reconciliation": not still_prior_admin,
            "has_admin": still_prior_admin,
            "remaining_members": narrowed_candidates,
        }

    if not has_admin and not stored_member_ids:
        await db._deactivate_guild_profile_tx(tx, guild_id, profile_key)
        return {
            "profile_key": profile_key,
            "deactivated": True,
            "pending_reconciliation": False,
            "has_admin": False,
            "remaining_members": [],
        }
    if has_admin:
        return {
            "profile_key": profile_key,
            "deactivated": False,
            "pending_reconciliation": False,
            "has_admin": True,
            "remaining_members": stored_member_ids,
        }
    await tx.execute(
        """INSERT INTO guild_profile_feed_reconciliation
           (guild_id, profile_key, had_admin_source, candidate_member_ids)
           VALUES (?, ?, 0, ?)
           ON CONFLICT(guild_id, profile_key) DO UPDATE SET
               had_admin_source = excluded.had_admin_source,
               candidate_member_ids = excluded.candidate_member_ids""",
        (guild_id, profile_key, json.dumps(stored_member_ids)),
    )
    return {
        "profile_key": profile_key,
        "deactivated": False,
        "pending_reconciliation": True,
        "has_admin": False,
        "remaining_members": stored_member_ids,
    }


async def disable_guild_feed_preference_and_reconcile(
    db: Any,
    guild_id: int,
    discord_id: int,
    *,
    member_ids: Optional[Iterable[int]] = None,
) -> Optional[dict[str, Any]]:
    """Atomically opt a member out and deactivate/record reconciliation for their profile."""
    async with db._transaction() as tx:
        linked = await tx.fetchone(
            "SELECT profile_key FROM linked_users WHERE discord_id = ? LIMIT 1",
            (discord_id,),
        )
        if not linked:
            return None
        profile_key = linked["profile_key"]
        if member_ids is not None:
            await db._resolve_pending_reconciliation_tx(
                tx, guild_id, member_ids, profile_key=profile_key
            )
        await tx.execute(
            """INSERT INTO guild_feed_preferences (guild_id, discord_id, enabled, updated_at)
               VALUES (?, ?, ?, CURRENT_TIMESTAMP)
               ON CONFLICT(guild_id, discord_id) DO UPDATE SET
                   enabled = excluded.enabled, updated_at = CURRENT_TIMESTAMP""",
            (guild_id, discord_id, False),
        )
        return await db._reconcile_removed_profile_source_tx(
            tx,
            guild_id,
            profile_key,
            present_member_ids=member_ids,
            removed_discord_id=discord_id,
        )


async def remove_guild_admin_follow_and_reconcile(
    db: Any,
    guild_id: int,
    username: str,
    *,
    member_ids: Optional[Iterable[int]] = None,
) -> dict[str, Any]:
    """Atomically remove a guild admin follow and reconcile that profile's feed state."""
    profile_key = username.strip().lower()
    async with db._transaction() as tx:
        exists = await tx.fetchone(
            "SELECT 1 FROM guild_admin_follows WHERE guild_id = ? AND profile_key = ? LIMIT 1",
            (guild_id, profile_key),
        )
        if exists is None:
            return {
                "removed": False,
                "profile_key": profile_key,
                "deactivated": False,
                "pending_reconciliation": False,
                "has_admin": False,
                "remaining_members": [],
            }
        if member_ids is not None:
            await db._resolve_pending_reconciliation_tx(
                tx, guild_id, member_ids, profile_key=profile_key
            )
        deleted = await tx.execute(
            "DELETE FROM guild_admin_follows WHERE guild_id = ? AND profile_key = ?",
            (guild_id, profile_key),
        )
        reconciled = await db._reconcile_removed_profile_source_tx(
            tx,
            guild_id,
            profile_key,
            present_member_ids=member_ids,
            removed_admin=True,
        )
        return {"removed": deleted > 0, **reconciled}


async def reconcile_guild_feed_activity(
    db: Any,
    guild_id: int,
    eligible_profile_keys: Iterable[str],
    *,
    now: Optional[Any] = None,
    present_member_ids: Optional[Iterable[int]] = None,
) -> dict[str, list[str]]:
    """Pause departed profiles and fresh-baseline profiles that resume atomically."""
    eligible = {key.strip().lower() for key in eligible_profile_keys if key}
    now_sql = db._format_utc_sql_timestamp(now)
    deactivated, activated = [], []
    async with db._transaction() as tx:
        pending_deactivated: set[str] = set()
        if present_member_ids is not None:
            pending_deactivated.update(
                await db._resolve_pending_reconciliation_tx(tx, guild_id, present_member_ids)
            )
        existing = await tx.fetchall(
            "SELECT profile_key, active FROM guild_profile_feed_activity WHERE guild_id = ?",
            (guild_id,),
        )
        states = {row["profile_key"]: bool(row["active"]) for row in existing}
        for profile_key in sorted(pending_deactivated - eligible):
            deactivated.append(profile_key)
        for profile_key, was_active in states.items():
            if was_active and profile_key not in eligible:
                await db._deactivate_guild_profile_tx(tx, guild_id, profile_key)
                if profile_key not in pending_deactivated:
                    deactivated.append(profile_key)
        for profile_key in sorted(eligible):
            if not states.get(profile_key, False):
                if now_sql is not None:
                    await tx.execute(
                        """INSERT INTO guild_profile_feed_activity
                           (guild_id, profile_key, active, last_active_at)
                           VALUES (?, ?, 1, ?)
                           ON CONFLICT(guild_id, profile_key) DO UPDATE SET
                               active = 1, last_active_at = excluded.last_active_at""",
                        (guild_id, profile_key, now_sql),
                    )
                else:
                    await tx.execute(
                        """INSERT INTO guild_profile_feed_activity
                           (guild_id, profile_key, active, last_active_at)
                           VALUES (?, ?, 1, CURRENT_TIMESTAMP)
                           ON CONFLICT(guild_id, profile_key) DO UPDATE SET
                               active = 1, last_active_at = CURRENT_TIMESTAMP""",
                        (guild_id, profile_key),
                    )
                await tx.execute(
                    "DELETE FROM profile_feed_sources WHERE guild_id = ? AND profile_key = ?",
                    (guild_id, profile_key),
                )
                await tx.execute(
                    db._CLEAR_UNSENT_FEED_DECISIONS_SQL,
                    (guild_id, profile_key, guild_id, profile_key),
                )
                activated.append(profile_key)
        return {"activated": activated, "deactivated": deactivated}


def empty_feed_status() -> dict[str, Any]:
    return {
        "active": False,
        "last_active_at": None,
        "pending_reconciliation": False,
        "pending_continuity": None,
        "recipient_gap": None,
        "has_attributed_recipient_gaps": False,
        "initialized_sources": [],
        "suppression_counts": {},
        "delivery_counts": {},
        "source_health": [],
    }


async def get_guild_feed_statuses(
    db: Any, guild_id: int, profile_keys: Iterable[str]
) -> dict[str, dict[str, Any]]:
    """Fetch feed status dicts for many profiles in bounded guild- and profile-scoped chunks."""
    normalized_keys = list(dict.fromkeys(
        str(key).strip().lower() for key in profile_keys if key and str(key).strip()
    ))
    if not normalized_keys:
        return {}

    results: dict[str, dict[str, Any]] = {
        key: db._empty_feed_status() for key in normalized_keys
    }

    for chunk in _iter_sql_chunks(normalized_keys, fixed_params=3):
        placeholders = ",".join("?" for _ in chunk)
        params = [guild_id, *chunk]
        activity_params = [guild_id, guild_id, guild_id, *chunk]

        activity_rows = await db._fetchall(
            f"""SELECT profile.profile_key, COALESCE(activity.active, 0) AS active,
                       activity.last_active_at,
                       pending.profile_key IS NOT NULL AS pending_reconciliation,
                       pending.had_admin_source AS pending_had_admin_source,
                       pending.candidate_member_ids AS pending_candidate_member_ids,
                       rg.gap_since AS recipient_gap_since,
                       rg.reason AS recipient_gap_reason,
                       rg.last_error AS recipient_gap_last_error,
                       EXISTS (
                           SELECT 1 FROM guild_profile_feed_recipient_gaps AS any_rg
                           WHERE any_rg.profile_key = profile.profile_key
                       ) AS has_attributed_recipient_gaps
                FROM profiles AS profile
                LEFT JOIN guild_profile_feed_activity AS activity
                  ON activity.guild_id = ? AND activity.profile_key = profile.profile_key
                LEFT JOIN guild_profile_feed_reconciliation AS pending
                  ON pending.guild_id = ? AND pending.profile_key = profile.profile_key
                LEFT JOIN guild_profile_feed_recipient_gaps AS rg
                  ON rg.guild_id = ? AND rg.profile_key = profile.profile_key
                WHERE profile.profile_key IN ({placeholders})""",
            activity_params,
        )
        for row in activity_rows:
            entry = results[row["profile_key"]]
            entry["active"] = bool(row["active"])
            entry["last_active_at"] = row["last_active_at"]
            pending_rec = bool(row["pending_reconciliation"])
            entry["pending_reconciliation"] = pending_rec
            if pending_rec:
                candidate_ids = json.loads(row["pending_candidate_member_ids"] or "[]")
                entry["pending_continuity"] = {
                    "had_admin_source": bool(row["pending_had_admin_source"]),
                    "candidate_member_count": len(candidate_ids),
                }
            entry["has_attributed_recipient_gaps"] = bool(row["has_attributed_recipient_gaps"])
            if row["recipient_gap_since"] is not None or row["recipient_gap_reason"] is not None:
                entry["recipient_gap"] = {
                    "gap_since": row["recipient_gap_since"],
                    "reason": row["recipient_gap_reason"],
                    "last_error": row["recipient_gap_last_error"],
                }

        source_rows = await db._fetchall(
            f"""SELECT profile_key, source
                FROM profile_feed_sources
                WHERE guild_id = ? AND profile_key IN ({placeholders})
                ORDER BY profile_key, source""",
            params,
        )
        for row in source_rows:
            results[row["profile_key"]]["initialized_sources"].append(row["source"])

        decision_rows = await db._fetchall(
            f"""SELECT profile_key, decision, COUNT(*) AS count
                FROM profile_feed_decisions
                WHERE guild_id = ? AND profile_key IN ({placeholders})
                GROUP BY profile_key, decision""",
            params,
        )
        for row in decision_rows:
            results[row["profile_key"]]["suppression_counts"][row["decision"]] = row["count"]

        delivery_rows = await db._fetchall(
            f"""SELECT profile_key, delivery_state, COUNT(*) AS count
                FROM profile_feed_messages
                WHERE guild_id = ? AND profile_key IN ({placeholders})
                GROUP BY profile_key, delivery_state""",
            params,
        )
        for row in delivery_rows:
            results[row["profile_key"]]["delivery_counts"][row["delivery_state"]] = row["count"]

    for chunk in _iter_sql_chunks(normalized_keys, fixed_params=0):
        placeholders = ",".join("?" for _ in chunk)
        health_rows = await db._fetchall(
            f"""SELECT profile_key, source, attempts, successes, last_attempt_at, last_success_at,
                       coverage, pages_read, last_continuous_boundary_event_id,
                       last_continuous_boundary_at, unresolved_gap_since,
                       unresolved_gap_until, gap_reason, last_error
                FROM profile_feed_source_health
                WHERE profile_key IN ({placeholders})
                ORDER BY profile_key, source""",
            chunk,
        )
        for row in health_rows:
            results[row["profile_key"]]["source_health"].append(row)

    return results


async def get_guild_profile_feed_status(db: Any, guild_id: int, profile_key: str) -> dict:
    normalized = str(profile_key).strip().lower()
    if not normalized:
        return db._empty_feed_status()
    statuses = await db.get_guild_feed_statuses(guild_id, [normalized])
    return statuses[normalized]


async def get_profile_feed_source(db: Any, profile_key: str, source: str) -> Optional[Any]:
    return await db._fetchone(
        """SELECT source, attempts, successes, last_attempt_at, last_success_at,
                  coverage, pages_read, last_continuous_boundary_event_id,
                  last_continuous_boundary_at, unresolved_gap_since,
                  unresolved_gap_until, gap_reason, last_error
           FROM profile_feed_source_health WHERE profile_key = ? AND source = ?""",
        (profile_key.strip().lower(), source),
    )


async def get_guild_profile_activation_at(db: Any, guild_id: int, profile_key: str) -> Optional[str]:
    row = await db._fetchone(
        """SELECT last_active_at FROM guild_profile_feed_activity
           WHERE guild_id = ? AND profile_key = ? AND active = 1""",
        (guild_id, profile_key.strip().lower()),
    )
    return row["last_active_at"] if row else None


async def record_feed_source_attempt(
    db: Any, profile_key: str, source: str, *, now: Optional[Any] = None,
) -> None:
    now_sql = db._now_utc_sql_timestamp(now)
    await db._execute(
        """INSERT INTO profile_feed_source_health
           (profile_key, source, attempts, last_attempt_at)
           VALUES (?, ?, 1, ?)
           ON CONFLICT(profile_key, source) DO UPDATE SET
               attempts = profile_feed_source_health.attempts + 1,
               last_attempt_at = excluded.last_attempt_at""",
        (profile_key.strip().lower(), source, now_sql),
    )


async def record_feed_source_success(
    db: Any,
    profile_key: str,
    source: str,
    *,
    coverage: str,
    pages_read: Optional[int],
    boundary_event_id: Optional[str] = None,
    defer_boundary_commit: bool = False,
    gap_until: Optional[Any] = None,
    now: Optional[Any] = None,
) -> None:
    closes_gap = coverage == "exhausted" or (coverage == "boundary_reached" and bool(boundary_event_id))
    preserves_gap = closes_gap or coverage == "latest_page"
    commit_boundary = bool(closes_gap and not defer_boundary_commit)
    now_sql = db._now_utc_sql_timestamp(now)
    # Event occurrence timestamps do not prove the endpoint of a coverage
    # gap. Keep the argument for compatible callers, but never persist it.
    _ = gap_until
    await db._execute(
        """INSERT INTO profile_feed_source_health
           (profile_key, source, attempts, successes, last_attempt_at, last_success_at,
            coverage, pages_read, last_continuous_boundary_event_id,
            last_continuous_boundary_at, unresolved_gap_since, unresolved_gap_until,
            gap_reason, last_error)
           VALUES (?, ?, 0, 1, NULL, ?, ?, ?,
                   CASE WHEN ? THEN ? ELSE NULL END,
                   CASE WHEN ? THEN ? ELSE NULL END,
                   NULL, NULL,
                   CASE WHEN ? THEN NULL WHEN ? THEN NULL ELSE ? END, NULL)
           ON CONFLICT(profile_key, source) DO UPDATE SET
               successes = profile_feed_source_health.successes + 1,
               last_success_at = excluded.last_success_at,
               coverage = excluded.coverage,
               pages_read = excluded.pages_read,
               last_continuous_boundary_event_id = CASE
                   WHEN ? THEN COALESCE(excluded.last_continuous_boundary_event_id, profile_feed_source_health.last_continuous_boundary_event_id)
                   ELSE profile_feed_source_health.last_continuous_boundary_event_id
               END,
               last_continuous_boundary_at = CASE
                   WHEN ? THEN excluded.last_continuous_boundary_at
                   ELSE profile_feed_source_health.last_continuous_boundary_at
               END,
               unresolved_gap_since = CASE
                   WHEN ? THEN NULL
                   WHEN ? THEN profile_feed_source_health.unresolved_gap_since
                   ELSE COALESCE(profile_feed_source_health.unresolved_gap_since,
                                 profile_feed_source_health.last_continuous_boundary_at)
               END,
               unresolved_gap_until = NULL,
               gap_reason = CASE
                   WHEN ? THEN NULL
                   WHEN ? THEN profile_feed_source_health.gap_reason
                   ELSE COALESCE(profile_feed_source_health.gap_reason, ?)
               END,
               last_error = NULL""",
        (
            profile_key.strip().lower(),
            source,
            now_sql,
            coverage,
            pages_read,
            commit_boundary,
            boundary_event_id,
            commit_boundary,
            now_sql,
            commit_boundary,
            preserves_gap,
            coverage,
            commit_boundary,
            commit_boundary,
            commit_boundary,
            preserves_gap,
            commit_boundary,
            preserves_gap,
            coverage,
        ),
    )


async def commit_feed_source_boundary(
    db: Any,
    profile_key: str,
    source: str,
    *,
    coverage: str,
    boundary_event_id: Optional[str] = None,
    now: Optional[Any] = None,
) -> None:
    """Advance continuous boundary and clear unresolved gaps after delivery finishes."""
    closes_gap = coverage == "exhausted" or (coverage == "boundary_reached" and bool(boundary_event_id))
    if not closes_gap:
        return
    now_sql = db._now_utc_sql_timestamp(now)
    await db._execute(
        """UPDATE profile_feed_source_health
           SET last_continuous_boundary_event_id = COALESCE(?, last_continuous_boundary_event_id),
               last_continuous_boundary_at = ?,
               unresolved_gap_since = NULL,
               unresolved_gap_until = NULL,
               gap_reason = NULL
           WHERE profile_key = ? AND source = ?""",
        (boundary_event_id, now_sql, profile_key.strip().lower(), source),
    )


async def record_feed_source_failure(db: Any, profile_key: str, source: str, error: str) -> None:
    await db._execute(
        """INSERT INTO profile_feed_source_health
           (profile_key, source, attempts, last_attempt_at, last_error,
            unresolved_gap_since, unresolved_gap_until, gap_reason)
           VALUES (?, ?, 0, NULL, ?, NULL, NULL, NULL)
           ON CONFLICT(profile_key, source) DO UPDATE SET
               last_error = excluded.last_error,
               unresolved_gap_since = COALESCE(
                   profile_feed_source_health.unresolved_gap_since,
                   profile_feed_source_health.last_continuous_boundary_at
               ),
               unresolved_gap_until = NULL,
               gap_reason = CASE
                   WHEN profile_feed_source_health.gap_reason IS NOT NULL
                       THEN profile_feed_source_health.gap_reason
                   WHEN profile_feed_source_health.last_continuous_boundary_at IS NOT NULL
                       THEN 'source_failure'
                   ELSE NULL
               END""",
        (profile_key.strip().lower(), source, str(error)[:500]),
    )


async def get_pending_feed_observations(db: Any, guild_id: int, profile_key: str) -> List[Any]:
    """Return validated eligible events that have no terminal delivery record."""
    return await db._fetchall(
        """SELECT decision.event_id, decision.source, decision.payload
           FROM profile_feed_decisions AS decision
           WHERE decision.guild_id = ? AND decision.profile_key = ?
             AND decision.decision = 'eligible'
             AND NOT EXISTS (
                 SELECT 1 FROM profile_feed_messages AS message
                 WHERE message.guild_id = decision.guild_id
                   AND message.profile_key = decision.profile_key
                   AND message.event_id = decision.event_id
                   AND message.delivery_state IN ('baseline', 'sent', 'pending', 'unknown', 'deleted')
             )
           ORDER BY decision.observed_at, decision.event_id""",
        (guild_id, profile_key.strip().lower()),
    )


async def get_feed_event_decision(
    db: Any, guild_id: int, profile_key: str, event_id: str
) -> Optional[Any]:
    return await db._fetchone(
        """SELECT source, event_date, decision, reason, payload, observed_at
           FROM profile_feed_decisions
           WHERE guild_id = ? AND profile_key = ? AND event_id = ?""",
        (guild_id, profile_key.strip().lower(), event_id),
    )


async def get_feed_event_decisions(
    db: Any, guild_id: int, profile_key: str, event_ids: Iterable[str]
) -> dict[str, Any]:
    """Bulk fetch stored feed event decisions for a guild/profile/alias set."""
    normalized_key = profile_key.strip().lower()
    unique_ids = list(dict.fromkeys(event_id for event_id in event_ids if event_id))
    if not unique_ids:
        return {}
    decisions: dict[str, Any] = {}
    for chunk in _iter_sql_chunks(unique_ids, fixed_params=2):
        placeholders = ",".join("?" for _ in chunk)
        rows = await db._fetchall(
            f"""SELECT event_id, source, event_date, decision, reason, payload, observed_at
                FROM profile_feed_decisions
                WHERE guild_id = ? AND profile_key = ? AND event_id IN ({placeholders})""",
            [guild_id, normalized_key, *chunk],
        )
        for row in rows:
            decisions[row["event_id"]] = row
    return decisions


async def resolve_feed_event_decision(
    db: Any,
    guild_id: int,
    profile_key: str,
    event_id: str,
    payload: str,
    event_date: Optional[str],
    *,
    decision: str = "eligible",
    reason: str = "resolved_by_stable_reference",
) -> None:
    """Allow an ambiguous candidate to resolve only after strong identity evidence."""
    await db.persist_feed_identity_decisions(
        guild_id,
        profile_key,
        operations=[{
            "kind": "resolve",
            "event_id": event_id,
            "decision": decision,
            "reason": reason,
            "event_date": event_date,
            "payload": payload,
        }],
    )


async def persist_feed_identity_decisions(
    db: Any,
    guild_id: int,
    profile_key: str,
    *,
    operations: Iterable[dict[str, Any]],
) -> None:
    """Apply ordered ambiguity and strong-reference decisions in bounded SQL batches.

    Ambiguity is insert-once, so a retry keeps the first observation time. A
    stable-reference resolution only updates an existing ambiguous decision
    and cannot alter protected delivery/archive records.
    """
    normalized_key = profile_key.strip().lower()
    ordered_operations = []
    event_ids = []
    resolution_ids = set()
    for operation in operations:
        event_id = operation.get("event_id")
        kind = operation.get("kind")
        if not event_id or kind not in {"ambiguity", "resolve"}:
            continue
        ordered_operations.append(dict(operation))
        if event_id not in event_ids:
            event_ids.append(event_id)
        if kind == "resolve":
            resolution_ids.add(event_id)
    if not ordered_operations:
        return

    async with db._transaction() as tx:
        current_rows: dict[str, Any] = {}
        for chunk in _iter_sql_chunks(event_ids, fixed_params=2):
            placeholders = ",".join("?" for _ in chunk)
            rows = await tx.fetchall(
                f"""SELECT event_id, source, event_date, decision, reason, payload, observed_at
                    FROM profile_feed_decisions
                    WHERE guild_id = ? AND profile_key = ? AND event_id IN ({placeholders})""",
                [guild_id, normalized_key, *chunk],
            )
            current_rows.update((row["event_id"], row) for row in rows)

        protected_ids: set[str] = set()
        for chunk in _iter_sql_chunks(sorted(resolution_ids), fixed_params=2):
            placeholders = ",".join("?" for _ in chunk)
            rows = await tx.fetchall(
                f"""SELECT event_id FROM profile_feed_messages
                    WHERE guild_id = ? AND profile_key = ? AND event_id IN ({placeholders})
                      AND (message_id IS NOT NULL
                           OR delivery_state IN ('sent', 'deleted', 'pending', 'unknown'))""",
                [guild_id, normalized_key, *chunk],
            )
            protected_ids.update(row["event_id"] for row in rows)

        states = {
            event_id: row["decision"] if row is not None else None
            for event_id, row in current_rows.items()
        }
        for event_id in event_ids:
            states.setdefault(event_id, None)
        inserts: dict[str, dict[str, Any]] = {}
        revisions: dict[str, dict[str, Any]] = {}

        # Replay request-local operations in order. This retains first-seen
        # timestamps while allowing later strong evidence in the same request
        # to resolve only a held ambiguous candidate.
        for operation in ordered_operations:
            event_id = operation["event_id"]
            if operation["kind"] == "ambiguity":
                if states[event_id] is not None:
                    continue
                inserts[event_id] = {
                    "event_id": event_id,
                    "source": operation["source"],
                    "event_date": operation.get("event_date"),
                    "decision": "held_ambiguous",
                    "reason": operation["reason"],
                    "payload": operation["payload"],
                    "observed_at": db._format_utc_sql_timestamp(
                        operation.get("observed_at")
                    ),
                }
                states[event_id] = "held_ambiguous"
                continue

            if states[event_id] != "held_ambiguous" or event_id in protected_ids:
                continue
            resolved_decision = operation.get("decision") or "eligible"
            resolved_reason = operation.get("reason") or "resolved_by_stable_reference"
            if event_id in inserts:
                inserted = inserts[event_id]
                inserted.update({
                    "decision": resolved_decision,
                    "reason": resolved_reason,
                    "event_date": operation.get("event_date") or inserted["event_date"],
                    "payload": operation["payload"],
                })
            else:
                revisions[event_id] = {
                    "event_id": event_id,
                    "decision": resolved_decision,
                    "reason": resolved_reason,
                    "event_date": operation.get("event_date"),
                    "payload": operation["payload"],
                }
            states[event_id] = resolved_decision

        insert_rows = [
            (
                guild_id,
                normalized_key,
                item["event_id"],
                item["source"],
                item["event_date"],
                item["decision"],
                item["reason"],
                item["payload"],
                item["observed_at"],
            )
            for item in inserts.values()
        ]
        for batch in _iter_bounded_row_batches(
            insert_rows,
            params_per_row=9,
            fixed_params=0,
            fixed_sql_bytes=256,
        ):
            values_sql = ", ".join(
                ["(?, ?, ?, ?, ?, ?, ?, ?, COALESCE(?, CURRENT_TIMESTAMP))"] * len(batch)
            )
            await tx.execute(
                f"""INSERT OR IGNORE INTO profile_feed_decisions
                    (guild_id, profile_key, event_id, source, event_date, decision, reason, payload, observed_at)
                    VALUES {values_sql}""",
                [value for row in batch for value in row],
            )

        revision_rows = [
            (event_id, item["decision"], item["reason"], item["event_date"], item["payload"])
            for event_id, item in revisions.items()
        ]
        for batch in _iter_bounded_row_batches(
            revision_rows,
            params_per_row=5,
            fixed_params=2,
            fixed_sql_bytes=1024,
        ):
            values_sql = ", ".join(["(?, ?, ?, ?, ?)"] * len(batch))
            await tx.execute(
                f"""WITH resolutions(event_id, decision, reason, event_date, payload) AS (VALUES {values_sql})
                    UPDATE profile_feed_decisions
                    SET decision = (SELECT decision FROM resolutions
                                    WHERE resolutions.event_id = profile_feed_decisions.event_id),
                        reason = (SELECT reason FROM resolutions
                                  WHERE resolutions.event_id = profile_feed_decisions.event_id),
                        event_date = COALESCE(
                            (SELECT event_date FROM resolutions
                             WHERE resolutions.event_id = profile_feed_decisions.event_id),
                            profile_feed_decisions.event_date
                        ),
                        payload = (SELECT payload FROM resolutions
                                   WHERE resolutions.event_id = profile_feed_decisions.event_id)
                    WHERE guild_id = ? AND profile_key = ?
                      AND decision = 'held_ambiguous'
                      AND event_id IN (SELECT event_id FROM resolutions)
                      AND NOT EXISTS (
                          SELECT 1 FROM profile_feed_messages AS message
                          WHERE message.guild_id = profile_feed_decisions.guild_id
                            AND message.profile_key = profile_feed_decisions.profile_key
                            AND message.event_id = profile_feed_decisions.event_id
                            AND (message.message_id IS NOT NULL
                                 OR message.delivery_state IN ('sent', 'deleted', 'pending', 'unknown'))
                      )""",
                [value for row in batch for value in row] + [guild_id, normalized_key],
            )


async def has_feed_source(db: Any, guild_id: int, profile_key: str, source: str) -> bool:
    return await db._fetchone(
        "SELECT 1 FROM profile_feed_sources WHERE guild_id = ? AND profile_key = ? AND source = ?",
        (guild_id, profile_key.strip().lower(), source),
    ) is not None


async def record_feed_event_decision(
    db: Any,
    guild_id: int,
    profile_key: str,
    event_id: str,
    *,
    source: str,
    event_date: Optional[str],
    decision: str,
    reason: str,
    payload: str,
    observed_at: Optional[Any] = None,
) -> Any:
    """Persist the first decision; retries cannot age an event into a new post."""
    await db._execute(
        """INSERT OR IGNORE INTO profile_feed_decisions
           (guild_id, profile_key, event_id, source, event_date, decision, reason, payload, observed_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, COALESCE(?, CURRENT_TIMESTAMP))""",
        (guild_id, profile_key.strip().lower(), event_id, source, event_date,
         decision, reason, payload, db._format_utc_sql_timestamp(observed_at)),
    )
    return await db.get_feed_event_decision(guild_id, profile_key, event_id)


async def revise_unsent_eligible_feed_decision(
    db: Any,
    guild_id: int,
    profile_key: str,
    event_id: str,
    *,
    decision: str,
    reason: str,
    event_date: Optional[str],
    payload: str,
    allowed_from: Iterable[str] = ("eligible",),
) -> None:
    """Apply new evidence to unsent eligible/resolved work without altering archive decisions."""
    valid_from = tuple(dict.fromkeys(d for d in allowed_from if d in {"eligible", "held_ambiguous"}))
    if not valid_from:
        return
    placeholders = ",".join("?" for _ in valid_from)
    await db._execute(
        f"""UPDATE profile_feed_decisions
           SET decision = ?, reason = ?, event_date = ?, payload = ?
           WHERE guild_id = ? AND profile_key = ? AND event_id = ?
             AND decision IN ({placeholders})
             AND NOT EXISTS (
                 SELECT 1 FROM profile_feed_messages AS message
                 WHERE message.guild_id = profile_feed_decisions.guild_id
                   AND message.profile_key = profile_feed_decisions.profile_key
                   AND message.event_id = profile_feed_decisions.event_id
                   AND (message.message_id IS NOT NULL
                        OR message.delivery_state IN ('sent', 'deleted', 'pending', 'unknown'))
             )""",
        (decision, reason, event_date, payload, guild_id, profile_key.strip().lower(), event_id, *valid_from),
    )


async def persist_feed_event_decisions(
    db: Any,
    guild_id: int,
    profile_key: str,
    *,
    inserts: Iterable[dict[str, Any]] = (),
    revisions: Iterable[dict[str, Any]] = (),
    alias_groups: Iterable[Iterable[str]] = (),
) -> dict[str, Any]:
    """Atomically persist new alias decisions and conditional revisions with conflict reconciliation."""
    normalized_key = profile_key.strip().lower()
    insert_map: dict[str, dict[str, Any]] = {}
    for item in inserts:
        eid = item.get("event_id")
        if not eid:
            continue
        insert_map[eid] = {
            "event_id": eid,
            "source": item["source"],
            "event_date": item.get("event_date"),
            "decision": item["decision"],
            "reason": item["reason"],
            "payload": item["payload"],
            "observed_sql": db._format_utc_sql_timestamp(item.get("observed_at")),
        }

    revision_map: dict[str, dict[str, Any]] = {}
    for item in revisions:
        eid = item.get("event_id")
        if not eid:
            continue
        allowed = tuple(dict.fromkeys(item.get("allowed_from") or ("eligible",)))
        revision_map[eid] = {
            "event_id": eid,
            "event_date": item.get("event_date"),
            "decision": item["decision"],
            "reason": item["reason"],
            "payload": item["payload"],
            "allowed_from": allowed,
        }

    # Merge overlapping identities so a strict concurrent decision applies
    # to every alias in the connected event group in the same transaction.
    groups = group_event_aliases(alias_groups)
    all_ids = list(dict.fromkeys([
        *insert_map, *revision_map,
        *(eid for group in groups for eid in group),
    ]))
    if not all_ids:
        return {}

    priority = FEED_DECISION_PRIORITY
    async with db._transaction() as tx:
        current_rows: dict[str, Any] = {}
        for chunk in _iter_sql_chunks(all_ids, fixed_params=2):
            placeholders = ",".join("?" for _ in chunk)
            rows = await tx.fetchall(
                f"""SELECT event_id, source, event_date, decision, reason, payload, observed_at
                    FROM profile_feed_decisions
                    WHERE guild_id = ? AND profile_key = ? AND event_id IN ({placeholders})""",
                [guild_id, normalized_key, *chunk],
            )
            for row in rows:
                current_rows[row["event_id"]] = row

        for group in groups:
            candidates = []
            for eid in group:
                if eid in revision_map and eid in current_rows and current_rows[eid]["decision"] in revision_map[eid]["allowed_from"]:
                    candidates.append({**dict(current_rows[eid]), **revision_map[eid]})
                elif eid in current_rows:
                    candidates.append(dict(current_rows[eid]))
                if eid in insert_map:
                    candidates.append(insert_map[eid])
            if not candidates:
                continue
            chosen = min(candidates, key=lambda row: priority.get(row["decision"], 99))
            for eid in group:
                if eid in insert_map:
                    item = insert_map[eid]
                    if priority.get(chosen["decision"], 99) < priority.get(item["decision"], 99):
                        for field in ("decision", "reason", "event_date", "payload"):
                            item[field] = chosen[field]
                        item["observed_sql"] = chosen.get("observed_at", chosen.get("observed_sql"))
                cur = current_rows.get(eid)
                if cur is not None:
                    if cur["decision"] == "eligible" and priority.get(chosen["decision"], 99) < priority["eligible"]:
                        revision_map[eid] = {
                            **{field: chosen[field] for field in ("decision", "reason", "event_date", "payload")},
                            "event_id": eid,
                            "allowed_from": ("eligible",),
                        }
                    elif eid in revision_map and cur["decision"] in revision_map[eid]["allowed_from"]:
                        rev = revision_map[eid]
                        if priority.get(chosen["decision"], 99) < priority.get(rev["decision"], 99):
                            for field in ("decision", "reason", "event_date", "payload"):
                                rev[field] = chosen[field]

        to_insert_rows: list[tuple[Any, ...]] = []
        candidate_revisions: dict[str, dict[str, Any]] = {}

        for eid, item in insert_map.items():
            cur = current_rows.get(eid)
            if cur is None:
                to_insert_rows.append((
                    guild_id,
                    normalized_key,
                    eid,
                    item["source"],
                    item["event_date"],
                    item["decision"],
                    item["reason"],
                    item["payload"],
                    item["observed_sql"],
                ))
            elif cur["decision"] == "eligible":
                if priority.get(item["decision"], 99) < priority.get(cur["decision"], 99):
                    candidate_revisions[eid] = {
                        "decision": item["decision"],
                        "reason": item["reason"],
                        "event_date": item["event_date"],
                        "payload": item["payload"],
                        "allowed_from": ("eligible",),
                    }

        for eid, item in revision_map.items():
            cur = current_rows.get(eid)
            if cur is None or cur["decision"] not in item.get("allowed_from", ("eligible",)):
                continue
            if cur["decision"] == "eligible":
                if priority.get(item["decision"], 99) <= priority.get(cur["decision"], 99):
                    candidate_revisions[eid] = item
            elif item["decision"] != cur["decision"]:
                candidate_revisions[eid] = item

        eligible_update_rows: list[tuple[Any, ...]] = []
        held_update_rows: list[tuple[Any, ...]] = []
        if candidate_revisions:
            protected_ids: set[str] = set()
            for chunk in _iter_sql_chunks(list(candidate_revisions.keys()), fixed_params=2):
                placeholders = ",".join("?" for _ in chunk)
                rows = await tx.fetchall(
                    f"""SELECT event_id FROM profile_feed_messages
                        WHERE guild_id = ? AND profile_key = ? AND event_id IN ({placeholders})
                          AND (message_id IS NOT NULL
                               OR delivery_state IN ('sent', 'deleted', 'pending', 'unknown'))""",
                    [guild_id, normalized_key, *chunk],
                )
                protected_ids.update(row["event_id"] for row in rows)
            for eid, rev in candidate_revisions.items():
                if eid in protected_ids:
                    continue
                cur = current_rows[eid]
                row_tuple = (
                    guild_id,
                    normalized_key,
                    eid,
                    cur["source"],
                    rev["event_date"],
                    rev["decision"],
                    rev["reason"],
                    rev["payload"],
                    cur["observed_at"],
                )
                if cur["decision"] == "held_ambiguous":
                    held_update_rows.append(row_tuple)
                else:
                    eligible_update_rows.append(row_tuple)

        for batch in _iter_bounded_row_batches(
            to_insert_rows,
            params_per_row=9,
            fixed_params=0,
            fixed_sql_bytes=256,
        ):
            values_sql = ", ".join(["(?, ?, ?, ?, ?, ?, ?, ?, COALESCE(?, CURRENT_TIMESTAMP))"] * len(batch))
            flat_params = [value for row in batch for value in row]
            await tx.execute(
                f"""INSERT OR IGNORE INTO profile_feed_decisions
                    (guild_id, profile_key, event_id, source, event_date, decision, reason, payload, observed_at)
                    VALUES {values_sql}""",
                flat_params,
            )

        for source_state, target_rows in (
            ("eligible", eligible_update_rows),
            ("held_ambiguous", held_update_rows),
        ):
            for batch in _iter_bounded_row_batches(
                target_rows,
                params_per_row=9,
                fixed_params=0,
                fixed_sql_bytes=384,
            ):
                values_sql = ", ".join(["(?, ?, ?, ?, ?, ?, ?, ?, ?)"] * len(batch))
                flat_params = [value for row in batch for value in row]
                await tx.execute(
                    f"""INSERT INTO profile_feed_decisions
                        (guild_id, profile_key, event_id, source, event_date, decision, reason, payload, observed_at)
                        VALUES {values_sql}
                        ON CONFLICT(guild_id, profile_key, event_id) DO UPDATE SET
                            decision = excluded.decision,
                            reason = excluded.reason,
                            event_date = excluded.event_date,
                            payload = excluded.payload
                        WHERE profile_feed_decisions.decision = '{source_state}'""",
                    flat_params,
                )

        authoritative: dict[str, Any] = {}
        for chunk in _iter_sql_chunks(all_ids, fixed_params=2):
            placeholders = ",".join("?" for _ in chunk)
            rows = await tx.fetchall(
                f"""SELECT event_id, source, event_date, decision, reason, payload, observed_at
                    FROM profile_feed_decisions
                    WHERE guild_id = ? AND profile_key = ? AND event_id IN ({placeholders})""",
                [guild_id, normalized_key, *chunk],
            )
            for row in rows:
                authoritative[row["event_id"]] = row
        return authoritative


async def get_feed_decision_counts(db: Any, guild_id: int, profile_key: str) -> dict[str, int]:
    rows = await db._fetchall(
        """SELECT decision, COUNT(*) AS count FROM profile_feed_decisions
           WHERE guild_id = ? AND profile_key = ?
           GROUP BY decision""",
        (guild_id, profile_key.strip().lower()),
    )
    return {row["decision"]: row["count"] for row in rows}


async def get_guild_feed_recovery_profiles(db: Any, guild_id: int) -> List[Any]:
    """Last active recipients still enrolled, when Discord membership is unavailable.

    This is recovery bookkeeping only: cached membership never authorizes a
    Discord send. A complete member snapshot must still precede delivery.
    """
    return await db._fetchall(
        """SELECT p.profile_key, p.display_username, p.avatar_url
           FROM guild_profile_feed_activity AS active
           JOIN profiles AS p ON p.profile_key = active.profile_key
           WHERE active.guild_id = ? AND active.active = 1
             AND (
                 EXISTS (
                     SELECT 1 FROM guild_feed_preferences AS pref
                     JOIN linked_users AS linked ON linked.discord_id = pref.discord_id
                     WHERE pref.guild_id = active.guild_id AND pref.enabled = 1
                       AND linked.profile_key = active.profile_key
                 )
                 OR EXISTS (
                     SELECT 1 FROM guild_admin_follows AS follow
                     WHERE follow.guild_id = active.guild_id
                       AND follow.profile_key = active.profile_key
                 )
             ) ORDER BY p.profile_key""",
        (guild_id,),
    )


async def record_feed_target_gap(
    db: Any,
    profile_key: str,
    *,
    guild_id: Optional[int] = None,
    guild_ids: Optional[Iterable[int]] = None,
    reason: str = "unavailable_feed_target",
    last_error: Optional[str] = None,
    now: Optional[Any] = None,
) -> None:
    """Record a recipient delivery gap and pin the global boundary hold."""
    normalized_key = profile_key.strip().lower()
    now_sql = db._now_utc_sql_timestamp(now)
    gap_reason = reason or "unavailable_feed_target"
    err_text = str(last_error)[:500] if last_error else None
    target_guilds: list[int] = []
    if guild_id is not None:
        target_guilds.append(int(guild_id))
    if guild_ids is not None:
        for gid in guild_ids:
            igid = int(gid)
            if igid not in target_guilds:
                target_guilds.append(igid)

    statements: list[tuple[str, tuple | list]] = [
        (
            """UPDATE profile_feed_source_health
               SET unresolved_gap_since = COALESCE(unresolved_gap_since, ?),
                   unresolved_gap_until = NULL,
                   gap_reason = CASE
                       WHEN unresolved_gap_since IS NULL
                           THEN ?
                       ELSE COALESCE(gap_reason, ?)
                   END
               WHERE profile_key = ? AND source = 'activity'""",
            (now_sql, gap_reason, gap_reason, normalized_key),
        )
    ]
    if target_guilds:
        gap_rows = [
            (gid, normalized_key, now_sql, gap_reason, err_text)
            for gid in target_guilds
        ]
        for batch in _iter_bounded_row_batches(
            gap_rows,
            params_per_row=5,
            fixed_params=0,
            fixed_sql_bytes=320,
        ):
            values_sql = ", ".join(["(?, ?, ?, ?, ?)"] * len(batch))
            flat_params = [value for row in batch for value in row]
            statements.append((
                f"""INSERT INTO guild_profile_feed_recipient_gaps
                        (guild_id, profile_key, gap_since, reason, last_error)
                    VALUES {values_sql}
                    ON CONFLICT(guild_id, profile_key) DO UPDATE SET
                        gap_since = COALESCE(guild_profile_feed_recipient_gaps.gap_since, excluded.gap_since),
                        reason = excluded.reason,
                        last_error = excluded.last_error""",
                flat_params,
            ))
    await db._execute_batch(statements)


async def clear_guild_feed_recipient_gaps(
    db: Any, profile_key: str, guild_ids: Iterable[int]
) -> None:
    """Clear recipient gaps for guilds whose required delivery/edit pass succeeded."""
    normalized_key = profile_key.strip().lower()
    target_guilds = list(dict.fromkeys(int(gid) for gid in guild_ids))
    if not target_guilds:
        return
    statements: list[tuple[str, list[Any]]] = []
    for chunk in _iter_sql_chunks(target_guilds, fixed_params=1):
        placeholders = ",".join("?" for _ in chunk)
        statements.append((
            f"""DELETE FROM guild_profile_feed_recipient_gaps
                WHERE profile_key = ? AND guild_id IN ({placeholders})""",
            [normalized_key, *chunk],
        ))
    await db._execute_batch(statements)


async def is_journal_entry_delivered(
    db: Any, guild_id: int, profile_key: str, entry_id: str
) -> bool:
    """Check whether a profile event was delivered or baselined in this guild."""
    row = await db._fetchone(
        """SELECT 1 FROM profile_seen_journal_deliveries
           WHERE guild_id = ? AND profile_key = ? AND entry_id = ?""",
        (guild_id, profile_key.strip().lower(), entry_id),
    )
    return row is not None


async def get_delivered_journal_entry_ids(
    db: Any, guild_id: int, profile_key: str, entry_ids: Iterable[str]
) -> set[str]:
    """Bulk fetch delivered or baselined entry IDs within SQL bind budgets."""
    normalized_key = profile_key.strip().lower()
    unique_ids = list(dict.fromkeys(entry_id for entry_id in entry_ids if entry_id))
    if not unique_ids:
        return set()
    found: set[str] = set()
    for chunk in _iter_sql_chunks(unique_ids, fixed_params=2):
        placeholders = ",".join("?" for _ in chunk)
        rows = await db._fetchall(
            f"""SELECT entry_id FROM profile_seen_journal_deliveries
                WHERE guild_id = ? AND profile_key = ? AND entry_id IN ({placeholders})""",
            [guild_id, normalized_key, *chunk],
        )
        found.update(row["entry_id"] for row in rows)
    return found


async def get_feed_sources(db: Any, guild_id: int, profile_key: str) -> set[str]:
    """Return initialized feed source identifiers for a guild/profile."""
    rows = await db._fetchall(
        "SELECT source FROM profile_feed_sources WHERE guild_id = ? AND profile_key = ?",
        (guild_id, profile_key.strip().lower()),
    )
    return {row["source"] for row in rows}


async def seed_feed_source(
    db: Any,
    guild_id: int,
    profile_key: str,
    source: str,
    entry_ids: Iterable[str],
    *,
    baseline_messages: Any = (),
    baseline_decisions: Any = (),
) -> bool:
    """Baseline a new source once, atomically; later polls deliver new IDs."""
    profile_key = profile_key.strip().lower()
    params = (guild_id, profile_key, source)
    unique_ids = list(
        dict.fromkeys(
            entry_id
            for entry_id in entry_ids
            if entry_id and (not isinstance(entry_id, str) or entry_id.strip())
        )
    )
    seen_rows = [(guild_id, profile_key, entry_id) for entry_id in unique_ids]
    default_source = source[:-len("-display-v1")] if source.endswith("-display-v1") else source
    decision_map: dict[str, tuple[Any, ...]] = {}
    for item in baseline_decisions:
        eid = item.get("event_id")
        if not eid or (isinstance(eid, str) and not eid.strip()):
            continue
        decision_map[eid] = (
            guild_id,
            profile_key,
            eid,
            item.get("source") or default_source,
            item.get("event_date"),
            item.get("decision") or "baseline",
            item.get("reason") or "initial_or_resume_source_baseline",
            item["payload"],
            db._format_utc_sql_timestamp(item.get("observed_at")),
        )
    decision_rows = list(decision_map.values())
    async with db._transaction() as tx:
        existing = await tx.fetchone(
            "SELECT 1 FROM profile_feed_sources WHERE guild_id = ? AND profile_key = ? AND source = ?",
            params,
        )
        if existing is not None:
            return False
        for batch in _iter_bounded_row_batches(
            seen_rows,
            params_per_row=3,
            fixed_params=0,
            fixed_sql_bytes=128,
        ):
            values_sql = ", ".join(["(?, ?, ?)"] * len(batch))
            flat_params = [value for row in batch for value in row]
            await tx.execute(
                f"""INSERT OR IGNORE INTO profile_seen_journal_deliveries
                    (guild_id, profile_key, entry_id) VALUES {values_sql}""",
                flat_params,
            )
        message_rows = [
            (guild_id, profile_key, event_id, payload)
            for event_id, payload in baseline_messages
            if event_id and (not isinstance(event_id, str) or event_id.strip())
        ]
        for batch in _iter_bounded_row_batches(
            message_rows, params_per_row=4, fixed_sql_bytes=256
        ):
            values_sql = ", ".join(["(?, ?, ?, ?, NULL, NULL, 'baseline')"] * len(batch))
            await tx.execute(
                f"""INSERT OR IGNORE INTO profile_feed_messages
                    (guild_id, profile_key, event_id, payload, message_id, channel_id, delivery_state)
                    VALUES {values_sql}""",
                [value for row in batch for value in row],
            )
        for batch in _iter_bounded_row_batches(
            decision_rows,
            params_per_row=9,
            fixed_params=0,
            fixed_sql_bytes=256,
        ):
            values_sql = ", ".join(
                ["(?, ?, ?, ?, ?, ?, ?, ?, COALESCE(?, CURRENT_TIMESTAMP))"] * len(batch)
            )
            await tx.execute(
                f"""INSERT OR IGNORE INTO profile_feed_decisions
                    (guild_id, profile_key, event_id, source, event_date, decision, reason, payload, observed_at)
                    VALUES {values_sql}""",
                [value for row in batch for value in row],
            )
        await tx.execute("INSERT OR IGNORE INTO profile_feed_sources VALUES (?, ?, ?)", params)
        return True


async def seed_feed_source_with_messages(
    db: Any,
    guild_id: int,
    profile_key: str,
    source: str,
    messages: Any,
    *,
    baseline_decisions: Any = (),
) -> bool:
    """Initialize distinct baseline payloads, seen IDs, decisions, and source marker atomically."""
    unique = dict(messages)
    return await db.seed_feed_source(
        guild_id, profile_key, source, unique,
        baseline_messages=list(unique.items()),
        baseline_decisions=baseline_decisions,
    )


def clear_feed_delivery_tokens(
    db: Any, guild_id: int, profile_key: str, event_ids: Iterable[str]
) -> None:
    normalized_key = profile_key.strip().lower()
    for eid in event_ids:
        if eid:
            db._active_pending_delivery_tokens.pop((guild_id, normalized_key, eid), None)


def release_feed_delivery_ownership(
    db: Any,
    guild_id: int,
    profile_key: str,
    event_ids: Iterable[str],
    owner_token: Optional[str] = None,
    *,
    recheck_recovery: bool = True,
) -> None:
    """Release active in-memory send ownership for an attempt token."""
    normalized_key = profile_key.strip().lower()
    removed_any = False
    for eid in dict.fromkeys(event_ids):
        if not eid:
            continue
        key = (guild_id, normalized_key, eid)
        tokens = db._active_pending_delivery_tokens.get(key)
        if not tokens:
            continue
        if owner_token is None:
            db._active_pending_delivery_tokens.pop(key, None)
            removed_any = True
        else:
            if owner_token in tokens:
                tokens.discard(owner_token)
                removed_any = True
            if not tokens:
                db._active_pending_delivery_tokens.pop(key, None)
    if removed_any and recheck_recovery:
        db._stale_pending_recovery_checked = False


async def ensure_stale_pending_recovered(db: Any) -> None:
    if db._stale_pending_recovery_checked:
        return
    async with db._stale_recovery_lock:
        if db._stale_pending_recovery_checked:
            return
        await db.recover_stale_feed_deliveries()


async def recover_stale_feed_deliveries(
    db: Any, guild_id: Optional[int] = None, profile_key: Optional[str] = None
) -> int:
    """Mark unowned pending delivery intents unknown without touching live sends.

    Ownership and recovery timing:
    - Live in-process send intents are registered by attempt token in
      ``_active_pending_delivery_tokens`` after ``begin_feed_delivery`` commits, and cleared
      via terminal persistence or ``release_feed_delivery_ownership``.
    - Unowned ``pending`` rows (from prior processes or abandoned same-process attempts whose
      tokens were released without DB finalization) recover safely on demand without
      touching concurrent live sends holding active tokens.
    """
    if guild_id is not None and profile_key is not None:
        normalized_key = profile_key.strip().lower()
        rows = await db._fetchall(
            """SELECT guild_id, profile_key, event_id
               FROM profile_feed_messages
               WHERE guild_id = ? AND profile_key = ? AND delivery_state = 'pending'""",
            (guild_id, normalized_key),
        )
    else:
        rows = await db._fetchall(
            """SELECT guild_id, profile_key, event_id
               FROM profile_feed_messages
               WHERE delivery_state = 'pending'"""
        )
    if not rows:
        if guild_id is None and profile_key is None:
            db._stale_pending_recovery_checked = True
        return 0
    stale_by_scope: dict[tuple[int, str], list[str]] = {}
    skipped_active = False
    for row in rows:
        scope = (int(row["guild_id"]), row["profile_key"])
        eid = row["event_id"]
        if db._active_pending_delivery_tokens.get((scope[0], scope[1], eid)):
            skipped_active = True
            continue
        stale_by_scope.setdefault(scope, []).append(eid)
    if not stale_by_scope:
        if guild_id is None and profile_key is None:
            db._stale_pending_recovery_checked = not skipped_active
        return 0
    statements = []
    recovered = 0
    for (scope_guild_id, scope_profile_key), event_ids in stale_by_scope.items():
        unique_ids = list(dict.fromkeys(event_ids))
        recovered += len(unique_ids)
        for chunk in _iter_sql_chunks(unique_ids, fixed_params=2):
            placeholders = ",".join("?" for _ in chunk)
            statements.append((
                f"""UPDATE profile_feed_messages SET delivery_state = 'unknown'
                    WHERE guild_id = ? AND profile_key = ? AND delivery_state = 'pending'
                      AND event_id IN ({placeholders})""",
                [scope_guild_id, scope_profile_key, *chunk],
            ))
    await db._execute_batch(statements)
    if guild_id is None and profile_key is None:
        db._stale_pending_recovery_checked = not skipped_active
    return recovered


async def get_feed_messages(db: Any, guild_id: int, profile_key: str):
    """Retain source aliases and Discord message IDs across polls/restarts without mutating live pending intents."""
    await db._ensure_stale_pending_recovered()
    profile_key = profile_key.strip().lower()
    return await db._fetchall(
        """SELECT event_id, payload, message_id, channel_id, delivery_state
           FROM profile_feed_messages WHERE guild_id = ? AND profile_key = ?""",
        (guild_id, profile_key),
    )


def iter_relevant_history_batches(
    ids: list[str],
    games: list[str],
    refs: list[str],
    *,
    max_json_bytes: int = 32 * 1024,
) -> Iterable[tuple[list[str], list[str], list[str]]]:
    """Yield (ids, games, refs) batches keeping combined JSON parameter size bounded."""
    batch_ids: list[str] = []
    batch_games: list[str] = []
    batch_refs: list[str] = []
    current_bytes = 6  # three empty "[]" arrays

    def _item_bytes(value: str) -> int:
        raw = len(value.encode("utf-8"))
        if any(ord(ch) < 32 or ord(ch) > 126 or ch in ('"', "\\") for ch in value):
            return len(value) * 12 + 3
        return raw + 3

    for kind, items in (("id", ids), ("game", games), ("ref", refs)):
        for item in items:
            cost = _item_bytes(item)
            if (batch_ids or batch_games or batch_refs) and current_bytes + cost > max_json_bytes:
                yield batch_ids, batch_games, batch_refs
                batch_ids, batch_games, batch_refs = [], [], []
                current_bytes = 6
            if kind == "id":
                batch_ids.append(item)
            elif kind == "game":
                batch_games.append(item)
            else:
                batch_refs.append(item)
            current_bytes += cost
    if batch_ids or batch_games or batch_refs:
        yield batch_ids, batch_games, batch_refs


async def get_relevant_feed_messages(
    db: Any,
    guild_id: int,
    profile_key: str,
    *,
    event_ids: Iterable[str] = (),
    game_slugs: Iterable[str] = (),
    references: Iterable[str] = (),
) -> List[Any]:
    """Fetch only archive rows relevant to incoming/pending IDs, aliases, games, and references.

    Runs abandoned-pending recovery unconditionally before checking whether any
    history seeds exist. Decomposes the lookup into five indexed UNION branches
    over (guild_id, profile_key) using the PK, guarded scalar expression indexes
    (activity_id, game_slug, reference), and profile_feed_message_aliases before
    fetching matched rows via CROSS JOIN on the primary key.
    """
    await db._ensure_stale_pending_recovered()
    normalized_key = profile_key.strip().lower()

    frontier_ids = list(dict.fromkeys(
        eid for eid in event_ids if isinstance(eid, str) and eid.strip()
    ))
    frontier_games = list(dict.fromkeys(
        slug for slug in game_slugs if isinstance(slug, str) and slug.strip()
    ))
    frontier_refs = list(dict.fromkeys(
        ref for ref in references if isinstance(ref, str) and ref.strip()
    ))
    if not (frontier_ids or frontier_games or frontier_refs):
        return []

    seen_ids: set[str] = set()
    seen_games: set[str] = set()
    seen_refs: set[str] = set()
    rows_by_event_id: dict[str, Any] = {}

    query = """
        WITH target_ids(id) AS (SELECT value FROM json_each(?)),
             target_games(slug) AS (SELECT value FROM json_each(?)),
             target_refs(ref) AS (SELECT value FROM json_each(?)),
             matched_keys(event_id) AS (
                 SELECT event_id
                 FROM profile_feed_messages
                 WHERE guild_id = ? AND profile_key = ?
                   AND event_id IN (SELECT id FROM target_ids)
                 UNION
                 SELECT event_id
                 FROM profile_feed_messages
                 WHERE guild_id = ? AND profile_key = ?
                   AND (CASE WHEN json_valid(payload)
                                  AND json_type(payload) = 'object'
                                  AND json_type(payload, '$.activity_id') = 'text'
                             THEN json_extract(payload, '$.activity_id') END)
                       IN (SELECT id FROM target_ids)
                 UNION
                 SELECT event_id
                 FROM profile_feed_messages
                 WHERE guild_id = ? AND profile_key = ?
                   AND (CASE WHEN json_valid(payload)
                                  AND json_type(payload) = 'object'
                                  AND json_type(payload, '$.game_slug') = 'text'
                             THEN json_extract(payload, '$.game_slug') END)
                       IN (SELECT slug FROM target_games)
                 UNION
                 SELECT event_id
                 FROM profile_feed_messages
                 WHERE guild_id = ? AND profile_key = ?
                   AND (CASE WHEN json_valid(payload)
                                  AND json_type(payload) = 'object'
                                  AND json_type(payload, '$.reference') = 'text'
                             THEN json_extract(payload, '$.reference') END)
                       IN (SELECT ref FROM target_refs)
                 UNION
                 SELECT alias.event_id
                 FROM profile_feed_message_aliases AS alias
                 WHERE alias.guild_id = ? AND alias.profile_key = ?
                   AND alias.alias_id IN (SELECT id FROM target_ids)
             )
        SELECT m.event_id, m.payload, m.message_id, m.channel_id, m.delivery_state
        FROM matched_keys AS k
        CROSS JOIN profile_feed_messages AS m
          ON m.guild_id = ? AND m.profile_key = ? AND m.event_id = k.event_id
    """

    while frontier_ids or frontier_games or frontier_refs:
        seen_ids.update(frontier_ids)
        seen_games.update(frontier_games)
        seen_refs.update(frontier_refs)

        next_ids: list[str] = []
        next_games: list[str] = []
        next_refs: list[str] = []
        queued_ids: set[str] = set()
        queued_games: set[str] = set()
        queued_refs: set[str] = set()

        for batch_ids, batch_games, batch_refs in db._iter_relevant_history_batches(
            frontier_ids, frontier_games, frontier_refs
        ):
            fetched = await db._fetchall(
                query,
                (
                    json.dumps(batch_ids),
                    json.dumps(batch_games),
                    json.dumps(batch_refs),
                    guild_id,
                    normalized_key,
                    guild_id,
                    normalized_key,
                    guild_id,
                    normalized_key,
                    guild_id,
                    normalized_key,
                    guild_id,
                    normalized_key,
                    guild_id,
                    normalized_key,
                ),
            )
            for row in fetched:
                eid = row["event_id"]
                if eid not in rows_by_event_id:
                    rows_by_event_id[eid] = row
                if isinstance(eid, str) and eid.strip() and eid not in seen_ids and eid not in queued_ids:
                    queued_ids.add(eid)
                    next_ids.append(eid)
                raw_payload = row["payload"]
                try:
                    data = json.loads(raw_payload)
                except (TypeError, ValueError):
                    continue
                if not isinstance(data, dict):
                    continue
                act_id = data.get("activity_id")
                if isinstance(act_id, str) and act_id.strip() and act_id not in seen_ids and act_id not in queued_ids:
                    queued_ids.add(act_id)
                    next_ids.append(act_id)
                raw_aliases = data.get("event_ids")
                if isinstance(raw_aliases, list):
                    for alias in raw_aliases:
                        if isinstance(alias, str) and alias.strip() and alias not in seen_ids and alias not in queued_ids:
                            queued_ids.add(alias)
                            next_ids.append(alias)
                slug = data.get("game_slug")
                if isinstance(slug, str) and slug.strip() and slug not in seen_games and slug not in queued_games:
                    queued_games.add(slug)
                    next_games.append(slug)
                ref = data.get("reference")
                if isinstance(ref, str) and ref.strip() and ref not in seen_refs and ref not in queued_refs:
                    queued_refs.add(ref)
                    next_refs.append(ref)

        frontier_ids = next_ids
        frontier_games = next_games
        frontier_refs = next_refs

    return list(rows_by_event_id.values())


async def seed_feed_message(
    db: Any, guild_id: int, profile_key: str, event_ids: Any, payload: str
) -> None:
    """Record baseline events only when absent, preserving delivered mappings."""
    profile_key = profile_key.strip().lower()
    aliases = list(dict.fromkeys(event_id for event_id in event_ids if event_id))
    if not aliases:
        return
    msg_rows = [(guild_id, profile_key, event_id, payload) for event_id in aliases]
    seen_rows = [(guild_id, profile_key, event_id) for event_id in aliases]
    statements = []
    for batch in _iter_bounded_row_batches(
        msg_rows,
        params_per_row=4,
        fixed_params=0,
        fixed_sql_bytes=192,
    ):
        values_sql = ", ".join(["(?, ?, ?, ?, NULL, NULL, 'baseline')"] * len(batch))
        flat_params = [value for row in batch for value in row]
        statements.append((
            f"""INSERT OR IGNORE INTO profile_feed_messages
                (guild_id, profile_key, event_id, payload, message_id, channel_id, delivery_state)
                VALUES {values_sql}""",
            flat_params,
        ))
    for batch in _iter_bounded_row_batches(
        seen_rows,
        params_per_row=3,
        fixed_params=0,
        fixed_sql_bytes=128,
    ):
        values_sql = ", ".join(["(?, ?, ?)"] * len(batch))
        flat_params = [value for row in batch for value in row]
        statements.append((
            f"""INSERT OR IGNORE INTO profile_seen_journal_deliveries
                (guild_id, profile_key, entry_id) VALUES {values_sql}""",
            flat_params,
        ))
    await db._execute_batch(statements)


async def store_feed_message(
    db: Any,
    guild_id: int,
    profile_key: str,
    event_ids: Any,
    payload: str,
    message_id: Any = None,
    channel_id: Any = None,
    delivery_state: Optional[str] = None,
    recorded_at: Optional[Any] = None,
) -> None:
    profile_key = profile_key.strip().lower()
    aliases = list(dict.fromkeys(event_id for event_id in event_ids if event_id))
    if not aliases:
        return
    saved_state = delivery_state or ("sent" if message_id is not None else "baseline")
    recorded_at_ts = (
        db._format_utc_sql_timestamp(recorded_at)
        if saved_state == "sent"
        else None
    )
    msg_rows = [
        (guild_id, profile_key, event_id, payload, message_id, channel_id, saved_state, recorded_at_ts)
        for event_id in aliases
    ]
    statements = []
    row_placeholder = (
        "(?, ?, ?, ?, ?, ?, ?, COALESCE(?, CURRENT_TIMESTAMP))"
        if saved_state == "sent"
        else "(?, ?, ?, ?, ?, ?, ?, ?)"
    )
    for batch in _iter_bounded_row_batches(
        msg_rows,
        params_per_row=8,
        fixed_params=0,
        fixed_sql_bytes=640,
    ):
        values_sql = ", ".join([row_placeholder] * len(batch))
        flat_params = [value for row in batch for value in row]
        statements.append((f"""
            INSERT INTO profile_feed_messages
                (guild_id, profile_key, event_id, payload, message_id, channel_id, delivery_state, recorded_at)
            VALUES {values_sql}
            ON CONFLICT(guild_id, profile_key, event_id) DO UPDATE SET
                payload = excluded.payload,
                message_id = COALESCE(excluded.message_id, profile_feed_messages.message_id),
                channel_id = COALESCE(excluded.channel_id, profile_feed_messages.channel_id),
                delivery_state = CASE
                    WHEN excluded.delivery_state IN ('unknown', 'deleted') THEN excluded.delivery_state
                    WHEN excluded.message_id IS NOT NULL THEN 'sent'
                    ELSE profile_feed_messages.delivery_state
                END,
                recorded_at = COALESCE(
                    profile_feed_messages.recorded_at,
                    CASE
                        WHEN excluded.delivery_state = 'sent' OR excluded.message_id IS NOT NULL
                        THEN COALESCE(excluded.recorded_at, CURRENT_TIMESTAMP)
                        ELSE NULL
                    END
                )
        """, flat_params))
    if saved_state != "unknown":
        seen_rows = [(guild_id, profile_key, event_id) for event_id in aliases]
        for batch in _iter_bounded_row_batches(
            seen_rows,
            params_per_row=3,
            fixed_params=0,
            fixed_sql_bytes=128,
        ):
            values_sql = ", ".join(["(?, ?, ?)"] * len(batch))
            flat_params = [value for row in batch for value in row]
            statements.append((
                f"""INSERT OR IGNORE INTO profile_seen_journal_deliveries
                    (guild_id, profile_key, entry_id) VALUES {values_sql}""",
                flat_params,
            ))
    await db._execute_batch(statements)
    if saved_state != "pending":
        db._clear_feed_delivery_tokens(guild_id, profile_key, aliases)


async def begin_feed_delivery(
    db: Any,
    guild_id: int,
    profile_key: str,
    event_ids: Iterable[str],
    payload: str,
    channel_id: int,
    *,
    owner_token: Optional[str] = None,
) -> bool:
    """Persist a send intent before calling Discord; return false for known or uncertain events."""
    await db._ensure_stale_pending_recovered()
    profile_key = profile_key.strip().lower()
    aliases = list(dict.fromkeys(event_id for event_id in event_ids if event_id))
    if not aliases:
        return False
    token = owner_token or "__unscoped__"
    pending_rows = [(guild_id, profile_key, event_id, payload, channel_id) for event_id in aliases]
    try:
        async with db._transaction() as tx:
            for chunk in _iter_sql_chunks(aliases, fixed_params=2):
                placeholders = ",".join("?" for _ in chunk)
                query = f"""SELECT delivery_state FROM profile_feed_messages
                    WHERE guild_id = ? AND profile_key = ? AND event_id IN ({placeholders})"""
                existing = await tx.fetchone(query, [guild_id, profile_key, *chunk])
                if existing is not None:
                    return False
            for batch in _iter_bounded_row_batches(
                pending_rows,
                params_per_row=5,
                fixed_params=0,
                fixed_sql_bytes=192,
            ):
                values_sql = ", ".join(["(?, ?, ?, ?, NULL, ?, 'pending')"] * len(batch))
                flat_params = [value for row in batch for value in row]
                await tx.execute(
                    f"""INSERT OR IGNORE INTO profile_feed_messages
                        (guild_id, profile_key, event_id, payload, message_id, channel_id, delivery_state)
                        VALUES {values_sql}""",
                    flat_params,
                )
    except BaseException:
        db._stale_pending_recovery_checked = False
        raise
    for eid in aliases:
        db._active_pending_delivery_tokens.setdefault(
            (guild_id, profile_key, eid), set()
        ).add(token)
    return True


async def release_feed_delivery(
    db: Any, guild_id: int, profile_key: str, event_ids: Iterable[str]
) -> None:
    """Remove a pending intent after Discord definitively rejected a send."""
    aliases = list(dict.fromkeys(event_id for event_id in event_ids if event_id))
    if not aliases:
        return
    normalized_key = profile_key.strip().lower()
    statements = []
    for chunk in _iter_sql_chunks(aliases, fixed_params=2):
        placeholders = ",".join("?" for _ in chunk)
        statements.append((
            f"""DELETE FROM profile_feed_messages
                WHERE guild_id = ? AND profile_key = ? AND delivery_state = 'pending'
                  AND event_id IN ({placeholders})""",
            [guild_id, normalized_key, *chunk],
        ))
    await db._execute_batch(statements)
    db._clear_feed_delivery_tokens(guild_id, normalized_key, aliases)


async def mark_feed_delivery_unknown(
    db: Any, guild_id: int, profile_key: str, event_ids: Iterable[str]
) -> None:
    aliases = list(dict.fromkeys(event_id for event_id in event_ids if event_id))
    if not aliases:
        return
    normalized_key = profile_key.strip().lower()
    statements = []
    for chunk in _iter_sql_chunks(aliases, fixed_params=2):
        placeholders = ",".join("?" for _ in chunk)
        statements.append((
            f"""UPDATE profile_feed_messages SET delivery_state = 'unknown'
                WHERE guild_id = ? AND profile_key = ? AND delivery_state = 'pending'
                  AND event_id IN ({placeholders})""",
            [guild_id, normalized_key, *chunk],
        ))
    await db._execute_batch(statements)
    db._clear_feed_delivery_tokens(guild_id, normalized_key, aliases)


async def mark_feed_message_deleted(
    db: Any, guild_id: int, profile_key: str, event_ids: Iterable[str], payload: str
) -> None:
    aliases = list(dict.fromkeys(event_id for event_id in event_ids if event_id))
    if not aliases:
        return
    normalized_key = profile_key.strip().lower()
    statements = []
    for chunk in _iter_sql_chunks(aliases, fixed_params=3):
        placeholders = ",".join("?" for _ in chunk)
        statements.append((
            f"""UPDATE profile_feed_messages SET delivery_state = 'deleted', payload = ?
                WHERE guild_id = ? AND profile_key = ? AND event_id IN ({placeholders})""",
            [payload, guild_id, normalized_key, *chunk],
        ))
    await db._execute_batch(statements)
    db._clear_feed_delivery_tokens(guild_id, normalized_key, aliases)


async def get_profile_activity_observations(
    db: Any, profile_key: str, source: str, keys: Iterable[str],
) -> dict[str, str]:
    result = {}
    for chunk in _iter_sql_chunks(list(dict.fromkeys(keys)), fixed_params=2):
        placeholders = ",".join("?" for _ in chunk)
        rows = await db._fetchall(
            f"""SELECT observation_key, fingerprint FROM profile_activity_observations
                WHERE profile_key = ? AND source = ? AND observation_key IN ({placeholders})""",
            [profile_key, source, *chunk],
        )
        result.update((row["observation_key"], row["fingerprint"]) for row in rows)
    return result


async def record_profile_activity_observations(
    db: Any,
    profile_key: str,
    source: str,
    observations: Iterable[tuple[str, str]],
) -> dict[str, Any]:
    """Persist profile-level relevant event fingerprints and report newly observed changes."""
    normalized_key = profile_key.strip().lower()
    deduped = {}
    for key, fingerprint in observations:
        if key:
            deduped[key] = fingerprint
    items = list(deduped.items())

    async with db._transaction() as tx:
        initialized = (
            await tx.fetchone(
                "SELECT 1 FROM profile_activity_observation_sources WHERE profile_key = ? AND source = ?",
                (normalized_key, source),
            )
            is None
        )
        if initialized:
            await tx.execute(
                """INSERT OR IGNORE INTO profile_activity_observation_sources
                   (profile_key, source) VALUES (?, ?)""",
                (normalized_key, source),
            )
        existing_map: dict[str, str] = {}
        for chunk in _iter_sql_chunks(items, fixed_params=2):
            chunk_keys = [key for key, _ in chunk]
            placeholders = ",".join("?" for _ in chunk_keys)
            rows = await tx.fetchall(
                f"""SELECT observation_key, fingerprint
                    FROM profile_activity_observations
                    WHERE profile_key = ? AND source = ? AND observation_key IN ({placeholders})""",
                [normalized_key, source, *chunk_keys],
            )
            for row in rows:
                existing_map[row["observation_key"]] = row["fingerprint"]
        new_rows: list[tuple[str, str, str, str]] = []
        changed_rows: list[tuple[str, str, str, str]] = []
        for key, fingerprint in items:
            prev = existing_map.get(key)
            if prev is None:
                new_rows.append((normalized_key, source, key, fingerprint))
            elif prev != fingerprint:
                changed_rows.append((normalized_key, source, key, fingerprint))
        for batch in _iter_bounded_row_batches(
            new_rows,
            params_per_row=4,
            fixed_params=0,
            fixed_sql_bytes=192,
        ):
            values_sql = ", ".join(["(?, ?, ?, ?, CURRENT_TIMESTAMP)"] * len(batch))
            flat_params = [value for row in batch for value in row]
            await tx.execute(
                f"""INSERT INTO profile_activity_observations
                    (profile_key, source, observation_key, fingerprint, observed_at)
                    VALUES {values_sql}""",
                flat_params,
            )
        for batch in _iter_bounded_row_batches(
            changed_rows,
            params_per_row=4,
            fixed_params=0,
            fixed_sql_bytes=256,
        ):
            values_sql = ", ".join(["(?, ?, ?, ?, CURRENT_TIMESTAMP)"] * len(batch))
            flat_params = [value for row in batch for value in row]
            await tx.execute(
                f"""INSERT INTO profile_activity_observations
                    (profile_key, source, observation_key, fingerprint, observed_at)
                    VALUES {values_sql}
                    ON CONFLICT(profile_key, source, observation_key) DO UPDATE SET
                        fingerprint = excluded.fingerprint,
                        observed_at = CURRENT_TIMESTAMP""",
                flat_params,
            )
        new_count = len(new_rows)
        updated_count = len(changed_rows)
        return {
            "initialized": initialized,
            "new": new_count,
            "updated": updated_count,
            "changed": new_count + updated_count,
        }


async def set_profile_refresh_pending(db: Any, profile_key: str, token: str) -> None:
    """Persist a pending library refresh token that survives cooldown skips and restarts."""
    normalized_key = profile_key.strip().lower()
    await db._execute(
        """INSERT INTO profile_activity_observations
           (profile_key, source, observation_key, fingerprint, observed_at)
           VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
           ON CONFLICT(profile_key, source, observation_key) DO UPDATE SET
               fingerprint = excluded.fingerprint,
               observed_at = CURRENT_TIMESTAMP""",
        (normalized_key, db._LIBRARY_SYNC_SOURCE, db._PENDING_REFRESH_KEY, token),
    )


async def get_profile_refresh_pending_token(db: Any, profile_key: str) -> Optional[str]:
    """Return the active pending library refresh token for a profile, if any."""
    row = await db._fetchone(
        """SELECT fingerprint FROM profile_activity_observations
           WHERE profile_key = ? AND source = ? AND observation_key = ?""",
        (profile_key.strip().lower(), db._LIBRARY_SYNC_SOURCE, db._PENDING_REFRESH_KEY),
    )
    return row["fingerprint"] if row else None


async def clear_profile_refresh_pending(
    db: Any, profile_key: str, *, token: Optional[str] = None
) -> bool:
    """Clear a pending refresh token unless a newer observation superseded it."""
    normalized_key = profile_key.strip().lower()
    if token is not None:
        deleted = await db._rowcount_execute(
            """DELETE FROM profile_activity_observations
               WHERE profile_key = ? AND source = ? AND observation_key = ?
                 AND fingerprint = ?""",
            (normalized_key, db._LIBRARY_SYNC_SOURCE, db._PENDING_REFRESH_KEY, token),
        )
    else:
        deleted = await db._rowcount_execute(
            """DELETE FROM profile_activity_observations
               WHERE profile_key = ? AND source = ? AND observation_key = ?""",
            (normalized_key, db._LIBRARY_SYNC_SOURCE, db._PENDING_REFRESH_KEY),
        )
    return deleted > 0


async def prune_stale_history(
    db: Any,
    now: Any = None,
    *,
    retention_days: int = 90,
    keep_recent_per_profile: int = 200,
) -> dict[str, int]:
    """Safely prune aged auxiliary history without risking feed dedup or pending sync tokens.

    Invariants preserved:
    - Never deletes ``_library_sync`` rows in ``profile_activity_observations``.
    - Keeps at least the latest ``keep_recent_per_profile`` rows per profile/source
      in ``profile_activity_observations``, ``profile_seen_journal_deliveries``,
      and ``profile_seen_journal_logs``.
    - Never deletes ``profile_feed_messages`` rows (needed for edits and dedup).
    - Only prunes aged ``profile_feed_decisions`` for inactive/unconfigured guilds
      that are not referenced by ``profile_feed_messages``.
    """
    current_now = now or datetime.now(timezone.utc)
    if current_now.tzinfo is None:
        current_now = current_now.replace(tzinfo=timezone.utc)
    cutoff = db._format_utc_sql_timestamp(
        current_now - timedelta(days=max(7, int(retention_days)))
    )
    keep_n = max(1, int(keep_recent_per_profile))

    async with db._transaction() as tx:
        pruned_observations = await tx.execute(
            """DELETE FROM profile_activity_observations
               WHERE rowid IN (
                   SELECT rowid FROM (
                       SELECT rowid, observed_at,
                              ROW_NUMBER() OVER (
                                  PARTITION BY profile_key, source
                                  ORDER BY observed_at DESC, rowid DESC
                              ) AS rn
                       FROM profile_activity_observations
                       WHERE source != ?
                   )
                   WHERE rn > ? AND observed_at < ?
               )""",
            (db._LIBRARY_SYNC_SOURCE, keep_n, cutoff),
        )
        pruned_journal_deliveries = await tx.execute(
            """DELETE FROM profile_seen_journal_deliveries
               WHERE rowid IN (
                   SELECT rowid FROM (
                       SELECT rowid, posted_at,
                              ROW_NUMBER() OVER (
                                  PARTITION BY guild_id, profile_key
                                  ORDER BY posted_at DESC, rowid DESC
                              ) AS rn
                       FROM profile_seen_journal_deliveries
                   )
                   WHERE rn > ? AND posted_at < ?
               )""",
            (keep_n, cutoff),
        )
        pruned_journal_logs = await tx.execute(
            """DELETE FROM profile_seen_journal_logs
               WHERE rowid IN (
                   SELECT rowid FROM (
                       SELECT rowid, posted_at,
                              ROW_NUMBER() OVER (
                                  PARTITION BY profile_key
                                  ORDER BY posted_at DESC, rowid DESC
                              ) AS rn
                       FROM profile_seen_journal_logs
                   )
                   WHERE rn > ? AND posted_at < ?
               )""",
            (keep_n, cutoff),
        )
        pruned_feed_decisions = await tx.execute(
            """DELETE FROM profile_feed_decisions
               WHERE observed_at < ?
                 AND (
                     guild_id NOT IN (SELECT guild_id FROM guild_settings)
                     OR NOT EXISTS (
                         SELECT 1 FROM guild_profile_feed_activity AS gpa
                         WHERE gpa.guild_id = profile_feed_decisions.guild_id
                           AND gpa.profile_key = profile_feed_decisions.profile_key
                           AND gpa.active = 1
                     )
                 )
                 AND NOT EXISTS (
                     SELECT 1 FROM profile_feed_messages AS pfm
                     WHERE pfm.guild_id = profile_feed_decisions.guild_id
                       AND pfm.profile_key = profile_feed_decisions.profile_key
                       AND pfm.event_id = profile_feed_decisions.event_id
                 )""",
            (cutoff,),
        )

    return {
        "observations": pruned_observations,
        "journal_deliveries": pruned_journal_deliveries,
        "journal_logs": pruned_journal_logs,
        "feed_decisions": pruned_feed_decisions,
    }


async def get_profiles_poll_cadence_metadata(
    db: Any, profile_keys: Iterable[str]
) -> dict[str, dict[str, Any]]:
    """Bulk-load source health, gap, baseline, and recency metadata for adaptive poll tiering."""
    normalized_keys = sorted({
        str(key).strip().lower()
        for key in profile_keys
        if key and str(key).strip()
    })
    results: dict[str, dict[str, Any]] = {
        key: {
            "healthy_sources": set(),
            "has_source_gap": False,
            "has_recipient_gap": False,
            "has_pending_reconciliation": False,
            "has_pending_delivery": False,
            "initialized_guild_sources": {},
            "last_activity_at": None,
        }
        for key in normalized_keys
    }
    if not normalized_keys:
        return results

    def _update_latest(entry: dict[str, Any], candidate: Optional[str]) -> None:
        if not candidate:
            return
        current = entry["last_activity_at"]
        if current is None or str(candidate) > str(current):
            entry["last_activity_at"] = str(candidate)

    for chunk in _iter_sql_chunks(normalized_keys, fixed_params=1):
        placeholders = ",".join("?" for _ in chunk)

        health_rows = await db._fetchall(
            f"""SELECT profile_key, source, successes, unresolved_gap_since, last_error
                FROM profile_feed_source_health
                WHERE profile_key IN ({placeholders})""",
            chunk,
        )
        for row in health_rows:
            entry = results[row["profile_key"]]
            if int(row["successes"] or 0) > 0:
                entry["healthy_sources"].add(row["source"])
            if row["unresolved_gap_since"] is not None or row["last_error"] is not None:
                entry["has_source_gap"] = True

        recipient_gap_rows = await db._fetchall(
            f"""SELECT DISTINCT profile_key
                FROM guild_profile_feed_recipient_gaps
                WHERE profile_key IN ({placeholders})""",
            chunk,
        )
        for row in recipient_gap_rows:
            results[row["profile_key"]]["has_recipient_gap"] = True

        reconciliation_rows = await db._fetchall(
            f"""SELECT DISTINCT profile_key
                FROM guild_profile_feed_reconciliation
                WHERE profile_key IN ({placeholders})""",
            chunk,
        )
        for row in reconciliation_rows:
            results[row["profile_key"]]["has_pending_reconciliation"] = True

        pending_delivery_rows = await db._fetchall(
            f"""SELECT DISTINCT profile_key
                FROM profile_feed_messages
                WHERE delivery_state IN ('pending', 'unknown')
                  AND profile_key IN ({placeholders})""",
            chunk,
        )
        for row in pending_delivery_rows:
            results[row["profile_key"]]["has_pending_delivery"] = True

        source_rows = await db._fetchall(
            f"""SELECT guild_id, profile_key, source
                FROM profile_feed_sources
                WHERE profile_key IN ({placeholders})""",
            chunk,
        )
        for row in source_rows:
            entry = results[row["profile_key"]]
            entry["initialized_guild_sources"].setdefault(row["guild_id"], set()).add(row["source"])

        obs_rows = await db._fetchall(
            f"""SELECT profile_key, MAX(observed_at) AS latest_at
                FROM profile_activity_observations
                WHERE source != ? AND profile_key IN ({placeholders})
                GROUP BY profile_key""",
            [db._LIBRARY_SYNC_SOURCE, *chunk],
        )
        for row in obs_rows:
            _update_latest(results[row["profile_key"]], row["latest_at"])

        decision_rows = await db._fetchall(
            f"""SELECT profile_key, MAX(observed_at) AS latest_at
                FROM profile_feed_decisions
                WHERE decision = 'eligible' AND profile_key IN ({placeholders})
                GROUP BY profile_key""",
            chunk,
        )
        for row in decision_rows:
            _update_latest(results[row["profile_key"]], row["latest_at"])

    return results
