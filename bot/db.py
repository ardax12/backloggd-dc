"""Database Manager for Backloggd Discord Bot.

Supports both local SQLite (aiosqlite) and Turso / libSQL Cloud Database (libsql-client).
"""

import asyncio
import json
import logging
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any, Iterable, List, Optional

import aiosqlite
import libsql_client

from bot.config import DATABASE_PATH, LIBSQL_AUTH_TOKEN, LIBSQL_DATABASE_URL
from bot.feed_policy import FEED_DECISION_PRIORITY, group_event_aliases
from bot.db_adapter import execute_batch_statements, open_transaction

logger = logging.getLogger("backloggd.db")

_MAX_SQL_BIND_PARAMS = 800
_MAX_SQL_BATCH_BYTES = 56 * 1024


def _iter_sql_chunks(items: list[Any], *, fixed_params: int = 0) -> Iterable[list[Any]]:
    """Yield non-empty slices sized so fixed_params + len(chunk) <= _MAX_SQL_BIND_PARAMS."""
    chunk_size = max(1, _MAX_SQL_BIND_PARAMS - fixed_params)
    for start in range(0, len(items), chunk_size):
        yield items[start:start + chunk_size]


def _estimate_row_payload_bytes(row: tuple[Any, ...]) -> int:
    """Conservatively estimate JSON/SQL serialized byte cost for one multi-row VALUES tuple."""
    total = 16 + len(row) * 4
    for value in row:
        if value is None:
            total += 6
        elif isinstance(value, bool):
            total += 6
        elif isinstance(value, (int, float)):
            total += 24
        else:
            total += len(str(value).encode("utf-8")) + 8
    return total


def _iter_bounded_row_batches(
    rows: list[tuple[Any, ...]],
    *,
    params_per_row: int,
    fixed_params: int = 0,
    fixed_sql_bytes: int = 512,
    max_bytes: int = _MAX_SQL_BATCH_BYTES,
) -> Iterable[list[tuple[Any, ...]]]:
    """Yield row batches respecting both bind-parameter budget and serialized statement size."""
    max_rows_by_params = max(1, (_MAX_SQL_BIND_PARAMS - fixed_params) // max(1, params_per_row))
    batch: list[tuple[Any, ...]] = []
    current_bytes = fixed_sql_bytes
    for row in rows:
        row_bytes = _estimate_row_payload_bytes(row)
        if batch and (len(batch) >= max_rows_by_params or current_bytes + row_bytes > max_bytes):
            yield batch
            batch = []
            current_bytes = fixed_sql_bytes
        batch.append(row)
        current_bytes += row_bytes
    if batch:
        yield batch


class Database:
    def __init__(
        self,
        db_path: Optional[str] = None,
        turso_url: Optional[str] = None,
        turso_token: Optional[str] = None,
    ):
        self.db_path = db_path or DATABASE_PATH
        if turso_url is not None:
            raw_url = turso_url.strip().strip("'\"")
        elif db_path is not None and db_path != DATABASE_PATH:
            # Custom db_path provided (e.g. test_db.db), do not default to cloud Turso
            raw_url = ""
        else:
            raw_url = LIBSQL_DATABASE_URL.strip().strip("'\"")

        # Normalize URL to HTTPS for reliable cloud API communication.
        # Libsql WebSocket (hrana2) gets rejected by modern Turso cloud regions with HTTP 400,
        # whereas Turso's HTTPS endpoint (/v1/execute) is fully supported and reliable.
        if raw_url.startswith("libsql://"):
            self.turso_url = "https://" + raw_url[len("libsql://"):].rstrip("/")
        elif raw_url.startswith("wss://"):
            self.turso_url = "https://" + raw_url[len("wss://"):].rstrip("/")
        elif raw_url.startswith("ws://"):
            self.turso_url = "http://" + raw_url[len("ws://"):].rstrip("/")
        elif raw_url:
            self.turso_url = raw_url.rstrip("/")
        else:
            self.turso_url = ""

        self.turso_token = (
            turso_token if turso_token is not None else LIBSQL_AUTH_TOKEN
        ).strip().strip("'\"")
        self._conn: Optional[aiosqlite.Connection] = None
        self._client: Optional[libsql_client.Client] = None
        self._operation_lock = asyncio.Lock()
        self._lock_owner_task: Optional[asyncio.Task] = None
        self._active_pending_delivery_tokens: dict[tuple[int, str, str], set[str]] = {}
        self._stale_pending_recovery_checked: bool = False
        self._stale_recovery_lock = asyncio.Lock()

    @property
    def _active_pending_deliveries(self) -> set[tuple[int, str, str]]:
        return {key for key, tokens in self._active_pending_delivery_tokens.items() if tokens}

    @property
    def is_turso(self) -> bool:
        """Check whether using Turso cloud backend."""
        return bool(self.turso_url)

    async def connect(self) -> None:
        """Establish connection and create database tables if they do not exist."""
        self._active_pending_delivery_tokens.clear()
        self._stale_pending_recovery_checked = False
        if self.is_turso:
            logger.info("Connecting to Turso cloud database (%s)...", self.turso_url)
            self._client = libsql_client.create_client(
                url=self.turso_url,
                auth_token=self.turso_token or None,
            )
            await self._create_tables()
            from bot.db_migrations import run_migrations
            await run_migrations(self)
            logger.info("Connected to Turso cloud database.")
        else:
            Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
            self._conn = await aiosqlite.connect(self.db_path)
            self._conn.row_factory = aiosqlite.Row
            await self._conn.execute("PRAGMA foreign_keys = ON;")
            await self._create_tables()
            from bot.db_migrations import run_migrations
            await run_migrations(self)
            logger.info("Connected to local SQLite database at %s", self.db_path)

    @property
    def is_connected(self) -> bool:
        """Check if database connection is active."""
        if self.is_turso:
            is_closed = getattr(self._client, "closed", getattr(self._client, "_closed", False))
            return self._client is not None and not is_closed
        return self._conn is not None and getattr(self._conn, "_running", False)

    @asynccontextmanager
    async def _acquire_operation_lock(self):
        current = asyncio.current_task()
        if current is not None and self._lock_owner_task is current:
            raise RuntimeError("Re-entrant acquisition of non-reentrant Database._operation_lock")
        async with self._operation_lock:
            self._lock_owner_task = current
            try:
                yield
            finally:
                self._lock_owner_task = None

    def _transaction(self):
        return open_transaction(self)

    async def close(self) -> None:
        """Close active database connection."""
        async with self._acquire_operation_lock():
            self._active_pending_delivery_tokens.clear()
            self._stale_pending_recovery_checked = False
            if self._client:
                await self._client.close()
                self._client = None
                logger.info("Closed Turso client connection.")
            if self._conn:
                await self._conn.close()
                self._conn = None
                logger.info("Closed SQLite connection.")

    # =========================================================================
    # Internal Query Helpers (Unified backend abstraction)
    # =========================================================================

    async def _execute(self, query: str, params: tuple | list = ()) -> None:
        """Execute a write statement (INSERT/UPDATE/CREATE/DELETE)."""
        async with self._acquire_operation_lock():
            if self.is_turso:
                await self._client.execute(query, list(params))
            else:
                await self._conn.execute(query, params)
                await self._conn.commit()

    async def _fetchone(self, query: str, params: tuple | list = ()) -> Optional[Any]:
        """Fetch a single row."""
        async with self._acquire_operation_lock():
            if self.is_turso:
                rs = await self._client.execute(query, list(params))
                return rs.rows[0] if rs.rows else None
            async with self._conn.execute(query, params) as cursor:
                return await cursor.fetchone()

    async def _fetchall(self, query: str, params: tuple | list = ()) -> List[Any]:
        """Fetch all rows."""
        async with self._acquire_operation_lock():
            if self.is_turso:
                rs = await self._client.execute(query, list(params))
                return list(rs.rows)
            async with self._conn.execute(query, params) as cursor:
                return await cursor.fetchall()

    async def _rowcount_execute(self, query: str, params: tuple | list = ()) -> int:
        """Execute a write statement and return affected rows count."""
        async with self._acquire_operation_lock():
            if self.is_turso:
                rs = await self._client.execute(query, list(params))
                return rs.rows_affected
            async with self._conn.cursor() as cursor:
                await cursor.execute(query, params)
                await self._conn.commit()
                return cursor.rowcount

    async def _get_applied_schema_version(self) -> int:
        """Return highest applied schema migration version, or 0 if unversioned."""
        table = await self._fetchone(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'schema_migrations';"
        )
        if not table:
            return 0
        row = await self._fetchone("SELECT MAX(version) AS version FROM schema_migrations;")
        return int(row["version"]) if row and row["version"] is not None else 0

    async def _create_tables(self) -> None:
        """Initialize foundational schemas and pre-migration compatibility state where needed."""
        # Foundational active tables needed before versioned migrations run.
        await self._execute("""
            CREATE TABLE IF NOT EXISTS linked_users (
                discord_id INTEGER PRIMARY KEY,
                backloggd_username TEXT NOT NULL UNIQUE,
                avatar_url TEXT,
                last_synced_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        await self._execute("""
            CREATE TABLE IF NOT EXISTS cached_games (
                slug TEXT PRIMARY KEY,
                title TEXT NOT NULL,
                cover_url TEXT,
                release_year INTEGER,
                community_rating REAL
            );
        """)
        await self._execute("""
            CREATE TABLE IF NOT EXISTS guild_settings (
                guild_id INTEGER PRIMARY KEY,
                feed_channel_id INTEGER NOT NULL
            );
        """)

        schema_version = await self._get_applied_schema_version()

        # Pre-v1 user_ratings table (replaced by a compatibility VIEW in migration 1).
        if schema_version < 1:
            rating_object = await self._fetchone(
                "SELECT type FROM sqlite_master WHERE name = 'user_ratings';"
            )
            if not rating_object or rating_object["type"] == "table":
                await self._execute("""
                    CREATE TABLE IF NOT EXISTS user_ratings (
                        discord_id INTEGER NOT NULL,
                        game_slug TEXT NOT NULL,
                        rating REAL,
                        status TEXT NOT NULL,
                        is_liked BOOLEAN DEFAULT 0,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        PRIMARY KEY (discord_id, game_slug),
                        FOREIGN KEY (discord_id) REFERENCES linked_users (discord_id) ON DELETE CASCADE,
                        FOREIGN KEY (game_slug) REFERENCES cached_games (slug) ON DELETE CASCADE
                    );
                """)

        # Pre-v2 legacy feed/journal tables and backfill (migrated into profile_feed_* in migration 2).
        if schema_version < 2:
            await self._execute("""
                CREATE TABLE IF NOT EXISTS feed_sources (
                    guild_id INTEGER NOT NULL,
                    discord_id INTEGER NOT NULL,
                    source TEXT NOT NULL,
                    PRIMARY KEY (guild_id, discord_id, source)
                );
            """)
            await self._execute("""
                CREATE TABLE IF NOT EXISTS feed_messages (
                    guild_id INTEGER NOT NULL,
                    discord_id INTEGER NOT NULL,
                    event_id TEXT NOT NULL,
                    payload TEXT NOT NULL,
                    message_id INTEGER,
                    channel_id INTEGER,
                    PRIMARY KEY (guild_id, discord_id, event_id)
                );
            """)
            await self._execute("""
                CREATE TABLE IF NOT EXISTS seen_journal_logs (
                    entry_id TEXT PRIMARY KEY,
                    discord_id INTEGER NOT NULL,
                    posted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """)
            await self._execute("""
                CREATE TABLE IF NOT EXISTS seen_journal_deliveries (
                    guild_id INTEGER NOT NULL,
                    entry_id TEXT NOT NULL,
                    discord_id INTEGER NOT NULL,
                    posted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (guild_id, entry_id)
                );
            """)
            await self._execute("""
                INSERT OR IGNORE INTO seen_journal_deliveries
                    (guild_id, entry_id, discord_id, posted_at)
                SELECT gs.guild_id, old.entry_id, old.discord_id, old.posted_at
                FROM seen_journal_logs AS old
                CROSS JOIN guild_settings AS gs;
            """)

    async def _execute_batch_results(
        self, statements: List[tuple[str, tuple | list]]
    ) -> List[Any]:
        """Execute multiple statements atomically and return Turso result sets when applicable."""
        return await execute_batch_statements(self, statements)

    async def _execute_batch(self, statements: List[tuple[str, tuple | list]]) -> None:
        """Execute multiple write statements atomically on SQLite or Turso."""
        await self._execute_batch_results(statements)

    @staticmethod
    def _format_utc_sql_timestamp(value: Optional[Any]) -> Optional[str]:
        if value is None:
            return None
        from datetime import datetime, timezone
        if isinstance(value, datetime):
            dt = value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)
            return dt.strftime("%Y-%m-%d %H:%M:%S")
        return str(value)

    @classmethod
    def _now_utc_sql_timestamp(cls, value: Optional[Any]) -> str:
        from datetime import datetime, timezone
        current = value if value is not None else datetime.now(timezone.utc)
        return cls._format_utc_sql_timestamp(current)

    # =========================================================================
    # User Account Linking
    # =========================================================================

    async def link_user(self, discord_id: int, username: str, avatar_url: Optional[str] = None) -> bool:
        """Connect a Discord member to a normalized shared profile, maintaining feed continuity."""
        from bot.ui.helpers import clean_image_url
        clean_avatar = clean_image_url(avatar_url)
        display_username = username.strip()
        profile_key = display_username.lower()
        async with self._transaction() as tx:
            existing = await tx.fetchone(
                "SELECT profile_key FROM linked_users WHERE discord_id = ? LIMIT 1;",
                (discord_id,),
            )
            old_profile_key = existing["profile_key"] if existing else None
            if old_profile_key != profile_key:
                await self._prepare_relinked_user_preferences_tx(tx, discord_id, profile_key)
            await tx.execute(
                """
                INSERT INTO profiles (profile_key, display_username, avatar_url)
                VALUES (?, ?, ?)
                ON CONFLICT(profile_key) DO UPDATE SET
                    display_username = excluded.display_username,
                    avatar_url = COALESCE(excluded.avatar_url, profiles.avatar_url)
                """,
                (profile_key, display_username, clean_avatar),
            )
            await tx.execute(
                """
                INSERT INTO linked_users (discord_id, profile_key, backloggd_username, avatar_url, last_synced_at)
                VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
                ON CONFLICT(discord_id) DO UPDATE SET
                    profile_key = excluded.profile_key,
                    backloggd_username = excluded.backloggd_username,
                    avatar_url = COALESCE(excluded.avatar_url, linked_users.avatar_url),
                    last_synced_at = CURRENT_TIMESTAMP;
                """,
                (discord_id, profile_key, display_username, clean_avatar),
            )
            if old_profile_key is not None and old_profile_key != profile_key:
                active_guilds = await tx.fetchall(
                    "SELECT guild_id FROM guild_profile_feed_activity WHERE profile_key = ? AND active = 1",
                    (old_profile_key,),
                )
                for active_row in active_guilds:
                    await self._reconcile_removed_profile_source_tx(
                        tx,
                        active_row["guild_id"],
                        old_profile_key,
                        present_member_ids=None,
                        removed_discord_id=discord_id,
                    )
        return True

    async def link_user_if_unlinked(
        self, discord_id: int, username: str, avatar_url: Optional[str] = None
    ) -> bool:
        """Create a member link only if no link exists, without replacing a concurrent winner."""
        from bot.ui.helpers import clean_image_url

        clean_avatar = clean_image_url(avatar_url)
        display_username = username.strip()
        profile_key = display_username.lower()
        async with self._transaction() as tx:
            existing = await tx.fetchone(
                "SELECT 1 FROM linked_users WHERE discord_id = ? LIMIT 1;", (discord_id,)
            )
            if existing is not None:
                return False

            await self._prepare_relinked_user_preferences_tx(tx, discord_id, profile_key)
            await tx.execute(
                """
                INSERT INTO profiles (profile_key, display_username, avatar_url)
                VALUES (?, ?, ?)
                ON CONFLICT(profile_key) DO UPDATE SET
                    display_username = excluded.display_username,
                    avatar_url = COALESCE(excluded.avatar_url, profiles.avatar_url)
                """,
                (profile_key, display_username, clean_avatar),
            )
            inserted = await tx.execute(
                """
                INSERT INTO linked_users
                    (discord_id, profile_key, backloggd_username, avatar_url, last_synced_at)
                VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
                ON CONFLICT(discord_id) DO NOTHING;
                """,
                (discord_id, profile_key, display_username, clean_avatar),
            )
            return inserted > 0

    async def unlink_user(self, discord_id: int) -> bool:
        """Remove this member's connection and deactivate guild feeds with no remaining source."""
        async with self._transaction() as tx:
            row = await tx.fetchone(
                "SELECT profile_key FROM linked_users WHERE discord_id = ?;", (discord_id,)
            )
            if not row:
                return False
            profile_key = row["profile_key"]
            deleted = await tx.execute(
                "DELETE FROM linked_users WHERE discord_id = ?;", (discord_id,)
            )
            if deleted <= 0:
                return False
            active_guilds = await tx.fetchall(
                "SELECT guild_id FROM guild_profile_feed_activity WHERE profile_key = ? AND active = 1",
                (profile_key,),
            )
            for active_row in active_guilds:
                await self._reconcile_removed_profile_source_tx(
                    tx,
                    active_row["guild_id"],
                    profile_key,
                    present_member_ids=None,
                    removed_discord_id=discord_id,
                )
            return True

    async def get_linked_user(self, discord_id: int) -> Optional[Any]:
        """Fetch linked user record by Discord ID."""
        return await self._fetchone(
            "SELECT discord_id, profile_key, backloggd_username, avatar_url, last_synced_at FROM linked_users WHERE discord_id = ?;",
            (discord_id,)
        )

    async def get_profile_connections(
        self,
        username: str,
        member_ids: List[int],
        exclude_discord_id: Optional[int] = None,
    ) -> List[Any]:
        """List only this profile's links among explicitly supplied guild members."""
        members = list(dict.fromkeys(int(value) for value in member_ids))
        if not members:
            return []
        excluded = exclude_discord_id if exclude_discord_id is not None else -1
        connections = []
        clean_username = username.strip()
        for member_chunk in _iter_sql_chunks(members, fixed_params=2):
            placeholders = ",".join("?" for _ in member_chunk)
            query = f"""
                SELECT discord_id FROM linked_users
                WHERE profile_key = LOWER(TRIM(?))
                  AND discord_id IN ({placeholders})
                  AND discord_id <> ?
                ORDER BY discord_id
            """
            params = [clean_username, *member_chunk, excluded]
            connections.extend(await self._fetchall(query, params))
        connections.sort(key=lambda row: row["discord_id"])
        return connections

    async def get_profile_sync_info(self, username: str) -> Optional[dict[str, Any]]:
        """Return the profile's last complete sync time, sync health, and cached row count."""
        row = await self._fetchone("""
            SELECT profiles.profile_key, profiles.display_username,
                   profiles.last_synced_at, profiles.last_sync_attempt_at,
                   profiles.last_sync_error,
                   COUNT(profile_ratings.game_slug) AS game_count
            FROM profiles
            LEFT JOIN profile_ratings ON profile_ratings.profile_key = profiles.profile_key
            WHERE profiles.profile_key = LOWER(TRIM(?))
            GROUP BY profiles.profile_key, profiles.display_username,
                     profiles.last_synced_at, profiles.last_sync_attempt_at,
                     profiles.last_sync_error
        """, (username.strip(),))
        return dict(row) if row is not None else None

    async def get_profiles_sync_info(self, profile_keys: Iterable[str]) -> dict[str, dict[str, Any]]:
        """Return sync freshness metadata keyed by normalized profile_key."""
        normalized = list(dict.fromkeys(key.strip().lower() for key in profile_keys if key and key.strip()))
        result: dict[str, dict[str, Any]] = {}
        for chunk in _iter_sql_chunks(normalized):
            placeholders = ",".join("?" for _ in chunk)
            rows = await self._fetchall(f"""
                SELECT profiles.profile_key, profiles.display_username,
                       profiles.last_synced_at, profiles.last_sync_attempt_at,
                       profiles.last_sync_error,
                       COUNT(profile_ratings.game_slug) AS game_count
                FROM profiles
                LEFT JOIN profile_ratings ON profile_ratings.profile_key = profiles.profile_key
                WHERE profiles.profile_key IN ({placeholders})
                GROUP BY profiles.profile_key, profiles.display_username,
                         profiles.last_synced_at, profiles.last_sync_attempt_at,
                         profiles.last_sync_error
            """, chunk)
            for row in rows:
                result[row["profile_key"]] = dict(row)
        return result

    async def is_profile_library_subscribed(self, username: str) -> bool:
        """Return true when at least one Discord link consumes this profile's library."""
        row = await self._fetchone(
            "SELECT 1 FROM linked_users WHERE profile_key = LOWER(TRIM(?)) LIMIT 1",
            (username.strip(),),
        )
        return row is not None

    async def get_profiles_library_subscription_map(
        self, profile_keys: Iterable[str],
    ) -> dict[str, bool]:
        """Return linked-member subscription evidence for normalized profiles in bounded chunks."""
        normalized = list(dict.fromkeys(
            key.strip().lower() for key in profile_keys if key and key.strip()
        ))
        result = {key: False for key in normalized}
        for chunk in _iter_sql_chunks(normalized):
            placeholders = ",".join("?" for _ in chunk)
            rows = await self._fetchall(
                f"""SELECT DISTINCT profile_key FROM linked_users
                    WHERE profile_key IN ({placeholders})""",
                chunk,
            )
            for row in rows:
                result[row["profile_key"]] = True
        return result

    async def get_profile(self, username: str) -> Optional[Any]:
        """Fetch canonical profile data without attributing it to one Discord member."""
        return await self._fetchone("""
            SELECT profile_key, display_username, avatar_url, last_synced_at,
                   last_sync_attempt_at, last_sync_error
            FROM profiles WHERE profile_key = LOWER(TRIM(?))
        """, (username.strip(),))

    async def get_profile_ratings_dict(self, username: str) -> dict[str, Optional[float]]:
        """Return the shared profile's slug-to-rating mapping."""
        rows = await self._fetchall("""
            SELECT game_slug, rating FROM profile_ratings
            WHERE profile_key = LOWER(TRIM(?))
        """, (username.strip(),))
        return {row["game_slug"]: row["rating"] for row in rows}

    async def get_profile_library_entries(self, username: str) -> dict[str, dict[str, Any]]:
        """Return the shared profile's slug-to-{rating, status} mapping."""
        rows = await self._fetchall("""
            SELECT game_slug, rating, status FROM profile_ratings
            WHERE profile_key = LOWER(TRIM(?))
        """, (username.strip(),))
        return {
            row["game_slug"]: {"rating": row["rating"], "status": row["status"]}
            for row in rows
        }

    async def get_profile_stats(self, username: str) -> dict:
        """Fetch aggregate rating stats once for a Backloggd profile."""
        row = await self._fetchone("""
            SELECT COUNT(*) AS total_games,
                   COUNT(rating) AS rated_count,
                   AVG(rating) AS average_rating
            FROM profile_ratings
            WHERE profile_key = LOWER(TRIM(?))
        """, (username.strip(),))
        return {
            "total_games": row["total_games"] if row else 0,
            "rated_count": row["rated_count"] if row else 0,
            "average_rating": (
                round(row["average_rating"], 2)
                if row and row["average_rating"] is not None
                else None
            ),
        }

    async def get_guild_linked_users(self, member_ids: List[int]) -> List[Any]:
        """Fetch all linked users present in a specific guild."""
        members = list(dict.fromkeys(int(value) for value in member_ids))
        if not members:
            return []
        rows: List[Any] = []
        for chunk in _iter_sql_chunks(members):
            placeholders = ",".join("?" for _ in chunk)
            query = f"SELECT discord_id, profile_key, backloggd_username, avatar_url FROM linked_users WHERE discord_id IN ({placeholders});"
            rows.extend(await self._fetchall(query, chunk))
        rows.sort(key=lambda row: row["discord_id"])
        return rows

    async def get_guild_linked_profiles(
        self,
        member_ids: List[int],
        exclude_profile_key: Optional[str] = None,
    ) -> List[dict[str, Any]]:
        """Fetch distinct linked profiles present among the supplied guild members."""
        members = list(dict.fromkeys(int(value) for value in member_ids))
        if not members:
            return []
        excluded = exclude_profile_key.strip().lower() if exclude_profile_key and exclude_profile_key.strip() else None
        exclusion = " AND p.profile_key <> ?" if excluded else ""
        fixed_params = 1 if excluded else 0
        profiles: dict[str, dict[str, Any]] = {}
        for chunk in _iter_sql_chunks(members, fixed_params=fixed_params):
            placeholders = ",".join("?" for _ in chunk)
            params: list[Any] = list(chunk)
            if excluded:
                params.append(excluded)
            query = f"""
                SELECT DISTINCT p.profile_key, p.display_username, p.avatar_url
                FROM linked_users AS u
                JOIN profiles AS p ON p.profile_key = u.profile_key
                WHERE u.discord_id IN ({placeholders}){exclusion}
                ORDER BY p.profile_key
            """
            for row in await self._fetchall(query, params):
                profiles[row["profile_key"]] = dict(row)
        return [profiles[key] for key in sorted(profiles)]

    async def get_guild_linked_profiles_with_sync_info(
        self,
        member_ids: List[int],
        exclude_profile_key: Optional[str] = None,
    ) -> List[dict[str, Any]]:
        """Fetch distinct linked guild profiles with library sync timestamps and errors."""
        members = list(dict.fromkeys(int(value) for value in member_ids))
        if not members:
            return []
        excluded = exclude_profile_key.strip().lower() if exclude_profile_key and exclude_profile_key.strip() else None
        exclusion = " AND p.profile_key <> ?" if excluded else ""
        fixed_params = 1 if excluded else 0
        profiles: dict[str, dict[str, Any]] = {}
        for chunk in _iter_sql_chunks(members, fixed_params=fixed_params):
            placeholders = ",".join("?" for _ in chunk)
            params: list[Any] = list(chunk)
            if excluded:
                params.append(excluded)
            query = f"""
                SELECT DISTINCT p.profile_key, p.display_username, p.avatar_url,
                       p.last_synced_at, p.last_sync_attempt_at, p.last_sync_error
                FROM linked_users AS u
                JOIN profiles AS p ON p.profile_key = u.profile_key
                WHERE u.discord_id IN ({placeholders}){exclusion}
                ORDER BY p.profile_key
            """
            for row in await self._fetchall(query, params):
                profiles[row["profile_key"]] = dict(row)
        return [profiles[key] for key in sorted(profiles)]

    async def set_guild_feed_preference(
        self,
        guild_id: int,
        discord_id: int,
        enabled: bool,
        *,
        member_ids: Optional[Iterable[int]] = None,
    ) -> Optional[str]:
        """Set a member's server-local feed preference; return their profile key."""
        async with self._transaction() as tx:
            linked = await tx.fetchone(
                "SELECT profile_key FROM linked_users WHERE discord_id = ? LIMIT 1",
                (discord_id,),
            )
            if not linked:
                return None
            profile_key = linked["profile_key"]
            if enabled:
                await self._prepare_profile_source_addition_tx(
                    tx, guild_id, profile_key, present_member_ids=member_ids,
                )
            await tx.execute(
                """INSERT INTO guild_feed_preferences (guild_id, discord_id, enabled, updated_at)
                   VALUES (?, ?, ?, CURRENT_TIMESTAMP)
                   ON CONFLICT(guild_id, discord_id) DO UPDATE SET
                       enabled = excluded.enabled, updated_at = CURRENT_TIMESTAMP""",
                (guild_id, discord_id, bool(enabled)),
            )
            return profile_key

    async def get_guild_feed_preference(self, guild_id: int, discord_id: int) -> bool:
        """Absent member preferences are disabled."""
        row = await self._fetchone(
            "SELECT enabled FROM guild_feed_preferences WHERE guild_id = ? AND discord_id = ?",
            (guild_id, discord_id),
        )
        return bool(row and row["enabled"])

    async def get_guild_feed_profiles(
        self, guild_id: int, member_ids: Iterable[int]
    ) -> List[Any]:
        """Retained member-only opt-in profile lookup isolating member preferences from admin follows."""
        members = list(dict.fromkeys(int(value) for value in member_ids))
        profiles = {}
        for chunk in _iter_sql_chunks(members, fixed_params=1):
            placeholders = ",".join("?" for _ in chunk)
            rows = await self._fetchall(
                f"""SELECT DISTINCT p.profile_key, p.display_username, p.avatar_url
                    FROM guild_feed_preferences AS pref
                    JOIN linked_users AS u ON u.discord_id = pref.discord_id
                    JOIN profiles AS p ON p.profile_key = u.profile_key
                    WHERE pref.guild_id = ? AND pref.enabled = 1
                      AND pref.discord_id IN ({placeholders})
                    ORDER BY p.profile_key""",
                [guild_id, *chunk],
            )
            profiles.update((row["profile_key"], row) for row in rows)
        return [profiles[key] for key in sorted(profiles)]

    async def get_legacy_feed_guilds_pending(self) -> List[int]:
        """Configured legacy guilds whose authoritative membership pass is pending."""
        rows = await self._fetchall(
            """SELECT legacy.guild_id
               FROM guild_feed_legacy_migrations AS legacy
               JOIN guild_settings AS settings ON settings.guild_id = legacy.guild_id
               WHERE legacy.completed = 0 ORDER BY legacy.guild_id"""
        )
        return [row["guild_id"] for row in rows]

    async def complete_legacy_feed_migration(
        self, guild_id: int, present_member_ids: Iterable[int]
    ) -> bool:
        """Enroll only migration-scope links present in a complete member snapshot."""
        members = list(dict.fromkeys(int(value) for value in present_member_ids))
        async with self._transaction() as tx:
            pending = await tx.fetchone(
                "SELECT completed FROM guild_feed_legacy_migrations WHERE guild_id = ?",
                (guild_id,),
            )
            if not pending or pending["completed"]:
                return False
            eligible_ids: list[int] = []
            for chunk in _iter_sql_chunks(members, fixed_params=1):
                placeholders = ",".join("?" for _ in chunk)
                eligible = await tx.fetchall(
                    f"""SELECT scope.discord_id
                        FROM guild_feed_legacy_scope AS scope
                        JOIN linked_users AS linked
                          ON linked.discord_id = scope.discord_id
                         AND linked.profile_key = scope.profile_key
                        WHERE scope.guild_id = ? AND scope.discord_id IN ({placeholders})""",
                    [guild_id, *chunk],
                )
                eligible_ids.extend(int(row["discord_id"]) for row in eligible)
            pref_rows = [(guild_id, did) for did in dict.fromkeys(eligible_ids)]
            for batch in _iter_bounded_row_batches(
                pref_rows,
                params_per_row=2,
                fixed_params=0,
                fixed_sql_bytes=128,
            ):
                values_sql = ", ".join(["(?, ?, 1)"] * len(batch))
                flat_params = [value for row in batch for value in row]
                await tx.execute(
                    f"""INSERT OR IGNORE INTO guild_feed_preferences
                        (guild_id, discord_id, enabled) VALUES {values_sql}""",
                    flat_params,
                )
            await tx.execute(
                """UPDATE guild_feed_legacy_migrations
                   SET completed = 1, completed_at = CURRENT_TIMESTAMP
                   WHERE guild_id = ? AND completed = 0""",
                (guild_id,),
            )
            return True

    _CLEAR_UNSENT_FEED_DECISIONS_SQL = """
        DELETE FROM profile_feed_decisions
        WHERE guild_id = ? AND profile_key = ?
          AND decision IN ('baseline', 'eligible')
          AND event_id NOT IN (
              SELECT event_id FROM profile_feed_messages
              WHERE guild_id = ? AND profile_key = ?
                AND delivery_state IN ('sent', 'deleted', 'pending', 'unknown')
          )
    """

    async def _deactivate_guild_profile_tx(self, tx, guild_id: int, profile_key: str) -> None:
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
            self._CLEAR_UNSENT_FEED_DECISIONS_SQL,
            (guild_id, profile_key, guild_id, profile_key),
        )

    async def _prepare_profile_source_addition_tx(
        self,
        tx,
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
                await self._resolve_pending_reconciliation_tx(
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
                await self._deactivate_guild_profile_tx(tx, guild_id, normalized_key)
            return

        if not prior_member_ids:
            await self._deactivate_guild_profile_tx(tx, guild_id, normalized_key)
            return

        await tx.execute(
            """INSERT INTO guild_profile_feed_reconciliation
               (guild_id, profile_key, had_admin_source, candidate_member_ids)
               VALUES (?, ?, 0, ?)
               ON CONFLICT(guild_id, profile_key) DO NOTHING""",
            (guild_id, normalized_key, json.dumps(prior_member_ids)),
        )

    async def _prepare_relinked_user_preferences_tx(
        self, tx, discord_id: int, profile_key: str
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
            await self._prepare_profile_source_addition_tx(
                tx, row["guild_id"], normalized_key, present_member_ids=None,
            )

    async def _resolve_pending_reconciliation_tx(
        self,
        tx,
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
                await self._deactivate_guild_profile_tx(tx, guild_id, pkey)
                deactivated.append(pkey)
            await tx.execute(
                "DELETE FROM guild_profile_feed_reconciliation WHERE guild_id = ? AND profile_key = ?",
                (guild_id, pkey),
            )
        return deactivated

    async def _reconcile_removed_profile_source_tx(
        self,
        tx,
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
                await self._deactivate_guild_profile_tx(tx, guild_id, profile_key)
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
                    await self._deactivate_guild_profile_tx(tx, guild_id, profile_key)
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
            await self._deactivate_guild_profile_tx(tx, guild_id, profile_key)
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
        self,
        guild_id: int,
        discord_id: int,
        *,
        member_ids: Optional[Iterable[int]] = None,
    ) -> Optional[dict[str, Any]]:
        """Atomically opt a member out and deactivate/record reconciliation for their profile."""
        async with self._transaction() as tx:
            linked = await tx.fetchone(
                "SELECT profile_key FROM linked_users WHERE discord_id = ? LIMIT 1",
                (discord_id,),
            )
            if not linked:
                return None
            profile_key = linked["profile_key"]
            if member_ids is not None:
                await self._resolve_pending_reconciliation_tx(
                    tx, guild_id, member_ids, profile_key=profile_key
                )
            await tx.execute(
                """INSERT INTO guild_feed_preferences (guild_id, discord_id, enabled, updated_at)
                   VALUES (?, ?, ?, CURRENT_TIMESTAMP)
                   ON CONFLICT(guild_id, discord_id) DO UPDATE SET
                       enabled = excluded.enabled, updated_at = CURRENT_TIMESTAMP""",
                (guild_id, discord_id, False),
            )
            return await self._reconcile_removed_profile_source_tx(
                tx,
                guild_id,
                profile_key,
                present_member_ids=member_ids,
                removed_discord_id=discord_id,
            )

    async def remove_guild_admin_follow_and_reconcile(
        self,
        guild_id: int,
        username: str,
        *,
        member_ids: Optional[Iterable[int]] = None,
    ) -> dict[str, Any]:
        """Atomically remove a guild admin follow and reconcile that profile's feed state."""
        profile_key = username.strip().lower()
        async with self._transaction() as tx:
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
                await self._resolve_pending_reconciliation_tx(
                    tx, guild_id, member_ids, profile_key=profile_key
                )
            deleted = await tx.execute(
                "DELETE FROM guild_admin_follows WHERE guild_id = ? AND profile_key = ?",
                (guild_id, profile_key),
            )
            reconciled = await self._reconcile_removed_profile_source_tx(
                tx,
                guild_id,
                profile_key,
                present_member_ids=member_ids,
                removed_admin=True,
            )
            return {"removed": deleted > 0, **reconciled}

    async def reconcile_guild_feed_activity(
        self,
        guild_id: int,
        eligible_profile_keys: Iterable[str],
        *,
        now: Optional[Any] = None,
        present_member_ids: Optional[Iterable[int]] = None,
    ) -> dict[str, list[str]]:
        """Pause departed profiles and fresh-baseline profiles that resume atomically."""
        eligible = {key.strip().lower() for key in eligible_profile_keys if key}
        now_sql = self._format_utc_sql_timestamp(now)
        deactivated, activated = [], []
        async with self._transaction() as tx:
            pending_deactivated: set[str] = set()
            if present_member_ids is not None:
                pending_deactivated.update(
                    await self._resolve_pending_reconciliation_tx(tx, guild_id, present_member_ids)
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
                    await self._deactivate_guild_profile_tx(tx, guild_id, profile_key)
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
                        self._CLEAR_UNSENT_FEED_DECISIONS_SQL,
                        (guild_id, profile_key, guild_id, profile_key),
                    )
                    activated.append(profile_key)
            return {"activated": activated, "deactivated": deactivated}

    async def get_guild_profile_member_sources(
        self, guild_id: int, profile_key: str, member_ids: Iterable[int]
    ) -> List[int]:
        """Return current member IDs that actively enroll the given profile."""
        members = list(dict.fromkeys(int(value) for value in member_ids))
        normalized_key = profile_key.strip().lower()
        matching = []
        for chunk in _iter_sql_chunks(members, fixed_params=2):
            placeholders = ",".join("?" for _ in chunk)
            rows = await self._fetchall(
                f"""SELECT pref.discord_id FROM guild_feed_preferences AS pref
                    JOIN linked_users AS linked ON linked.discord_id = pref.discord_id
                    WHERE pref.guild_id = ? AND pref.enabled = 1
                      AND linked.profile_key = ?
                      AND pref.discord_id IN ({placeholders})
                    ORDER BY pref.discord_id""",
                [guild_id, normalized_key, *chunk],
            )
            matching.extend(row["discord_id"] for row in rows)
        matching.sort()
        return matching

    async def add_guild_admin_follow(
        self,
        guild_id: int,
        username: str,
        avatar_url: Optional[str],
        added_by: int,
        *,
        member_ids: Optional[Iterable[int]] = None,
    ) -> bool:
        """Add an idempotent server follow to a validated public profile."""
        from bot.ui.helpers import clean_image_url
        display_username = username.strip()
        profile_key = display_username.lower()
        clean_avatar = clean_image_url(avatar_url)
        async with self._transaction() as tx:
            await self._prepare_profile_source_addition_tx(
                tx, guild_id, profile_key, present_member_ids=member_ids,
            )
            await tx.execute(
                """INSERT INTO profiles (profile_key, display_username, avatar_url)
                   VALUES (?, ?, ?)
                   ON CONFLICT(profile_key) DO UPDATE SET
                       display_username = excluded.display_username,
                       avatar_url = COALESCE(excluded.avatar_url, profiles.avatar_url)""",
                (profile_key, display_username, clean_avatar),
            )
            inserted = await tx.execute(
                """INSERT OR IGNORE INTO guild_admin_follows
                   (guild_id, profile_key, display_username, avatar_url, added_by)
                   VALUES (?, ?, ?, ?, ?)""",
                (guild_id, profile_key, display_username, clean_avatar, added_by),
            )
            return inserted > 0

    async def remove_guild_admin_follow(self, guild_id: int, username: str) -> bool:
        return await self._rowcount_execute(
            "DELETE FROM guild_admin_follows WHERE guild_id = ? AND profile_key = LOWER(TRIM(?))",
            (guild_id, username),
        ) > 0

    async def has_guild_admin_follow(self, guild_id: int, profile_key: str) -> bool:
        return await self._fetchone(
            "SELECT 1 FROM guild_admin_follows WHERE guild_id = ? AND profile_key = LOWER(TRIM(?))",
            (guild_id, profile_key),
        ) is not None

    @staticmethod
    def _empty_feed_status() -> dict[str, Any]:
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
        self, guild_id: int, profile_keys: Iterable[str]
    ) -> dict[str, dict[str, Any]]:
        """Fetch feed status dicts for many profiles in bounded guild- and profile-scoped chunks."""
        normalized_keys = list(dict.fromkeys(
            str(key).strip().lower() for key in profile_keys if key and str(key).strip()
        ))
        if not normalized_keys:
            return {}

        results: dict[str, dict[str, Any]] = {
            key: self._empty_feed_status() for key in normalized_keys
        }

        for chunk in _iter_sql_chunks(normalized_keys, fixed_params=3):
            placeholders = ",".join("?" for _ in chunk)
            params = [guild_id, *chunk]
            activity_params = [guild_id, guild_id, guild_id, *chunk]

            activity_rows = await self._fetchall(
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

            source_rows = await self._fetchall(
                f"""SELECT profile_key, source
                    FROM profile_feed_sources
                    WHERE guild_id = ? AND profile_key IN ({placeholders})
                    ORDER BY profile_key, source""",
                params,
            )
            for row in source_rows:
                results[row["profile_key"]]["initialized_sources"].append(row["source"])

            decision_rows = await self._fetchall(
                f"""SELECT profile_key, decision, COUNT(*) AS count
                    FROM profile_feed_decisions
                    WHERE guild_id = ? AND profile_key IN ({placeholders})
                    GROUP BY profile_key, decision""",
                params,
            )
            for row in decision_rows:
                results[row["profile_key"]]["suppression_counts"][row["decision"]] = row["count"]

            delivery_rows = await self._fetchall(
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
            health_rows = await self._fetchall(
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

    async def get_guild_profile_feed_status(self, guild_id: int, profile_key: str) -> dict:
        normalized = str(profile_key).strip().lower()
        if not normalized:
            return self._empty_feed_status()
        statuses = await self.get_guild_feed_statuses(guild_id, [normalized])
        return statuses[normalized]


    async def get_profile_feed_source(self, profile_key: str, source: str) -> Optional[Any]:
        return await self._fetchone(
            """SELECT source, attempts, successes, last_attempt_at, last_success_at,
                      coverage, pages_read, last_continuous_boundary_event_id,
                      last_continuous_boundary_at, unresolved_gap_since,
                      unresolved_gap_until, gap_reason, last_error
               FROM profile_feed_source_health WHERE profile_key = ? AND source = ?""",
            (profile_key.strip().lower(), source),
        )

    async def get_guild_profile_activation_at(self, guild_id: int, profile_key: str) -> Optional[str]:
        row = await self._fetchone(
            """SELECT last_active_at FROM guild_profile_feed_activity
               WHERE guild_id = ? AND profile_key = ? AND active = 1""",
            (guild_id, profile_key.strip().lower()),
        )
        return row["last_active_at"] if row else None

    async def record_feed_source_attempt(
        self, profile_key: str, source: str, *, now: Optional[Any] = None,
    ) -> None:
        now_sql = self._now_utc_sql_timestamp(now)
        await self._execute(
            """INSERT INTO profile_feed_source_health
               (profile_key, source, attempts, last_attempt_at)
               VALUES (?, ?, 1, ?)
               ON CONFLICT(profile_key, source) DO UPDATE SET
                   attempts = profile_feed_source_health.attempts + 1,
                   last_attempt_at = excluded.last_attempt_at""",
            (profile_key.strip().lower(), source, now_sql),
        )

    async def record_feed_source_success(
        self,
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
        now_sql = self._now_utc_sql_timestamp(now)
        # Event occurrence timestamps do not prove the endpoint of a coverage
        # gap. Keep the argument for compatible callers, but never persist it.
        _ = gap_until
        await self._execute(
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
        self,
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
        now_sql = self._now_utc_sql_timestamp(now)
        await self._execute(
            """UPDATE profile_feed_source_health
               SET last_continuous_boundary_event_id = COALESCE(?, last_continuous_boundary_event_id),
                   last_continuous_boundary_at = ?,
                   unresolved_gap_since = NULL,
                   unresolved_gap_until = NULL,
                   gap_reason = NULL
               WHERE profile_key = ? AND source = ?""",
            (boundary_event_id, now_sql, profile_key.strip().lower(), source),
        )

    async def record_feed_source_failure(self, profile_key: str, source: str, error: str) -> None:
        await self._execute(
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

    async def get_pending_feed_observations(self, guild_id: int, profile_key: str) -> List[Any]:
        """Return validated eligible events that have no terminal delivery record."""
        return await self._fetchall(
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
        self, guild_id: int, profile_key: str, event_id: str
    ) -> Optional[Any]:
        return await self._fetchone(
            """SELECT source, event_date, decision, reason, payload, observed_at
               FROM profile_feed_decisions
               WHERE guild_id = ? AND profile_key = ? AND event_id = ?""",
            (guild_id, profile_key.strip().lower(), event_id),
        )

    async def get_feed_event_decisions(
        self, guild_id: int, profile_key: str, event_ids: Iterable[str]
    ) -> dict[str, Any]:
        """Bulk fetch stored feed event decisions for a guild/profile/alias set."""
        normalized_key = profile_key.strip().lower()
        unique_ids = list(dict.fromkeys(event_id for event_id in event_ids if event_id))
        if not unique_ids:
            return {}
        decisions: dict[str, Any] = {}
        for chunk in _iter_sql_chunks(unique_ids, fixed_params=2):
            placeholders = ",".join("?" for _ in chunk)
            rows = await self._fetchall(
                f"""SELECT event_id, source, event_date, decision, reason, payload, observed_at
                    FROM profile_feed_decisions
                    WHERE guild_id = ? AND profile_key = ? AND event_id IN ({placeholders})""",
                [guild_id, normalized_key, *chunk],
            )
            for row in rows:
                decisions[row["event_id"]] = row
        return decisions

    async def resolve_feed_event_decision(
        self, guild_id: int, profile_key: str, event_id: str, payload: str,
        event_date: Optional[str], *, decision: str = "eligible",
        reason: str = "resolved_by_stable_reference",
    ) -> None:
        """Allow an ambiguous candidate to resolve only after strong identity evidence."""
        await self.persist_feed_identity_decisions(
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
        self,
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

        async with self._transaction() as tx:
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
                        "observed_at": self._format_utc_sql_timestamp(
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

    async def has_feed_source(self, guild_id: int, profile_key: str, source: str) -> bool:
        return await self._fetchone(
            "SELECT 1 FROM profile_feed_sources WHERE guild_id = ? AND profile_key = ? AND source = ?",
            (guild_id, profile_key.strip().lower(), source),
        ) is not None

    async def record_feed_event_decision(
        self, guild_id: int, profile_key: str, event_id: str, *, source: str,
        event_date: Optional[str], decision: str, reason: str, payload: str,
        observed_at: Optional[Any] = None,
    ) -> Any:
        """Persist the first decision; retries cannot age an event into a new post."""
        await self._execute(
            """INSERT OR IGNORE INTO profile_feed_decisions
               (guild_id, profile_key, event_id, source, event_date, decision, reason, payload, observed_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, COALESCE(?, CURRENT_TIMESTAMP))""",
            (guild_id, profile_key.strip().lower(), event_id, source, event_date,
             decision, reason, payload, self._format_utc_sql_timestamp(observed_at)),
        )
        return await self.get_feed_event_decision(guild_id, profile_key, event_id)

    async def revise_unsent_eligible_feed_decision(
        self, guild_id: int, profile_key: str, event_id: str, *,
        decision: str, reason: str, event_date: Optional[str], payload: str,
        allowed_from: Iterable[str] = ("eligible",),
    ) -> None:
        """Apply new evidence to unsent eligible/resolved work without altering archive decisions."""
        valid_from = tuple(dict.fromkeys(d for d in allowed_from if d in {"eligible", "held_ambiguous"}))
        if not valid_from:
            return
        placeholders = ",".join("?" for _ in valid_from)
        await self._execute(
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
        self,
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
                "observed_sql": self._format_utc_sql_timestamp(item.get("observed_at")),
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
        async with self._transaction() as tx:
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

    async def get_feed_decision_counts(self, guild_id: int, profile_key: str) -> dict[str, int]:
        rows = await self._fetchall(
            """SELECT decision, COUNT(*) AS count FROM profile_feed_decisions
               WHERE guild_id = ? AND profile_key = ?
               GROUP BY decision""",
            (guild_id, profile_key.strip().lower()),
        )
        return {row["decision"]: row["count"] for row in rows}

    async def get_guild_admin_follows(self, guild_id: int) -> List[Any]:
        return await self._fetchall(
            """SELECT profile_key, display_username, avatar_url, added_by, created_at
               FROM guild_admin_follows WHERE guild_id = ? ORDER BY profile_key""",
            (guild_id,),
        )

    async def get_guild_feed_recovery_profiles(self, guild_id: int) -> List[Any]:
        """Last active recipients still enrolled, when Discord membership is unavailable.

        This is recovery bookkeeping only: cached membership never authorizes a
        Discord send. A complete member snapshot must still precede delivery.
        """
        return await self._fetchall(
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
        self,
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
        now_sql = self._now_utc_sql_timestamp(now)
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
        await self._execute_batch(statements)

    async def clear_guild_feed_recipient_gaps(
        self, profile_key: str, guild_ids: Iterable[int]
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
        await self._execute_batch(statements)

    async def get_guild_feed_candidates(
        self, guild_id: int, member_ids: Iterable[int]
    ) -> List[dict]:
        """Union current member enrollment and admin follows by public profile."""
        members = list(dict.fromkeys(int(value) for value in member_ids))
        candidates: dict[str, dict] = {}
        for chunk in _iter_sql_chunks(members, fixed_params=1):
            placeholders = ",".join("?" for _ in chunk)
            rows = await self._fetchall(
                f"""SELECT p.profile_key, p.display_username, p.avatar_url, pref.discord_id
                    FROM guild_feed_preferences AS pref
                    JOIN linked_users AS linked ON linked.discord_id = pref.discord_id
                    JOIN profiles AS p ON p.profile_key = linked.profile_key
                    WHERE pref.guild_id = ? AND pref.enabled = 1
                      AND pref.discord_id IN ({placeholders})
                    ORDER BY p.profile_key, pref.discord_id""",
                [guild_id, *chunk],
            )
            for row in rows:
                candidate = candidates.setdefault(row["profile_key"], {
                    "profile_key": row["profile_key"],
                    "display_username": row["display_username"],
                    "avatar_url": row["avatar_url"],
                    "member_ids": [],
                    "admin": False,
                })
                candidate["member_ids"].append(row["discord_id"])
        for row in await self.get_guild_admin_follows(guild_id):
            candidate = candidates.setdefault(row["profile_key"], {
                "profile_key": row["profile_key"],
                "display_username": row["display_username"],
                "avatar_url": row["avatar_url"],
                "member_ids": [],
                "admin": False,
            })
            candidate["admin"] = True
            candidate["avatar_url"] = candidate["avatar_url"] or row["avatar_url"]
        for candidate in candidates.values():
            candidate["member_ids"].sort()
            member = bool(candidate["member_ids"])
            candidate["sources"] = "both" if member and candidate["admin"] else "member" if member else "admin"
        return [candidates[key] for key in sorted(candidates)]

    # =========================================================================
    # Cached Games & Ratings
    # =========================================================================

    async def upsert_game(
        self,
        slug: str,
        title: str,
        cover_url: Optional[str] = None,
        release_year: Optional[int] = None,
        community_rating: Optional[float] = None
    ) -> None:
        """Insert or update metadata for a game in cache."""
        from bot.ui.helpers import clean_image_url
        clean_cover = clean_image_url(cover_url)
        query = """
            INSERT INTO cached_games (slug, title, cover_url, release_year, community_rating)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(slug) DO UPDATE SET
                title = excluded.title,
                cover_url = COALESCE(excluded.cover_url, cached_games.cover_url),
                release_year = COALESCE(excluded.release_year, cached_games.release_year),
                community_rating = COALESCE(excluded.community_rating, cached_games.community_rating);
        """
        await self._execute(query, (slug, title, clean_cover, release_year, community_rating))

    async def upsert_user_rating(
        self,
        discord_id: int,
        game_slug: str,
        rating: Optional[float],
        status: str,
        is_liked: bool = False
    ) -> None:
        """Retained Discord-ID compatibility shim writing through linked_users to shared profile_ratings."""
        query = """
            INSERT INTO profile_ratings (profile_key, game_slug, rating, status, is_liked, updated_at)
            SELECT profile_key, ?, ?, ?, ?, CURRENT_TIMESTAMP
            FROM linked_users WHERE discord_id = ?
            ON CONFLICT(profile_key, game_slug) DO UPDATE SET
                rating = excluded.rating,
                status = excluded.status,
                is_liked = excluded.is_liked,
                updated_at = CURRENT_TIMESTAMP;
        """
        await self._execute(query, (game_slug, rating, status, is_liked, discord_id))

    async def get_server_ratings_for_game(self, guild_member_ids: List[int], game_slug: str) -> List[Any]:
        """Fetch one rating/status row per Backloggd profile connected in this guild."""
        if not guild_member_ids:
            return []
        profiles = await self.get_guild_linked_profiles(guild_member_ids)
        profile_keys = [row["profile_key"] for row in profiles]
        if not profile_keys:
            return []
        rows: List[Any] = []
        for chunk in _iter_sql_chunks(profile_keys, fixed_params=1):
            placeholders = ",".join("?" for _ in chunk)
            query = f"""
                SELECT r.profile_key, p.display_username AS backloggd_username,
                       p.avatar_url, r.rating, r.status, r.is_liked
                FROM profile_ratings r
                JOIN profiles p ON p.profile_key = r.profile_key
                WHERE r.profile_key IN ({placeholders}) AND r.game_slug = ?;
            """
            rows.extend(await self._fetchall(query, [*chunk, game_slug]))
        rows.sort(
            key=lambda r: (
                r["rating"] is None,
                -(r["rating"] if r["rating"] is not None else 0.0),
                r["backloggd_username"],
            )
        )
        return rows

    async def get_user_backlog(self, discord_id: int) -> List[Any]:
        """Retrieve unrated backlog and wishlist games for a user."""
        query = """
            SELECT g.slug, g.title, g.cover_url, g.release_year, g.community_rating, r.rating, r.is_liked
            FROM profile_ratings r
            JOIN cached_games g ON r.game_slug = g.slug
            WHERE r.profile_key = (
                SELECT profile_key FROM linked_users WHERE discord_id = ?
            )
              AND r.status IN ('backlog', 'wishlist')
              AND r.rating IS NULL;
        """
        return await self._fetchall(query, (discord_id,))

    _LIBRARY_SYNC_SOURCE = "__library_sync__"
    _PENDING_REFRESH_KEY = "__pending_refresh__"

    async def replace_user_library(
        self,
        discord_id: int,
        expected_username: str,
        games: Iterable[Any],
        *,
        synced_at: Optional[Any] = None,
        clear_pending_token: Optional[str] = None,
    ) -> bool:
        """Retained Discord-ID fallback delegating to replace_profile_library(..., require_linked=True)."""
        return await self.replace_profile_library(
            expected_username,
            games,
            synced_at=synced_at,
            require_linked=True,
            clear_pending_token=clear_pending_token,
        )

    async def replace_profile_library(
        self,
        username: str,
        games: Iterable[Any],
        *,
        synced_at: Optional[Any] = None,
        require_linked: bool = True,
        clear_pending_token: Optional[str] = None,
    ) -> bool:
        """Atomically replace the shared profile library and record sync completion."""
        from bot.ui.helpers import clean_image_url

        profile_key = username.strip().lower()
        cached_game_rows: list[tuple[Any, ...]] = []
        rating_rows: list[tuple[Any, ...]] = []
        sqlite_rating_rows: list[tuple[Any, ...]] = []
        for game in games:
            slug = game.slug
            status_val = game.status.value if hasattr(game.status, "value") else str(game.status)
            rating_val = getattr(game, "rating", None)
            liked_val = bool(getattr(game, "is_liked", False))
            cached_game_rows.append((
                slug,
                game.title,
                clean_image_url(getattr(game, "cover_url", None)),
                getattr(game, "release_year", None),
                getattr(game, "community_rating", None),
            ))
            rating_rows.append((slug, rating_val, status_val, liked_val))
            sqlite_rating_rows.append((profile_key, slug, rating_val, status_val, liked_val))

        synced_sql = self._format_utc_sql_timestamp(synced_at)
        ownership_sql = "SELECT 1 FROM linked_users WHERE profile_key = ? LIMIT 1"

        if self.is_turso:
            statements = []
            if require_linked:
                statements.append((ownership_sql, (profile_key,)))
                guard = f"AND EXISTS ({ownership_sql})"
                profile_args = (profile_key, profile_key)
            else:
                guard = ""
                profile_args = (profile_key,)
            if synced_sql is not None:
                statements.append((
                    f"""UPDATE profiles
                        SET last_synced_at = ?, last_sync_attempt_at = ?, last_sync_error = NULL
                        WHERE profile_key = ? {guard}""",
                    (synced_sql, synced_sql, *profile_args),
                ))
                statements.append((
                    f"""UPDATE linked_users SET last_synced_at = ?
                        WHERE profile_key = ? {guard}""",
                    (synced_sql, *profile_args),
                ))
            else:
                statements.append((
                    f"""UPDATE profiles
                        SET last_synced_at = CURRENT_TIMESTAMP, last_sync_attempt_at = CURRENT_TIMESTAMP, last_sync_error = NULL
                        WHERE profile_key = ? {guard}""",
                    profile_args,
                ))
                statements.append((
                    f"""UPDATE linked_users SET last_synced_at = CURRENT_TIMESTAMP
                        WHERE profile_key = ? {guard}""",
                    profile_args,
                ))

            for batch in _iter_bounded_row_batches(
                cached_game_rows,
                params_per_row=5,
                fixed_params=1 if require_linked else 0,
                fixed_sql_bytes=480,
            ):
                values_sql = ", ".join(["(?, ?, ?, ?, ?)"] * len(batch))
                flat_params = [value for row in batch for value in row]
                if require_linked:
                    statements.append((f"""
                        INSERT INTO cached_games (slug, title, cover_url, release_year, community_rating)
                        SELECT v.column1, v.column2, v.column3, v.column4, v.column5
                        FROM (VALUES {values_sql}) AS v
                        WHERE EXISTS ({ownership_sql})
                        ON CONFLICT(slug) DO UPDATE SET
                            title = excluded.title,
                            cover_url = COALESCE(excluded.cover_url, cached_games.cover_url),
                            release_year = COALESCE(excluded.release_year, cached_games.release_year),
                            community_rating = COALESCE(excluded.community_rating, cached_games.community_rating)
                    """, (*flat_params, profile_key)))
                else:
                    statements.append((f"""
                        INSERT INTO cached_games (slug, title, cover_url, release_year, community_rating)
                        VALUES {values_sql}
                        ON CONFLICT(slug) DO UPDATE SET
                            title = excluded.title,
                            cover_url = COALESCE(excluded.cover_url, cached_games.cover_url),
                            release_year = COALESCE(excluded.release_year, cached_games.release_year),
                            community_rating = COALESCE(excluded.community_rating, cached_games.community_rating)
                    """, tuple(flat_params)))

            statements.append((
                f"DELETE FROM profile_ratings WHERE profile_key = ? {guard}",
                profile_args,
            ))

            where_clause = f"WHERE EXISTS ({ownership_sql})" if require_linked else "WHERE 1 = 1"
            fixed_rating_params = 2 if require_linked else 1
            for batch in _iter_bounded_row_batches(
                rating_rows,
                params_per_row=4,
                fixed_params=fixed_rating_params,
                fixed_sql_bytes=480,
            ):
                values_sql = ", ".join(["(?, ?, ?, ?)"] * len(batch))
                flat_params = [value for row in batch for value in row]
                insert_args = (profile_key, *flat_params)
                if require_linked:
                    insert_args = (*insert_args, profile_key)
                statements.append((f"""
                    INSERT INTO profile_ratings
                        (profile_key, game_slug, rating, status, is_liked, updated_at)
                    SELECT ?, v.column1, v.column2, v.column3, v.column4, CURRENT_TIMESTAMP
                    FROM (VALUES {values_sql}) AS v
                    {where_clause}
                    ON CONFLICT(profile_key, game_slug) DO UPDATE SET
                        rating = excluded.rating,
                        status = excluded.status,
                        is_liked = excluded.is_liked,
                        updated_at = CURRENT_TIMESTAMP
                """, insert_args))

            if clear_pending_token is not None:
                clear_args = (
                    profile_key,
                    self._LIBRARY_SYNC_SOURCE,
                    self._PENDING_REFRESH_KEY,
                    clear_pending_token,
                )
                if require_linked:
                    clear_args = (*clear_args, profile_key)
                statements.append((
                    f"""DELETE FROM profile_activity_observations
                        WHERE profile_key = ? AND source = ? AND observation_key = ?
                          AND fingerprint = ? {guard}""",
                    clear_args,
                ))
            results = await self._execute_batch_results(statements)
            if require_linked:
                return bool(results and results[0].rows)
            return True

        async with self._transaction() as tx:
            if require_linked:
                link = await tx.fetchone(ownership_sql, (profile_key,))
                if not link:
                    return False

            if synced_sql is not None:
                await tx.execute(
                    """UPDATE profiles
                       SET last_synced_at = ?, last_sync_attempt_at = ?, last_sync_error = NULL
                       WHERE profile_key = ?""",
                    (synced_sql, synced_sql, profile_key),
                )
                await tx.execute(
                    "UPDATE linked_users SET last_synced_at = ? WHERE profile_key = ?",
                    (synced_sql, profile_key),
                )
            else:
                await tx.execute(
                    """UPDATE profiles
                       SET last_synced_at = CURRENT_TIMESTAMP,
                           last_sync_attempt_at = CURRENT_TIMESTAMP,
                           last_sync_error = NULL
                       WHERE profile_key = ?""",
                    (profile_key,),
                )
                await tx.execute(
                    "UPDATE linked_users SET last_synced_at = CURRENT_TIMESTAMP WHERE profile_key = ?",
                    (profile_key,),
                )
            if cached_game_rows:
                await tx.executemany("""
                    INSERT INTO cached_games (slug, title, cover_url, release_year, community_rating)
                    VALUES (?, ?, ?, ?, ?)
                    ON CONFLICT(slug) DO UPDATE SET
                        title = excluded.title,
                        cover_url = COALESCE(excluded.cover_url, cached_games.cover_url),
                        release_year = COALESCE(excluded.release_year, cached_games.release_year),
                        community_rating = COALESCE(excluded.community_rating, cached_games.community_rating)
                """, cached_game_rows)
            await tx.execute(
                "DELETE FROM profile_ratings WHERE profile_key = ?;", (profile_key,)
            )
            if sqlite_rating_rows:
                await tx.executemany("""
                    INSERT INTO profile_ratings
                        (profile_key, game_slug, rating, status, is_liked, updated_at)
                    VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                    ON CONFLICT(profile_key, game_slug) DO UPDATE SET
                        rating = excluded.rating,
                        status = excluded.status,
                        is_liked = excluded.is_liked,
                        updated_at = CURRENT_TIMESTAMP
                """, sqlite_rating_rows)
            if clear_pending_token is not None:
                await tx.execute(
                    """DELETE FROM profile_activity_observations
                       WHERE profile_key = ? AND source = ? AND observation_key = ?
                         AND fingerprint = ?""",
                    (
                        profile_key,
                        self._LIBRARY_SYNC_SOURCE,
                        self._PENDING_REFRESH_KEY,
                        clear_pending_token,
                    ),
                )
            return True

    async def record_profile_sync_failure(
        self,
        username: str,
        error: str,
        *,
        attempted_at: Optional[Any] = None,
    ) -> None:
        """Record a failed library refresh without erasing cached ratings or last_synced_at."""
        profile_key = username.strip().lower()
        attempted_sql = self._format_utc_sql_timestamp(attempted_at)
        error_text = str(error)[:500]
        if attempted_sql is not None:
            await self._execute(
                """UPDATE profiles
                   SET last_sync_attempt_at = ?, last_sync_error = ?
                   WHERE profile_key = ?""",
                (attempted_sql, error_text, profile_key),
            )
        else:
            await self._execute(
                """UPDATE profiles
                   SET last_sync_attempt_at = CURRENT_TIMESTAMP, last_sync_error = ?
                   WHERE profile_key = ?""",
                (error_text, profile_key),
            )

    async def get_multi_profile_ratings_dict(
        self,
        profile_keys: List[str],
    ) -> dict[str, dict[str, Optional[float]]]:
        """Return one cached rating map per normalized Backloggd profile."""
        normalized_keys = list(dict.fromkeys(key.strip().lower() for key in profile_keys if key.strip()))
        result: dict[str, dict[str, Optional[float]]] = {key: {} for key in normalized_keys}
        for key_chunk in _iter_sql_chunks(normalized_keys):
            placeholders = ",".join("?" for _ in key_chunk)
            rows = await self._fetchall(
                f"""SELECT profile_key, game_slug, rating
                    FROM profile_ratings WHERE profile_key IN ({placeholders})""",
                key_chunk,
            )
            for row in rows:
                result[row["profile_key"]][row["game_slug"]] = row["rating"]
        return result

    async def get_games_by_slugs(self, slugs: List[str]) -> dict[str, Any]:
        """Fetch cached game records by slugs in bounded batch queries."""
        clean_slugs = list(dict.fromkeys(slug for slug in slugs if slug))
        if not clean_slugs:
            return {}
        result: dict[str, Any] = {}
        for chunk in _iter_sql_chunks(clean_slugs):
            placeholders = ",".join("?" for _ in chunk)
            query = (
                "SELECT slug, title, cover_url, release_year, community_rating "
                f"FROM cached_games WHERE slug IN ({placeholders});"
            )
            for row in await self._fetchall(query, chunk):
                result[row["slug"]] = row
        return result

    # =========================================================================
    # Server Leaderboards & Statistics
    # =========================================================================

    async def get_guild_stats_context(self, member_ids: List[int]) -> dict[str, Any]:
        """Resolve linked-member count, distinct profiles, and sync metadata in one pass."""
        members = list(dict.fromkeys(int(value) for value in member_ids))
        if not members:
            return {
                "linked_members": 0,
                "profile_keys": [],
                "profiles": [],
            }
        linked_discord_ids: set[int] = set()
        profiles: dict[str, dict[str, Any]] = {}
        for chunk in _iter_sql_chunks(members):
            placeholders = ",".join("?" for _ in chunk)
            query = f"""
                SELECT u.discord_id,
                       p.profile_key, p.display_username, p.avatar_url,
                       p.last_synced_at, p.last_sync_attempt_at, p.last_sync_error
                FROM linked_users AS u
                JOIN profiles AS p ON p.profile_key = u.profile_key
                WHERE u.discord_id IN ({placeholders})
                ORDER BY p.profile_key, u.discord_id
            """
            for row in await self._fetchall(query, chunk):
                linked_discord_ids.add(int(row["discord_id"]))
                key = row["profile_key"]
                if key not in profiles:
                    profiles[key] = {
                        "profile_key": key,
                        "display_username": row["display_username"],
                        "avatar_url": row["avatar_url"],
                        "last_synced_at": row["last_synced_at"],
                        "last_sync_attempt_at": row["last_sync_attempt_at"],
                        "last_sync_error": row["last_sync_error"],
                    }
        sorted_keys = sorted(profiles)
        return {
            "linked_members": len(linked_discord_ids),
            "profile_keys": sorted_keys,
            "profiles": [profiles[key] for key in sorted_keys],
        }

    async def _get_guild_linked_profile_keys(self, guild_member_ids: List[int]) -> List[str]:
        if not guild_member_ids:
            return []
        profiles = await self.get_guild_linked_profiles(guild_member_ids)
        return [row["profile_key"] for row in profiles]

    async def get_server_top_rated_for_profiles(
        self, profile_keys: List[str], limit: int = 10, min_ratings: int = 2
    ) -> List[dict[str, Any]]:
        """Query distinct canonical profile keys; return plain result dictionaries."""
        if not profile_keys or limit <= 0:
            return []
        aggregates: dict[str, dict[str, Any]] = {}
        for chunk in _iter_sql_chunks(profile_keys):
            placeholders = ",".join("?" for _ in chunk)
            query = f"""
                SELECT g.slug, g.title, g.cover_url, g.release_year,
                       SUM(r.rating) AS rating_sum,
                       COUNT(r.rating) AS rating_count
                FROM profile_ratings r
                JOIN cached_games g ON r.game_slug = g.slug
                WHERE r.profile_key IN ({placeholders}) AND r.rating IS NOT NULL
                GROUP BY g.slug;
            """
            for row in await self._fetchall(query, chunk):
                entry = aggregates.setdefault(
                    row["slug"],
                    {
                        "slug": row["slug"],
                        "title": row["title"],
                        "cover_url": row["cover_url"],
                        "release_year": row["release_year"],
                        "rating_sum": 0.0,
                        "rating_count": 0,
                    },
                )
                entry["rating_sum"] += float(row["rating_sum"] or 0.0)
                entry["rating_count"] += int(row["rating_count"] or 0)
        results = []
        for entry in aggregates.values():
            count = entry["rating_count"]
            if count >= min_ratings and count > 0:
                results.append({
                    "slug": entry["slug"],
                    "title": entry["title"],
                    "cover_url": entry["cover_url"],
                    "release_year": entry["release_year"],
                    "avg_rating": entry["rating_sum"] / count,
                    "rating_count": count,
                })
        results.sort(key=lambda row: (-row["avg_rating"], -row["rating_count"], row["slug"]))
        return results[:limit]

    async def get_server_top_rated(self, guild_member_ids: List[int], limit: int = 10, min_ratings: int = 2) -> List[dict[str, Any]]:
        """Get top games by average rating among distinct linked profiles."""
        if not guild_member_ids or limit <= 0:
            return []
        profile_keys = await self._get_guild_linked_profile_keys(guild_member_ids)
        return await self.get_server_top_rated_for_profiles(profile_keys, limit=limit, min_ratings=min_ratings)

    async def get_server_most_played_for_profiles(
        self, profile_keys: List[str], limit: int = 10
    ) -> List[dict[str, Any]]:
        """Query distinct canonical profile keys; return plain result dictionaries."""
        if not profile_keys or limit <= 0:
            return []
        aggregates: dict[str, dict[str, Any]] = {}
        for chunk in _iter_sql_chunks(profile_keys):
            placeholders = ",".join("?" for _ in chunk)
            query = f"""
                SELECT g.slug, g.title, g.cover_url, g.release_year,
                       COUNT(r.profile_key) AS play_count,
                       SUM(r.rating) AS rating_sum,
                       COUNT(r.rating) AS rating_count
                FROM profile_ratings r
                JOIN cached_games g ON r.game_slug = g.slug
                WHERE r.profile_key IN ({placeholders}) AND r.status = 'played'
                GROUP BY g.slug;
            """
            for row in await self._fetchall(query, chunk):
                entry = aggregates.setdefault(
                    row["slug"],
                    {
                        "slug": row["slug"],
                        "title": row["title"],
                        "cover_url": row["cover_url"],
                        "release_year": row["release_year"],
                        "play_count": 0,
                        "rating_sum": 0.0,
                        "rating_count": 0,
                    },
                )
                entry["play_count"] += int(row["play_count"] or 0)
                if row["rating_sum"] is not None:
                    entry["rating_sum"] += float(row["rating_sum"])
                entry["rating_count"] += int(row["rating_count"] or 0)
        results = []
        for entry in aggregates.values():
            rating_count = entry["rating_count"]
            results.append({
                "slug": entry["slug"],
                "title": entry["title"],
                "cover_url": entry["cover_url"],
                "release_year": entry["release_year"],
                "play_count": entry["play_count"],
                "avg_rating": (entry["rating_sum"] / rating_count) if rating_count > 0 else None,
            })
        results.sort(
            key=lambda row: (
                -row["play_count"],
                row["avg_rating"] is None,
                -(row["avg_rating"] if row["avg_rating"] is not None else 0.0),
                row["slug"],
            )
        )
        return results[:limit]

    async def get_server_most_played(self, guild_member_ids: List[int], limit: int = 10) -> List[dict[str, Any]]:
        """Get most completed games by distinct linked profiles."""
        if not guild_member_ids or limit <= 0:
            return []
        profile_keys = await self._get_guild_linked_profile_keys(guild_member_ids)
        return await self.get_server_most_played_for_profiles(profile_keys, limit=limit)

    async def get_server_shame_list_for_profiles(
        self, profile_keys: List[str], limit: int = 10
    ) -> List[dict[str, Any]]:
        """Query distinct canonical profile keys; return plain result dictionaries."""
        if not profile_keys or limit <= 0:
            return []
        aggregates: dict[str, dict[str, Any]] = {}
        for chunk in _iter_sql_chunks(profile_keys):
            placeholders = ",".join("?" for _ in chunk)
            query = f"""
                SELECT g.slug, g.title, g.cover_url, g.release_year,
                       COUNT(r.profile_key) AS backlog_count
                FROM profile_ratings r
                JOIN cached_games g ON r.game_slug = g.slug
                WHERE r.profile_key IN ({placeholders}) AND r.status = 'backlog'
                GROUP BY g.slug;
            """
            for row in await self._fetchall(query, chunk):
                entry = aggregates.setdefault(
                    row["slug"],
                    {
                        "slug": row["slug"],
                        "title": row["title"],
                        "cover_url": row["cover_url"],
                        "release_year": row["release_year"],
                        "backlog_count": 0,
                    },
                )
                entry["backlog_count"] += int(row["backlog_count"] or 0)
        results = list(aggregates.values())
        results.sort(key=lambda row: (-row["backlog_count"], row["slug"]))
        return results[:limit]

    async def get_server_shame_list(self, guild_member_ids: List[int], limit: int = 10) -> List[dict[str, Any]]:
        """Get games most frequently sitting unplayed in linked profiles' backlogs."""
        if not guild_member_ids or limit <= 0:
            return []
        profile_keys = await self._get_guild_linked_profile_keys(guild_member_ids)
        return await self.get_server_shame_list_for_profiles(profile_keys, limit=limit)

    async def get_server_polarizing_for_profiles(
        self, profile_keys: List[str], limit: int = 10, min_ratings: int = 2
    ) -> List[dict[str, Any]]:
        """Query distinct canonical profile keys; return plain result dictionaries."""
        if not profile_keys or limit <= 0:
            return []
        aggregates: dict[str, dict[str, Any]] = {}
        for chunk in _iter_sql_chunks(profile_keys):
            placeholders = ",".join("?" for _ in chunk)
            query = f"""
                SELECT g.slug, g.title, g.cover_url, g.release_year,
                       MAX(r.rating) AS max_rating,
                       MIN(r.rating) AS min_rating,
                       SUM(r.rating) AS rating_sum,
                       COUNT(r.rating) AS rating_count
                FROM profile_ratings r
                JOIN cached_games g ON r.game_slug = g.slug
                WHERE r.profile_key IN ({placeholders}) AND r.rating IS NOT NULL
                GROUP BY g.slug;
            """
            for row in await self._fetchall(query, chunk):
                row_max = float(row["max_rating"])
                row_min = float(row["min_rating"])
                entry = aggregates.get(row["slug"])
                if entry is None:
                    aggregates[row["slug"]] = {
                        "slug": row["slug"],
                        "title": row["title"],
                        "cover_url": row["cover_url"],
                        "release_year": row["release_year"],
                        "max_rating": row_max,
                        "min_rating": row_min,
                        "rating_sum": float(row["rating_sum"] or 0.0),
                        "rating_count": int(row["rating_count"] or 0),
                    }
                else:
                    if row_max > entry["max_rating"]:
                        entry["max_rating"] = row_max
                    if row_min < entry["min_rating"]:
                        entry["min_rating"] = row_min
                    entry["rating_sum"] += float(row["rating_sum"] or 0.0)
                    entry["rating_count"] += int(row["rating_count"] or 0)
        results = []
        for entry in aggregates.values():
            count = entry["rating_count"]
            delta = entry["max_rating"] - entry["min_rating"]
            if count >= min_ratings and delta > 0:
                results.append({
                    "slug": entry["slug"],
                    "title": entry["title"],
                    "cover_url": entry["cover_url"],
                    "release_year": entry["release_year"],
                    "rating_delta": delta,
                    "avg_rating": entry["rating_sum"] / count,
                    "rating_count": count,
                })
        results.sort(key=lambda row: (-row["rating_delta"], -row["rating_count"], row["slug"]))
        return results[:limit]

    async def get_server_polarizing(self, guild_member_ids: List[int], limit: int = 10, min_ratings: int = 2) -> List[dict[str, Any]]:
        """Get games with the highest rating disparity (max - min) among distinct linked profiles."""
        if not guild_member_ids or limit <= 0:
            return []
        profile_keys = await self._get_guild_linked_profile_keys(guild_member_ids)
        return await self.get_server_polarizing_for_profiles(profile_keys, limit=limit, min_ratings=min_ratings)

    async def get_server_hidden_gems_for_profiles(
        self, profile_keys: List[str], limit: int = 10
    ) -> List[dict[str, Any]]:
        """Query distinct canonical profile keys; return plain result dictionaries."""
        if not profile_keys or limit <= 0:
            return []
        aggregates: dict[str, dict[str, Any]] = {}
        for chunk in _iter_sql_chunks(profile_keys):
            placeholders = ",".join("?" for _ in chunk)
            query = f"""
                SELECT g.slug, g.title, g.cover_url, g.release_year,
                       SUM(r.rating) AS rating_sum,
                       COUNT(r.rating) AS rating_count
                FROM profile_ratings r
                JOIN cached_games g ON r.game_slug = g.slug
                WHERE r.profile_key IN ({placeholders}) AND r.rating IS NOT NULL
                GROUP BY g.slug;
            """
            for row in await self._fetchall(query, chunk):
                entry = aggregates.setdefault(
                    row["slug"],
                    {
                        "slug": row["slug"],
                        "title": row["title"],
                        "cover_url": row["cover_url"],
                        "release_year": row["release_year"],
                        "rating_sum": 0.0,
                        "rating_count": 0,
                    },
                )
                entry["rating_sum"] += float(row["rating_sum"] or 0.0)
                entry["rating_count"] += int(row["rating_count"] or 0)
        results = []
        for entry in aggregates.values():
            count = entry["rating_count"]
            if 0 < count <= 2:
                avg_rating = entry["rating_sum"] / count
                if avg_rating >= 4.5:
                    results.append({
                        "slug": entry["slug"],
                        "title": entry["title"],
                        "cover_url": entry["cover_url"],
                        "release_year": entry["release_year"],
                        "avg_rating": avg_rating,
                        "rating_count": count,
                    })
        results.sort(key=lambda row: (-row["avg_rating"], row["rating_count"], row["slug"]))
        return results[:limit]

    async def get_server_hidden_gems(self, guild_member_ids: List[int], limit: int = 10) -> List[dict[str, Any]]:
        """Get high-scoring games (>= 4.5) rated by only 1 or 2 distinct linked profiles."""
        if not guild_member_ids or limit <= 0:
            return []
        profile_keys = await self._get_guild_linked_profile_keys(guild_member_ids)
        return await self.get_server_hidden_gems_for_profiles(profile_keys, limit=limit)

    async def get_server_stats_for_profiles(
        self,
        profile_keys: List[str],
        *,
        linked_members: int,
        include_library_row_counts: bool = True,
    ) -> dict:
        """Query distinct canonical profile keys; return plain result dictionaries."""
        profile_count = len(profile_keys)
        if not profile_keys:
            result = {
                "linked_members": linked_members,
                "profile_count": 0,
                "server_avg_rating": None,
                "top_game": None,
            }
            if include_library_row_counts:
                result["total_logged"] = 0
                result["total_played"] = 0
            return result

        total_logged = 0
        total_played = 0
        rating_sum = 0.0
        rating_count = 0
        played_counts: dict[str, dict[str, Any]] = {}

        for chunk in _iter_sql_chunks(profile_keys):
            placeholders = ",".join("?" for _ in chunk)
            if include_library_row_counts:
                summary_sql = f"""
                SELECT COUNT(*) AS total_logged,
                       SUM(CASE WHEN status = 'played' THEN 1 ELSE 0 END) AS total_played,
                       SUM(rating) AS rating_sum,
                       COUNT(rating) AS rating_count
                FROM profile_ratings
                WHERE profile_key IN ({placeholders});
                """
            else:
                summary_sql = f"""
                SELECT SUM(rating) AS rating_sum,
                       COUNT(rating) AS rating_count
                FROM profile_ratings
                WHERE profile_key IN ({placeholders});
                """
            row = await self._fetchone(summary_sql, chunk)
            if row:
                if include_library_row_counts:
                    total_logged += int(row["total_logged"] or 0)
                    total_played += int(row["total_played"] or 0)
                if row["rating_sum"] is not None:
                    rating_sum += float(row["rating_sum"])
                rating_count += int(row["rating_count"] or 0)

            top_rows = await self._fetchall(
                f"""
                SELECT g.slug, g.title, COUNT(r.profile_key) AS count
                FROM profile_ratings r
                JOIN cached_games g ON r.game_slug = g.slug
                WHERE r.profile_key IN ({placeholders}) AND r.status = 'played'
                GROUP BY g.slug;
                """,
                chunk,
            )
            for top_row in top_rows:
                entry = played_counts.setdefault(
                    top_row["slug"],
                    {"slug": top_row["slug"], "title": top_row["title"], "count": 0},
                )
                entry["count"] += int(top_row["count"] or 0)

        avg_rating = round(rating_sum / rating_count, 2) if rating_count > 0 else None
        top_game = None
        if played_counts:
            best = min(played_counts.values(), key=lambda item: (-item["count"], item["slug"]))
            top_game = f"{best['title']} ({best['count']} profile{'s' if best['count'] != 1 else ''})"

        result = {
            "linked_members": linked_members,
            "profile_count": profile_count,
            "server_avg_rating": avg_rating,
            "top_game": top_game,
        }
        if include_library_row_counts:
            result["total_logged"] = total_logged
            result["total_played"] = total_played
        return result

    async def get_server_stats(self, guild_member_ids: List[int]) -> dict:
        """Compute aggregate community metrics for a server."""
        if not guild_member_ids:
            return {
                "linked_members": 0,
                "profile_count": 0,
                "total_logged": 0,
                "total_played": 0,
                "server_avg_rating": None,
                "top_game": None,
            }

        context = await self.get_guild_stats_context(guild_member_ids)
        return await self.get_server_stats_for_profiles(
            context["profile_keys"],
            linked_members=context["linked_members"],
            include_library_row_counts=True,
        )

    # =========================================================================
    # Guild Settings & Feed Deduplication
    # =========================================================================

    async def set_feed_channel(self, guild_id: int, channel_id: int) -> None:
        """Configure or update the feed channel for a guild."""
        query = """
            INSERT INTO guild_settings (guild_id, feed_channel_id)
            VALUES (?, ?)
            ON CONFLICT(guild_id) DO UPDATE SET feed_channel_id = excluded.feed_channel_id;
        """
        await self._execute(query, (guild_id, channel_id))

    async def unset_feed_channel(self, guild_id: int) -> bool:
        """Remove feed channel setting for a guild and pause subscriptions atomically."""
        pause_statements = [
            (
                """UPDATE guild_profile_feed_activity SET active = 0
                   WHERE guild_id = ? AND active = 1""",
                (guild_id,),
            ),
            (
                "DELETE FROM profile_feed_sources WHERE guild_id = ?",
                (guild_id,),
            ),
            (
                "DELETE FROM guild_profile_feed_reconciliation WHERE guild_id = ?",
                (guild_id,),
            ),
            (
                "DELETE FROM guild_profile_feed_recipient_gaps WHERE guild_id = ?",
                (guild_id,),
            ),
            (
                """DELETE FROM profile_feed_decisions
                   WHERE guild_id = ?
                     AND decision IN ('baseline', 'eligible')
                     AND event_id NOT IN (
                         SELECT event_id FROM profile_feed_messages
                         WHERE guild_id = ?
                           AND profile_key = profile_feed_decisions.profile_key
                           AND delivery_state IN ('sent', 'deleted', 'pending', 'unknown')
                     )""",
                (guild_id, guild_id),
            ),
        ]
        async with self._transaction() as tx:
            deleted = await tx.execute(
                "DELETE FROM guild_settings WHERE guild_id = ?;", (guild_id,)
            )
            if deleted > 0:
                for sql, args in pause_statements:
                    await tx.execute(sql, args)
            return deleted > 0

    async def get_feed_channel(self, guild_id: int) -> Optional[int]:
        """Get designated feed channel ID for a guild."""
        row = await self._fetchone("SELECT feed_channel_id FROM guild_settings WHERE guild_id = ?;", (guild_id,))
        return row["feed_channel_id"] if row else None

    async def get_all_guild_settings(self) -> List[Any]:
        """Retrieve all guilds with a configured feed channel."""
        return await self._fetchall("SELECT guild_id, feed_channel_id FROM guild_settings;")

    async def is_journal_entry_delivered(
        self, guild_id: int, profile_key: str, entry_id: str
    ) -> bool:
        """Check whether a profile event was delivered or baselined in this guild."""
        row = await self._fetchone(
            """SELECT 1 FROM profile_seen_journal_deliveries
               WHERE guild_id = ? AND profile_key = ? AND entry_id = ?""",
            (guild_id, profile_key.strip().lower(), entry_id),
        )
        return row is not None

    async def get_delivered_journal_entry_ids(
        self, guild_id: int, profile_key: str, entry_ids: Iterable[str]
    ) -> set[str]:
        """Bulk fetch delivered or baselined entry IDs within SQL bind budgets."""
        normalized_key = profile_key.strip().lower()
        unique_ids = list(dict.fromkeys(entry_id for entry_id in entry_ids if entry_id))
        if not unique_ids:
            return set()
        found: set[str] = set()
        for chunk in _iter_sql_chunks(unique_ids, fixed_params=2):
            placeholders = ",".join("?" for _ in chunk)
            rows = await self._fetchall(
                f"""SELECT entry_id FROM profile_seen_journal_deliveries
                    WHERE guild_id = ? AND profile_key = ? AND entry_id IN ({placeholders})""",
                [guild_id, normalized_key, *chunk],
            )
            found.update(row["entry_id"] for row in rows)
        return found

    async def get_feed_sources(self, guild_id: int, profile_key: str) -> set[str]:
        """Return initialized feed source identifiers for a guild/profile."""
        rows = await self._fetchall(
            "SELECT source FROM profile_feed_sources WHERE guild_id = ? AND profile_key = ?",
            (guild_id, profile_key.strip().lower()),
        )
        return {row["source"] for row in rows}

    async def seed_feed_source(
        self,
        guild_id: int,
        profile_key: str,
        source: str,
        entry_ids: Iterable[str],
        *,
        baseline_messages=(),
        baseline_decisions=(),
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
                self._format_utc_sql_timestamp(item.get("observed_at")),
            )
        decision_rows = list(decision_map.values())
        async with self._transaction() as tx:
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
        self,
        guild_id: int,
        profile_key: str,
        source: str,
        messages,
        *,
        baseline_decisions=(),
    ) -> bool:
        """Initialize distinct baseline payloads, seen IDs, decisions, and source marker atomically."""
        unique = dict(messages)
        return await self.seed_feed_source(
            guild_id, profile_key, source, unique,
            baseline_messages=list(unique.items()),
            baseline_decisions=baseline_decisions,
        )

    def _clear_feed_delivery_tokens(
        self, guild_id: int, profile_key: str, event_ids: Iterable[str]
    ) -> None:
        normalized_key = profile_key.strip().lower()
        for eid in event_ids:
            if eid:
                self._active_pending_delivery_tokens.pop((guild_id, normalized_key, eid), None)

    def release_feed_delivery_ownership(
        self,
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
            tokens = self._active_pending_delivery_tokens.get(key)
            if not tokens:
                continue
            if owner_token is None:
                self._active_pending_delivery_tokens.pop(key, None)
                removed_any = True
            else:
                if owner_token in tokens:
                    tokens.discard(owner_token)
                    removed_any = True
                if not tokens:
                    self._active_pending_delivery_tokens.pop(key, None)
        if removed_any and recheck_recovery:
            self._stale_pending_recovery_checked = False

    async def _ensure_stale_pending_recovered(self) -> None:
        if self._stale_pending_recovery_checked:
            return
        async with self._stale_recovery_lock:
            if self._stale_pending_recovery_checked:
                return
            await self.recover_stale_feed_deliveries()

    async def recover_stale_feed_deliveries(
        self, guild_id: Optional[int] = None, profile_key: Optional[str] = None
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
            rows = await self._fetchall(
                """SELECT guild_id, profile_key, event_id
                   FROM profile_feed_messages
                   WHERE guild_id = ? AND profile_key = ? AND delivery_state = 'pending'""",
                (guild_id, normalized_key),
            )
        else:
            rows = await self._fetchall(
                """SELECT guild_id, profile_key, event_id
                   FROM profile_feed_messages
                   WHERE delivery_state = 'pending'"""
            )
        if not rows:
            if guild_id is None and profile_key is None:
                self._stale_pending_recovery_checked = True
            return 0
        stale_by_scope: dict[tuple[int, str], list[str]] = {}
        skipped_active = False
        for row in rows:
            scope = (int(row["guild_id"]), row["profile_key"])
            eid = row["event_id"]
            if self._active_pending_delivery_tokens.get((scope[0], scope[1], eid)):
                skipped_active = True
                continue
            stale_by_scope.setdefault(scope, []).append(eid)
        if not stale_by_scope:
            if guild_id is None and profile_key is None:
                self._stale_pending_recovery_checked = not skipped_active
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
        await self._execute_batch(statements)
        if guild_id is None and profile_key is None:
            self._stale_pending_recovery_checked = not skipped_active
        return recovered

    async def get_feed_messages(self, guild_id: int, profile_key: str):
        """Retain source aliases and Discord message IDs across polls/restarts without mutating live pending intents."""
        await self._ensure_stale_pending_recovered()
        profile_key = profile_key.strip().lower()
        return await self._fetchall(
            """SELECT event_id, payload, message_id, channel_id, delivery_state
               FROM profile_feed_messages WHERE guild_id = ? AND profile_key = ?""",
            (guild_id, profile_key),
        )

    async def seed_feed_message(self, guild_id: int, profile_key: str, event_ids,
                                payload: str):
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
        await self._execute_batch(statements)

    async def store_feed_message(self, guild_id: int, profile_key: str, event_ids,
                                 payload: str, message_id=None, channel_id=None,
                                 delivery_state: Optional[str] = None):
        profile_key = profile_key.strip().lower()
        aliases = list(dict.fromkeys(event_id for event_id in event_ids if event_id))
        if not aliases:
            return
        saved_state = delivery_state or ("sent" if message_id is not None else "baseline")
        msg_rows = [
            (guild_id, profile_key, event_id, payload, message_id, channel_id, saved_state)
            for event_id in aliases
        ]
        statements = []
        for batch in _iter_bounded_row_batches(
            msg_rows,
            params_per_row=7,
            fixed_params=0,
            fixed_sql_bytes=512,
        ):
            values_sql = ", ".join(["(?, ?, ?, ?, ?, ?, ?)"] * len(batch))
            flat_params = [value for row in batch for value in row]
            statements.append((f"""
                INSERT INTO profile_feed_messages
                    (guild_id, profile_key, event_id, payload, message_id, channel_id, delivery_state)
                VALUES {values_sql}
                ON CONFLICT(guild_id, profile_key, event_id) DO UPDATE SET
                    payload = excluded.payload,
                    message_id = COALESCE(excluded.message_id, profile_feed_messages.message_id),
                    channel_id = COALESCE(excluded.channel_id, profile_feed_messages.channel_id),
                    delivery_state = CASE
                        WHEN excluded.delivery_state IN ('unknown', 'deleted') THEN excluded.delivery_state
                        WHEN excluded.message_id IS NOT NULL THEN 'sent'
                        ELSE profile_feed_messages.delivery_state
                    END
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
        await self._execute_batch(statements)
        if saved_state != "pending":
            self._clear_feed_delivery_tokens(guild_id, profile_key, aliases)

    async def begin_feed_delivery(
        self,
        guild_id: int,
        profile_key: str,
        event_ids: Iterable[str],
        payload: str,
        channel_id: int,
        *,
        owner_token: Optional[str] = None,
    ) -> bool:
        """Persist a send intent before calling Discord; return false for known or uncertain events."""
        await self._ensure_stale_pending_recovered()
        profile_key = profile_key.strip().lower()
        aliases = list(dict.fromkeys(event_id for event_id in event_ids if event_id))
        if not aliases:
            return False
        token = owner_token or "__unscoped__"
        pending_rows = [(guild_id, profile_key, event_id, payload, channel_id) for event_id in aliases]
        try:
            async with self._transaction() as tx:
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
            self._stale_pending_recovery_checked = False
            raise
        for eid in aliases:
            self._active_pending_delivery_tokens.setdefault(
                (guild_id, profile_key, eid), set()
            ).add(token)
        return True

    async def release_feed_delivery(self, guild_id: int, profile_key: str, event_ids: Iterable[str]) -> None:
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
        await self._execute_batch(statements)
        self._clear_feed_delivery_tokens(guild_id, normalized_key, aliases)

    async def mark_feed_delivery_unknown(self, guild_id: int, profile_key: str, event_ids: Iterable[str]) -> None:
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
        await self._execute_batch(statements)
        self._clear_feed_delivery_tokens(guild_id, normalized_key, aliases)

    async def mark_feed_message_deleted(
        self, guild_id: int, profile_key: str, event_ids: Iterable[str], payload: str
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
        await self._execute_batch(statements)
        self._clear_feed_delivery_tokens(guild_id, normalized_key, aliases)

    async def record_profile_activity_observations(
        self,
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

        async with self._transaction() as tx:
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

    async def set_profile_refresh_pending(self, profile_key: str, token: str) -> None:
        """Persist a pending library refresh token that survives cooldown skips and restarts."""
        normalized_key = profile_key.strip().lower()
        await self._execute(
            """INSERT INTO profile_activity_observations
               (profile_key, source, observation_key, fingerprint, observed_at)
               VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
               ON CONFLICT(profile_key, source, observation_key) DO UPDATE SET
                   fingerprint = excluded.fingerprint,
                   observed_at = CURRENT_TIMESTAMP""",
            (normalized_key, self._LIBRARY_SYNC_SOURCE, self._PENDING_REFRESH_KEY, token),
        )

    async def get_profile_refresh_pending_token(self, profile_key: str) -> Optional[str]:
        """Return the active pending library refresh token for a profile, if any."""
        row = await self._fetchone(
            """SELECT fingerprint FROM profile_activity_observations
               WHERE profile_key = ? AND source = ? AND observation_key = ?""",
            (profile_key.strip().lower(), self._LIBRARY_SYNC_SOURCE, self._PENDING_REFRESH_KEY),
        )
        return row["fingerprint"] if row else None

    async def clear_profile_refresh_pending(
        self, profile_key: str, *, token: Optional[str] = None
    ) -> bool:
        """Clear a pending refresh token unless a newer observation superseded it."""
        normalized_key = profile_key.strip().lower()
        if token is not None:
            deleted = await self._rowcount_execute(
                """DELETE FROM profile_activity_observations
                   WHERE profile_key = ? AND source = ? AND observation_key = ?
                     AND fingerprint = ?""",
                (normalized_key, self._LIBRARY_SYNC_SOURCE, self._PENDING_REFRESH_KEY, token),
            )
        else:
            deleted = await self._rowcount_execute(
                """DELETE FROM profile_activity_observations
                   WHERE profile_key = ? AND source = ? AND observation_key = ?""",
                (normalized_key, self._LIBRARY_SYNC_SOURCE, self._PENDING_REFRESH_KEY),
            )
        return deleted > 0
