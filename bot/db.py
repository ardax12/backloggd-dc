"""Database Manager for Backloggd Discord Bot.

Supports both local SQLite (aiosqlite) and Turso / libSQL Cloud Database (libsql-client).
"""

import logging
import asyncio
from pathlib import Path
from typing import Any, Iterable, List, Optional

import aiosqlite
import libsql_client

from bot.config import DATABASE_PATH, TURSO_AUTH_TOKEN, TURSO_DATABASE_URL

logger = logging.getLogger("backloggd.db")


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
            raw_url = TURSO_DATABASE_URL.strip().strip("'\"")

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
            turso_token if turso_token is not None else TURSO_AUTH_TOKEN
        ).strip().strip("'\"")
        self._conn: Optional[aiosqlite.Connection] = None
        self._client: Optional[libsql_client.Client] = None
        self._operation_lock = asyncio.Lock()

    @property
    def is_turso(self) -> bool:
        """Check whether using Turso cloud backend."""
        return bool(self.turso_url)

    async def connect(self) -> None:
        """Establish connection and create database tables if they do not exist."""
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

    async def close(self) -> None:
        """Close active database connection."""
        async with self._operation_lock:
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
        async with self._operation_lock:
            if self.is_turso:
                await self._client.execute(query, list(params))
            else:
                await self._conn.execute(query, params)
                await self._conn.commit()

    async def _fetchone(self, query: str, params: tuple | list = ()) -> Optional[Any]:
        """Fetch a single row."""
        async with self._operation_lock:
            if self.is_turso:
                rs = await self._client.execute(query, list(params))
                return rs.rows[0] if rs.rows else None
            async with self._conn.execute(query, params) as cursor:
                return await cursor.fetchone()

    async def _fetchall(self, query: str, params: tuple | list = ()) -> List[Any]:
        """Fetch all rows."""
        async with self._operation_lock:
            if self.is_turso:
                rs = await self._client.execute(query, list(params))
                return list(rs.rows)
            async with self._conn.execute(query, params) as cursor:
                return await cursor.fetchall()

    async def _rowcount_execute(self, query: str, params: tuple | list = ()) -> int:
        """Execute a write statement and return affected rows count."""
        async with self._operation_lock:
            if self.is_turso:
                rs = await self._client.execute(query, list(params))
                return rs.rows_affected
            async with self._conn.cursor() as cursor:
                await cursor.execute(query, params)
                await self._conn.commit()
                return cursor.rowcount

    async def _create_tables(self) -> None:
        """Initialize all required schemas and indexes."""
        # 1. Linked users
        await self._execute("""
            CREATE TABLE IF NOT EXISTS linked_users (
                discord_id INTEGER PRIMARY KEY,
                backloggd_username TEXT NOT NULL UNIQUE,
                avatar_url TEXT,
                last_synced_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        # 2. Cached games
        await self._execute("""
            CREATE TABLE IF NOT EXISTS cached_games (
                slug TEXT PRIMARY KEY,
                title TEXT NOT NULL,
                cover_url TEXT,
                release_year INTEGER,
                community_rating REAL
            );
        """)

        # 3. User ratings and status logs
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

        # 4. Guild settings for automatic feed
        await self._execute("""
            CREATE TABLE IF NOT EXISTS guild_settings (
                guild_id INTEGER PRIMARY KEY,
                feed_channel_id INTEGER NOT NULL
            );
        """)

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

        # 5. Seen journal logs deduplication
        await self._execute("""
            CREATE TABLE IF NOT EXISTS seen_journal_logs (
                entry_id TEXT PRIMARY KEY,
                discord_id INTEGER NOT NULL,
                posted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)

        # Delivery state is per guild. Migrate old global seen markers to all
        # currently configured feeds so deployment does not replay old entries.
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

    async def _execute_batch(self, statements: List[tuple[str, tuple | list]]) -> None:
        """Execute multiple write statements atomically on SQLite or Turso."""
        if not statements:
            return
        async with self._operation_lock:
            if self.is_turso:
                await self._client.batch([(sql, list(args)) for sql, args in statements])
                return
            await self._conn.execute("BEGIN IMMEDIATE")
            try:
                for sql, args in statements:
                    await self._conn.execute(sql, args)
                await self._conn.commit()
            except BaseException:
                await self._conn.rollback()
                raise

    @staticmethod
    def _format_utc_sql_timestamp(value: Optional[Any]) -> Optional[str]:
        if value is None:
            return None
        from datetime import datetime, timezone
        if isinstance(value, datetime):
            dt = value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)
            return dt.strftime("%Y-%m-%d %H:%M:%S")
        return str(value)

    # =========================================================================
    # User Account Linking
    # =========================================================================

    async def link_user(self, discord_id: int, username: str, avatar_url: Optional[str] = None) -> bool:
        """Connect a Discord member to a normalized shared profile."""
        from bot.ui.helpers import clean_image_url
        clean_avatar = clean_image_url(avatar_url)
        profile_key = username.strip().lower()
        await self._execute_batch([
            ("""
                INSERT INTO profiles (profile_key, display_username, avatar_url)
                VALUES (?, ?, ?)
                ON CONFLICT(profile_key) DO UPDATE SET
                    display_username = excluded.display_username,
                    avatar_url = COALESCE(excluded.avatar_url, profiles.avatar_url)
            """, (profile_key, username.strip(), clean_avatar)),
            ("""
                INSERT INTO linked_users (discord_id, profile_key, backloggd_username, avatar_url, last_synced_at)
                VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
                ON CONFLICT(discord_id) DO UPDATE SET
                    profile_key = excluded.profile_key,
                    backloggd_username = excluded.backloggd_username,
                    avatar_url = COALESCE(excluded.avatar_url, linked_users.avatar_url),
                    last_synced_at = CURRENT_TIMESTAMP;
            """, (discord_id, profile_key, username, clean_avatar)),
        ])
        return True

    async def unlink_user(self, discord_id: int) -> bool:
        """Remove this member's connection and deactivate guild feeds with no remaining source."""
        async with self._operation_lock:
            if self.is_turso:
                transaction = self._client.transaction()
                try:
                    row_rs = await transaction.execute(
                        "SELECT profile_key FROM linked_users WHERE discord_id = ?", [discord_id]
                    )
                    row = row_rs.rows[0] if row_rs.rows else None
                    if not row:
                        await transaction.commit()
                        return False
                    profile_key = row["profile_key"]
                    del_rs = await transaction.execute(
                        "DELETE FROM linked_users WHERE discord_id = ?", [discord_id]
                    )
                    if del_rs.rows_affected <= 0:
                        await transaction.commit()
                        return False
                    active_rs = await transaction.execute(
                        "SELECT guild_id FROM guild_profile_feed_activity WHERE profile_key = ? AND active = 1",
                        [profile_key],
                    )
                    for active_row in active_rs.rows:
                        guild_id = active_row["guild_id"]
                        other_member = await transaction.execute(
                            """SELECT 1 FROM linked_users AS u
                               JOIN guild_feed_preferences AS pref ON pref.discord_id = u.discord_id
                               WHERE u.profile_key = ? AND pref.guild_id = ? AND pref.enabled = 1
                               LIMIT 1""",
                            [profile_key, guild_id],
                        )
                        admin_follow = await transaction.execute(
                            "SELECT 1 FROM guild_admin_follows WHERE guild_id = ? AND profile_key = ? LIMIT 1",
                            [guild_id, profile_key],
                        )
                        if not other_member.rows and not admin_follow.rows:
                            await transaction.execute(
                                "UPDATE guild_profile_feed_activity SET active = 0 WHERE guild_id = ? AND profile_key = ?",
                                [guild_id, profile_key],
                            )
                            await transaction.execute(
                                "DELETE FROM profile_feed_sources WHERE guild_id = ? AND profile_key = ?",
                                [guild_id, profile_key],
                            )
                            await transaction.execute(
                                """DELETE FROM profile_feed_decisions
                                   WHERE guild_id = ? AND profile_key = ?
                                     AND decision IN ('baseline', 'eligible')
                                     AND event_id NOT IN (
                                         SELECT event_id FROM profile_feed_messages
                                         WHERE guild_id = ? AND profile_key = ?
                                           AND delivery_state IN ('sent', 'deleted', 'pending', 'unknown')
                                     )""",
                                [guild_id, profile_key, guild_id, profile_key],
                            )
                    await transaction.commit()
                    return True
                except BaseException:
                    await transaction.rollback()
                    raise

            await self._conn.execute("BEGIN IMMEDIATE")
            try:
                async with self._conn.execute(
                    "SELECT profile_key FROM linked_users WHERE discord_id = ?;", (discord_id,)
                ) as cursor:
                    row = await cursor.fetchone()
                if not row:
                    await self._conn.commit()
                    return False
                profile_key = row["profile_key"]
                async with self._conn.execute(
                    "DELETE FROM linked_users WHERE discord_id = ?;", (discord_id,)
                ) as cursor:
                    deleted = cursor.rowcount
                if deleted > 0:
                    async with self._conn.execute(
                        "SELECT guild_id FROM guild_profile_feed_activity WHERE profile_key = ? AND active = 1",
                        (profile_key,),
                    ) as cursor:
                        active_guilds = await cursor.fetchall()
                    for active_row in active_guilds:
                        guild_id = active_row["guild_id"]
                        async with self._conn.execute(
                            """SELECT 1 FROM linked_users AS u
                               JOIN guild_feed_preferences AS pref ON pref.discord_id = u.discord_id
                               WHERE u.profile_key = ? AND pref.guild_id = ? AND pref.enabled = 1
                               LIMIT 1""",
                            (profile_key, guild_id),
                        ) as cursor:
                            has_member = await cursor.fetchone() is not None
                        async with self._conn.execute(
                            "SELECT 1 FROM guild_admin_follows WHERE guild_id = ? AND profile_key = ? LIMIT 1",
                            (guild_id, profile_key),
                        ) as cursor:
                            has_admin = await cursor.fetchone() is not None
                        if not has_member and not has_admin:
                            await self._conn.execute(
                                "UPDATE guild_profile_feed_activity SET active = 0 WHERE guild_id = ? AND profile_key = ?",
                                (guild_id, profile_key),
                            )
                            await self._conn.execute(
                                "DELETE FROM profile_feed_sources WHERE guild_id = ? AND profile_key = ?",
                                (guild_id, profile_key),
                            )
                            await self._conn.execute(
                                """DELETE FROM profile_feed_decisions
                                   WHERE guild_id = ? AND profile_key = ?
                                     AND decision IN ('baseline', 'eligible')
                                     AND event_id NOT IN (
                                         SELECT event_id FROM profile_feed_messages
                                         WHERE guild_id = ? AND profile_key = ?
                                           AND delivery_state IN ('sent', 'deleted', 'pending', 'unknown')
                                     )""",
                                (guild_id, profile_key, guild_id, profile_key),
                            )
                await self._conn.commit()
                return deleted > 0
            except BaseException:
                await self._conn.rollback()
                raise

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
        if not member_ids:
            return []
        excluded = exclude_discord_id if exclude_discord_id is not None else -1
        connections = []
        for start in range(0, len(member_ids), 800):
            member_chunk = member_ids[start:start + 800]
            placeholders = ",".join("?" for _ in member_chunk)
            query = f"""
                SELECT discord_id FROM linked_users
                WHERE profile_key = LOWER(TRIM(?))
                  AND discord_id IN ({placeholders})
                  AND discord_id <> ?
                ORDER BY discord_id
            """
            params = [username.strip(), *member_chunk, excluded]
            connections.extend(await self._fetchall(query, params))
        return connections

    async def get_profile_sync_info(self, username: str) -> Optional[Any]:
        """Return the profile's last complete sync time, sync health, and cached row count."""
        return await self._fetchone("""
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

    async def get_profiles_sync_info(self, profile_keys: Iterable[str]) -> dict[str, Any]:
        """Return sync freshness metadata keyed by normalized profile_key."""
        normalized = list(dict.fromkeys(key.strip().lower() for key in profile_keys if key and key.strip()))
        result: dict[str, Any] = {}
        for start in range(0, len(normalized), 800):
            chunk = normalized[start:start + 800]
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
                result[row["profile_key"]] = row
        return result

    async def is_profile_library_subscribed(self, username: str) -> bool:
        """Return true when at least one Discord link consumes this profile's library."""
        row = await self._fetchone(
            "SELECT 1 FROM linked_users WHERE profile_key = LOWER(TRIM(?)) LIMIT 1",
            (username.strip(),),
        )
        return row is not None

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

    async def get_all_linked_users(self) -> List[Any]:
        """Fetch all linked users."""
        return await self._fetchall("SELECT discord_id, profile_key, backloggd_username, avatar_url FROM linked_users;")

    async def get_guild_linked_users(self, member_ids: List[int]) -> List[Any]:
        """Fetch all linked users present in a specific guild."""
        if not member_ids:
            return []
        placeholders = ",".join("?" for _ in member_ids)
        query = f"SELECT discord_id, profile_key, backloggd_username, avatar_url FROM linked_users WHERE discord_id IN ({placeholders});"
        return await self._fetchall(query, member_ids)

    async def get_guild_linked_profiles(
        self,
        member_ids: List[int],
        exclude_profile_key: Optional[str] = None,
    ) -> List[Any]:
        """Fetch distinct linked profiles present among the supplied guild members."""
        if not member_ids:
            return []
        placeholders = ",".join("?" for _ in member_ids)
        exclusion = " AND p.profile_key <> ?" if exclude_profile_key else ""
        params = list(member_ids)
        if exclude_profile_key:
            params.append(exclude_profile_key.strip().lower())
        query = f"""
            SELECT DISTINCT p.profile_key, p.display_username, p.avatar_url
            FROM linked_users AS u
            JOIN profiles AS p ON p.profile_key = u.profile_key
            WHERE u.discord_id IN ({placeholders}){exclusion}
            ORDER BY p.profile_key
        """
        return await self._fetchall(query, params)

    async def get_guild_linked_profiles_with_sync_info(
        self,
        member_ids: List[int],
        exclude_profile_key: Optional[str] = None,
    ) -> List[Any]:
        """Fetch distinct linked guild profiles with library sync timestamps and errors."""
        if not member_ids:
            return []
        placeholders = ",".join("?" for _ in member_ids)
        exclusion = " AND p.profile_key <> ?" if exclude_profile_key else ""
        params = list(member_ids)
        if exclude_profile_key:
            params.append(exclude_profile_key.strip().lower())
        query = f"""
            SELECT DISTINCT p.profile_key, p.display_username, p.avatar_url,
                   p.last_synced_at, p.last_sync_attempt_at, p.last_sync_error
            FROM linked_users AS u
            JOIN profiles AS p ON p.profile_key = u.profile_key
            WHERE u.discord_id IN ({placeholders}){exclusion}
            ORDER BY p.profile_key
        """
        return await self._fetchall(query, params)

    async def set_guild_feed_preference(
        self, guild_id: int, discord_id: int, enabled: bool
    ) -> Optional[str]:
        """Set a member's server-local feed preference; return their profile key."""
        linked = await self.get_linked_user(discord_id)
        if not linked:
            return None
        await self._execute(
            """INSERT INTO guild_feed_preferences (guild_id, discord_id, enabled, updated_at)
               VALUES (?, ?, ?, CURRENT_TIMESTAMP)
               ON CONFLICT(guild_id, discord_id) DO UPDATE SET
                   enabled = excluded.enabled, updated_at = CURRENT_TIMESTAMP""",
            (guild_id, discord_id, bool(enabled)),
        )
        return linked["profile_key"]

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
        """Return distinct enabled member profiles among current guild members."""
        members = list(dict.fromkeys(int(value) for value in member_ids))
        profiles = {}
        for start in range(0, len(members), 800):
            chunk = members[start:start + 800]
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
            ) if chunk else []
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
        async with self._operation_lock:
            if self.is_turso:
                transaction = self._client.transaction()
                try:
                    pending_rs = await transaction.execute(
                        "SELECT completed FROM guild_feed_legacy_migrations WHERE guild_id = ?",
                        [guild_id],
                    )
                    pending = pending_rs.rows[0] if pending_rs.rows else None
                    if not pending or pending["completed"]:
                        await transaction.commit()
                        return False
                    for start in range(0, len(members), 800):
                        chunk = members[start:start + 800]
                        if not chunk:
                            continue
                        placeholders = ",".join("?" for _ in chunk)
                        eligible_rs = await transaction.execute(
                            f"""SELECT scope.discord_id
                                FROM guild_feed_legacy_scope AS scope
                                JOIN linked_users AS linked
                                  ON linked.discord_id = scope.discord_id
                                 AND linked.profile_key = scope.profile_key
                                WHERE scope.guild_id = ? AND scope.discord_id IN ({placeholders})""",
                            [guild_id, *chunk],
                        )
                        for row in eligible_rs.rows:
                            await transaction.execute(
                                """INSERT OR IGNORE INTO guild_feed_preferences
                                   (guild_id, discord_id, enabled) VALUES (?, ?, 1)""",
                                [guild_id, row["discord_id"]],
                            )
                    await transaction.execute(
                        """UPDATE guild_feed_legacy_migrations
                           SET completed = 1, completed_at = CURRENT_TIMESTAMP
                           WHERE guild_id = ? AND completed = 0""",
                        [guild_id],
                    )
                    await transaction.commit()
                    return True
                except BaseException:
                    await transaction.rollback()
                    raise

            await self._conn.execute("BEGIN IMMEDIATE")
            try:
                async with self._conn.execute(
                    "SELECT completed FROM guild_feed_legacy_migrations WHERE guild_id = ?",
                    (guild_id,),
                ) as cursor:
                    pending = await cursor.fetchone()
                if not pending or pending["completed"]:
                    await self._conn.commit()
                    return False
                for start in range(0, len(members), 800):
                    chunk = members[start:start + 800]
                    if not chunk:
                        continue
                    placeholders = ",".join("?" for _ in chunk)
                    async with self._conn.execute(
                        f"""SELECT scope.discord_id
                            FROM guild_feed_legacy_scope AS scope
                            JOIN linked_users AS linked
                              ON linked.discord_id = scope.discord_id
                             AND linked.profile_key = scope.profile_key
                            WHERE scope.guild_id = ? AND scope.discord_id IN ({placeholders})""",
                        [guild_id, *chunk],
                    ) as cursor:
                        eligible = await cursor.fetchall()
                    for row in eligible:
                        await self._conn.execute(
                            """INSERT OR IGNORE INTO guild_feed_preferences
                               (guild_id, discord_id, enabled) VALUES (?, ?, 1)""",
                            (guild_id, row["discord_id"]),
                        )
                await self._conn.execute(
                    """UPDATE guild_feed_legacy_migrations
                       SET completed = 1, completed_at = CURRENT_TIMESTAMP
                       WHERE guild_id = ? AND completed = 0""",
                    (guild_id,),
                )
                await self._conn.commit()
                return True
            except BaseException:
                await self._conn.rollback()
                raise

    async def reconcile_guild_feed_activity(
        self,
        guild_id: int,
        eligible_profile_keys: Iterable[str],
        *,
        now: Optional[Any] = None,
    ) -> dict[str, list[str]]:
        """Pause departed profiles and fresh-baseline profiles that resume atomically."""
        eligible = {key.strip().lower() for key in eligible_profile_keys if key}
        now_sql = self._format_utc_sql_timestamp(now)
        deactivated, activated = [], []
        clear_unsent_decisions_sql = """
            DELETE FROM profile_feed_decisions
            WHERE guild_id = ? AND profile_key = ?
              AND decision IN ('baseline', 'eligible')
              AND event_id NOT IN (
                  SELECT event_id FROM profile_feed_messages
                  WHERE guild_id = ? AND profile_key = ?
                    AND delivery_state IN ('sent', 'deleted', 'pending', 'unknown')
              )
        """
        async with self._operation_lock:
            if self.is_turso:
                transaction = self._client.transaction()
                try:
                    existing_rs = await transaction.execute(
                        "SELECT profile_key, active FROM guild_profile_feed_activity WHERE guild_id = ?",
                        [guild_id],
                    )
                    states = {row["profile_key"]: bool(row["active"]) for row in existing_rs.rows}
                    for profile_key, was_active in states.items():
                        if was_active and profile_key not in eligible:
                            await transaction.execute(
                                """UPDATE guild_profile_feed_activity SET active = 0
                                   WHERE guild_id = ? AND profile_key = ?""",
                                [guild_id, profile_key],
                            )
                            await transaction.execute(
                                "DELETE FROM profile_feed_sources WHERE guild_id = ? AND profile_key = ?",
                                [guild_id, profile_key],
                            )
                            await transaction.execute(
                                clear_unsent_decisions_sql,
                                [guild_id, profile_key, guild_id, profile_key],
                            )
                            deactivated.append(profile_key)
                    for profile_key in sorted(eligible):
                        if not states.get(profile_key, False):
                            if now_sql is not None:
                                await transaction.execute(
                                    """INSERT INTO guild_profile_feed_activity
                                       (guild_id, profile_key, active, last_active_at)
                                       VALUES (?, ?, 1, ?)
                                       ON CONFLICT(guild_id, profile_key) DO UPDATE SET
                                           active = 1, last_active_at = excluded.last_active_at""",
                                    [guild_id, profile_key, now_sql],
                                )
                            else:
                                await transaction.execute(
                                    """INSERT INTO guild_profile_feed_activity
                                       (guild_id, profile_key, active, last_active_at)
                                       VALUES (?, ?, 1, CURRENT_TIMESTAMP)
                                       ON CONFLICT(guild_id, profile_key) DO UPDATE SET
                                           active = 1, last_active_at = CURRENT_TIMESTAMP""",
                                    [guild_id, profile_key],
                                )
                            await transaction.execute(
                                "DELETE FROM profile_feed_sources WHERE guild_id = ? AND profile_key = ?",
                                [guild_id, profile_key],
                            )
                            await transaction.execute(
                                clear_unsent_decisions_sql,
                                [guild_id, profile_key, guild_id, profile_key],
                            )
                            activated.append(profile_key)
                    await transaction.commit()
                    return {"activated": activated, "deactivated": deactivated}
                except BaseException:
                    await transaction.rollback()
                    raise

            await self._conn.execute("BEGIN IMMEDIATE")
            try:
                async with self._conn.execute(
                    "SELECT profile_key, active FROM guild_profile_feed_activity WHERE guild_id = ?",
                    (guild_id,),
                ) as cursor:
                    existing = await cursor.fetchall()
                states = {row["profile_key"]: bool(row["active"]) for row in existing}
                for profile_key, was_active in states.items():
                    if was_active and profile_key not in eligible:
                        await self._conn.execute(
                            """UPDATE guild_profile_feed_activity SET active = 0
                               WHERE guild_id = ? AND profile_key = ?""",
                            (guild_id, profile_key),
                        )
                        await self._conn.execute(
                            "DELETE FROM profile_feed_sources WHERE guild_id = ? AND profile_key = ?",
                            (guild_id, profile_key),
                        )
                        await self._conn.execute(
                            clear_unsent_decisions_sql,
                            (guild_id, profile_key, guild_id, profile_key),
                        )
                        deactivated.append(profile_key)
                for profile_key in sorted(eligible):
                    if not states.get(profile_key, False):
                        if now_sql is not None:
                            await self._conn.execute(
                                """INSERT INTO guild_profile_feed_activity
                                   (guild_id, profile_key, active, last_active_at)
                                   VALUES (?, ?, 1, ?)
                                   ON CONFLICT(guild_id, profile_key) DO UPDATE SET
                                       active = 1, last_active_at = excluded.last_active_at""",
                                (guild_id, profile_key, now_sql),
                            )
                        else:
                            await self._conn.execute(
                                """INSERT INTO guild_profile_feed_activity
                                   (guild_id, profile_key, active, last_active_at)
                                   VALUES (?, ?, 1, CURRENT_TIMESTAMP)
                                   ON CONFLICT(guild_id, profile_key) DO UPDATE SET
                                       active = 1, last_active_at = CURRENT_TIMESTAMP""",
                                (guild_id, profile_key),
                            )
                        await self._conn.execute(
                            "DELETE FROM profile_feed_sources WHERE guild_id = ? AND profile_key = ?",
                            (guild_id, profile_key),
                        )
                        await self._conn.execute(
                            clear_unsent_decisions_sql,
                            (guild_id, profile_key, guild_id, profile_key),
                        )
                        activated.append(profile_key)
                await self._conn.commit()
                return {"activated": activated, "deactivated": deactivated}
            except BaseException:
                await self._conn.rollback()
                raise

    async def pause_guild_feed_activity(self, guild_id: int) -> None:
        """Pause all subscriptions while preserving archived messages and aliases."""
        await self._execute_batch([
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
        ])

    async def get_guild_profile_member_sources(
        self, guild_id: int, profile_key: str, member_ids: Iterable[int]
    ) -> List[int]:
        """Return current member IDs that actively enroll the given profile."""
        members = list(dict.fromkeys(int(value) for value in member_ids))
        matching = []
        for start in range(0, len(members), 800):
            chunk = members[start:start + 800]
            placeholders = ",".join("?" for _ in chunk)
            rows = await self._fetchall(
                f"""SELECT pref.discord_id FROM guild_feed_preferences AS pref
                    JOIN linked_users AS linked ON linked.discord_id = pref.discord_id
                    WHERE pref.guild_id = ? AND pref.enabled = 1
                      AND linked.profile_key = ?
                      AND pref.discord_id IN ({placeholders})
                    ORDER BY pref.discord_id""",
                [guild_id, profile_key.strip().lower(), *chunk],
            ) if chunk else []
            matching.extend(row["discord_id"] for row in rows)
        return matching

    async def add_guild_admin_follow(
        self, guild_id: int, username: str, avatar_url: Optional[str], added_by: int
    ) -> bool:
        """Add an idempotent server follow to a validated public profile."""
        from bot.ui.helpers import clean_image_url
        display_username = username.strip()
        profile_key = display_username.lower()
        clean_avatar = clean_image_url(avatar_url)
        await self._execute(
            """INSERT INTO profiles (profile_key, display_username, avatar_url)
               VALUES (?, ?, ?)
               ON CONFLICT(profile_key) DO UPDATE SET
                   display_username = excluded.display_username,
                   avatar_url = COALESCE(excluded.avatar_url, profiles.avatar_url)""",
            (profile_key, display_username, clean_avatar),
        )
        inserted = await self._rowcount_execute(
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

    async def get_guild_profile_feed_status(self, guild_id: int, profile_key: str) -> dict:
        profile_key = profile_key.strip().lower()
        activity = await self._fetchone(
            """SELECT active, last_active_at FROM guild_profile_feed_activity
               WHERE guild_id = ? AND profile_key = ?""",
            (guild_id, profile_key),
        )
        sources = await self._fetchall(
            """SELECT source FROM profile_feed_sources
               WHERE guild_id = ? AND profile_key = ? ORDER BY source""",
            (guild_id, profile_key),
        )
        return {
            "active": bool(activity and activity["active"]),
            "last_active_at": activity["last_active_at"] if activity else None,
            "initialized_sources": [row["source"] for row in sources],
            "suppression_counts": await self.get_feed_decision_counts(guild_id, profile_key),
            "delivery_counts": await self.get_feed_message_delivery_counts(guild_id, profile_key),
            "source_health": await self.get_profile_feed_source_health(profile_key),
        }

    async def get_feed_message_delivery_counts(self, guild_id: int, profile_key: str) -> dict[str, int]:
        rows = await self._fetchall(
            """SELECT delivery_state, COUNT(*) AS count FROM profile_feed_messages
               WHERE guild_id = ? AND profile_key = ?
               GROUP BY delivery_state""",
            (guild_id, profile_key.strip().lower()),
        )
        return {row["delivery_state"]: row["count"] for row in rows}

    async def get_profile_feed_source_health(self, profile_key: str) -> List[Any]:
        return await self._fetchall(
            """SELECT source, attempts, successes, last_attempt_at, last_success_at,
                      coverage, pages_read, last_continuous_boundary_event_id,
                      last_continuous_boundary_at, unresolved_gap_since,
                      unresolved_gap_until, gap_reason, last_error
               FROM profile_feed_source_health WHERE profile_key = ? ORDER BY source""",
            (profile_key.strip().lower(),),
        )

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

    async def record_feed_source_attempt(self, profile_key: str, source: str) -> None:
        await self._execute(
            """INSERT INTO profile_feed_source_health
               (profile_key, source, attempts, last_attempt_at)
               VALUES (?, ?, 1, CURRENT_TIMESTAMP)
               ON CONFLICT(profile_key, source) DO UPDATE SET
                   attempts = profile_feed_source_health.attempts + 1,
                   last_attempt_at = CURRENT_TIMESTAMP""",
            (profile_key.strip().lower(), source),
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
    ) -> None:
        closes_gap = coverage == "exhausted" or (coverage == "boundary_reached" and bool(boundary_event_id))
        commit_boundary = bool(closes_gap and not defer_boundary_commit)
        gap_until_sql = self._format_utc_sql_timestamp(gap_until)
        await self._execute(
            """INSERT INTO profile_feed_source_health
               (profile_key, source, attempts, successes, last_attempt_at, last_success_at,
                coverage, pages_read, last_continuous_boundary_event_id,
                last_continuous_boundary_at, unresolved_gap_since, unresolved_gap_until,
                gap_reason, last_error)
               VALUES (?, ?, 0, 1, NULL, CURRENT_TIMESTAMP, ?, ?,
                       CASE WHEN ? THEN ? ELSE NULL END,
                       CASE WHEN ? THEN CURRENT_TIMESTAMP ELSE NULL END,
                       CASE WHEN ? THEN NULL ELSE CURRENT_TIMESTAMP END,
                       CASE WHEN ? THEN NULL ELSE COALESCE(?, CURRENT_TIMESTAMP) END,
                       CASE WHEN ? THEN NULL ELSE ? END, NULL)
               ON CONFLICT(profile_key, source) DO UPDATE SET
                   successes = profile_feed_source_health.successes + 1,
                   last_success_at = CURRENT_TIMESTAMP,
                   coverage = excluded.coverage,
                   pages_read = excluded.pages_read,
                   last_continuous_boundary_event_id = CASE
                       WHEN ? THEN COALESCE(excluded.last_continuous_boundary_event_id, profile_feed_source_health.last_continuous_boundary_event_id)
                       ELSE profile_feed_source_health.last_continuous_boundary_event_id
                   END,
                   last_continuous_boundary_at = CASE
                       WHEN ? THEN CURRENT_TIMESTAMP
                       ELSE profile_feed_source_health.last_continuous_boundary_at
                   END,
                   unresolved_gap_since = CASE
                       WHEN ? THEN NULL
                       WHEN ? THEN profile_feed_source_health.unresolved_gap_since
                       ELSE COALESCE(
                           profile_feed_source_health.unresolved_gap_since,
                           profile_feed_source_health.last_continuous_boundary_at,
                           profile_feed_source_health.last_success_at,
                           CURRENT_TIMESTAMP
                       )
                   END,
                   unresolved_gap_until = CASE
                       WHEN ? THEN NULL
                       WHEN ? THEN profile_feed_source_health.unresolved_gap_until
                       ELSE COALESCE(?, CURRENT_TIMESTAMP)
                   END,
                   gap_reason = CASE
                       WHEN ? THEN NULL
                       WHEN ? THEN profile_feed_source_health.gap_reason
                       ELSE COALESCE(profile_feed_source_health.gap_reason, ?)
                   END,
                   last_error = NULL""",
            (
                profile_key.strip().lower(),
                source,
                coverage,
                pages_read,
                commit_boundary,
                boundary_event_id,
                commit_boundary,
                bool(closes_gap),
                bool(closes_gap),
                gap_until_sql,
                bool(closes_gap),
                coverage,
                commit_boundary,
                commit_boundary,
                commit_boundary,
                bool(closes_gap),
                commit_boundary,
                bool(closes_gap),
                gap_until_sql,
                commit_boundary,
                bool(closes_gap),
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
    ) -> None:
        """Advance continuous boundary and clear unresolved gaps after delivery finishes."""
        closes_gap = coverage == "exhausted" or (coverage == "boundary_reached" and bool(boundary_event_id))
        if not closes_gap:
            return
        await self._execute(
            """UPDATE profile_feed_source_health
               SET last_continuous_boundary_event_id = COALESCE(?, last_continuous_boundary_event_id),
                   last_continuous_boundary_at = CURRENT_TIMESTAMP,
                   unresolved_gap_since = NULL,
                   unresolved_gap_until = NULL,
                   gap_reason = NULL
               WHERE profile_key = ? AND source = ?""",
            (boundary_event_id, profile_key.strip().lower(), source),
        )

    async def record_feed_source_failure(self, profile_key: str, source: str, error: str) -> None:
        await self._execute(
            """INSERT INTO profile_feed_source_health
               (profile_key, source, attempts, last_attempt_at, last_error)
               VALUES (?, ?, 0, NULL, ?)
               ON CONFLICT(profile_key, source) DO UPDATE SET last_error = excluded.last_error""",
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

    async def resolve_feed_event_decision(
        self, guild_id: int, profile_key: str, event_id: str, payload: str,
        event_date: Optional[str],
    ) -> None:
        """Allow an ambiguous candidate to resolve only after strong identity evidence."""
        await self._execute(
            """UPDATE profile_feed_decisions
               SET decision = 'eligible', reason = 'resolved_by_stable_reference',
                   event_date = COALESCE(?, event_date), payload = ?
               WHERE guild_id = ? AND profile_key = ? AND event_id = ?
                 AND decision = 'held_ambiguous'""",
            (event_date, payload, guild_id, profile_key.strip().lower(), event_id),
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
    ) -> None:
        """Apply new evidence to eligible work without altering archive decisions."""
        await self._execute(
            """UPDATE profile_feed_decisions
               SET decision = ?, reason = ?, event_date = ?, payload = ?
               WHERE guild_id = ? AND profile_key = ? AND event_id = ?
                 AND decision = 'eligible'
                 AND NOT EXISTS (
                     SELECT 1 FROM profile_feed_messages AS message
                     WHERE message.guild_id = profile_feed_decisions.guild_id
                       AND message.profile_key = profile_feed_decisions.profile_key
                       AND message.event_id = profile_feed_decisions.event_id
                       AND (message.message_id IS NOT NULL
                            OR message.delivery_state IN ('sent', 'deleted', 'pending', 'unknown'))
                 )""",
            (decision, reason, event_date, payload, guild_id, profile_key.strip().lower(), event_id),
        )

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

    async def get_guild_feed_candidates(
        self, guild_id: int, member_ids: Iterable[int]
    ) -> List[dict]:
        """Union current member enrollment and admin follows by public profile."""
        members = list(dict.fromkeys(int(value) for value in member_ids))
        candidates: dict[str, dict] = {}
        for start in range(0, len(members), 800):
            chunk = members[start:start + 800]
            if not chunk:
                continue
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
        """Insert or update a game row in the linked profile's shared library."""
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
        placeholders = ",".join("?" for _ in guild_member_ids)
        query = f"""
            SELECT r.profile_key, p.display_username AS backloggd_username,
                   p.avatar_url, r.rating, r.status, r.is_liked
            FROM profile_ratings r
            JOIN profiles p ON p.profile_key = r.profile_key
            WHERE r.profile_key IN (
                SELECT DISTINCT profile_key FROM linked_users
                WHERE discord_id IN ({placeholders})
            ) AND r.game_slug = ?
            ORDER BY (r.rating IS NULL), r.rating DESC, p.display_username ASC;
        """
        params = list(guild_member_ids) + [game_slug]
        return await self._fetchall(query, params)

    async def get_user_stats(self, discord_id: int) -> dict:
        """Fetch user's aggregate stats from cached ratings."""
        query = """
            SELECT COUNT(*) as total_games,
                   COUNT(rating) as rated_count,
                   AVG(rating) as average_rating
            FROM user_ratings
            WHERE discord_id = ?;
        """
        row = await self._fetchone(query, (discord_id,))
        if not row or not row["total_games"]:
            return {"total_games": 0, "rated_count": 0, "average_rating": None}
        return {
            "total_games": row["total_games"],
            "rated_count": row["rated_count"],
            "average_rating": round(row["average_rating"], 2) if row["average_rating"] is not None else None
        }

    async def get_user_recent_ratings(self, discord_id: int, limit: int = 5) -> List[Any]:
        """Fetch user's most recently logged ratings from database."""
        query = """
            SELECT g.title as game_title, g.slug as game_slug, r.rating, r.status, r.updated_at
            FROM user_ratings r
            JOIN cached_games g ON r.game_slug = g.slug
            WHERE r.discord_id = ?
            ORDER BY r.updated_at DESC
            LIMIT ?;
        """
        return await self._fetchall(query, (discord_id, limit))

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
        """Atomically replace the shared profile library if the link still matches."""
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

        game_rows = [
            (
                game.slug,
                game.title,
                clean_image_url(getattr(game, "cover_url", None)),
                getattr(game, "release_year", None),
                getattr(game, "community_rating", None),
                getattr(game, "rating", None),
                game.status.value if hasattr(game.status, "value") else str(game.status),
                bool(getattr(game, "is_liked", False)),
            )
            for game in games
        ]
        profile_key = username.strip().lower()
        synced_sql = self._format_utc_sql_timestamp(synced_at)
        ownership_sql = "SELECT 1 FROM linked_users WHERE profile_key = ? LIMIT 1"

        async with self._operation_lock:
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
                for row in game_rows:
                    statements.append(("""
                        INSERT INTO cached_games (slug, title, cover_url, release_year, community_rating)
                        VALUES (?, ?, ?, ?, ?)
                        ON CONFLICT(slug) DO UPDATE SET
                            title = excluded.title,
                            cover_url = COALESCE(excluded.cover_url, cached_games.cover_url),
                            release_year = COALESCE(excluded.release_year, cached_games.release_year),
                            community_rating = COALESCE(excluded.community_rating, cached_games.community_rating)
                    """, row[:5]))
                statements.append((
                    f"DELETE FROM profile_ratings WHERE profile_key = ? {guard}",
                    profile_args,
                ))
                where_clause = f"WHERE EXISTS ({ownership_sql})" if require_linked else ""
                for row in game_rows:
                    insert_args = (profile_key, row[0], row[5], row[6], row[7])
                    if require_linked:
                        insert_args = (*insert_args, profile_key)
                    statements.append((f"""
                        INSERT INTO profile_ratings
                            (profile_key, game_slug, rating, status, is_liked, updated_at)
                        SELECT ?, ?, ?, ?, ?, CURRENT_TIMESTAMP
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
                results = await self._client.batch(statements)
                if require_linked:
                    return bool(results and results[0].rows)
                return True

            await self._conn.execute("BEGIN IMMEDIATE")
            try:
                if require_linked:
                    async with self._conn.execute(ownership_sql, (profile_key,)) as cursor:
                        link = await cursor.fetchone()
                    if not link:
                        await self._conn.rollback()
                        return False

                if synced_sql is not None:
                    await self._conn.execute(
                        """UPDATE profiles
                           SET last_synced_at = ?, last_sync_attempt_at = ?, last_sync_error = NULL
                           WHERE profile_key = ?""",
                        (synced_sql, synced_sql, profile_key),
                    )
                    await self._conn.execute(
                        "UPDATE linked_users SET last_synced_at = ? WHERE profile_key = ?",
                        (synced_sql, profile_key),
                    )
                else:
                    await self._conn.execute(
                        """UPDATE profiles
                           SET last_synced_at = CURRENT_TIMESTAMP,
                               last_sync_attempt_at = CURRENT_TIMESTAMP,
                               last_sync_error = NULL
                           WHERE profile_key = ?""",
                        (profile_key,),
                    )
                    await self._conn.execute(
                        "UPDATE linked_users SET last_synced_at = CURRENT_TIMESTAMP WHERE profile_key = ?",
                        (profile_key,),
                    )
                for row in game_rows:
                    await self._conn.execute("""
                        INSERT INTO cached_games (slug, title, cover_url, release_year, community_rating)
                        VALUES (?, ?, ?, ?, ?)
                        ON CONFLICT(slug) DO UPDATE SET
                            title = excluded.title,
                            cover_url = COALESCE(excluded.cover_url, cached_games.cover_url),
                            release_year = COALESCE(excluded.release_year, cached_games.release_year),
                            community_rating = COALESCE(excluded.community_rating, cached_games.community_rating)
                    """, row[:5])
                await self._conn.execute(
                    "DELETE FROM profile_ratings WHERE profile_key = ?;", (profile_key,)
                )
                for row in game_rows:
                    await self._conn.execute("""
                        INSERT INTO profile_ratings
                            (profile_key, game_slug, rating, status, is_liked, updated_at)
                        VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                    """, (profile_key, row[0], row[5], row[6], row[7]))
                if clear_pending_token is not None:
                    await self._conn.execute(
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
                await self._conn.commit()
                return True
            except BaseException:
                await self._conn.rollback()
                raise

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

    async def get_user_ratings_dict(self, discord_id: int) -> dict[str, Optional[float]]:
        """Return slug -> rating dictionary for similarity computation."""
        query = "SELECT game_slug, rating FROM user_ratings WHERE discord_id = ?;"
        rows = await self._fetchall(query, (discord_id,))
        return {row["game_slug"]: row["rating"] for row in rows}

    async def get_multi_user_ratings_dict(self, discord_ids: List[int]) -> dict[int, dict[str, Optional[float]]]:
        """Return discord_id -> {slug: rating} for multiple users in a single batch query (eliminating N+1)."""
        if not discord_ids:
            return {}
        placeholders = ",".join("?" for _ in discord_ids)
        query = f"SELECT discord_id, game_slug, rating FROM user_ratings WHERE discord_id IN ({placeholders});"
        rows = await self._fetchall(query, discord_ids)
        result: dict[int, dict[str, Optional[float]]] = {uid: {} for uid in discord_ids}
        for row in rows:
            result[row["discord_id"]][row["game_slug"]] = row["rating"]
        return result

    async def get_multi_profile_ratings_dict(
        self,
        profile_keys: List[str],
    ) -> dict[str, dict[str, Optional[float]]]:
        """Return one cached rating map per normalized Backloggd profile."""
        normalized_keys = list(dict.fromkeys(key.strip().lower() for key in profile_keys if key.strip()))
        result: dict[str, dict[str, Optional[float]]] = {key: {} for key in normalized_keys}
        for start in range(0, len(normalized_keys), 800):
            key_chunk = normalized_keys[start:start + 800]
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
        """Fetch cached game records by slugs in a single batch query."""
        if not slugs:
            return {}
        placeholders = ",".join("?" for _ in slugs)
        query = f"SELECT slug, title, cover_url, release_year, community_rating FROM cached_games WHERE slug IN ({placeholders});"
        rows = await self._fetchall(query, slugs)
        return {row["slug"]: row for row in rows}

    # =========================================================================
    # Server Leaderboards & Statistics
    # =========================================================================

    async def get_server_top_rated(self, guild_member_ids: List[int], limit: int = 10, min_ratings: int = 2) -> List[Any]:
        """Get top games by average rating among distinct linked profiles."""
        if not guild_member_ids:
            return []
        placeholders = ",".join("?" for _ in guild_member_ids)
        query = f"""
            SELECT g.slug, g.title, g.cover_url, g.release_year,
                   AVG(r.rating) as avg_rating,
                   COUNT(r.rating) as rating_count
            FROM profile_ratings r
            JOIN cached_games g ON r.game_slug = g.slug
            WHERE r.profile_key IN (
                SELECT DISTINCT profile_key FROM linked_users
                WHERE discord_id IN ({placeholders})
            ) AND r.rating IS NOT NULL
            GROUP BY g.slug
            HAVING rating_count >= ?
            ORDER BY avg_rating DESC, rating_count DESC
            LIMIT ?;
        """
        params = list(guild_member_ids) + [min_ratings, limit]
        return await self._fetchall(query, params)

    async def get_server_most_played(self, guild_member_ids: List[int], limit: int = 10) -> List[Any]:
        """Get most completed games by distinct linked profiles."""
        if not guild_member_ids:
            return []
        placeholders = ",".join("?" for _ in guild_member_ids)
        query = f"""
            SELECT g.slug, g.title, g.cover_url, g.release_year,
                   COUNT(r.profile_key) as play_count,
                   AVG(r.rating) as avg_rating
            FROM profile_ratings r
            JOIN cached_games g ON r.game_slug = g.slug
            WHERE r.profile_key IN (
                SELECT DISTINCT profile_key FROM linked_users
                WHERE discord_id IN ({placeholders})
            ) AND r.status = 'played'
            GROUP BY g.slug
            ORDER BY play_count DESC, avg_rating DESC
            LIMIT ?;
        """
        params = list(guild_member_ids) + [limit]
        return await self._fetchall(query, params)

    async def get_server_shame_list(self, guild_member_ids: List[int], limit: int = 10) -> List[Any]:
        """Get games most frequently sitting unplayed in linked profiles' backlogs."""
        if not guild_member_ids:
            return []
        placeholders = ",".join("?" for _ in guild_member_ids)
        query = f"""
            SELECT g.slug, g.title, g.cover_url, g.release_year,
                   COUNT(r.profile_key) as backlog_count
            FROM profile_ratings r
            JOIN cached_games g ON r.game_slug = g.slug
            WHERE r.profile_key IN (
                SELECT DISTINCT profile_key FROM linked_users
                WHERE discord_id IN ({placeholders})
            ) AND r.status = 'backlog'
            GROUP BY g.slug
            ORDER BY backlog_count DESC
            LIMIT ?;
        """
        params = list(guild_member_ids) + [limit]
        return await self._fetchall(query, params)

    async def get_server_polarizing(self, guild_member_ids: List[int], limit: int = 10, min_ratings: int = 2) -> List[Any]:
        """Get games with the highest rating disparity/variance among server members."""
        if not guild_member_ids:
            return []
        placeholders = ",".join("?" for _ in guild_member_ids)
        query = f"""
            SELECT g.slug, g.title, g.cover_url, g.release_year,
                   (MAX(r.rating) - MIN(r.rating)) as rating_delta,
                   AVG(r.rating) as avg_rating,
                   COUNT(r.rating) as rating_count
            FROM profile_ratings r
            JOIN cached_games g ON r.game_slug = g.slug
            WHERE r.profile_key IN (
                SELECT DISTINCT profile_key FROM linked_users
                WHERE discord_id IN ({placeholders})
            ) AND r.rating IS NOT NULL
            GROUP BY g.slug
            HAVING rating_count >= ? AND rating_delta > 0
            ORDER BY rating_delta DESC, rating_count DESC
            LIMIT ?;
        """
        params = list(guild_member_ids) + [min_ratings, limit]
        return await self._fetchall(query, params)

    async def get_server_hidden_gems(self, guild_member_ids: List[int], limit: int = 10) -> List[Any]:
        """Get high-scoring games (>= 4.5) played by only 1 or 2 server members."""
        if not guild_member_ids:
            return []
        placeholders = ",".join("?" for _ in guild_member_ids)
        query = f"""
            SELECT g.slug, g.title, g.cover_url, g.release_year,
                   AVG(r.rating) as avg_rating,
                   COUNT(r.rating) as rating_count
            FROM profile_ratings r
            JOIN cached_games g ON r.game_slug = g.slug
            WHERE r.profile_key IN (
                SELECT DISTINCT profile_key FROM linked_users
                WHERE discord_id IN ({placeholders})
            ) AND r.rating IS NOT NULL
            GROUP BY g.slug
            HAVING avg_rating >= 4.5 AND rating_count <= 2
            ORDER BY avg_rating DESC, rating_count ASC
            LIMIT ?;
        """
        params = list(guild_member_ids) + [limit]
        return await self._fetchall(query, params)

    async def get_server_stats(self, guild_member_ids: List[int]) -> dict:
        """Compute aggregate community metrics for a server."""
        if not guild_member_ids:
            return {
                "linked_members": 0,
                "profile_count": 0,
                "total_logged": 0,
                "total_played": 0,
                "server_avg_rating": None,
                "top_game": None
            }

        placeholders = ",".join("?" for _ in guild_member_ids)

        # 1. Distinct member connections and distinct Backloggd profiles.
        link_row = await self._fetchone(
            f"""SELECT COUNT(DISTINCT discord_id) AS member_count,
                       COUNT(DISTINCT profile_key) AS profile_count
                FROM linked_users WHERE discord_id IN ({placeholders});""",
            guild_member_ids
        )
        linked_members = link_row["member_count"] if link_row else 0
        profile_count = link_row["profile_count"] if link_row else 0

        # 2. Logs, Played count, and average rating
        query_metrics = f"""
            SELECT COUNT(*) as total_logged,
                   SUM(CASE WHEN status = 'played' THEN 1 ELSE 0 END) as total_played,
                   AVG(rating) as avg_rating
            FROM profile_ratings
            WHERE profile_key IN (
                SELECT DISTINCT profile_key FROM linked_users
                WHERE discord_id IN ({placeholders})
            );
        """
        row = await self._fetchone(query_metrics, guild_member_ids)
        total_logged = (row["total_logged"] if row else 0) or 0
        total_played = (row["total_played"] if row else 0) or 0
        avg_rating = round(row["avg_rating"], 2) if row and row["avg_rating"] is not None else None

        # 3. Most popular game
        query_top_game = f"""
            SELECT g.title, COUNT(r.profile_key) as count
            FROM profile_ratings r
            JOIN cached_games g ON r.game_slug = g.slug
            WHERE r.profile_key IN (
                SELECT DISTINCT profile_key FROM linked_users
                WHERE discord_id IN ({placeholders})
            ) AND r.status = 'played'
            GROUP BY g.slug
            ORDER BY count DESC
            LIMIT 1;
        """
        top_row = await self._fetchone(query_top_game, guild_member_ids)
        top_game = (
            f"{top_row['title']} ({top_row['count']} profile{'s' if top_row['count'] != 1 else ''})"
            if top_row else None
        )

        return {
            "linked_members": linked_members,
            "profile_count": profile_count,
            "total_logged": total_logged,
            "total_played": total_played,
            "server_avg_rating": avg_rating,
            "top_game": top_game
        }

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
        async with self._operation_lock:
            if self.is_turso:
                transaction = self._client.transaction()
                try:
                    del_rs = await transaction.execute(
                        "DELETE FROM guild_settings WHERE guild_id = ?;", [guild_id]
                    )
                    if del_rs.rows_affected > 0:
                        for sql, args in pause_statements:
                            await transaction.execute(sql, list(args))
                    await transaction.commit()
                    return del_rs.rows_affected > 0
                except BaseException:
                    await transaction.rollback()
                    raise

            await self._conn.execute("BEGIN IMMEDIATE")
            try:
                async with self._conn.execute(
                    "DELETE FROM guild_settings WHERE guild_id = ?;", (guild_id,)
                ) as cursor:
                    deleted = cursor.rowcount
                if deleted > 0:
                    for sql, args in pause_statements:
                        await self._conn.execute(sql, args)
                await self._conn.commit()
                return deleted > 0
            except BaseException:
                await self._conn.rollback()
                raise

    async def get_feed_channel(self, guild_id: int) -> Optional[int]:
        """Get designated feed channel ID for a guild."""
        row = await self._fetchone("SELECT feed_channel_id FROM guild_settings WHERE guild_id = ?;", (guild_id,))
        return row["feed_channel_id"] if row else None

    async def get_all_guild_settings(self) -> List[Any]:
        """Retrieve all guilds with a configured feed channel."""
        return await self._fetchall("SELECT guild_id, feed_channel_id FROM guild_settings;")

    async def is_journal_entry_seen(self, entry_id: str) -> bool:
        """Check if a playthrough journal entry was already recorded/posted."""
        row = await self._fetchone("SELECT 1 FROM seen_journal_logs WHERE entry_id = ?;", (entry_id,))
        return row is not None

    async def mark_journal_entry_seen(self, entry_id: str, discord_id: int) -> None:
        """Record a journal entry as seen to prevent duplicate posts."""
        await self._execute(
            "INSERT OR IGNORE INTO seen_journal_logs (entry_id, discord_id) VALUES (?, ?);",
            (entry_id, discord_id)
        )

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

    async def mark_journal_entry_delivered(
        self, guild_id: int, profile_key: str, entry_id: str
    ) -> None:
        """Record a journal entry only after its guild feed send succeeds."""
        await self._execute(
            """INSERT OR IGNORE INTO profile_seen_journal_deliveries
                (guild_id, profile_key, entry_id) VALUES (?, ?, ?);""",
            (guild_id, profile_key.strip().lower(), entry_id),
        )

    async def seed_journal_entries_for_guild(
        self, guild_id: int, profile_key: str, entry_ids: Iterable[str]
    ) -> None:
        """Mark current journal entries as already present when a feed is enabled."""
        for entry_id in set(entry_ids):
            if entry_id:
                await self.mark_journal_entry_delivered(guild_id, profile_key, entry_id)


    async def seed_feed_source(self, guild_id: int, profile_key: str,
                               source: str, entry_ids: Iterable[str]) -> bool:
        """Baseline a new source once, atomically; later polls deliver new IDs."""
        profile_key = profile_key.strip().lower()
        async with self._operation_lock:
            query = "SELECT 1 FROM profile_feed_sources WHERE guild_id = ? AND profile_key = ? AND source = ?"
            params = (guild_id, profile_key, source)
            if self.is_turso:
                result = await self._client.execute(query, list(params))
                exists = bool(result.rows)
            else:
                async with self._conn.execute(query, params) as cursor:
                    exists = await cursor.fetchone() is not None
            if exists:
                return False
            statements = [(
                "INSERT OR IGNORE INTO profile_seen_journal_deliveries (guild_id, profile_key, entry_id) VALUES (?, ?, ?)",
                (guild_id, profile_key, entry_id),
            ) for entry_id in set(entry_ids) if entry_id]
            statements.append(("INSERT OR IGNORE INTO profile_feed_sources VALUES (?, ?, ?)", params))
            if self.is_turso:
                await self._client.batch(statements)
            else:
                await self._conn.execute("BEGIN IMMEDIATE")
                try:
                    for sql, args in statements:
                        await self._conn.execute(sql, args)
                    await self._conn.commit()
                except BaseException:
                    await self._conn.rollback()
                    raise
            return True


    async def get_feed_messages(self, guild_id: int, profile_key: str):
        """Retain source aliases and Discord message IDs across polls/restarts."""
        profile_key = profile_key.strip().lower()
        # A pending write without a confirmed response may have crossed a process
        # restart. Mark it uncertain so the next poll never blindly reposts it.
        await self._execute(
            """UPDATE profile_feed_messages SET delivery_state = 'unknown'
               WHERE guild_id = ? AND profile_key = ? AND delivery_state = 'pending'""",
            (guild_id, profile_key),
        )
        return await self._fetchall(
            """SELECT event_id, payload, message_id, channel_id, delivery_state
               FROM profile_feed_messages WHERE guild_id = ? AND profile_key = ?""",
            (guild_id, profile_key),
        )

    async def seed_feed_message(self, guild_id: int, profile_key: str, event_ids,
                                payload: str):
        """Record baseline events only when absent, preserving delivered mappings."""
        profile_key = profile_key.strip().lower()
        statements = []
        for event_id in event_ids:
            statements.append((
                """INSERT OR IGNORE INTO profile_feed_messages
                   (guild_id, profile_key, event_id, payload, message_id, channel_id, delivery_state)
                   VALUES (?, ?, ?, ?, NULL, NULL, 'baseline')""",
                (guild_id, profile_key, event_id, payload),
            ))
            statements.append((
                """INSERT OR IGNORE INTO profile_seen_journal_deliveries
                   (guild_id, profile_key, entry_id) VALUES (?, ?, ?)""",
                (guild_id, profile_key, event_id),
            ))
        async with self._operation_lock:
            if self.is_turso:
                await self._client.batch(statements)
            else:
                await self._conn.execute("BEGIN IMMEDIATE")
                try:
                    for sql, args in statements:
                        await self._conn.execute(sql, args)
                    await self._conn.commit()
                except BaseException:
                    await self._conn.rollback()
                    raise

    async def store_feed_message(self, guild_id: int, profile_key: str, event_ids,
                                 payload: str, message_id=None, channel_id=None,
                                 delivery_state: Optional[str] = None):
        profile_key = profile_key.strip().lower()
        saved_state = delivery_state or ("sent" if message_id is not None else "baseline")
        statements = []
        for event_id in event_ids:
            statements.append(("""
                INSERT INTO profile_feed_messages
                    (guild_id, profile_key, event_id, payload, message_id, channel_id, delivery_state)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(guild_id, profile_key, event_id) DO UPDATE SET
                    payload = excluded.payload,
                    message_id = COALESCE(excluded.message_id, profile_feed_messages.message_id),
                    channel_id = COALESCE(excluded.channel_id, profile_feed_messages.channel_id),
                    delivery_state = CASE
                        WHEN excluded.delivery_state IN ('unknown', 'deleted') THEN excluded.delivery_state
                        WHEN excluded.message_id IS NOT NULL THEN 'sent'
                        ELSE profile_feed_messages.delivery_state
                    END
            """, (
                guild_id, profile_key, event_id, payload, message_id, channel_id,
                saved_state,
            )))
            if saved_state != "unknown":
                statements.append(("""
                    INSERT OR IGNORE INTO profile_seen_journal_deliveries
                        (guild_id, profile_key, entry_id) VALUES (?, ?, ?)
                """, (guild_id, profile_key, event_id)))
        async with self._operation_lock:
            if self.is_turso:
                await self._client.batch(statements)
            else:
                await self._conn.execute("BEGIN IMMEDIATE")
                try:
                    for sql, args in statements:
                        await self._conn.execute(sql, args)
                    await self._conn.commit()
                except BaseException:
                    await self._conn.rollback()
                    raise

    async def begin_feed_delivery(
        self, guild_id: int, profile_key: str, event_ids: Iterable[str], payload: str, channel_id: int
    ) -> bool:
        """Persist a send intent before calling Discord; return false for known or uncertain events."""
        profile_key = profile_key.strip().lower()
        aliases = list(dict.fromkeys(event_id for event_id in event_ids if event_id))
        if not aliases:
            return False
        placeholders = ",".join("?" for _ in aliases)
        async with self._operation_lock:
            query = f"""SELECT delivery_state FROM profile_feed_messages
                WHERE guild_id = ? AND profile_key = ? AND event_id IN ({placeholders})"""
            params = [guild_id, profile_key, *aliases]
            if self.is_turso:
                transaction = self._client.transaction()
                try:
                    result = await transaction.execute(query, params)
                    if result.rows:
                        await transaction.commit()
                        return False
                    for event_id in aliases:
                        await transaction.execute(
                            """INSERT OR IGNORE INTO profile_feed_messages
                               (guild_id, profile_key, event_id, payload, message_id, channel_id, delivery_state)
                               VALUES (?, ?, ?, ?, NULL, ?, 'pending')""",
                            [guild_id, profile_key, event_id, payload, channel_id],
                        )
                    await transaction.commit()
                    return True
                except BaseException:
                    await transaction.rollback()
                    raise

            await self._conn.execute("BEGIN IMMEDIATE")
            try:
                async with self._conn.execute(query, params) as cursor:
                    existing = await cursor.fetchone()
                if existing:
                    await self._conn.commit()
                    return False
                for event_id in aliases:
                    await self._conn.execute(
                        """INSERT OR IGNORE INTO profile_feed_messages
                           (guild_id, profile_key, event_id, payload, message_id, channel_id, delivery_state)
                           VALUES (?, ?, ?, ?, NULL, ?, 'pending')""",
                        (guild_id, profile_key, event_id, payload, channel_id),
                    )
                await self._conn.commit()
                return True
            except BaseException:
                await self._conn.rollback()
                raise

    async def release_feed_delivery(self, guild_id: int, profile_key: str, event_ids: Iterable[str]) -> None:
        """Remove a pending intent after Discord definitively rejected a send."""
        aliases = list(dict.fromkeys(event_id for event_id in event_ids if event_id))
        if not aliases:
            return
        placeholders = ",".join("?" for _ in aliases)
        await self._rowcount_execute(
            f"""DELETE FROM profile_feed_messages
                WHERE guild_id = ? AND profile_key = ? AND delivery_state = 'pending'
                  AND event_id IN ({placeholders})""",
            [guild_id, profile_key.strip().lower(), *aliases],
        )

    async def mark_feed_delivery_unknown(self, guild_id: int, profile_key: str, event_ids: Iterable[str]) -> None:
        aliases = list(dict.fromkeys(event_id for event_id in event_ids if event_id))
        if not aliases:
            return
        placeholders = ",".join("?" for _ in aliases)
        await self._execute(
            f"""UPDATE profile_feed_messages SET delivery_state = 'unknown'
                WHERE guild_id = ? AND profile_key = ? AND delivery_state = 'pending'
                  AND event_id IN ({placeholders})""",
            [guild_id, profile_key.strip().lower(), *aliases],
        )

    async def mark_feed_message_deleted(
        self, guild_id: int, profile_key: str, event_ids: Iterable[str], payload: str
    ) -> None:
        aliases = list(dict.fromkeys(event_id for event_id in event_ids if event_id))
        if not aliases:
            return
        placeholders = ",".join("?" for _ in aliases)
        await self._execute(
            f"""UPDATE profile_feed_messages SET delivery_state = 'deleted', payload = ?
                WHERE guild_id = ? AND profile_key = ? AND event_id IN ({placeholders})""",
            [payload, guild_id, profile_key.strip().lower(), *aliases],
        )

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

        async with self._operation_lock:
            if self.is_turso:
                transaction = self._client.transaction()
                try:
                    src_rs = await transaction.execute(
                        "SELECT 1 FROM profile_activity_observation_sources WHERE profile_key = ? AND source = ?",
                        [normalized_key, source],
                    )
                    initialized = not bool(src_rs.rows)
                    if initialized:
                        await transaction.execute(
                            """INSERT OR IGNORE INTO profile_activity_observation_sources
                               (profile_key, source) VALUES (?, ?)""",
                            [normalized_key, source],
                        )
                    existing_map: dict[str, str] = {}
                    for start in range(0, len(items), 800):
                        chunk_keys = [key for key, _ in items[start:start + 800]]
                        if not chunk_keys:
                            continue
                        placeholders = ",".join("?" for _ in chunk_keys)
                        rows_rs = await transaction.execute(
                            f"""SELECT observation_key, fingerprint
                                FROM profile_activity_observations
                                WHERE profile_key = ? AND source = ? AND observation_key IN ({placeholders})""",
                            [normalized_key, source, *chunk_keys],
                        )
                        for row in rows_rs.rows:
                            existing_map[row["observation_key"]] = row["fingerprint"]
                    new_count = 0
                    updated_count = 0
                    for key, fingerprint in items:
                        prev = existing_map.get(key)
                        if prev is None:
                            new_count += 1
                            await transaction.execute(
                                """INSERT INTO profile_activity_observations
                                   (profile_key, source, observation_key, fingerprint, observed_at)
                                   VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)""",
                                [normalized_key, source, key, fingerprint],
                            )
                        elif prev != fingerprint:
                            updated_count += 1
                            await transaction.execute(
                                """UPDATE profile_activity_observations
                                   SET fingerprint = ?, observed_at = CURRENT_TIMESTAMP
                                   WHERE profile_key = ? AND source = ? AND observation_key = ?""",
                                [fingerprint, normalized_key, source, key],
                            )
                    await transaction.commit()
                    return {
                        "initialized": initialized,
                        "new": new_count,
                        "updated": updated_count,
                        "changed": new_count + updated_count,
                    }
                except BaseException:
                    await transaction.rollback()
                    raise

            await self._conn.execute("BEGIN IMMEDIATE")
            try:
                async with self._conn.execute(
                    "SELECT 1 FROM profile_activity_observation_sources WHERE profile_key = ? AND source = ?",
                    (normalized_key, source),
                ) as cursor:
                    initialized = await cursor.fetchone() is None
                if initialized:
                    await self._conn.execute(
                        """INSERT OR IGNORE INTO profile_activity_observation_sources
                           (profile_key, source) VALUES (?, ?)""",
                        (normalized_key, source),
                    )
                existing_map = {}
                for start in range(0, len(items), 800):
                    chunk_keys = [key for key, _ in items[start:start + 800]]
                    if not chunk_keys:
                        continue
                    placeholders = ",".join("?" for _ in chunk_keys)
                    async with self._conn.execute(
                        f"""SELECT observation_key, fingerprint
                            FROM profile_activity_observations
                            WHERE profile_key = ? AND source = ? AND observation_key IN ({placeholders})""",
                        [normalized_key, source, *chunk_keys],
                    ) as cursor:
                        for row in await cursor.fetchall():
                            existing_map[row["observation_key"]] = row["fingerprint"]
                new_count = 0
                updated_count = 0
                for key, fingerprint in items:
                    prev = existing_map.get(key)
                    if prev is None:
                        new_count += 1
                        await self._conn.execute(
                            """INSERT INTO profile_activity_observations
                               (profile_key, source, observation_key, fingerprint, observed_at)
                               VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)""",
                            (normalized_key, source, key, fingerprint),
                        )
                    elif prev != fingerprint:
                        updated_count += 1
                        await self._conn.execute(
                            """UPDATE profile_activity_observations
                               SET fingerprint = ?, observed_at = CURRENT_TIMESTAMP
                               WHERE profile_key = ? AND source = ? AND observation_key = ?""",
                            (fingerprint, normalized_key, source, key),
                        )
                await self._conn.commit()
                return {
                    "initialized": initialized,
                    "new": new_count,
                    "updated": updated_count,
                    "changed": new_count + updated_count,
                }
            except BaseException:
                await self._conn.rollback()
                raise

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
