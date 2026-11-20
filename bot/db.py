"""Database Manager for Backloggd Discord Bot.

Supports both local SQLite (aiosqlite) and Turso / libSQL Cloud Database (libsql-client).
"""

import asyncio
import logging
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any, Iterable, List, Optional

import aiosqlite
import libsql_client

from bot import db_feed, db_guilds, db_library, db_stats, db_users
from bot.config import DATABASE_PATH, LIBSQL_AUTH_TOKEN, LIBSQL_DATABASE_URL
from bot.db_adapter import execute_batch_statements, normalize_libsql_row, open_transaction

_MAX_SQL_BIND_PARAMS = db_stats.MAX_SQL_BIND_PARAMS
_iter_sql_chunks = db_stats.iter_sql_chunks
_MAX_SQL_BATCH_BYTES = db_library.MAX_SQL_BATCH_BYTES
_estimate_row_payload_bytes = db_library.estimate_row_payload_bytes
_iter_bounded_row_batches = db_library.iter_bounded_row_batches
_GUILD_FEED_SETTING_KEYS = db_guilds.GUILD_FEED_SETTING_KEYS

logger = logging.getLogger("backloggd.db")


class Database:
    _CLEAR_UNSENT_FEED_DECISIONS_SQL = db_feed.CLEAR_UNSENT_FEED_DECISIONS_SQL
    _ROULETTE_POOL_STATUSES = db_library.ROULETTE_POOL_STATUSES
    _LIBRARY_SYNC_SOURCE = "__library_sync__"
    _PENDING_REFRESH_KEY = "__pending_refresh__"

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
            if str(self.db_path) != ":memory:":
                Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
            self._conn = await aiosqlite.connect(self.db_path)
            self._conn.row_factory = aiosqlite.Row
            await self._conn.execute("PRAGMA busy_timeout = 5000;")
            await self._conn.execute("PRAGMA journal_mode = WAL;")
            await self._conn.execute("PRAGMA synchronous = NORMAL;")
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
                try:
                    if str(self.db_path) != ":memory:":
                        await self._conn.execute("PRAGMA wal_checkpoint(TRUNCATE);")
                except Exception:
                    logger.debug(
                        "Ignoring non-fatal error during SQLite WAL checkpoint on close",
                        exc_info=True,
                    )
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
                if self._client is None:
                    raise RuntimeError("Database is not connected.")
                await self._client.execute(query, list(params))
            else:
                if self._conn is None:
                    raise RuntimeError("Database is not connected.")
                await self._conn.execute(query, params)
                await self._conn.commit()

    async def _fetchone(self, query: str, params: tuple | list = ()) -> Optional[Any]:
        """Fetch a single row."""
        async with self._acquire_operation_lock():
            if self.is_turso:
                if self._client is None:
                    raise RuntimeError("Database is not connected.")
                rs = await self._client.execute(query, list(params))
                return normalize_libsql_row(rs.rows[0]) if rs.rows else None
            if self._conn is None:
                raise RuntimeError("Database is not connected.")
            async with self._conn.execute(query, params) as cursor:
                return await cursor.fetchone()

    async def _fetchall(self, query: str, params: tuple | list = ()) -> List[Any]:
        """Fetch all rows."""
        async with self._acquire_operation_lock():
            if self.is_turso:
                if self._client is None:
                    raise RuntimeError("Database is not connected.")
                rs = await self._client.execute(query, list(params))
                return [normalize_libsql_row(row) for row in rs.rows]
            if self._conn is None:
                raise RuntimeError("Database is not connected.")
            async with self._conn.execute(query, params) as cursor:
                return list(await cursor.fetchall())

    async def _rowcount_execute(self, query: str, params: tuple | list = ()) -> int:
        """Execute a write statement and return affected rows count."""
        async with self._acquire_operation_lock():
            if self.is_turso:
                if self._client is None:
                    raise RuntimeError("Database is not connected.")
                rs = await self._client.execute(query, list(params))
                return rs.rows_affected
            if self._conn is None:
                raise RuntimeError("Database is not connected.")
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
        formatted = cls._format_utc_sql_timestamp(current)
        return formatted if formatted is not None else ""

    # =========================================================================
    # User Account Linking (delegated to bot.db_users)
    # =========================================================================

    async def link_user(self, discord_id: int, username: str, avatar_url: Optional[str] = None) -> bool:
        """Connect a Discord member to a normalized shared profile, maintaining feed continuity."""
        return await db_users.link_user(self, discord_id, username, avatar_url=avatar_url)

    async def link_user_if_unlinked(
        self, discord_id: int, username: str, avatar_url: Optional[str] = None
    ) -> bool:
        """Create a member link only if no link exists, without replacing a concurrent winner."""
        return await db_users.link_user_if_unlinked(self, discord_id, username, avatar_url=avatar_url)

    async def unlink_user(self, discord_id: int) -> bool:
        """Remove this member's connection and deactivate guild feeds with no remaining source."""
        return await db_users.unlink_user(self, discord_id)

    async def get_linked_user(self, discord_id: int) -> Optional[Any]:
        """Fetch linked user record by Discord ID."""
        return await db_users.get_linked_user(self, discord_id)

    async def get_profile_connections(
        self,
        username: str,
        member_ids: List[int],
        exclude_discord_id: Optional[int] = None,
    ) -> List[Any]:
        """List only this profile's links among explicitly supplied guild members."""
        return await db_users.get_profile_connections(
            self, username, member_ids, exclude_discord_id=exclude_discord_id
        )

    async def get_profile_sync_info(self, username: str) -> Optional[dict[str, Any]]:
        """Return the profile's last complete sync time, sync health, cached count, and pending token."""
        return await db_users.get_profile_sync_info(self, username)

    async def get_profiles_sync_info(self, profile_keys: Iterable[str]) -> dict[str, dict[str, Any]]:
        """Return sync freshness metadata keyed by normalized profile_key."""
        return await db_users.get_profiles_sync_info(self, profile_keys)

    async def get_profiles_refresh_pending_tokens(
        self, profile_keys: Iterable[str],
    ) -> dict[str, Optional[str]]:
        """Return active pending refresh tokens keyed by normalized profile_key in bounded chunks."""
        return await db_users.get_profiles_refresh_pending_tokens(self, profile_keys)

    async def is_profile_library_subscribed(self, username: str) -> bool:
        """Return true when at least one Discord link consumes this profile's library."""
        return await db_users.is_profile_library_subscribed(self, username)

    async def get_profiles_library_subscription_map(
        self, profile_keys: Iterable[str],
    ) -> dict[str, bool]:
        """Return linked-member subscription evidence for normalized profiles in bounded chunks."""
        return await db_users.get_profiles_library_subscription_map(self, profile_keys)

    async def get_profile(self, username: str) -> Optional[Any]:
        """Fetch canonical profile data without attributing it to one Discord member."""
        return await db_users.get_profile(self, username)

    async def get_profile_ratings_dict(self, username: str) -> dict[str, Optional[float]]:
        """Return the shared profile's slug-to-rating mapping."""
        return await db_users.get_profile_ratings_dict(self, username)

    async def get_profile_library_entries(self, username: str) -> dict[str, dict[str, Any]]:
        """Return the shared profile's slug-to-{rating, status} mapping."""
        return await db_users.get_profile_library_entries(self, username)

    async def get_cached_profile_rating_evidence(self, profile_key: str, game_slugs: Iterable[str]) -> dict:
        """Read just the named games and their library snapshot timestamp."""
        return await db_users.get_cached_profile_rating_evidence(self, profile_key, game_slugs)

    async def get_profile_stats(self, username: str) -> dict:
        """Fetch aggregate rating stats once for a Backloggd profile."""
        return await db_users.get_profile_stats(self, username)

    async def get_guild_linked_users(self, member_ids: List[int]) -> List[Any]:
        """Fetch all linked users present in a specific guild."""
        return await db_users.get_guild_linked_users(self, member_ids)

    async def get_guild_linked_profiles(
        self,
        member_ids: List[int],
        exclude_profile_key: Optional[str] = None,
    ) -> List[dict[str, Any]]:
        """Fetch distinct linked profiles present among the supplied guild members."""
        return await db_users.get_guild_linked_profiles(
            self, member_ids, exclude_profile_key=exclude_profile_key
        )

    async def get_guild_linked_profiles_with_sync_info(
        self,
        member_ids: List[int],
        exclude_profile_key: Optional[str] = None,
    ) -> List[dict[str, Any]]:
        """Fetch distinct linked guild profiles with library sync timestamps, errors, and pending tokens."""
        return await db_users.get_guild_linked_profiles_with_sync_info(
            self, member_ids, exclude_profile_key=exclude_profile_key
        )

    # =========================================================================
    # Guild Preferences, Follows & Settings (delegated to bot.db_guilds)
    # =========================================================================

    async def set_guild_feed_preference(
        self,
        guild_id: int,
        discord_id: int,
        enabled: bool,
        *,
        member_ids: Optional[Iterable[int]] = None,
    ) -> Optional[str]:
        """Set a member's server-local feed preference; return their profile key."""
        return await db_guilds.set_guild_feed_preference(
            self, guild_id, discord_id, enabled, member_ids=member_ids
        )

    async def get_guild_feed_preference(self, guild_id: int, discord_id: int) -> bool:
        """Return whether a member opted into this guild's activity feed."""
        return await db_guilds.get_guild_feed_preference(self, guild_id, discord_id)

    async def get_guild_feed_profiles(
        self, guild_id: int, member_ids: Iterable[int]
    ) -> List[Any]:
        """Return distinct Backloggd profiles opted into this guild's activity feed."""
        return await db_guilds.get_guild_feed_profiles(self, guild_id, member_ids)

    async def get_legacy_feed_guilds_pending(self) -> List[int]:
        """Return guilds whose pre-opt-in feed channel has not been migrated yet."""
        return await db_guilds.get_legacy_feed_guilds_pending(self)

    async def complete_legacy_feed_migration(
        self, guild_id: int, present_member_ids: Iterable[int]
    ) -> bool:
        """Opt in currently present linked members for a legacy feed guild once."""
        return await db_guilds.complete_legacy_feed_migration(self, guild_id, present_member_ids)

    async def get_guild_profile_member_sources(
        self, guild_id: int, profile_key: str, member_ids: Iterable[int]
    ) -> List[int]:
        """Return current member IDs that actively enroll the given profile."""
        return await db_guilds.get_guild_profile_member_sources(
            self, guild_id, profile_key, member_ids
        )

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
        return await db_guilds.add_guild_admin_follow(
            self, guild_id, username, avatar_url, added_by, member_ids=member_ids
        )

    async def remove_guild_admin_follow(self, guild_id: int, username: str) -> bool:
        return await db_guilds.remove_guild_admin_follow(self, guild_id, username)

    async def has_guild_admin_follow(self, guild_id: int, profile_key: str) -> bool:
        return await db_guilds.has_guild_admin_follow(self, guild_id, profile_key)

    async def get_guild_admin_follows(self, guild_id: int) -> List[Any]:
        return await db_guilds.get_guild_admin_follows(self, guild_id)

    async def get_guild_feed_candidates(
        self, guild_id: int, member_ids: Iterable[int]
    ) -> List[dict]:
        """Union current member enrollment and admin follows by public profile."""
        return await db_guilds.get_guild_feed_candidates(self, guild_id, member_ids)

    async def set_feed_channel(self, guild_id: int, channel_id: int) -> None:
        """Configure or update the feed channel for a guild."""
        await db_guilds.set_feed_channel(self, guild_id, channel_id)

    async def unset_feed_channel(self, guild_id: int) -> bool:
        """Remove feed channel setting for a guild and pause subscriptions atomically."""
        return await db_guilds.unset_feed_channel(self, guild_id)

    async def get_feed_channel(self, guild_id: int) -> Optional[int]:
        """Get configured feed channel ID for a guild."""
        return await db_guilds.get_feed_channel(self, guild_id)

    async def get_all_guild_settings(self) -> List[Any]:
        """Retrieve all guilds with configured feed channels."""
        return await db_guilds.get_all_guild_settings(self)

    async def get_guild_command_cooldowns(self, guild_id: int) -> dict[str, int]:
        """Return per-command cooldown overrides for a guild."""
        return await db_guilds.get_guild_command_cooldowns(self, guild_id)

    async def set_guild_command_cooldown(
        self,
        guild_id: int,
        command_name: str,
        seconds: int,
    ) -> None:
        """Persist a guild-scoped cooldown override for one command."""
        await db_guilds.set_guild_command_cooldown(
            self, guild_id, command_name, seconds
        )

    async def reset_guild_command_cooldown(
        self,
        guild_id: int,
        command_name: Optional[str] = None,
    ) -> int:
        """Remove one or all guild-scoped cooldown overrides."""
        return await db_guilds.reset_guild_command_cooldown(
            self, guild_id, command_name=command_name
        )

    async def get_guild_feed_filters(self, guild_id: int) -> dict[str, bool]:
        """Return effective feed filter and delivery settings for one guild."""
        return await db_guilds.get_guild_feed_filters(self, guild_id)

    async def get_all_guild_feed_filters(
        self, guild_ids: Iterable[int]
    ) -> dict[int, dict[str, bool]]:
        """Bulk fetch feed category filters and options for multiple guilds within SQL bind budgets."""
        return await db_guilds.get_all_guild_feed_filters(self, guild_ids)

    async def set_guild_feed_filters(
        self,
        guild_id: int,
        *,
        allow_started: Optional[bool] = None,
        allow_finished: Optional[bool] = None,
        allow_replays: Optional[bool] = None,
        allow_ratings: Optional[bool] = None,
        allow_reviews: Optional[bool] = None,
        spoiler_reviews: Optional[bool] = None,
    ) -> dict[str, bool]:
        """Update one or more guild feed filter or option flags and return the effective config."""
        return await db_guilds.set_guild_feed_filters(
            self,
            guild_id,
            allow_started=allow_started,
            allow_finished=allow_finished,
            allow_replays=allow_replays,
            allow_ratings=allow_ratings,
            allow_reviews=allow_reviews,
            spoiler_reviews=spoiler_reviews,
        )

    async def set_guild_digest(
        self,
        guild_id: int,
        channel_id: int,
        cadence: str,
        *,
        now: Optional[Any] = None,
    ) -> dict[str, Any]:
        """Configure or update the scheduled recap digest for a guild."""
        return await db_guilds.set_guild_digest(
            self, guild_id, channel_id, cadence, now=now
        )

    async def unset_guild_digest(self, guild_id: int) -> bool:
        """Remove a guild's scheduled digest configuration."""
        return await db_guilds.unset_guild_digest(self, guild_id)

    async def get_guild_digest(self, guild_id: int) -> Optional[dict[str, Any]]:
        """Fetch a guild's scheduled digest configuration, or None if unconfigured."""
        return await db_guilds.get_guild_digest(self, guild_id)

    async def get_all_guild_digests(self) -> List[dict[str, Any]]:
        """Return all enabled guild digest schedules."""
        return await db_guilds.get_all_guild_digests(self)

    async def mark_guild_digest_sent(
        self,
        guild_id: int,
        *,
        sent_at: Optional[Any] = None,
    ) -> None:
        """Record the UTC timestamp when a guild's digest was last sent."""
        await db_guilds.mark_guild_digest_sent(self, guild_id, sent_at=sent_at)

    # =========================================================================
    # Cached Games & Ratings (delegated to bot.db_library)
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
        await db_library.upsert_game(
            self,
            slug,
            title,
            cover_url=cover_url,
            release_year=release_year,
            community_rating=community_rating,
        )

    async def upsert_user_rating(
        self,
        discord_id: int,
        game_slug: str,
        rating: Optional[float],
        status: str,
        is_liked: bool = False
    ) -> None:
        """Retained Discord-ID compatibility shim writing through linked_users to shared profile_ratings."""
        await db_library.upsert_user_rating(
            self, discord_id, game_slug, rating, status, is_liked=is_liked
        )

    async def get_server_ratings_for_game(self, guild_member_ids: List[int], game_slug: str) -> List[Any]:
        """Fetch one rating/status row per Backloggd profile connected in this guild."""
        return await db_library.get_server_ratings_for_game(self, guild_member_ids, game_slug)

    async def get_user_backlog(self, discord_id: int, pool: str = "both") -> List[Any]:
        """Retrieve unrated backlog and/or wishlist games for a user."""
        return await db_library.get_user_backlog(self, discord_id, pool=pool)

    async def get_profile_backlog(self, profile_key: str, pool: str = "both") -> List[Any]:
        """Retrieve unrated backlog and/or wishlist games for a canonical Backloggd profile key."""
        return await db_library.get_profile_backlog(self, profile_key, pool=pool)

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
        return await db_library.replace_user_library(
            self,
            discord_id,
            expected_username,
            games,
            synced_at=synced_at,
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
        return await db_library.replace_profile_library(
            self,
            username,
            games,
            synced_at=synced_at,
            require_linked=require_linked,
            clear_pending_token=clear_pending_token,
        )

    async def record_profile_sync_failure(
        self,
        username: str,
        error: str,
        *,
        attempted_at: Optional[Any] = None,
    ) -> None:
        """Record a failed library refresh without erasing cached ratings or last_synced_at."""
        await db_library.record_profile_sync_failure(
            self, username, error, attempted_at=attempted_at
        )

    async def update_profile_played_count(
        self,
        username: str,
        played_count: int,
        *,
        observed_at: Optional[Any] = None,
    ) -> bool:
        """Persist an authoritative Played-tab count for a known profile."""
        return await db_library.update_profile_played_count(
            self, username, played_count, observed_at=observed_at
        )

    async def update_profiles_played_counts(
        self,
        counts: Iterable[tuple[str, int]],
        *,
        observed_at: Optional[Any] = None,
    ) -> int:
        """Persist authoritative Played-tab counts for known profiles atomically."""
        return await db_library.update_profiles_played_counts(
            self, counts, observed_at=observed_at
        )

    async def get_multi_profile_ratings_dict(
        self,
        profile_keys: List[str],
    ) -> dict[str, dict[str, Optional[float]]]:
        """Return one cached rating map per normalized Backloggd profile."""
        return await db_library.get_multi_profile_ratings_dict(self, profile_keys)

    async def get_games_by_slugs(self, slugs: List[str]) -> dict[str, Any]:
        """Fetch cached game records by slugs in bounded batch queries."""
        return await db_library.get_games_by_slugs(self, slugs)

    # =========================================================================
    # Server Leaderboards & Statistics (delegated to bot.db_stats)
    # =========================================================================

    async def get_guild_stats_context(self, member_ids: List[int]) -> dict[str, Any]:
        """Resolve linked-member count, distinct profiles, and sync metadata in one pass."""
        return await db_stats.get_guild_stats_context(self, member_ids)

    async def _get_guild_linked_profile_keys(self, guild_member_ids: List[int]) -> List[str]:
        if not guild_member_ids:
            return []
        profiles = await self.get_guild_linked_profiles(guild_member_ids)
        return [row["profile_key"] for row in profiles]

    async def get_server_top_rated_for_profiles(
        self, profile_keys: List[str], limit: int = 10, min_ratings: int = 2
    ) -> List[dict[str, Any]]:
        """Query distinct canonical profile keys; return plain result dictionaries."""
        return await db_stats.get_server_top_rated_for_profiles(
            self, profile_keys, limit=limit, min_ratings=min_ratings
        )

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
        return await db_stats.get_server_most_played_for_profiles(self, profile_keys, limit=limit)

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
        return await db_stats.get_server_shame_list_for_profiles(self, profile_keys, limit=limit)

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
        return await db_stats.get_server_polarizing_for_profiles(
            self, profile_keys, limit=limit, min_ratings=min_ratings
        )

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
        return await db_stats.get_server_hidden_gems_for_profiles(self, profile_keys, limit=limit)

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
        return await db_stats.get_server_stats_for_profiles(
            self,
            profile_keys,
            linked_members=linked_members,
            include_library_row_counts=include_library_row_counts,
        )

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

    async def get_server_currently_playing_for_profiles(
        self,
        profile_keys: List[str],
        *,
        limit: int = 50,
    ) -> List[dict[str, Any]]:
        """Query distinct canonical profile keys for games marked Playing."""
        return await db_stats.get_server_currently_playing_for_profiles(
            self,
            profile_keys,
            limit=limit,
        )

    async def get_server_currently_playing(
        self,
        guild_member_ids: List[int],
        *,
        limit: int = 50,
    ) -> List[dict[str, Any]]:
        """Get games currently being played by distinct linked profiles in a guild."""
        if not guild_member_ids or limit <= 0:
            return []
        profile_keys = await self._get_guild_linked_profile_keys(guild_member_ids)
        return await self.get_server_currently_playing_for_profiles(profile_keys, limit=limit)

    async def get_guild_activity_recap(
        self,
        guild_id: int,
        *,
        since_utc: Any,
    ) -> dict[str, Any]:
        """Summarize delivered feed announcements for a guild since a UTC timestamp."""
        return await db_stats.get_guild_activity_recap(
            self,
            guild_id,
            since_utc=since_utc,
        )

    # =========================================================================
    # Feed Reconciliation, Health, Decisions, Delivery & Observations (bot.db_feed)
    # =========================================================================

    async def _deactivate_guild_profile_tx(self, tx, guild_id: int, profile_key: str) -> None:
        await db_feed.deactivate_guild_profile_tx(self, tx, guild_id, profile_key)

    async def _prepare_profile_source_addition_tx(
        self,
        tx,
        guild_id: int,
        profile_key: str,
        *,
        present_member_ids: Optional[Iterable[int]] = None,
    ) -> None:
        await db_feed.prepare_profile_source_addition_tx(
            self, tx, guild_id, profile_key, present_member_ids=present_member_ids
        )

    async def _prepare_relinked_user_preferences_tx(
        self, tx, discord_id: int, profile_key: str
    ) -> None:
        await db_feed.prepare_relinked_user_preferences_tx(self, tx, discord_id, profile_key)

    async def _resolve_pending_reconciliation_tx(
        self,
        tx,
        guild_id: int,
        present_member_ids: Iterable[int],
        *,
        profile_key: Optional[str] = None,
    ) -> list[str]:
        return await db_feed.resolve_pending_reconciliation_tx(
            self, tx, guild_id, present_member_ids, profile_key=profile_key
        )

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
        return await db_feed.reconcile_removed_profile_source_tx(
            self,
            tx,
            guild_id,
            profile_key,
            present_member_ids=present_member_ids,
            removed_discord_id=removed_discord_id,
            removed_admin=removed_admin,
        )

    async def disable_guild_feed_preference_and_reconcile(
        self,
        guild_id: int,
        discord_id: int,
        *,
        member_ids: Optional[Iterable[int]] = None,
    ) -> Optional[dict[str, Any]]:
        """Atomically opt a member out and deactivate/record reconciliation for their profile."""
        return await db_feed.disable_guild_feed_preference_and_reconcile(
            self, guild_id, discord_id, member_ids=member_ids
        )

    async def remove_guild_admin_follow_and_reconcile(
        self,
        guild_id: int,
        username: str,
        *,
        member_ids: Optional[Iterable[int]] = None,
    ) -> dict[str, Any]:
        """Atomically remove a guild admin follow and reconcile that profile's feed state."""
        return await db_feed.remove_guild_admin_follow_and_reconcile(
            self, guild_id, username, member_ids=member_ids
        )

    async def reconcile_guild_feed_activity(
        self,
        guild_id: int,
        eligible_profile_keys: Iterable[str],
        *,
        now: Optional[Any] = None,
        present_member_ids: Optional[Iterable[int]] = None,
    ) -> dict[str, list[str]]:
        """Pause departed profiles and fresh-baseline profiles that resume atomically."""
        return await db_feed.reconcile_guild_feed_activity(
            self,
            guild_id,
            eligible_profile_keys,
            now=now,
            present_member_ids=present_member_ids,
        )

    @staticmethod
    def _empty_feed_status() -> dict[str, Any]:
        return db_feed.empty_feed_status()

    async def get_guild_feed_statuses(
        self, guild_id: int, profile_keys: Iterable[str]
    ) -> dict[str, dict[str, Any]]:
        """Fetch feed status dicts for many profiles in bounded guild- and profile-scoped chunks."""
        return await db_feed.get_guild_feed_statuses(self, guild_id, profile_keys)

    async def get_guild_profile_feed_status(self, guild_id: int, profile_key: str) -> dict:
        return await db_feed.get_guild_profile_feed_status(self, guild_id, profile_key)

    async def get_profile_feed_source(self, profile_key: str, source: str) -> Optional[Any]:
        return await db_feed.get_profile_feed_source(self, profile_key, source)

    async def get_guild_profile_activation_at(self, guild_id: int, profile_key: str) -> Optional[str]:
        return await db_feed.get_guild_profile_activation_at(self, guild_id, profile_key)

    async def record_feed_source_attempt(
        self, profile_key: str, source: str, *, now: Optional[Any] = None,
    ) -> None:
        await db_feed.record_feed_source_attempt(self, profile_key, source, now=now)

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
        await db_feed.record_feed_source_success(
            self,
            profile_key,
            source,
            coverage=coverage,
            pages_read=pages_read,
            boundary_event_id=boundary_event_id,
            defer_boundary_commit=defer_boundary_commit,
            gap_until=gap_until,
            now=now,
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
        await db_feed.commit_feed_source_boundary(
            self,
            profile_key,
            source,
            coverage=coverage,
            boundary_event_id=boundary_event_id,
            now=now,
        )

    async def record_feed_source_failure(self, profile_key: str, source: str, error: str) -> None:
        await db_feed.record_feed_source_failure(self, profile_key, source, error)

    async def get_pending_feed_observations(self, guild_id: int, profile_key: str) -> List[Any]:
        """Return validated eligible events that have no terminal delivery record."""
        return await db_feed.get_pending_feed_observations(self, guild_id, profile_key)

    async def get_feed_event_decision(
        self, guild_id: int, profile_key: str, event_id: str
    ) -> Optional[Any]:
        return await db_feed.get_feed_event_decision(self, guild_id, profile_key, event_id)

    async def get_feed_event_decisions(
        self, guild_id: int, profile_key: str, event_ids: Iterable[str]
    ) -> dict[str, Any]:
        """Bulk fetch stored feed event decisions for a guild/profile/alias set."""
        return await db_feed.get_feed_event_decisions(self, guild_id, profile_key, event_ids)

    async def resolve_feed_event_decision(
        self, guild_id: int, profile_key: str, event_id: str, payload: str,
        event_date: Optional[str], *, decision: str = "eligible",
        reason: str = "resolved_by_stable_reference",
    ) -> None:
        """Allow an ambiguous candidate to resolve only after strong identity evidence."""
        await db_feed.resolve_feed_event_decision(
            self,
            guild_id,
            profile_key,
            event_id,
            payload,
            event_date,
            decision=decision,
            reason=reason,
        )

    async def persist_feed_identity_decisions(
        self,
        guild_id: int,
        profile_key: str,
        *,
        operations: Iterable[dict[str, Any]],
    ) -> None:
        """Apply ordered ambiguity and strong-reference decisions in bounded SQL batches."""
        await db_feed.persist_feed_identity_decisions(
            self, guild_id, profile_key, operations=operations
        )

    async def has_feed_source(self, guild_id: int, profile_key: str, source: str) -> bool:
        return await db_feed.has_feed_source(self, guild_id, profile_key, source)

    async def record_feed_event_decision(
        self, guild_id: int, profile_key: str, event_id: str, *, source: str,
        event_date: Optional[str], decision: str, reason: str, payload: str,
        observed_at: Optional[Any] = None,
    ) -> Any:
        """Persist the first decision; retries cannot age an event into a new post."""
        return await db_feed.record_feed_event_decision(
            self,
            guild_id,
            profile_key,
            event_id,
            source=source,
            event_date=event_date,
            decision=decision,
            reason=reason,
            payload=payload,
            observed_at=observed_at,
        )

    async def revise_unsent_eligible_feed_decision(
        self, guild_id: int, profile_key: str, event_id: str, *,
        decision: str, reason: str, event_date: Optional[str], payload: str,
        allowed_from: Iterable[str] = ("eligible",),
    ) -> None:
        """Apply new evidence to unsent eligible/resolved work without altering archive decisions."""
        await db_feed.revise_unsent_eligible_feed_decision(
            self,
            guild_id,
            profile_key,
            event_id,
            decision=decision,
            reason=reason,
            event_date=event_date,
            payload=payload,
            allowed_from=allowed_from,
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
        return await db_feed.persist_feed_event_decisions(
            self,
            guild_id,
            profile_key,
            inserts=inserts,
            revisions=revisions,
            alias_groups=alias_groups,
        )

    async def get_feed_decision_counts(self, guild_id: int, profile_key: str) -> dict[str, int]:
        return await db_feed.get_feed_decision_counts(self, guild_id, profile_key)

    async def get_guild_feed_recovery_profiles(self, guild_id: int) -> List[Any]:
        """Last active recipients still enrolled, when Discord membership is unavailable."""
        return await db_feed.get_guild_feed_recovery_profiles(self, guild_id)

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
        await db_feed.record_feed_target_gap(
            self,
            profile_key,
            guild_id=guild_id,
            guild_ids=guild_ids,
            reason=reason,
            last_error=last_error,
            now=now,
        )

    async def clear_guild_feed_recipient_gaps(
        self, profile_key: str, guild_ids: Iterable[int]
    ) -> None:
        """Clear recipient gaps for guilds whose required delivery/edit pass succeeded."""
        await db_feed.clear_guild_feed_recipient_gaps(self, profile_key, guild_ids)

    async def is_journal_entry_delivered(
        self, guild_id: int, profile_key: str, entry_id: str
    ) -> bool:
        """Check whether a profile event was delivered or baselined in this guild."""
        return await db_feed.is_journal_entry_delivered(self, guild_id, profile_key, entry_id)

    async def get_delivered_journal_entry_ids(
        self, guild_id: int, profile_key: str, entry_ids: Iterable[str]
    ) -> set[str]:
        """Bulk fetch delivered or baselined entry IDs within SQL bind budgets."""
        return await db_feed.get_delivered_journal_entry_ids(self, guild_id, profile_key, entry_ids)

    async def get_feed_sources(self, guild_id: int, profile_key: str) -> set[str]:
        """Return initialized feed source identifiers for a guild/profile."""
        return await db_feed.get_feed_sources(self, guild_id, profile_key)

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
        return await db_feed.seed_feed_source(
            self,
            guild_id,
            profile_key,
            source,
            entry_ids,
            baseline_messages=baseline_messages,
            baseline_decisions=baseline_decisions,
        )

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
        return await db_feed.seed_feed_source_with_messages(
            self,
            guild_id,
            profile_key,
            source,
            messages,
            baseline_decisions=baseline_decisions,
        )

    def _clear_feed_delivery_tokens(
        self, guild_id: int, profile_key: str, event_ids: Iterable[str]
    ) -> None:
        db_feed.clear_feed_delivery_tokens(self, guild_id, profile_key, event_ids)

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
        db_feed.release_feed_delivery_ownership(
            self,
            guild_id,
            profile_key,
            event_ids,
            owner_token=owner_token,
            recheck_recovery=recheck_recovery,
        )

    async def _ensure_stale_pending_recovered(self) -> None:
        await db_feed.ensure_stale_pending_recovered(self)

    async def recover_stale_feed_deliveries(
        self, guild_id: Optional[int] = None, profile_key: Optional[str] = None
    ) -> int:
        """Mark unowned pending delivery intents unknown without touching live sends."""
        return await db_feed.recover_stale_feed_deliveries(
            self, guild_id=guild_id, profile_key=profile_key
        )

    async def get_feed_messages(self, guild_id: int, profile_key: str):
        """Retain source aliases and Discord message IDs across polls/restarts without mutating live pending intents."""
        return await db_feed.get_feed_messages(self, guild_id, profile_key)

    @staticmethod
    def _iter_relevant_history_batches(
        ids: list[str],
        games: list[str],
        refs: list[str],
        *,
        max_json_bytes: int = 32 * 1024,
    ) -> Iterable[tuple[list[str], list[str], list[str]]]:
        """Yield (ids, games, refs) batches keeping combined JSON parameter size bounded."""
        return db_feed.iter_relevant_history_batches(
            ids, games, refs, max_json_bytes=max_json_bytes
        )

    async def get_relevant_feed_messages(
        self,
        guild_id: int,
        profile_key: str,
        *,
        event_ids: Iterable[str] = (),
        game_slugs: Iterable[str] = (),
        references: Iterable[str] = (),
    ) -> List[Any]:
        """Fetch only archive rows relevant to incoming/pending IDs, aliases, games, and references."""
        return await db_feed.get_relevant_feed_messages(
            self,
            guild_id,
            profile_key,
            event_ids=event_ids,
            game_slugs=game_slugs,
            references=references,
        )

    async def seed_feed_message(self, guild_id: int, profile_key: str, event_ids, payload: str):
        """Record baseline events only when absent, preserving delivered mappings."""
        await db_feed.seed_feed_message(self, guild_id, profile_key, event_ids, payload)

    async def store_feed_message(
        self,
        guild_id: int,
        profile_key: str,
        event_ids,
        payload: str,
        message_id=None,
        channel_id=None,
        delivery_state: Optional[str] = None,
        recorded_at: Optional[Any] = None,
    ):
        await db_feed.store_feed_message(
            self,
            guild_id,
            profile_key,
            event_ids,
            payload,
            message_id=message_id,
            channel_id=channel_id,
            delivery_state=delivery_state,
            recorded_at=recorded_at,
        )

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
        return await db_feed.begin_feed_delivery(
            self,
            guild_id,
            profile_key,
            event_ids,
            payload,
            channel_id,
            owner_token=owner_token,
        )

    async def release_feed_delivery(self, guild_id: int, profile_key: str, event_ids: Iterable[str]) -> None:
        """Remove a pending intent after Discord definitively rejected a send."""
        await db_feed.release_feed_delivery(self, guild_id, profile_key, event_ids)

    async def mark_feed_delivery_unknown(self, guild_id: int, profile_key: str, event_ids: Iterable[str]) -> None:
        await db_feed.mark_feed_delivery_unknown(self, guild_id, profile_key, event_ids)

    async def mark_feed_message_deleted(
        self, guild_id: int, profile_key: str, event_ids: Iterable[str], payload: str
    ) -> None:
        await db_feed.mark_feed_message_deleted(self, guild_id, profile_key, event_ids, payload)

    async def get_profile_activity_observations(
        self, profile_key: str, source: str, keys: Iterable[str],
    ) -> dict[str, str]:
        return await db_feed.get_profile_activity_observations(self, profile_key, source, keys)

    async def record_profile_activity_observations(
        self,
        profile_key: str,
        source: str,
        observations: Iterable[tuple[str, str]],
    ) -> dict[str, Any]:
        """Persist profile-level relevant event fingerprints and report newly observed changes."""
        return await db_feed.record_profile_activity_observations(
            self, profile_key, source, observations
        )

    async def set_profile_refresh_pending(self, profile_key: str, token: str) -> None:
        """Persist a pending library refresh token that survives cooldown skips and restarts."""
        await db_feed.set_profile_refresh_pending(self, profile_key, token)

    async def get_profile_refresh_pending_token(self, profile_key: str) -> Optional[str]:
        """Return the active pending library refresh token for a profile, if any."""
        return await db_feed.get_profile_refresh_pending_token(self, profile_key)

    async def clear_profile_refresh_pending(
        self, profile_key: str, *, token: Optional[str] = None
    ) -> bool:
        """Clear a pending refresh token unless a newer observation superseded it."""
        return await db_feed.clear_profile_refresh_pending(self, profile_key, token=token)

    async def prune_stale_history(
        self,
        now: Any = None,
        *,
        retention_days: int = 90,
        keep_recent_per_profile: int = 200,
    ) -> dict[str, int]:
        """Safely prune aged auxiliary history without risking feed dedup or pending sync tokens."""
        return await db_feed.prune_stale_history(
            self,
            now=now,
            retention_days=retention_days,
            keep_recent_per_profile=keep_recent_per_profile,
        )

    async def get_profiles_poll_cadence_metadata(self, profile_keys: Iterable[str]) -> dict[str, dict[str, Any]]:
        """Bulk-load source health, gap, baseline, and recency metadata for adaptive poll tiering."""
        return await db_feed.get_profiles_poll_cadence_metadata(self, profile_keys)
