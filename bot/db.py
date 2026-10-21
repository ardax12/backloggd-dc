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
            logger.info("Connected to Turso cloud database.")
        else:
            Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
            self._conn = await aiosqlite.connect(self.db_path)
            self._conn.row_factory = aiosqlite.Row
            await self._conn.execute("PRAGMA foreign_keys = ON;")
            await self._create_tables()
            logger.info("Connected to local SQLite database at %s", self.db_path)

    @property
    def is_connected(self) -> bool:
        """Check if database connection is active."""
        if self.is_turso:
            return self._client is not None and not getattr(self._client, "_closed", False)
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

        # Indexes for fast lookup
        await self._execute("CREATE INDEX IF NOT EXISTS idx_ratings_slug ON user_ratings(game_slug);")
        await self._execute("CREATE INDEX IF NOT EXISTS idx_ratings_user ON user_ratings(discord_id);")
        await self._execute("CREATE INDEX IF NOT EXISTS idx_ratings_status ON user_ratings(status);")

        # 4. Guild settings for automatic feed
        await self._execute("""
            CREATE TABLE IF NOT EXISTS guild_settings (
                guild_id INTEGER PRIMARY KEY,
                feed_channel_id INTEGER NOT NULL
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

    # =========================================================================
    # User Account Linking
    # =========================================================================

    async def link_user(self, discord_id: int, username: str, avatar_url: Optional[str] = None) -> None:
        """Insert or update linked Backloggd username for a Discord user."""
        from bot.ui.helpers import clean_image_url
        clean_avatar = clean_image_url(avatar_url)
        query = """
            INSERT INTO linked_users (discord_id, backloggd_username, avatar_url, last_synced_at)
            VALUES (?, ?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT(discord_id) DO UPDATE SET
                backloggd_username = excluded.backloggd_username,
                avatar_url = COALESCE(excluded.avatar_url, linked_users.avatar_url),
                last_synced_at = CURRENT_TIMESTAMP;
        """
        await self._execute(query, (discord_id, username, clean_avatar))

    async def unlink_user(self, discord_id: int) -> bool:
        """Remove a linked user and cascade-delete their cached ratings."""
        deleted = await self._rowcount_execute("DELETE FROM linked_users WHERE discord_id = ?;", (discord_id,))
        return deleted > 0

    async def get_linked_user(self, discord_id: int) -> Optional[Any]:
        """Fetch linked user record by Discord ID."""
        return await self._fetchone(
            "SELECT discord_id, backloggd_username, avatar_url, last_synced_at FROM linked_users WHERE discord_id = ?;",
            (discord_id,)
        )

    async def get_linked_user_by_username(self, username: str) -> Optional[Any]:
        """Fetch linked user record by Backloggd username (case-insensitive)."""
        return await self._fetchone(
            "SELECT discord_id, backloggd_username, avatar_url, last_synced_at FROM linked_users WHERE LOWER(backloggd_username) = LOWER(?);",
            (username.strip(),)
        )

    async def get_all_linked_users(self) -> List[Any]:
        """Fetch all linked users."""
        return await self._fetchall("SELECT discord_id, backloggd_username, avatar_url FROM linked_users;")

    async def get_guild_linked_users(self, member_ids: List[int]) -> List[Any]:
        """Fetch all linked users present in a specific guild."""
        if not member_ids:
            return []
        placeholders = ",".join("?" for _ in member_ids)
        query = f"SELECT discord_id, backloggd_username, avatar_url FROM linked_users WHERE discord_id IN ({placeholders});"
        return await self._fetchall(query, member_ids)

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
        """Insert or update a user's individual rating and status for a game."""
        query = """
            INSERT INTO user_ratings (discord_id, game_slug, rating, status, is_liked, updated_at)
            VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT(discord_id, game_slug) DO UPDATE SET
                rating = excluded.rating,
                status = excluded.status,
                is_liked = excluded.is_liked,
                updated_at = CURRENT_TIMESTAMP;
        """
        await self._execute(query, (discord_id, game_slug, rating, status, is_liked))

    async def get_server_ratings_for_game(self, guild_member_ids: List[int], game_slug: str) -> List[Any]:
        """Fetch all ratings and statuses for a game from linked guild members."""
        if not guild_member_ids:
            return []
        placeholders = ",".join("?" for _ in guild_member_ids)
        query = f"""
            SELECT u.discord_id, u.backloggd_username, u.avatar_url, r.rating, r.status, r.is_liked
            FROM user_ratings r
            JOIN linked_users u ON r.discord_id = u.discord_id
            WHERE r.discord_id IN ({placeholders}) AND r.game_slug = ?
            ORDER BY (r.rating IS NULL), r.rating DESC, u.backloggd_username ASC;
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
        """Retrieve all games logged as backlog for a user."""
        query = """
            SELECT g.slug, g.title, g.cover_url, g.release_year, g.community_rating, r.rating, r.is_liked
            FROM user_ratings r
            JOIN cached_games g ON r.game_slug = g.slug
            WHERE r.discord_id = ? AND r.status = 'backlog';
        """
        return await self._fetchall(query, (discord_id,))

    async def replace_user_library(
        self,
        discord_id: int,
        expected_username: str,
        games: Iterable[Any],
    ) -> bool:
        """Atomically replace a user's ratings if the linked account still matches."""
        from bot.ui.helpers import clean_image_url

        game_rows = [
            (
                game.slug,
                game.title,
                clean_image_url(getattr(game, "cover_url", None)),
                getattr(game, "release_year", None),
                getattr(game, "community_rating", None),
                discord_id,
                getattr(game, "rating", None),
                game.status.value if hasattr(game.status, "value") else str(game.status),
                bool(getattr(game, "is_liked", False)),
            )
            for game in games
        ]
        ownership_sql = """SELECT 1 FROM linked_users
            WHERE discord_id = ? AND LOWER(backloggd_username) = LOWER(?)"""

        async with self._operation_lock:
            if self.is_turso:
                statements = [(ownership_sql, (discord_id, expected_username))]
                statements.append(("""
                    UPDATE linked_users SET last_synced_at = CURRENT_TIMESTAMP
                    WHERE discord_id = ? AND LOWER(backloggd_username) = LOWER(?)
                """, (discord_id, expected_username)))
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
                statements.append((f"""
                    DELETE FROM user_ratings
                    WHERE discord_id = ? AND EXISTS ({ownership_sql})
                """, (discord_id, discord_id, expected_username)))
                for row in game_rows:
                    statements.append((f"""
                        INSERT INTO user_ratings
                            (discord_id, game_slug, rating, status, is_liked, updated_at)
                        SELECT ?, ?, ?, ?, ?, CURRENT_TIMESTAMP
                        WHERE EXISTS ({ownership_sql})
                        ON CONFLICT(discord_id, game_slug) DO UPDATE SET
                            rating = excluded.rating,
                            status = excluded.status,
                            is_liked = excluded.is_liked,
                            updated_at = CURRENT_TIMESTAMP
                    """, (row[5], row[0], row[6], row[7], row[8], discord_id, expected_username)))
                results = await self._client.batch(statements)
                return bool(results and results[0].rows)

            await self._conn.execute("BEGIN IMMEDIATE")
            try:
                async with self._conn.execute(ownership_sql, (discord_id, expected_username)) as cursor:
                    still_linked = await cursor.fetchone()
                if not still_linked:
                    await self._conn.rollback()
                    return False

                await self._conn.execute("""
                    UPDATE linked_users SET last_synced_at = CURRENT_TIMESTAMP
                    WHERE discord_id = ? AND LOWER(backloggd_username) = LOWER(?)
                """, (discord_id, expected_username))
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
                    "DELETE FROM user_ratings WHERE discord_id = ?;", (discord_id,)
                )
                for row in game_rows:
                    await self._conn.execute("""
                        INSERT INTO user_ratings
                            (discord_id, game_slug, rating, status, is_liked, updated_at)
                        VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                    """, (row[5], row[0], row[6], row[7], row[8]))
                await self._conn.commit()
                return True
            except BaseException:
                await self._conn.rollback()
                raise

    async def get_user_ratings_dict(self, discord_id: int) -> dict[str, Optional[float]]:
        """Return slug -> rating dictionary for similarity computation."""
        query = "SELECT game_slug, rating FROM user_ratings WHERE discord_id = ?;"
        rows = await self._fetchall(query, (discord_id,))
        return {row["game_slug"]: row["rating"] for row in rows}

    # =========================================================================
    # Server Leaderboards & Statistics
    # =========================================================================

    async def get_server_top_rated(self, guild_member_ids: List[int], limit: int = 10, min_ratings: int = 2) -> List[Any]:
        """Get top games by average rating among server members."""
        if not guild_member_ids:
            return []
        placeholders = ",".join("?" for _ in guild_member_ids)
        query = f"""
            SELECT g.slug, g.title, g.cover_url, g.release_year,
                   AVG(r.rating) as avg_rating,
                   COUNT(r.rating) as rating_count
            FROM user_ratings r
            JOIN cached_games g ON r.game_slug = g.slug
            WHERE r.discord_id IN ({placeholders}) AND r.rating IS NOT NULL
            GROUP BY g.slug
            HAVING rating_count >= ?
            ORDER BY avg_rating DESC, rating_count DESC
            LIMIT ?;
        """
        params = list(guild_member_ids) + [min_ratings, limit]
        return await self._fetchall(query, params)

    async def get_server_most_played(self, guild_member_ids: List[int], limit: int = 10) -> List[Any]:
        """Get most completed games by server members."""
        if not guild_member_ids:
            return []
        placeholders = ",".join("?" for _ in guild_member_ids)
        query = f"""
            SELECT g.slug, g.title, g.cover_url, g.release_year,
                   COUNT(r.discord_id) as play_count,
                   AVG(r.rating) as avg_rating
            FROM user_ratings r
            JOIN cached_games g ON r.game_slug = g.slug
            WHERE r.discord_id IN ({placeholders}) AND r.status = 'played'
            GROUP BY g.slug
            ORDER BY play_count DESC, avg_rating DESC
            LIMIT ?;
        """
        params = list(guild_member_ids) + [limit]
        return await self._fetchall(query, params)

    async def get_server_shame_list(self, guild_member_ids: List[int], limit: int = 10) -> List[Any]:
        """Get games most frequently sitting unplayed in server members' backlogs."""
        if not guild_member_ids:
            return []
        placeholders = ",".join("?" for _ in guild_member_ids)
        query = f"""
            SELECT g.slug, g.title, g.cover_url, g.release_year,
                   COUNT(r.discord_id) as backlog_count
            FROM user_ratings r
            JOIN cached_games g ON r.game_slug = g.slug
            WHERE r.discord_id IN ({placeholders}) AND r.status = 'backlog'
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
            FROM user_ratings r
            JOIN cached_games g ON r.game_slug = g.slug
            WHERE r.discord_id IN ({placeholders}) AND r.rating IS NOT NULL
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
            FROM user_ratings r
            JOIN cached_games g ON r.game_slug = g.slug
            WHERE r.discord_id IN ({placeholders}) AND r.rating IS NOT NULL
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
                "total_logged": 0,
                "total_played": 0,
                "server_avg_rating": None,
                "top_game": None
            }

        placeholders = ",".join("?" for _ in guild_member_ids)

        # 1. Linked members count
        link_row = await self._fetchone(
            f"SELECT COUNT(*) as count FROM linked_users WHERE discord_id IN ({placeholders});",
            guild_member_ids
        )
        linked_members = link_row["count"] if link_row else 0

        # 2. Logs, Played count, and average rating
        query_metrics = f"""
            SELECT COUNT(*) as total_logged,
                   SUM(CASE WHEN status = 'played' THEN 1 ELSE 0 END) as total_played,
                   AVG(rating) as avg_rating
            FROM user_ratings
            WHERE discord_id IN ({placeholders});
        """
        row = await self._fetchone(query_metrics, guild_member_ids)
        total_logged = (row["total_logged"] if row else 0) or 0
        total_played = (row["total_played"] if row else 0) or 0
        avg_rating = round(row["avg_rating"], 2) if row and row["avg_rating"] is not None else None

        # 3. Most popular game
        query_top_game = f"""
            SELECT g.title, COUNT(r.discord_id) as count
            FROM user_ratings r
            JOIN cached_games g ON r.game_slug = g.slug
            WHERE r.discord_id IN ({placeholders}) AND r.status = 'played'
            GROUP BY g.slug
            ORDER BY count DESC
            LIMIT 1;
        """
        top_row = await self._fetchone(query_top_game, guild_member_ids)
        top_game = f"{top_row['title']} ({top_row['count']} players)" if top_row else None

        return {
            "linked_members": linked_members,
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
        """Remove feed channel setting for a guild."""
        deleted = await self._rowcount_execute("DELETE FROM guild_settings WHERE guild_id = ?;", (guild_id,))
        return deleted > 0

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

    async def is_journal_entry_delivered(self, guild_id: int, entry_id: str) -> bool:
        """Check whether an entry was delivered to a particular guild feed."""
        row = await self._fetchone(
            "SELECT 1 FROM seen_journal_deliveries WHERE guild_id = ? AND entry_id = ?;",
            (guild_id, entry_id),
        )
        return row is not None

    async def mark_journal_entry_delivered(
        self, guild_id: int, entry_id: str, discord_id: int
    ) -> None:
        """Record a journal entry only after its guild feed send succeeds."""
        await self._execute(
            """INSERT OR IGNORE INTO seen_journal_deliveries
                (guild_id, entry_id, discord_id) VALUES (?, ?, ?);""",
            (guild_id, entry_id, discord_id),
        )

    async def seed_journal_entries_for_guild(
        self, guild_id: int, discord_id: int, entry_ids: Iterable[str]
    ) -> None:
        """Mark current journal entries as already present when a feed is enabled."""
        for entry_id in set(entry_ids):
            if entry_id:
                await self.mark_journal_entry_delivered(guild_id, entry_id, discord_id)
