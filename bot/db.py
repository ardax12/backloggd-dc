"""Asynchronous SQLite Database Manager for Backloggd Discord Bot."""

import aiosqlite
import logging
from typing import Optional, List, Tuple
from pathlib import Path

logger = logging.getLogger("backloggd.db")

class Database:
    def __init__(self, db_path: str):
        self.db_path = db_path
        self._conn: Optional[aiosqlite.Connection] = None

    async def connect(self) -> None:
        """Establish connection and create database tables if they do not exist."""
        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
        self._conn = await aiosqlite.connect(self.db_path)
        self._conn.row_factory = aiosqlite.Row
        await self._conn.execute("PRAGMA foreign_keys = ON;")
        await self._create_tables()
        logger.info("Connected to SQLite database at %s", self.db_path)

    @property
    def is_connected(self) -> bool:
        """Check if database connection is active."""
        return self._conn is not None and getattr(self._conn, "_running", False)

    async def close(self) -> None:
        """Close SQLite connection."""
        if self._conn:
            await self._conn.close()
            self._conn = None
            logger.info("Closed SQLite connection.")

    async def _create_tables(self) -> None:
        """Initialize all required schemas and indexes."""
        async with self._conn.cursor() as cursor:
            # 1. Linked users
            await cursor.execute("""
                CREATE TABLE IF NOT EXISTS linked_users (
                    discord_id INTEGER PRIMARY KEY,
                    backloggd_username TEXT NOT NULL UNIQUE,
                    avatar_url TEXT,
                    last_synced_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """)

            # 2. Cached games
            await cursor.execute("""
                CREATE TABLE IF NOT EXISTS cached_games (
                    slug TEXT PRIMARY KEY,
                    title TEXT NOT NULL,
                    cover_url TEXT,
                    release_year INTEGER,
                    community_rating REAL
                );
            """)

            # 3. User ratings and status logs
            await cursor.execute("""
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
            await cursor.execute("CREATE INDEX IF NOT EXISTS idx_ratings_slug ON user_ratings(game_slug);")
            await cursor.execute("CREATE INDEX IF NOT EXISTS idx_ratings_user ON user_ratings(discord_id);")
            await cursor.execute("CREATE INDEX IF NOT EXISTS idx_ratings_status ON user_ratings(status);")

            # 4. Guild settings for automatic feed
            await cursor.execute("""
                CREATE TABLE IF NOT EXISTS guild_settings (
                    guild_id INTEGER PRIMARY KEY,
                    feed_channel_id INTEGER NOT NULL
                );
            """)

            # 5. Seen journal logs deduplication
            await cursor.execute("""
                CREATE TABLE IF NOT EXISTS seen_journal_logs (
                    entry_id TEXT PRIMARY KEY,
                    discord_id INTEGER NOT NULL,
                    posted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """)

            await self._conn.commit()

    # =========================================================================
    # User Account Linking
    # =========================================================================

    async def link_user(self, discord_id: int, username: str, avatar_url: Optional[str] = None) -> None:
        """Insert or update linked Backloggd username for a Discord user."""
        query = """
            INSERT INTO linked_users (discord_id, backloggd_username, avatar_url, last_synced_at)
            VALUES (?, ?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT(discord_id) DO UPDATE SET
                backloggd_username = excluded.backloggd_username,
                avatar_url = COALESCE(excluded.avatar_url, linked_users.avatar_url),
                last_synced_at = CURRENT_TIMESTAMP;
        """
        await self._conn.execute(query, (discord_id, username, avatar_url))
        await self._conn.commit()

    async def unlink_user(self, discord_id: int) -> bool:
        """Remove a linked user and cascade-delete their cached ratings."""
        async with self._conn.cursor() as cursor:
            await cursor.execute("DELETE FROM linked_users WHERE discord_id = ?;", (discord_id,))
            await self._conn.commit()
            return cursor.rowcount > 0

    async def get_linked_user(self, discord_id: int) -> Optional[aiosqlite.Row]:
        """Fetch linked user record by Discord ID."""
        async with self._conn.execute(
            "SELECT discord_id, backloggd_username, avatar_url, last_synced_at FROM linked_users WHERE discord_id = ?;",
            (discord_id,)
        ) as cursor:
            return await cursor.fetchone()

    async def get_linked_user_by_username(self, username: str) -> Optional[aiosqlite.Row]:
        """Fetch linked user record by Backloggd username (case-insensitive)."""
        async with self._conn.execute(
            "SELECT discord_id, backloggd_username, avatar_url, last_synced_at FROM linked_users WHERE LOWER(backloggd_username) = LOWER(?);",
            (username.strip(),)
        ) as cursor:
            return await cursor.fetchone()

    async def get_all_linked_users(self) -> List[aiosqlite.Row]:
        """Fetch all linked users."""
        async with self._conn.execute("SELECT discord_id, backloggd_username, avatar_url FROM linked_users;") as cursor:
            return await cursor.fetchall()

    async def get_guild_linked_users(self, member_ids: List[int]) -> List[aiosqlite.Row]:
        """Fetch all linked users present in a specific guild."""
        if not member_ids:
            return []
        placeholders = ",".join("?" for _ in member_ids)
        query = f"SELECT discord_id, backloggd_username, avatar_url FROM linked_users WHERE discord_id IN ({placeholders});"
        async with self._conn.execute(query, member_ids) as cursor:
            return await cursor.fetchall()

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
        query = """
            INSERT INTO cached_games (slug, title, cover_url, release_year, community_rating)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(slug) DO UPDATE SET
                title = excluded.title,
                cover_url = COALESCE(excluded.cover_url, cached_games.cover_url),
                release_year = COALESCE(excluded.release_year, cached_games.release_year),
                community_rating = COALESCE(excluded.community_rating, cached_games.community_rating);
        """
        await self._conn.execute(query, (slug, title, cover_url, release_year, community_rating))
        await self._conn.commit()

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
        await self._conn.execute(query, (discord_id, game_slug, rating, status, is_liked))
        await self._conn.commit()

    async def get_server_ratings_for_game(self, guild_member_ids: List[int], game_slug: str) -> List[aiosqlite.Row]:
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
        async with self._conn.execute(query, params) as cursor:
            return await cursor.fetchall()

    async def get_user_stats(self, discord_id: int) -> dict:
        """Fetch user's aggregate stats from cached ratings."""
        query = """
            SELECT COUNT(*) as total_games,
                   COUNT(rating) as rated_count,
                   AVG(rating) as average_rating
            FROM user_ratings
            WHERE discord_id = ?;
        """
        async with self._conn.execute(query, (discord_id,)) as cursor:
            row = await cursor.fetchone()
            if not row or not row["total_games"]:
                return {"total_games": 0, "rated_count": 0, "average_rating": None}
            return {
                "total_games": row["total_games"],
                "rated_count": row["rated_count"],
                "average_rating": round(row["average_rating"], 2) if row["average_rating"] is not None else None
            }

    async def get_user_recent_ratings(self, discord_id: int, limit: int = 5) -> List[aiosqlite.Row]:
        """Fetch user's most recently logged ratings from SQLite."""
        query = """
            SELECT g.title as game_title, g.slug as game_slug, r.rating, r.status, r.updated_at
            FROM user_ratings r
            JOIN cached_games g ON r.game_slug = g.slug
            WHERE r.discord_id = ?
            ORDER BY r.updated_at DESC
            LIMIT ?;
        """
        async with self._conn.execute(query, (discord_id, limit)) as cursor:
            return await cursor.fetchall()

    async def get_user_backlog(self, discord_id: int) -> List[aiosqlite.Row]:
        """Retrieve all games logged as backlog for a user."""
        query = """
            SELECT g.slug, g.title, g.cover_url, g.release_year, g.community_rating, r.rating, r.is_liked
            FROM user_ratings r
            JOIN cached_games g ON r.game_slug = g.slug
            WHERE r.discord_id = ? AND r.status = 'backlog';
        """
        async with self._conn.execute(query, (discord_id,)) as cursor:
            return await cursor.fetchall()

    async def get_user_ratings_dict(self, discord_id: int) -> dict[str, Optional[float]]:
        """Return slug -> rating dictionary for similarity computation."""
        query = "SELECT game_slug, rating FROM user_ratings WHERE discord_id = ?;"
        async with self._conn.execute(query, (discord_id,)) as cursor:
            rows = await cursor.fetchall()
            return {row["game_slug"]: row["rating"] for row in rows}

    # =========================================================================
    # Server Leaderboards & Statistics
    # =========================================================================

    async def get_server_top_rated(self, guild_member_ids: List[int], limit: int = 10, min_ratings: int = 2) -> List[aiosqlite.Row]:
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
        async with self._conn.execute(query, params) as cursor:
            return await cursor.fetchall()

    async def get_server_most_played(self, guild_member_ids: List[int], limit: int = 10) -> List[aiosqlite.Row]:
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
        async with self._conn.execute(query, params) as cursor:
            return await cursor.fetchall()

    async def get_server_shame_list(self, guild_member_ids: List[int], limit: int = 10) -> List[aiosqlite.Row]:
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
        async with self._conn.execute(query, params) as cursor:
            return await cursor.fetchall()

    async def get_server_polarizing(self, guild_member_ids: List[int], limit: int = 10, min_ratings: int = 2) -> List[aiosqlite.Row]:
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
        async with self._conn.execute(query, params) as cursor:
            return await cursor.fetchall()

    async def get_server_hidden_gems(self, guild_member_ids: List[int], limit: int = 10) -> List[aiosqlite.Row]:
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
        async with self._conn.execute(query, params) as cursor:
            return await cursor.fetchall()

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
        async with self._conn.execute(
            f"SELECT COUNT(*) as count FROM linked_users WHERE discord_id IN ({placeholders});",
            guild_member_ids
        ) as cursor:
            linked_members = (await cursor.fetchone())["count"]

        # 2. Logs, Played count, and average rating
        query_metrics = f"""
            SELECT COUNT(*) as total_logged,
                   SUM(CASE WHEN status = 'played' THEN 1 ELSE 0 END) as total_played,
                   AVG(rating) as avg_rating
            FROM user_ratings
            WHERE discord_id IN ({placeholders});
        """
        async with self._conn.execute(query_metrics, guild_member_ids) as cursor:
            row = await cursor.fetchone()
            total_logged = row["total_logged"] or 0
            total_played = row["total_played"] or 0
            avg_rating = round(row["avg_rating"], 2) if row["avg_rating"] is not None else None

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
        async with self._conn.execute(query_top_game, guild_member_ids) as cursor:
            top_row = await cursor.fetchone()
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
        await self._conn.execute(query, (guild_id, channel_id))
        await self._conn.commit()

    async def unset_feed_channel(self, guild_id: int) -> bool:
        """Remove feed channel setting for a guild."""
        async with self._conn.cursor() as cursor:
            await cursor.execute("DELETE FROM guild_settings WHERE guild_id = ?;", (guild_id,))
            await self._conn.commit()
            return cursor.rowcount > 0

    async def get_feed_channel(self, guild_id: int) -> Optional[int]:
        """Get designated feed channel ID for a guild."""
        async with self._conn.execute("SELECT feed_channel_id FROM guild_settings WHERE guild_id = ?;", (guild_id,)) as cursor:
            row = await cursor.fetchone()
            return row["feed_channel_id"] if row else None

    async def get_all_guild_settings(self) -> List[aiosqlite.Row]:
        """Retrieve all guilds with a configured feed channel."""
        async with self._conn.execute("SELECT guild_id, feed_channel_id FROM guild_settings;") as cursor:
            return await cursor.fetchall()

    async def is_journal_entry_seen(self, entry_id: str) -> bool:
        """Check if a playthrough journal entry was already recorded/posted."""
        async with self._conn.execute("SELECT 1 FROM seen_journal_logs WHERE entry_id = ?;", (entry_id,)) as cursor:
            return (await cursor.fetchone()) is not None

    async def mark_journal_entry_seen(self, entry_id: str, discord_id: int) -> None:
        """Record a journal entry as seen to prevent duplicate posts."""
        await self._conn.execute(
            "INSERT OR IGNORE INTO seen_journal_logs (entry_id, discord_id) VALUES (?, ?);",
            (entry_id, discord_id)
        )
        await self._conn.commit()
