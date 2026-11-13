"""Coalesced, completeness-verified Discord guild membership resolution."""

import asyncio
import logging
from typing import Optional

logger = logging.getLogger("backloggd.membership")

GUILD_MEMBERSHIP_UNAVAILABLE_MESSAGE = (
    "⚠️ Server membership is currently unavailable from Discord. Please try again shortly."
)


class GuildMembershipService:
    """Resolve complete guild member snapshots with per-guild request coalescing."""

    def __init__(self, *, chunk_timeout: float = 15.0):
        self.chunk_timeout = chunk_timeout
        self._inflight: dict[int, asyncio.Task[Optional[list]]] = {}

    @staticmethod
    def _complete_members(guild) -> Optional[list]:
        if guild is None:
            return None
        members = list(getattr(guild, "members", ()))
        if not getattr(guild, "chunked", True):
            return None
        member_count = getattr(guild, "member_count", None)
        if member_count is not None and len(members) < member_count:
            return None
        return members

    @staticmethod
    def _filter_members(members: list, exclude_member_id: Optional[int]) -> list:
        if exclude_member_id is None:
            return list(members)
        return [member for member in members if member.id != exclude_member_id]

    def _cleanup_task(self, guild_id: int, done_task: asyncio.Task) -> None:
        if self._inflight.get(guild_id) is done_task:
            self._inflight.pop(guild_id, None)

    async def _chunk_guild(self, guild) -> Optional[list]:
        guild_id = getattr(guild, "id", None)
        chunk_fn = getattr(guild, "chunk", None)
        if not callable(chunk_fn):
            logger.warning("Guild %s cache is incomplete and exposes no chunk()", guild_id)
            return None
        try:
            await asyncio.wait_for(chunk_fn(cache=True), timeout=self.chunk_timeout)
        except asyncio.CancelledError:
            raise
        except asyncio.TimeoutError:
            logger.warning(
                "Timed out after %.2fs waiting for member chunk on guild %s",
                self.chunk_timeout,
                guild_id,
            )
            return None
        except Exception as error:
            logger.warning("Could not chunk members for guild %s: %s", guild_id, error)
            return None

        complete = self._complete_members(guild)
        if complete is None:
            logger.warning(
                "Guild %s remained incomplete after chunking (chunked=%r, cached=%d, member_count=%r)",
                guild_id,
                getattr(guild, "chunked", None),
                len(getattr(guild, "members", ())),
                getattr(guild, "member_count", None),
            )
            return None
        return complete

    async def get_members(
        self,
        guild,
        *,
        exclude_member_id: Optional[int] = None,
    ) -> Optional[list]:
        """Return a complete member list for *guild*, or None when unavailable."""
        if guild is None:
            return None

        complete = self._complete_members(guild)
        if complete is not None:
            return self._filter_members(complete, exclude_member_id)

        guild_id = getattr(guild, "id", None)
        if guild_id is None:
            resolved = await self._chunk_guild(guild)
            return self._filter_members(resolved, exclude_member_id) if resolved is not None else None

        task = self._inflight.get(guild_id)
        if task is None or task.done():
            task = asyncio.create_task(self._chunk_guild(guild))
            self._inflight[guild_id] = task
            task.add_done_callback(lambda done_task, gid=guild_id: self._cleanup_task(gid, done_task))

        try:
            resolved = await asyncio.shield(task)
        except asyncio.CancelledError:
            raise
        if resolved is None:
            return None
        return self._filter_members(resolved, exclude_member_id)

    async def get_member_ids(
        self,
        guild,
        *,
        exclude_member_id: Optional[int] = None,
    ) -> Optional[list[int]]:
        """Return complete member IDs for *guild*, or None when unavailable."""
        members = await self.get_members(guild, exclude_member_id=exclude_member_id)
        if members is None:
            return None
        return [member.id for member in members]

    async def close(self) -> None:
        """Cancel any active chunk tasks and wait for clean teardown."""
        tasks = list(self._inflight.values())
        self._inflight.clear()
        for task in tasks:
            if not task.done():
                task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)


def get_membership_service(bot) -> GuildMembershipService:
    """Return the bot's shared GuildMembershipService, creating one if absent."""
    service = getattr(bot, "guild_membership", None)
    if isinstance(service, GuildMembershipService):
        return service
    service = GuildMembershipService()
    try:
        setattr(bot, "guild_membership", service)
    except Exception:
        pass
    return service


async def resolve_guild_member_ids(
    bot,
    guild,
    *,
    exclude_member_id: Optional[int] = None,
) -> Optional[list[int]]:
    """Convenience wrapper resolving complete guild member IDs via the bot service."""
    return await get_membership_service(bot).get_member_ids(
        guild, exclude_member_id=exclude_member_id
    )
