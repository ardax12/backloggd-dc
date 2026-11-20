"""Per-guild dynamic cooldown management and server moderator permission checks."""

from __future__ import annotations

import asyncio
import time
from collections import OrderedDict
from dataclasses import dataclass
from typing import Any, Callable, Literal, Optional

import discord
from discord import app_commands


@dataclass(frozen=True)
class CooldownSpec:
    command_name: str
    default_seconds: int
    min_seconds: int
    max_seconds: int
    per_guild: bool = False
    scope_label: str = "per member"


COOLDOWN_SPECS: dict[str, CooldownSpec] = {
    "refresh": CooldownSpec("refresh", 60, 10, 3600, per_guild=False, scope_label="per member"),
    "server-stats": CooldownSpec("server-stats", 30, 5, 3600, per_guild=True, scope_label="per server"),
    "compare": CooldownSpec("compare", 15, 5, 3600, per_guild=False, scope_label="per member"),
    "taste-twins": CooldownSpec("taste-twins", 15, 5, 3600, per_guild=False, scope_label="per member"),
    "recommend": CooldownSpec("recommend", 15, 5, 3600, per_guild=False, scope_label="per member"),
    "profile": CooldownSpec("profile", 10, 5, 3600, per_guild=False, scope_label="per member"),
    "server-top": CooldownSpec("server-top", 10, 5, 3600, per_guild=False, scope_label="per member"),
    "playing": CooldownSpec("playing", 10, 5, 3600, per_guild=False, scope_label="per member"),
    "game": CooldownSpec("game", 5, 3, 3600, per_guild=False, scope_label="per member"),
}

CooldownCommandName = Literal[
    "refresh",
    "server-stats",
    "compare",
    "taste-twins",
    "recommend",
    "profile",
    "server-top",
    "playing",
    "game",
]


def normalize_command_name(command_name: str) -> str:
    """Normalize and validate a cooldown-configurable command name."""
    if not isinstance(command_name, str):
        raise ValueError(f"Unknown cooldown command: {command_name!r}")
    normalized = command_name.strip().lstrip("/").lower()
    if normalized not in COOLDOWN_SPECS:
        valid = ", ".join(f"/{name}" for name in COOLDOWN_SPECS)
        raise ValueError(f"Unknown command {command_name!r}. Valid commands: {valid}.")
    return normalized


def get_cooldown_spec(command_name: str) -> CooldownSpec:
    """Return the CooldownSpec for a configurable command."""
    return COOLDOWN_SPECS[normalize_command_name(command_name)]


def validate_cooldown_seconds(command_name: str, seconds: int) -> int:
    """Validate cooldown seconds against the command's safe min/max bounds."""
    spec = get_cooldown_spec(command_name)
    if isinstance(seconds, bool) or not isinstance(seconds, int):
        raise ValueError(
            f"Cooldown for `/{spec.command_name}` must be an integer between {spec.min_seconds} and {spec.max_seconds} seconds."
        )
    if seconds < spec.min_seconds or seconds > spec.max_seconds:
        raise ValueError(
            f"Cooldown for `/{spec.command_name}` must be between {spec.min_seconds}s and {spec.max_seconds}s."
        )
    return seconds


def has_server_moderator_permissions(permissions: Any) -> bool:
    """Return True when permissions include any supported server moderator capability."""
    if permissions is None:
        return False
    return bool(
        getattr(permissions, "administrator", False)
        or getattr(permissions, "manage_guild", False)
        or getattr(permissions, "manage_channels", False)
        or getattr(permissions, "moderate_members", False)
    )


def is_server_moderator():
    """App-command check requiring Administrator, Manage Server, Manage Channels, or Moderate Members."""

    def _predicate(interaction: discord.Interaction) -> bool:
        if getattr(interaction, "guild_id", None) is None:
            raise app_commands.NoPrivateMessage("This command can only be used in a server.")
        permissions = getattr(interaction, "permissions", None)
        if permissions is None:
            permissions = getattr(getattr(interaction, "user", None), "guild_permissions", None)
        if has_server_moderator_permissions(permissions):
            return True
        raise app_commands.MissingPermissions(["manage_guild"])

    return app_commands.check(_predicate)


class GuildCooldownService:
    """Lazy per-guild cooldown cache backed by Database with generation invalidation."""

    def __init__(
        self,
        db: Optional[Any] = None,
        *,
        db_provider: Optional[Callable[[], Any]] = None,
        max_component_buckets: int = 2048,
    ):
        self._db = db
        self._db_provider = db_provider
        self._cache: dict[int, dict[str, int]] = {}
        self._generations: dict[int, int] = {}
        self._load_locks: dict[int, asyncio.Lock] = {}
        self._max_component_buckets = max(1, int(max_component_buckets))
        self._component_last_used: OrderedDict[tuple[Any, ...], float] = OrderedDict()

    def _resolve_db(self, db_override: Any = None) -> Any:
        if db_override is not None:
            return db_override
        if self._db_provider is not None:
            return self._db_provider()
        return self._db

    def _prune_component_buckets_for_guild(
        self,
        guild_id: int,
        *,
        keep_generation: Optional[int] = None,
    ) -> None:
        stale_keys = [
            key
            for key in self._component_last_used
            if len(key) >= 4
            and key[0] in ("guild", "member")
            and key[1] == guild_id
            and (keep_generation is None or key[-1] != keep_generation)
        ]
        for key in stale_keys:
            self._component_last_used.pop(key, None)

    def get_generation(self, guild_id: Optional[int]) -> int:
        if guild_id is None:
            return 0
        return self._generations.get(guild_id, 0)

    def invalidate_guild(self, guild_id: int) -> None:
        self._cache.pop(guild_id, None)
        self._generations[guild_id] = self._generations.get(guild_id, 0) + 1
        self._prune_component_buckets_for_guild(guild_id)
        lock = self._load_locks.get(guild_id)
        if lock is not None and not lock.locked():
            self._load_locks.pop(guild_id, None)

    async def _load_guild_locked(self, guild_id: int, *, db: Any = None) -> dict[str, int]:
        cached = self._cache.get(guild_id)
        if cached is not None:
            return cached

        active_db = self._resolve_db(db)
        if (
            active_db is None
            or not getattr(active_db, "is_connected", True)
            or not callable(getattr(active_db, "get_guild_command_cooldowns", None))
        ):
            return {}

        raw_overrides = await active_db.get_guild_command_cooldowns(guild_id)
        loaded: dict[str, int] = {}
        for raw_name, raw_seconds in (raw_overrides or {}).items():
            try:
                name = normalize_command_name(raw_name)
                seconds = validate_cooldown_seconds(name, int(raw_seconds))
            except (TypeError, ValueError):
                continue
            loaded[name] = seconds
        self._cache[guild_id] = loaded
        return loaded

    async def _ensure_guild_loaded(self, guild_id: int, *, db: Any = None) -> dict[str, int]:
        cached = self._cache.get(guild_id)
        if cached is not None:
            return cached

        lock = self._load_locks.setdefault(guild_id, asyncio.Lock())
        async with lock:
            return await self._load_guild_locked(guild_id, db=db)

    async def get_cooldown_seconds(
        self,
        guild_id: Optional[int],
        command_name: str,
        *,
        db: Any = None,
    ) -> int:
        spec = get_cooldown_spec(command_name)
        if guild_id is None:
            return spec.default_seconds
        overrides = await self._ensure_guild_loaded(guild_id, db=db)
        return overrides.get(spec.command_name, spec.default_seconds)

    async def get_guild_overrides(
        self, guild_id: int, *, db: Any = None
    ) -> dict[str, int]:
        overrides = await self._ensure_guild_loaded(guild_id, db=db)
        return dict(overrides)

    async def get_all_guild_cooldowns(
        self, guild_id: int, *, db: Any = None
    ) -> dict[str, int]:
        overrides = await self._ensure_guild_loaded(guild_id, db=db)
        return {
            name: overrides.get(name, spec.default_seconds)
            for name, spec in COOLDOWN_SPECS.items()
        }

    async def set_cooldown(
        self,
        guild_id: int,
        command_name: str,
        seconds: int,
        *,
        db: Any = None,
    ) -> int:
        spec = get_cooldown_spec(command_name)
        validated = validate_cooldown_seconds(spec.command_name, seconds)
        active_db = self._resolve_db(db)
        if active_db is None or not getattr(active_db, "is_connected", True):
            raise RuntimeError("Database is unavailable")

        lock = self._load_locks.setdefault(guild_id, asyncio.Lock())
        async with lock:
            # Load existing guild overrides under the lock so caching stays complete and serialized.
            await self._load_guild_locked(guild_id, db=active_db)
            await active_db.set_guild_command_cooldown(guild_id, spec.command_name, validated)
            self._cache.setdefault(guild_id, {})[spec.command_name] = validated
            new_gen = self._generations.get(guild_id, 0) + 1
            self._generations[guild_id] = new_gen
            self._prune_component_buckets_for_guild(guild_id, keep_generation=new_gen)
        return validated

    async def reset_cooldown(
        self,
        guild_id: int,
        command_name: Optional[str] = None,
        *,
        db: Any = None,
    ) -> int:
        target_name = normalize_command_name(command_name) if command_name is not None else None
        active_db = self._resolve_db(db)
        if active_db is None or not getattr(active_db, "is_connected", True):
            raise RuntimeError("Database is unavailable")

        lock = self._load_locks.setdefault(guild_id, asyncio.Lock())
        async with lock:
            removed = await active_db.reset_guild_command_cooldown(guild_id, target_name)
            if guild_id in self._cache:
                if target_name is None:
                    self._cache[guild_id].clear()
                else:
                    self._cache[guild_id].pop(target_name, None)
            new_gen = self._generations.get(guild_id, 0) + 1
            self._generations[guild_id] = new_gen
            self._prune_component_buckets_for_guild(guild_id, keep_generation=new_gen)
        return int(removed)

    async def check_component_cooldown(
        self,
        interaction: discord.Interaction,
        command_name: str,
        *,
        db: Any = None,
    ) -> Optional[float]:
        """Enforce a command's per-guild cooldown for interactive components.

        Returns remaining seconds when rate-limited, or None when allowed.
        """
        spec = get_cooldown_spec(command_name)
        guild_id = getattr(interaction, "guild_id", None)
        user_id = getattr(getattr(interaction, "user", None), "id", 0)
        seconds = await self.get_cooldown_seconds(guild_id, spec.command_name, db=db)
        generation = self.get_generation(guild_id)

        created_at = getattr(interaction, "created_at", None)
        now_ts = created_at.timestamp() if created_at is not None else time.monotonic()

        bucket_map = self._component_last_used

        if guild_id is None:
            key: tuple[Any, ...] = ("dm", user_id, spec.command_name, 0)
        elif spec.per_guild:
            key = ("guild", guild_id, spec.command_name, generation)
        else:
            key = ("member", guild_id, user_id, spec.command_name, generation)

        last_ts = bucket_map.get(key)
        if last_ts is not None:
            elapsed = now_ts - last_ts
            if 0 <= elapsed < seconds:
                bucket_map.move_to_end(key)
                return float(seconds - elapsed)

        if key not in bucket_map and len(bucket_map) >= self._max_component_buckets:
            cutoff = now_ts - 3600.0
            for stale_key in [k for k, ts in bucket_map.items() if ts < cutoff]:
                bucket_map.pop(stale_key, None)
            while len(bucket_map) >= self._max_component_buckets:
                bucket_map.popitem(last=False)

        bucket_map[key] = now_ts
        bucket_map.move_to_end(key)
        return None


_DEFAULT_COOLDOWN_SERVICE = GuildCooldownService()


def _resolve_cooldown_service(interaction: discord.Interaction) -> GuildCooldownService:
    client = getattr(interaction, "client", None)
    service = getattr(client, "cooldowns", None)
    if isinstance(service, GuildCooldownService):
        return service
    return _DEFAULT_COOLDOWN_SERVICE


def guild_cooldown(command_name: str):
    """Attach a per-guild dynamic cooldown check to an application command."""
    spec = get_cooldown_spec(command_name)

    async def _key(interaction: discord.Interaction):
        service = _resolve_cooldown_service(interaction)
        guild_id = getattr(interaction, "guild_id", None)
        user_id = getattr(getattr(interaction, "user", None), "id", 0)
        if guild_id is None:
            return (id(service), "dm", user_id, 0)
        db = getattr(getattr(interaction, "client", None), "db", None)
        await service.get_cooldown_seconds(guild_id, spec.command_name, db=db)
        generation = service.get_generation(guild_id)
        if spec.per_guild:
            return (id(service), "guild", guild_id, generation)
        return (id(service), "member", guild_id, user_id, generation)

    async def _factory(interaction: discord.Interaction) -> app_commands.Cooldown:
        service = _resolve_cooldown_service(interaction)
        guild_id = getattr(interaction, "guild_id", None)
        db = getattr(getattr(interaction, "client", None), "db", None)
        seconds = await service.get_cooldown_seconds(guild_id, spec.command_name, db=db)
        return app_commands.Cooldown(1, float(seconds))

    return app_commands.checks.dynamic_cooldown(_factory, key=_key)
