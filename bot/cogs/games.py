"""GamesCog: Handles /game with live Discord autocomplete and server score shelf."""

from collections import OrderedDict
from dataclasses import dataclass
from datetime import datetime, timezone
import time
import discord
from discord import app_commands
from discord.ext import commands
import logging
from typing import List, Optional
from uuid import uuid4
from bot.ui.embeds import create_game_embed, create_all_ratings_embed
from bot.ui.views import LinkButtonView, GameCardView, RatingsView
from bot.ui.helpers import build_backloggd_game_url
from bot.services.guild_membership import (
    GUILD_MEMBERSHIP_UNAVAILABLE_MESSAGE,
    resolve_guild_member_ids,
)
from bot.services.freshness import (
    ensure_profiles_fresh,
    format_aggregate_freshness,
    summarize_aggregate_freshness,
)

logger = logging.getLogger("backloggd.cogs.games")

# Reserved Discord autocomplete option.value tag prefixes.
# Discord limits choice names and values to 100 characters.
_DISCORD_CHOICE_LIMIT = 100
_SLUG_TAG_PREFIX = "slug:"
_FALLBACK_TAG_PREFIX = "slugref:"
_MAX_INLINE_SLUG_LEN = _DISCORD_CHOICE_LIMIT - len(_SLUG_TAG_PREFIX)
_MAX_AUTOCOMPLETE_CACHE = 500
_MAX_FALLBACK_SLUGS = 500
_FALLBACK_TTL_SECONDS = 1800.0


@dataclass(frozen=True, slots=True)
class _FallbackEntry:
    slug: str
    query: str
    created_at: float


class GamesCog(commands.Cog, name="Games"):
    def __init__(self, bot, *, clock=None, monotonic_clock=None, fallback_ttl_seconds: float = _FALLBACK_TTL_SECONDS):
        self.bot = bot
        self.clock = clock or (lambda: datetime.now(timezone.utc))
        self._monotonic = monotonic_clock or time.monotonic
        self._fallback_ttl_seconds = float(fallback_ttl_seconds)
        # Bounded caches kept independent so query-cache pruning never wipes fallback tokens.
        self._autocomplete_cache: OrderedDict[str, List[app_commands.Choice[str]]] = OrderedDict()
        self._autocomplete_slugs: OrderedDict[str, str] = OrderedDict()
        self._fallback_entries: OrderedDict[str, _FallbackEntry] = OrderedDict()

    def _prune_expired_fallbacks(self, now: float) -> Optional[dict[str, str]]:
        expired_queries: dict[str, str] = {}
        for token, entry in list(self._fallback_entries.items()):
            if now - entry.created_at > self._fallback_ttl_seconds:
                self._fallback_entries.pop(token, None)
                self._autocomplete_slugs.pop(token, None)
                expired_queries[token] = entry.query
        return expired_queries

    def _register_fallback_slug(self, slug: str, query: str) -> str:
        now = self._monotonic()
        self._prune_expired_fallbacks(now)
        while len(self._autocomplete_slugs) >= _MAX_FALLBACK_SLUGS:
            oldest_token, _ = self._autocomplete_slugs.popitem(last=False)
            self._fallback_entries.pop(oldest_token, None)
        token = f"{_FALLBACK_TAG_PREFIX}{uuid4().hex[:24]}"
        self._autocomplete_slugs[token] = slug
        self._fallback_entries[token] = _FallbackEntry(slug=slug, query=query, created_at=now)
        return token

    async def game_autocomplete(
        self,
        interaction: discord.Interaction,
        current: str
    ) -> List[app_commands.Choice[str]]:
        """Provides live search suggestions as the user types in /game."""
        clean_query = current.strip()
        if len(clean_query) < 2:
            return []

        cache_key = clean_query.lower()
        self._prune_expired_fallbacks(self._monotonic())
        if cache_key in self._autocomplete_cache:
            cached = self._autocomplete_cache[cache_key]
            if all(
                not choice.value.startswith(_FALLBACK_TAG_PREFIX)
                or choice.value in self._fallback_entries
                for choice in cached
            ):
                if isinstance(self._autocomplete_cache, OrderedDict):
                    self._autocomplete_cache.move_to_end(cache_key)
                return cached
            # TTL and capacity eviction can invalidate choices while the query
            # remains cached. A repeat query must offer resolvable selections.
            self._autocomplete_cache.pop(cache_key)

        try:
            results = await self.bot.backloggd.search_games(clean_query)
            choices = []
            for g in results[:15]:  # Discord limit is 25, 15 keeps it fast
                name = f"{g.title} ({g.release_year})" if g.release_year else g.title
                if len(name) > _DISCORD_CHOICE_LIMIT:
                    name = name[: _DISCORD_CHOICE_LIMIT - 3] + "..."
                if len(g.slug) <= _MAX_INLINE_SLUG_LEN:
                    value = f"{_SLUG_TAG_PREFIX}{g.slug}"
                else:
                    value = self._register_fallback_slug(g.slug, clean_query)
                choices.append(app_commands.Choice(name=name, value=value))

            while len(self._autocomplete_cache) >= _MAX_AUTOCOMPLETE_CACHE:
                if isinstance(self._autocomplete_cache, OrderedDict):
                    self._autocomplete_cache.popitem(last=False)
                else:
                    self._autocomplete_cache.pop(next(iter(self._autocomplete_cache)))
            self._autocomplete_cache[cache_key] = choices
            return choices
        except Exception as e:
            logger.warning("Autocomplete search error for '%s': %s", clean_query, e)
            return []

    async def _guild_freshness_note(self, member_ids: List[int]) -> Optional[str]:
        profile_getter = (
            getattr(self.bot.db, "get_guild_linked_profiles_with_sync_info", None)
            or getattr(self.bot.db, "get_guild_linked_profiles", None)
        )
        if profile_getter is None:
            return None
        profiles = await profile_getter(member_ids)
        if not profiles:
            return "No linked Backloggd profiles in cache yet."
        statuses = await ensure_profiles_fresh(
            self.bot, profiles, now=self.clock(), require_subscribed=True
        )
        return format_aggregate_freshness(summarize_aggregate_freshness(statuses.values()))

    @app_commands.command(name="game", description="Search for a game and view Backloggd ratings & server member scores")
    @app_commands.describe(title="Type game title to search with live autocomplete")
    @app_commands.autocomplete(title=game_autocomplete)
    async def game(self, interaction: discord.Interaction, title: str):
        await interaction.response.defer()

        query = title.strip()
        try:
            slug = None
            if query.startswith(_SLUG_TAG_PREFIX):
                slug = query[len(_SLUG_TAG_PREFIX):].strip()
                if not slug:
                    await interaction.followup.send(
                        "❌ Please select a game from the autocomplete list or enter a game title.",
                        ephemeral=True,
                    )
                    return
            elif query.startswith(_FALLBACK_TAG_PREFIX):
                expired = self._prune_expired_fallbacks(self._monotonic()) or {}
                entry = self._fallback_entries.get(query)
                if entry is not None:
                    slug = entry.slug
                    if isinstance(self._fallback_entries, OrderedDict):
                        self._fallback_entries.move_to_end(query)
                    if isinstance(self._autocomplete_slugs, OrderedDict) and query in self._autocomplete_slugs:
                        self._autocomplete_slugs.move_to_end(query)
                else:
                    slug = self._autocomplete_slugs.get(query)
                if not slug:
                    hint = f' for "{discord.utils.escape_markdown(expired[query])}"' if query in expired else ""
                    await interaction.followup.send(
                        f"⏳ That autocomplete selection{hint} has expired. "
                        "Please type the game title again and re-select it from the autocomplete list.",
                        ephemeral=True,
                    )
                    return
            elif query in self._autocomplete_slugs:
                slug = self._autocomplete_slugs[query]
            else:
                search_results = await self.bot.backloggd.search_games(query)
                if not search_results:
                    await interaction.followup.send(
                        f"❌ No games found matching **{query}** on Backloggd.",
                        ephemeral=True
                    )
                    return
                if len(search_results) > 1:
                    candidates = [
                        f"{game.title} ({game.release_year})" if getattr(game, "release_year", None) else game.title
                        for game in search_results[:5]
                    ]
                    options = "; ".join(candidates)
                    await interaction.followup.send(
                        f"I found several games matching **{query}**: {options}. Select the game from the autocomplete list and run `/game` again.",
                        ephemeral=True,
                    )
                    return
                slug = search_results[0].slug

            game_data = await self.bot.backloggd.get_game(slug)
            if not game_data:
                await interaction.followup.send(
                    f"❌ Could not retrieve details for game: **{slug}**.",
                    ephemeral=True
                )
                return

            # Cache the game metadata in SQLite
            await self.bot.db.upsert_game(
                slug=game_data.slug,
                title=game_data.title,
                cover_url=game_data.cover_url,
                release_year=game_data.release_year,
                community_rating=game_data.community_rating
            )

            # Retrieve scores from server members who linked their Backloggd account
            if interaction.guild:
                guild_member_ids = await resolve_guild_member_ids(self.bot, interaction.guild)
                if guild_member_ids is None:
                    await interaction.followup.send(
                        GUILD_MEMBERSHIP_UNAVAILABLE_MESSAGE,
                        ephemeral=True,
                    )
                    return
            else:
                guild_member_ids = [interaction.user.id]
            freshness_note = await self._guild_freshness_note(guild_member_ids)
            server_ratings = await self.bot.db.get_server_ratings_for_game(guild_member_ids, game_data.slug)

            guild_name = interaction.guild.name if interaction.guild else "Server"
            embed = create_game_embed(
                game_data,
                server_ratings,
                guild_name=guild_name,
                freshness_note=freshness_note,
            )
            game_url = build_backloggd_game_url(game_data.slug)

            async def on_view_all(btn_interaction: discord.Interaction):
                ratings_view = RatingsView(game_data.title, game_data.slug, guild_name, server_ratings)
                await btn_interaction.response.send_message(
                    embed=ratings_view.embed(), view=ratings_view, ephemeral=True
                )
                ratings_view.message = await btn_interaction.original_response()

            view = GameCardView(
                game_url=game_url,
                total_ratings=len(server_ratings),
                on_view_all=on_view_all if len(server_ratings) > 12 else None
            )

            view.message = await interaction.followup.send(embed=embed, view=view, wait=True)

        except Exception as e:
            logger.error("Error looking up game '%s': %s", title, e, exc_info=True)
            await interaction.followup.send(
                f"❌ An error occurred while fetching information for **{title}**.",
                ephemeral=True
            )

async def setup(bot):
    await bot.add_cog(GamesCog(bot))
