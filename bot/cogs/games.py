"""GamesCog: Handles /game with live Discord autocomplete and server score shelf."""

import asyncio
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
from bot.ui.helpers import build_backloggd_game_url, clamp_escaped_text
from bot.input_validation import escape_display_text
from bot.services.guild_membership import (
    GUILD_MEMBERSHIP_UNAVAILABLE_MESSAGE,
    resolve_guild_member_ids,
)
from bot.command_errors import defer_interaction, send_private_followup, send_public_followup
from bot.services.freshness import resolve_guild_aggregate_freshness

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
_DEFAULT_AUTOCOMPLETE_TIMEOUT_SECONDS = 2.0
_MAX_DISPLAY_EXCERPT_LEN = 120


def _format_display_excerpt(value: str, *, max_raw_len: int = _MAX_DISPLAY_EXCERPT_LEN) -> str:
    """Normalize whitespace, bound length, and escape Markdown/mentions once for display."""
    collapsed = " ".join(str(value).split())
    if len(collapsed) > max_raw_len:
        collapsed = collapsed[: max(1, max_raw_len - 3)].rstrip() + "..."
    return escape_display_text(collapsed)


@dataclass(frozen=True, slots=True)
class _FallbackEntry:
    slug: str
    query: str
    created_at: float


class GamesCog(commands.Cog, name="Games"):
    def __init__(
        self,
        bot,
        *,
        clock=None,
        monotonic_clock=None,
        fallback_ttl_seconds: float = _FALLBACK_TTL_SECONDS,
        autocomplete_timeout_seconds: float = _DEFAULT_AUTOCOMPLETE_TIMEOUT_SECONDS,
    ):
        self.bot = bot
        self.clock = clock or (lambda: datetime.now(timezone.utc))
        self._monotonic = monotonic_clock or time.monotonic
        self._fallback_ttl_seconds = float(fallback_ttl_seconds)
        self._autocomplete_timeout_seconds = float(autocomplete_timeout_seconds)
        if self._autocomplete_timeout_seconds <= 0:
            raise ValueError("autocomplete_timeout_seconds must be positive")
        # Bounded caches kept independent so query-cache pruning never wipes fallback tokens.
        self._autocomplete_cache: OrderedDict[str, List[app_commands.Choice[str]]] = OrderedDict()
        self._fallback_entries: OrderedDict[str, _FallbackEntry] = OrderedDict()

    def _prune_expired_fallbacks(self, now: float) -> Optional[dict[str, str]]:
        expired_queries: dict[str, str] = {}
        for token, entry in list(self._fallback_entries.items()):
            if now - entry.created_at > self._fallback_ttl_seconds:
                self._fallback_entries.pop(token, None)
                expired_queries[token] = entry.query
        return expired_queries

    def _register_fallback_slug(self, slug: str, query: str) -> str:
        now = self._monotonic()
        self._prune_expired_fallbacks(now)
        while len(self._fallback_entries) >= _MAX_FALLBACK_SLUGS:
            self._fallback_entries.popitem(last=False)
        token = f"{_FALLBACK_TAG_PREFIX}{uuid4().hex[:24]}"
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
            results = await asyncio.wait_for(
                self.bot.backloggd.search_games(clean_query),
                timeout=self._autocomplete_timeout_seconds,
            )
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
        except asyncio.CancelledError:
            raise
        except asyncio.TimeoutError:
            logger.warning(
                "Autocomplete search timed out after %.2fs for '%s'",
                self._autocomplete_timeout_seconds,
                clean_query,
            )
            return []
        except Exception as e:
            logger.warning("Autocomplete search error for '%s': %s", clean_query, e)
            return []

    @app_commands.command(name="game", description="Search for a game and view Backloggd ratings & server member scores")
    @app_commands.describe(title="Type game title to search with live autocomplete")
    @app_commands.autocomplete(title=game_autocomplete)
    async def game(self, interaction: discord.Interaction, title: str):
        await defer_interaction(interaction)

        query = title.strip()
        try:
            slug = None
            if query.startswith(_SLUG_TAG_PREFIX):
                slug = query[len(_SLUG_TAG_PREFIX):].strip()
                if not slug:
                    await send_private_followup(
                        interaction,
                        "❌ Please select a game from the autocomplete list or enter a game title.",
                        log=logger,
                    )
                    return
            elif query.startswith(_FALLBACK_TAG_PREFIX):
                expired = self._prune_expired_fallbacks(self._monotonic()) or {}
                entry = self._fallback_entries.get(query)
                if entry is not None:
                    slug = entry.slug
                    if isinstance(self._fallback_entries, OrderedDict):
                        self._fallback_entries.move_to_end(query)
                if not slug:
                    hint = f' for "{_format_display_excerpt(expired[query])}"' if query in expired else ""
                    await send_private_followup(
                        interaction,
                        clamp_escaped_text(
                            (
                                f"⏳ That autocomplete selection{hint} has expired. "
                                "Please type the game title again and re-select it from the autocomplete list."
                            ),
                            2000,
                        ),
                        log=logger,
                    )
                    return
            else:
                search_results = await self.bot.backloggd.search_games(query)
                if not search_results:
                    safe_query = _format_display_excerpt(query)
                    await send_private_followup(
                        interaction,
                        clamp_escaped_text(
                            f"❌ No games found matching **{safe_query}** on Backloggd.",
                            2000,
                        ),
                        log=logger,
                    )
                    return
                if len(search_results) > 1:
                    safe_query = _format_display_excerpt(query)
                    candidates = [
                        _format_display_excerpt(
                            f"{game.title} ({game.release_year})"
                            if getattr(game, "release_year", None)
                            else game.title
                        )
                        for game in search_results[:5]
                    ]
                    options = "; ".join(candidates)
                    suffix = ". Select the game from the autocomplete list and run `/game` again."
                    prefix = f"I found several games matching **{safe_query}**: "
                    max_opts_len = 2000 - len(prefix) - len(suffix)
                    if len(options) > max_opts_len:
                        options = clamp_escaped_text(options, max(3, max_opts_len))
                    await send_private_followup(
                        interaction,
                        clamp_escaped_text(f"{prefix}{options}{suffix}", 2000),
                        log=logger,
                    )
                    return
                slug = search_results[0].slug

            game_data = await self.bot.backloggd.get_game(slug)
            if not game_data:
                safe_slug = _format_display_excerpt(slug)
                await send_private_followup(
                    interaction,
                    clamp_escaped_text(
                        f"❌ Could not retrieve details for game: **{safe_slug}**.",
                        2000,
                    ),
                    log=logger,
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
                    await send_private_followup(
                        interaction,
                        GUILD_MEMBERSHIP_UNAVAILABLE_MESSAGE,
                        log=logger,
                    )
                    return
            else:
                guild_member_ids = [interaction.user.id]
            freshness_note = await resolve_guild_aggregate_freshness(
                self.bot,
                guild_member_ids,
                now=self.clock(),
                require_subscribed=True,
            )
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

            view.message = await send_public_followup(interaction, embed=embed, view=view, wait=True)

        except Exception as e:
            logger.error("Error looking up game '%s': %s", title, e, exc_info=True)
            safe_title = _format_display_excerpt(query or title)
            await send_private_followup(
                interaction,
                clamp_escaped_text(
                    f"❌ An error occurred while fetching information for **{safe_title}**.",
                    2000,
                ),
                log=logger,
            )

async def setup(bot):
    await bot.add_cog(GamesCog(bot))
