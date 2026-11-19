"""DiscoveryCog: Handles profile-based recommendations and an unplayed-game roulette."""

import asyncio
import logging
import random
from datetime import datetime, timezone
from typing import Literal, Optional

import discord
from backloggd.analytics import calculate_similarity_raw
from discord import app_commands
from discord.ext import commands

from bot.command_errors import defer_interaction, send_private_followup, send_public_followup
from bot.config import COLOR_PLAYED
from bot.input_validation import escape_display_text
from bot.recommendation_rules import (
    compute_taste_compatibility,
    eligible_pool_slugs,
    excluded_recommendation_slugs,
    has_uncertain_library_exclusion,
    numeric_ratings,
    qualifying_high_rated_games,
    rank_taste_matches,
    resolve_display_title,
    select_diverse_recommendations,
    select_friend_recommendations,
)
from bot.services.cooldowns import guild_cooldown
from bot.services.freshness import (
    ensure_profiles_fresh,
    format_aggregate_freshness,
    format_fetching_library_message,
    format_profile_freshness_line,
    summarize_aggregate_freshness,
)
from bot.services.guild_membership import (
    GUILD_MEMBERSHIP_UNAVAILABLE_MESSAGE,
    resolve_guild_member_ids,
)
from bot.ui.embeds import append_freshness_note, create_roulette_embed
from bot.ui.helpers import (
    build_backloggd_game_url,
    build_backloggd_user_url,
    clamp_escaped_text,
    rating_to_stars,
)
from bot.ui.views import RouletteView

logger = logging.getLogger("backloggd.cogs.discovery")

RoulettePool = Literal["Both", "Backlog", "Wishlist"]
RecommendPool = Literal["Any Unplayed", "Backlog", "Wishlist", "Backlog & Wishlist"]

_ROULETTE_POOLS: dict[str, tuple[str, str, str]] = {
    "both": ("both", "Both", "`Backlog` or `Wishlist`"),
    "backlog": ("backlog", "Backlog", "`Backlog`"),
    "wishlist": ("wishlist", "Wishlist", "`Wishlist`"),
}

_RECOMMEND_POOLS: dict[str, tuple[Optional[frozenset[str]], str, str]] = {
    "any unplayed": (None, "Any Unplayed", ""),
    "backlog": (frozenset({"backlog"}), "Backlog", "`Backlog`"),
    "wishlist": (frozenset({"wishlist"}), "Wishlist", "`Wishlist`"),
    "backlog & wishlist": (
        frozenset({"backlog", "wishlist"}),
        "Backlog & Wishlist",
        "`Backlog` or `Wishlist`",
    ),
}


class DiscoveryCog(commands.Cog, name="Discovery"):
    def __init__(self, bot, *, clock=None):
        self.bot = bot
        self.clock = clock or (lambda: datetime.now(timezone.utc))

    @staticmethod
    def _linked_profile_key(linked) -> str:
        if isinstance(linked, dict) and "profile_key" in linked:
            return str(linked["profile_key"]).strip().lower()
        if hasattr(linked, "keys") and "profile_key" in linked.keys():
            return str(linked["profile_key"]).strip().lower()
        return str(linked["backloggd_username"]).strip().lower()

    # =========================================================================
    # /roulette (Interactive Backlog Decision Picker)
    # =========================================================================

    @app_commands.command(name="roulette", description="Pick an unrated game from your Backlog or Wishlist")
    @app_commands.describe(
        user="Optional: Server member to spin the roulette for (defaults to you)",
        pool="Optional: Choose Backlog, Wishlist, or Both (defaults to Both)",
    )
    async def roulette(
        self,
        interaction: discord.Interaction,
        user: Optional[discord.Member] = None,
        pool: RoulettePool = "Both",
    ):
        pool_spec = _ROULETTE_POOLS.get(pool.strip().lower()) if isinstance(pool, str) else None
        if pool_spec is None:
            raise ValueError(f"Unsupported roulette pool: {pool!r}")
        pool_key, pool_label, pool_empty_label = pool_spec

        await defer_interaction(interaction)

        target_discord_id = user.id if user else interaction.user.id
        linked = await self.bot.db.get_linked_user(target_discord_id)

        if not linked:
            if user:
                safe_name = escape_display_text(user.display_name)
                msg = f"❌ **{safe_name}** does not have a linked Backloggd account yet."
            else:
                msg = "❌ You do not have a linked Backloggd account! Use `/link <username>` first."
            await send_private_followup(interaction, msg, log=logger)
            return

        username = linked["backloggd_username"]
        profile_key = self._linked_profile_key(linked)
        statuses = await ensure_profiles_fresh(
            self.bot,
            [(profile_key, username)],
            now=self.clock(),
            require_subscribed=True,
        )
        status = statuses.get(profile_key)

        backlog_games = await self.bot.db.get_user_backlog(target_discord_id, pool=pool_key)

        if not backlog_games:
            if status is not None and status.missing:
                await send_private_followup(
                    interaction,
                    format_fetching_library_message(username, status),
                    log=logger,
                )
                return
            safe_username = escape_display_text(username)
            await send_private_followup(
                interaction,
                f"ℹ️ **{safe_username}** has no unrated games in {pool_empty_label} in the local cache. Try `/refresh` to sync your latest library.",
                log=logger,
            )
            return

        chosen_game = random.choice(backlog_games)
        remaining = len(backlog_games)
        freshness_note = format_profile_freshness_line(status)
        reroll_lock = asyncio.Lock()
        seen_slugs = {chosen_game["slug"]}
        active_state = {"view": None, "generation": 0, "last_slug": chosen_game["slug"]}

        def _pick_next_game() -> tuple[dict, bool]:
            if len(backlog_games) <= 1:
                return random.choice(backlog_games), False
            unseen = [g for g in backlog_games if g["slug"] not in seen_slugs]
            if unseen:
                return random.choice(unseen), False
            candidates = [g for g in backlog_games if g["slug"] != active_state["last_slug"]]
            if not candidates:
                candidates = backlog_games
            return random.choice(candidates), True

        def _build_roulette_view(choice, *, generation: int) -> RouletteView:
            built_view = None

            async def on_reroll(btn_interaction: discord.Interaction):
                if not btn_interaction.response.is_done():
                    await defer_interaction(btn_interaction)
                async with reroll_lock:
                    if built_view is not active_state["view"] or generation != active_state["generation"]:
                        return
                    new_choice, reset_cycle = _pick_next_game()
                    next_generation = generation + 1
                    new_embed = create_roulette_embed(
                        username,
                        new_choice,
                        remaining,
                        freshness_note=freshness_note,
                        pool=pool_label,
                    )
                    new_view = _build_roulette_view(new_choice, generation=next_generation)
                    previous_view = active_state["view"]
                    parent_message = (
                        previous_view.message
                        if previous_view is not None and previous_view.message is not None
                        else btn_interaction.message
                    )
                    try:
                        edited_message = await btn_interaction.edit_original_response(
                            embed=new_embed, view=new_view
                        )
                    except BaseException:
                        new_view.retire()
                        raise
                    if reset_cycle:
                        seen_slugs.clear()
                    seen_slugs.add(new_choice["slug"])
                    active_state["last_slug"] = new_choice["slug"]
                    new_view.message = edited_message or parent_message
                    active_state["view"] = new_view
                    active_state["generation"] = next_generation
                    if previous_view is not None and previous_view is not new_view:
                        previous_view.retire()

            built_view = RouletteView(
                author_id=interaction.user.id,
                game_url=build_backloggd_game_url(choice["slug"]),
                on_reroll=on_reroll,
            )
            return built_view

        embed = create_roulette_embed(
            username,
            chosen_game,
            remaining,
            freshness_note=freshness_note,
            pool=pool_label,
        )
        view = _build_roulette_view(chosen_game, generation=0)
        active_state["view"] = view
        view.message = await send_public_followup(interaction, embed=embed, view=view, wait=True)

    # =========================================================================
    # /recommend (Taste-Matched Server Recommendations)
    # =========================================================================

    @app_commands.command(
        name="recommend",
        description="Get taste-matched recommendations or recommend your top games to a friend"
    )
    @app_commands.describe(
        user="Optional server member to recommend your games to (defaults to recommendations for you)",
        pool="Optional: Filter to Backlog, Wishlist, or Backlog & Wishlist (defaults to Any Unplayed)",
    )
    @app_commands.guild_only()
    @guild_cooldown("recommend")
    async def recommend(
        self,
        interaction: discord.Interaction,
        user: Optional[discord.Member] = None,
        pool: RecommendPool = "Any Unplayed",
    ):
        pool_spec = _RECOMMEND_POOLS.get(pool.strip().lower()) if isinstance(pool, str) else None
        if pool_spec is None:
            raise ValueError(f"Unsupported recommendation pool: {pool!r}")
        allowed_pool_statuses, pool_label, pool_empty_label = pool_spec

        await defer_interaction(interaction)

        if not interaction.guild:
            await send_private_followup(interaction, "❌ This command must be run inside a Discord server.", log=logger)
            return

        caller_discord_id = interaction.user.id
        caller_link = await self.bot.db.get_linked_user(caller_discord_id)
        now = self.clock()

        # =====================================================================
        # MODE 2: Recommend caller's games to a specific friend (/recommend @Friend)
        # =====================================================================
        if user and user.id != caller_discord_id:
            if not caller_link:
                await send_private_followup(
                    interaction,
                    "❌ You haven't linked your Backloggd account yet! Use `/link <username>` to recommend your games to others.",
                    log=logger,
                )
                return

            caller_username = caller_link["backloggd_username"]
            caller_key = self._linked_profile_key(caller_link)
            friend_discord_id = user.id
            friend_link = await self.bot.db.get_linked_user(friend_discord_id)
            if friend_link and self._linked_profile_key(friend_link) == caller_key:
                await send_private_followup(
                    interaction,
                    "ℹ️ Both Discord members link to the same Backloggd profile, so there is no separate profile to recommend to.",
                    log=logger,
                )
                return

            if allowed_pool_statuses is not None and not friend_link:
                safe_member_name = escape_display_text(user.display_name)
                await send_private_followup(
                    interaction,
                    clamp_escaped_text(
                        f"❌ Filtering recommendations to **{safe_member_name}**'s {pool_empty_label} requires them to link their Backloggd account with `/link` first.",
                        2000,
                    ),
                    log=logger,
                )
                return

            check_profiles = [(caller_key, caller_username)]
            if friend_link:
                check_profiles.append((self._linked_profile_key(friend_link), friend_link["backloggd_username"]))
            statuses = await ensure_profiles_fresh(
                self.bot, check_profiles, now=now, require_subscribed=True
            )
            caller_status = statuses.get(caller_key)

            caller_ratings = await self.bot.db.get_profile_ratings_dict(caller_key)
            safe_caller = escape_display_text(caller_username)
            if not caller_ratings:
                if caller_status is not None and caller_status.missing:
                    await send_private_followup(
                        interaction,
                        format_fetching_library_message(caller_username, caller_status),
                        log=logger,
                    )
                else:
                    await send_private_followup(
                        interaction,
                        clamp_escaped_text(
                            f"❌ **{safe_caller}** has no cached ratings yet. Use `/refresh` to sync your library first.",
                            2000,
                        ),
                        log=logger,
                    )
                return

            friend_username = friend_link["backloggd_username"] if friend_link else user.display_name
            safe_friend = escape_display_text(friend_username)
            friend_key = self._linked_profile_key(friend_link) if friend_link else None
            friend_status = statuses.get(friend_key) if friend_key else None
            friend_ratings = (
                await self.bot.db.get_profile_ratings_dict(friend_key) if friend_link else {}
            )
            get_entries = getattr(self.bot.db, "get_profile_library_entries", None)
            friend_entries = (
                await get_entries(friend_key) if (friend_link and get_entries is not None) else None
            )
            friend_excluded_slugs = (
                excluded_recommendation_slugs(friend_entries)
                if friend_entries is not None
                else set(friend_ratings.keys())
            )

            # Optional compatibility score if friend is linked and has ratings
            taste_pct = compute_taste_compatibility(
                caller_ratings, friend_ratings, similarity_fn=calculate_similarity_raw
            )

            # Pick caller's top-rated games (rated >= 4.0) not played, started, unknown, or rated by friend
            qualifying_games = qualifying_high_rated_games(caller_ratings)
            if not qualifying_games:
                await send_private_followup(
                    interaction,
                    "ℹ️ You do not have any games rated 4.0 or above in your cached library to recommend.",
                    log=logger,
                )
                return

            friend_pool_slugs: Optional[set[str]] = None
            if allowed_pool_statuses is not None:
                if not friend_entries and friend_status is not None and friend_status.missing:
                    await send_private_followup(
                        interaction,
                        format_fetching_library_message(friend_username, friend_status),
                        log=logger,
                    )
                    return
                friend_pool_slugs = eligible_pool_slugs(friend_entries, allowed_pool_statuses)
                if not friend_pool_slugs:
                    await send_private_followup(
                        interaction,
                        clamp_escaped_text(
                            f"ℹ️ **{safe_friend}** has no unrated games in {pool_empty_label} in the local cache. Try `/refresh` to sync their library.",
                            2000,
                        ),
                        log=logger,
                    )
                    return

            top_picks = select_friend_recommendations(
                qualifying_games,
                excluded_slugs=friend_excluded_slugs,
                pool_slugs=friend_pool_slugs,
                limit=5,
            )

            if not top_picks:
                if allowed_pool_statuses is not None:
                    await send_private_followup(
                        interaction,
                        clamp_escaped_text(
                            f"ℹ️ None of your 4.0+ rated games are currently unrated in **{safe_friend}**'s {pool_empty_label}.",
                            2000,
                        ),
                        log=logger,
                    )
                elif friend_link:
                    if has_uncertain_library_exclusion(caller_ratings, friend_entries):
                        await send_private_followup(
                            interaction,
                            clamp_escaped_text(
                                (
                                    f"ℹ️ No eligible 4.0+ recommendations were found for **{safe_friend}** "
                                    "(matching games with an unknown status in their library were conservatively excluded)."
                                ),
                                2000,
                            ),
                            log=logger,
                        )
                    else:
                        await send_private_followup(
                            interaction,
                            clamp_escaped_text(
                                f"🎉 **{safe_friend}** has already played, started, or rated all of your 4.0+ rated games.",
                                2000,
                            ),
                            log=logger,
                        )
                else:
                    await send_private_followup(
                        interaction,
                        "ℹ️ You do not have any games rated 4.0 or above in your cached library to recommend.",
                        log=logger,
                    )
                return

            cand_slugs = [slug for slug, _ in top_picks]
            cached_meta = await self.bot.db.get_games_by_slugs(cand_slugs) if cand_slugs else {}

            comp_str = f" • `{taste_pct}%` taste compatibility" if taste_pct is not None else ""
            caller_url = build_backloggd_user_url(caller_username)
            if allowed_pool_statuses is not None:
                desc = (
                    f"Hand-picked games from **[{safe_caller}]({caller_url})**'s library "
                    f"matching **{safe_friend}**'s {pool_label}{comp_str}:"
                )
            else:
                desc = f"Hand-picked games from **[{safe_caller}]({caller_url})**'s library{comp_str}:"
            if not friend_link:
                safe_member_name = escape_display_text(user.display_name)
                desc += f"\n*(Note: {safe_member_name} hasn't linked Backloggd yet, showing your all-time highest ratings)*"

            embed = discord.Embed(
                title=f"💡 Recommendations for {friend_username}"[:256],
                description=clamp_escaped_text(desc, 4096),
                color=COLOR_PLAYED
            )

            for slug, rating in top_picks:
                title = resolve_display_title(slug, cached_meta)
                url = build_backloggd_game_url(slug)
                stars = rating_to_stars(rating)
                embed.add_field(
                    name=f"🎮 {title}"[:256],
                    value=clamp_escaped_text(
                        (
                            f"• {safe_caller}'s Rating: {stars} (`{rating:.1f}`)\n"
                            f"• [View on Backloggd]({url})"
                        ),
                        1024,
                    ),
                    inline=False
                )

            freshness_lines = [format_profile_freshness_line(caller_status, label=safe_caller)]
            if friend_link and friend_key:
                freshness_lines.append(
                    format_profile_freshness_line(statuses.get(friend_key), label=safe_friend)
                )
            append_freshness_note(embed, " · ".join(line for line in freshness_lines if line))

            embed.set_footer(text=f"Recommended by {caller_username} via Backloggd Discord Bot"[:2048])
            await send_public_followup(interaction, embed=embed)
            return

        # =====================================================================
        # MODE 1: Discover recommendations from distinct server profiles with similar taste.
        # =====================================================================
        if not caller_link:
            await send_private_followup(
                interaction,
                "❌ You do not have a linked Backloggd account. Use `/link <username>` first.",
                log=logger,
            )
            return

        caller_username = caller_link["backloggd_username"]
        safe_caller = escape_display_text(caller_username)
        caller_key = self._linked_profile_key(caller_link)
        caller_statuses = await ensure_profiles_fresh(
            self.bot, [(caller_key, caller_username)], now=now, require_subscribed=True
        )
        caller_status = caller_statuses.get(caller_key)

        my_ratings = await self.bot.db.get_profile_ratings_dict(caller_key)

        if not my_ratings:
            if caller_status is not None and caller_status.missing:
                await send_private_followup(
                    interaction,
                    format_fetching_library_message(caller_username, caller_status),
                    log=logger,
                )
            else:
                await send_private_followup(
                    interaction,
                    clamp_escaped_text(
                        f"❌ **{safe_caller}** has no cached ratings yet. Use `/refresh` to sync your library.",
                        2000,
                    ),
                    log=logger,
                )
            return

        my_numeric_ratings = numeric_ratings(my_ratings)
        if not my_numeric_ratings:
            await send_private_followup(
                interaction,
                clamp_escaped_text(
                    f"ℹ️ **{safe_caller}** has no numeric game ratings in the cached library yet. Similarity needs rated games; unrated backlog and wishlist entries do not count.",
                    2000,
                ),
                log=logger,
            )
            return

        get_entries = getattr(self.bot.db, "get_profile_library_entries", None)
        my_entries = None
        my_pool_slugs: Optional[set[str]] = None
        if allowed_pool_statuses is not None:
            my_entries = await get_entries(caller_key) if get_entries is not None else None
            my_pool_slugs = eligible_pool_slugs(my_entries, allowed_pool_statuses)
            if not my_pool_slugs:
                await send_private_followup(
                    interaction,
                    clamp_escaped_text(
                        f"ℹ️ **{safe_caller}** has no unrated games in {pool_empty_label} in the local cache. Try `/refresh` to sync your library.",
                        2000,
                    ),
                    log=logger,
                )
                return

        guild_member_ids = await resolve_guild_member_ids(
            self.bot, interaction.guild, exclude_member_id=caller_discord_id
        )
        if guild_member_ids is None:
            await send_private_followup(
                interaction,
                GUILD_MEMBERSHIP_UNAVAILABLE_MESSAGE,
                log=logger,
            )
            return
        other_linked = await self.bot.db.get_guild_linked_profiles(
            guild_member_ids, exclude_profile_key=caller_key
        )

        if not other_linked:
            await send_private_followup(
                interaction,
                "ℹ️ No other distinct Backloggd profiles are linked in this server yet. Invite members to use `/link`.",
                log=logger,
            )
            return

        other_statuses = await ensure_profiles_fresh(
            self.bot, other_linked, now=now, require_subscribed=True
        )

        # Batch fetch ratings once per distinct profile.
        other_profile_keys = [other["profile_key"] for other in other_linked]
        all_other_ratings = await self.bot.db.get_multi_profile_ratings_dict(other_profile_keys)

        taste_matches, enough_rated_profiles = rank_taste_matches(
            my_numeric_ratings,
            other_linked,
            all_other_ratings,
            similarity_fn=calculate_similarity_raw,
        )

        if not taste_matches:
            if enough_rated_profiles:
                reason = "No other profile shares at least two numeric ratings with you yet."
            else:
                reason = "Other profiles need at least five numeric ratings before we can compare taste."
            await send_private_followup(
                interaction,
                f"ℹ️ {reason} Unrated backlog and wishlist entries do not count toward similarity.",
                log=logger,
            )
            return

        if my_entries is None and get_entries is not None and allowed_pool_statuses is None:
            my_entries = await get_entries(caller_key)
        my_excluded_slugs = (
            excluded_recommendation_slugs(my_entries)
            if my_entries is not None
            else set(my_ratings.keys())
        )
        raw_candidates = select_diverse_recommendations(
            taste_matches,
            excluded_slugs=my_excluded_slugs,
            pool_slugs=my_pool_slugs,
            limit=5,
        )

        if not raw_candidates:
            if allowed_pool_statuses is not None:
                await send_private_followup(
                    interaction,
                    f"ℹ️ None of your similar server profiles have rated any unrated games in your {pool_empty_label} 4.0 or above.",
                    log=logger,
                )
            elif any(
                has_uncertain_library_exclusion(other_ratings, my_entries)
                for _, _, _, other_ratings in taste_matches
            ):
                await send_private_followup(
                    interaction,
                    (
                        "ℹ️ No eligible 4.0+ recommendations were found from similar profiles "
                        "(matching games with an unknown status in your library were conservatively excluded)."
                    ),
                    log=logger,
                )
            else:
                await send_private_followup(
                    interaction,
                    "ℹ️ There are no eligible 4.0+ recommendations from similar profiles that you haven't already played, started, or rated.",
                    log=logger,
                )
            return

        # Batch resolve authentic game titles from cache
        cand_slugs = [c[0] for c in raw_candidates]
        cached_meta = await self.bot.db.get_games_by_slugs(cand_slugs) if cand_slugs else {}

        recommendations = [
            {
                "slug": slug,
                "title": resolve_display_title(slug, cached_meta),
                "rating": rating,
                "recommender": other_name,
                "match_pct": match_pct,
            }
            for slug, rating, other_name, match_pct in raw_candidates
        ]

        if allowed_pool_statuses is not None:
            mode1_desc = (
                f"Hand-picked games from your {pool_label} recommended by server profiles that share your taste:\n"
            )
        else:
            mode1_desc = "Hand-picked games from different server profiles that share your taste:\n"

        embed = discord.Embed(
            title=f"💡 Recommendations for {caller_username}"[:256],
            description=mode1_desc,
            color=COLOR_PLAYED
        )

        for rec in recommendations:
            url = build_backloggd_game_url(rec["slug"])
            stars = rating_to_stars(rec["rating"])
            safe_recommender = escape_display_text(rec["recommender"])
            embed.add_field(
                name=f"🎮 {rec['title']}"[:256],
                value=clamp_escaped_text(
                    (
                        f"• Recommended by **{safe_recommender}** ({rec['match_pct']}% taste match)\n"
                        f"• Their Rating: {stars} (`{rec['rating']:.1f}`)\n"
                        f"• [View on Backloggd]({url})"
                    ),
                    1024,
                ),
                inline=False
            )

        contributing = ([caller_status] if caller_status is not None else []) + list(other_statuses.values())
        freshness_note = format_aggregate_freshness(summarize_aggregate_freshness(contributing))
        append_freshness_note(embed, freshness_note)

        embed.set_footer(text="Diverse recommendations curated across your server taste matches")
        await send_public_followup(interaction, embed=embed)

async def setup(bot):
    await bot.add_cog(DiscoveryCog(bot))
