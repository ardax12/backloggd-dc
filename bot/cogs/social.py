"""SocialCog: Handles /compare taste duel and similarity between users."""

import asyncio
from datetime import datetime, timezone
from dataclasses import dataclass
import discord
from discord import app_commands
from discord.ext import commands
import logging
from typing import Optional, Dict
from backloggd.analytics import calculate_similarity_raw
from backloggd.exceptions import ResourceNotFoundError, UserNotFoundError
from bot.input_validation import (
    InvalidUsernameError,
    escape_display_text,
    normalize_backloggd_username,
    parse_discord_member_id,
)
from bot.ui.embeds import create_compare_embed, create_taste_twins_embed
from bot.ui.views import LinkButtonView, TasteTwinsView
from bot.ui.helpers import build_backloggd_user_url
from bot.recommendation_rules import numeric_ratings
from bot.services.guild_membership import (
    GUILD_MEMBERSHIP_UNAVAILABLE_MESSAGE,
    resolve_guild_member_ids,
)
from bot.command_errors import (
    defer_interaction,
    handle_component_error,
    send_private_followup,
    send_public_followup,
)
from bot.services.freshness import (
    ensure_profiles_fresh,
    format_aggregate_freshness,
    format_fetching_library_message,
    format_profile_freshness_line,
    summarize_aggregate_freshness,
)
from bot.services.task_lifecycle import await_owned_task

logger = logging.getLogger("backloggd.cogs.social")


class _ProfileNotFoundSignal(Exception):
    def __init__(self, username: str):
        super().__init__(username)
        self.username = username


@dataclass(frozen=True)
class _LibraryRatingSnapshot:
    """Request-scoped live results; public rating callers still receive a dict."""

    ratings: Dict[str, Optional[float]]
    titles: Dict[str, str]


@dataclass
class _LiveFetchGeneration:
    """Per-task-generation coalesced live fetch ownership record."""

    profile_key: str
    task: asyncio.Task
    waiters: int = 0


class SocialCog(commands.Cog, name="Social"):
    def __init__(
        self,
        bot,
        *,
        clock=None,
        live_fetch_concurrency: int = 2,
        live_fetch_timeout: float = 25.0,
    ):
        self.bot = bot
        self.clock = clock or (lambda: datetime.now(timezone.utc))
        self.live_fetch_concurrency = max(1, int(live_fetch_concurrency))
        self.live_fetch_timeout = float(live_fetch_timeout)
        self._live_library_semaphore = asyncio.Semaphore(self.live_fetch_concurrency)
        self._live_library_generations: Dict[str, _LiveFetchGeneration] = {}
        # Retired generations may still be unwinding after their last waiter leaves.
        self._tracked_live_tasks: set[asyncio.Task] = set()
        self._closing: bool = False
        self._draining_task: Optional[asyncio.Task] = None

    async def close(self) -> None:
        """Idempotently cancel and drain all tracked live-library fetches."""
        self._closing = True
        current = asyncio.current_task()
        draining = self._draining_task
        if draining is not None and not draining.done():
            await await_owned_task(draining)
            return

        async def _drain() -> None:
            drain_task = asyncio.current_task()
            while True:
                pending = [
                    task for task in list(self._tracked_live_tasks)
                    if not task.done() and task is not current and task is not drain_task
                ]
                if not pending:
                    break
                for task in pending:
                    if not task.cancelling():
                        task.cancel()
                await asyncio.gather(*pending, return_exceptions=True)
            self._live_library_generations.clear()

        draining = asyncio.create_task(_drain())
        self._draining_task = draining
        try:
            await await_owned_task(draining)
        finally:
            if self._draining_task is draining and draining.done():
                self._draining_task = None

    async def cog_unload(self) -> None:
        """Drain active live fetches when the cog is unloaded."""
        await self.close()

    async def _resolve_username(self, interaction: discord.Interaction, target: Optional[str]) -> Optional[str]:
        """Resolve a parameter into a Backloggd username (supports Discord mention or username)."""
        if not target:
            linked = await self.bot.db.get_linked_user(interaction.user.id)
            return linked["backloggd_username"] if linked else None

        member_id = parse_discord_member_id(target)
        if member_id is not None:
            linked = await self.bot.db.get_linked_user(member_id)
            return linked["backloggd_username"] if linked else None

        return normalize_backloggd_username(target)

    async def _is_subscribed(self, username: str) -> bool:
        checker = getattr(self.bot.db, "is_profile_library_subscribed", None)
        if checker is not None:
            return bool(await checker(username))
        sync_getter = getattr(self.bot.db, "get_profile_sync_info", None)
        if sync_getter is not None:
            info = await sync_getter(username)
            return info is not None
        return False

    async def _fetch_live_library_snapshot(self, username: str) -> _LibraryRatingSnapshot:
        async with self._live_library_semaphore:
            lib = await self.bot.backloggd.get_user_library(username)
        ratings = lib.to_ratings_dict()
        titles = {}
        games = getattr(lib, "games", None)
        if isinstance(games, dict):
            for raw_slug, game in games.items():
                slug = getattr(game, "slug", None)
                title = getattr(game, "title", None)
                slug = slug if isinstance(slug, str) and slug.strip() else raw_slug
                if (
                    isinstance(slug, str)
                    and slug.strip()
                    and isinstance(title, str)
                    and title.strip()
                ):
                    titles[slug] = title.strip()
        return _LibraryRatingSnapshot(ratings=ratings, titles=titles)

    async def _fetch_live_library_ratings(self, username: str) -> Dict[str, Optional[float]]:
        """Compatibility wrapper retaining the numeric ratings result shape."""
        return (await self._fetch_live_library_snapshot(username)).ratings

    async def _get_or_fetch_ratings(self, username: str) -> Dict[str, Optional[float]]:
        """Fetch user's library ratings mapping (slug -> score), coalescing live fetches."""
        return (await self._get_or_fetch_library_snapshot(username)).ratings

    async def _get_or_fetch_library_snapshot(self, username: str) -> _LibraryRatingSnapshot:
        """Return ratings plus live titles without persisting unlinked library data."""
        if self._closing:
            raise asyncio.CancelledError("Social cog is shutting down")

        profile = await self.bot.db.get_profile_sync_info(username)
        if self._closing:
            raise asyncio.CancelledError("Social cog is shutting down")
        if profile and profile["last_synced_at"] is not None:
            ratings = await self.bot.db.get_profile_ratings_dict(username)
            return _LibraryRatingSnapshot(ratings=ratings, titles={})

        profile_key = username.strip().lower()
        gen = self._live_library_generations.get(profile_key)
        if gen is None or gen.task.done():
            task = asyncio.create_task(self._fetch_live_library_snapshot(username))
            gen = _LiveFetchGeneration(profile_key=profile_key, task=task, waiters=0)
            self._live_library_generations[profile_key] = gen
            self._tracked_live_tasks.add(task)
            task.add_done_callback(self._tracked_live_tasks.discard)

            def _cleanup_live_task(
                completed: asyncio.Task,
                owned_gen: _LiveFetchGeneration = gen,
            ) -> None:
                key = owned_gen.profile_key
                if self._live_library_generations.get(key) is owned_gen:
                    self._live_library_generations.pop(key, None)
                if not completed.cancelled():
                    completed.exception()

            task.add_done_callback(_cleanup_live_task)

        gen.waiters += 1
        try:
            return await asyncio.wait_for(
                asyncio.shield(gen.task),
                timeout=self.live_fetch_timeout,
            )
        finally:
            gen.waiters = max(0, gen.waiters - 1)
            is_current = self._live_library_generations.get(profile_key) is gen
            if gen.waiters <= 0 or self._closing:
                if is_current:
                    self._live_library_generations.pop(profile_key, None)
                if not gen.task.done():
                    gen.task.cancel()

    @app_commands.command(name="compare", description="Compare video game tastes and ratings between two Backloggd users")
    @app_commands.describe(
        user1="First user (Backloggd username or @mention)",
        user2="Optional second user (defaults to you if comparing with user1)"
    )
    async def compare(
        self,
        interaction: discord.Interaction,
        user1: str,
        user2: Optional[str] = None
    ):
        await defer_interaction(interaction)

        # Target resolution
        caller_profile_key = None
        try:
            if user2 is None:
                # user1 is target, user2 is caller
                u1_name = await self._resolve_username(interaction, user1)
                u2_name = await self._resolve_username(interaction, None)
                caller_profile_key = u2_name.lower() if u2_name else None
                if not u2_name:
                    await send_private_followup(
                        interaction,
                        "❌ You haven't linked your Backloggd account yet! Use `/link <username>` or specify two users: `/compare <user1> <user2>`.",
                        log=logger,
                    )
                    return
            else:
                u1_name = await self._resolve_username(interaction, user1)
                u2_name = await self._resolve_username(interaction, user2)
                if parse_discord_member_id(user2) == interaction.user.id:
                    caller_profile_key = u2_name.lower() if u2_name else None
        except InvalidUsernameError as err:
            await send_private_followup(interaction, f"❌ {err}", log=logger)
            return

        if not u1_name or not u2_name:
            await send_private_followup(
                interaction,
                "❌ Could not resolve one or both users. Please check usernames or mentions.",
                log=logger,
            )
            return

        if u1_name.lower() == u2_name.lower():
            await send_private_followup(
                interaction,
                "ℹ️ Both inputs resolve to the same Backloggd profile, so there is no separate profile to compare.",
                log=logger,
            )
            return

        safe_u1 = escape_display_text(u1_name)
        safe_u2 = escape_display_text(u2_name)
        try:
            now = self.clock()
            statuses = await ensure_profiles_fresh(
                self.bot,
                [(u1_name.lower(), u1_name), (u2_name.lower(), u2_name)],
                now=now,
                require_subscribed=True,
            )
            s1 = statuses.get(u1_name.lower())
            s2 = statuses.get(u2_name.lower())
            sub1, sub2 = await asyncio.gather(
                self._is_subscribed(u1_name),
                self._is_subscribed(u2_name),
            )

            async def _load_ratings(name: str, subscribed: bool, status):
                try:
                    if subscribed and status is not None and status.missing and getattr(self.bot, "library_sync", None) is not None:
                        ratings = await self.bot.db.get_profile_ratings_dict(name)
                        return _LibraryRatingSnapshot(ratings=ratings, titles={})
                    return await self._get_or_fetch_library_snapshot(name)
                except (UserNotFoundError, ResourceNotFoundError) as exc:
                    raise _ProfileNotFoundSignal(name) from exc

            loaded = await asyncio.gather(
                _load_ratings(u1_name, sub1, s1),
                _load_ratings(u2_name, sub2, s2),
                return_exceptions=True,
            )
            for item in loaded:
                if isinstance(item, BaseException):
                    if isinstance(item, asyncio.CancelledError):
                        raise item
                    raise item
            snapshot1, snapshot2 = loaded
            ratings1, ratings2 = snapshot1.ratings, snapshot2.ratings

            if not ratings1 or not ratings2:
                if not ratings1 and sub1 and s1 is not None and s1.missing:
                    await send_private_followup(
                        interaction,
                        format_fetching_library_message(u1_name, s1),
                        log=logger,
                    )
                    return
                if not ratings2 and sub2 and s2 is not None and s2.missing:
                    await send_private_followup(
                        interaction,
                        format_fetching_library_message(u2_name, s2),
                        log=logger,
                    )
                    return
                empty_user = safe_u1 if not ratings1 else safe_u2
                empty_username = u1_name if not ratings1 else u2_name
                empty_library_message = (
                    "Your library has no logged games to compare yet."
                    if caller_profile_key == empty_username.lower()
                    else f"**{empty_user}**'s library has no logged games to compare yet."
                )
                await send_private_followup(
                    interaction,
                    f"❌ {empty_library_message}",
                    log=logger,
                )
                return

            rated1 = numeric_ratings(ratings1)
            rated2 = numeric_ratings(ratings2)
            if not rated1 or not rated2:
                unrated_user = safe_u1 if not rated1 else safe_u2
                await send_private_followup(
                    interaction,
                    f"ℹ️ **{unrated_user}** has no numeric ratings yet. Unrated backlog and wishlist entries do not count toward similarity.",
                    log=logger,
                )
                return

            shared_rated_slugs = set(rated1) & set(rated2)
            if not shared_rated_slugs:
                await send_private_followup(
                    interaction,
                    "ℹ️ These profiles have no shared numeric ratings to compare yet.",
                    log=logger,
                )
                return

            # Calculate similarity score using backloggd-py
            final_score, library_score, rating_score = calculate_similarity_raw(rated1, rated2)
            match_pct = final_score / 100.0

            # Find common rated games
            common_slugs = sorted(shared_rated_slugs)
            slug_lookup = getattr(self.bot.db, "get_games_by_slugs", None)
            cached_meta = (
                await slug_lookup(common_slugs)
                if common_slugs and callable(slug_lookup)
                else {}
            )
            shared_loves = []
            disagreements = []

            for slug in common_slugs:
                r1 = rated1[slug]
                r2 = rated2[slug]
                delta = abs(r1 - r2)
                meta = cached_meta.get(slug)
                cached_title = meta["title"] if meta else None
                if not isinstance(cached_title, str) or not cached_title.strip():
                    cached_title = None
                live_title = snapshot1.titles.get(slug) or snapshot2.titles.get(slug)
                if not isinstance(live_title, str) or not live_title.strip():
                    live_title = None
                title = cached_title.strip() if cached_title else (
                    live_title.strip() if live_title else slug.replace("-", " ").title()
                )
                # Shared loves (both rated 4.0+)
                if r1 >= 4.0 and r2 >= 4.0:
                    shared_loves.append({"title": title, "r1": r1, "r2": r2, "avg": (r1 + r2) / 2})
                # Disagreements (delta >= 1.0)
                if delta >= 1.0:
                    disagreements.append({"title": title, "r1": r1, "r2": r2, "delta": delta})

            shared_loves.sort(key=lambda x: (-x["avg"], x["title"]))
            disagreements.sort(key=lambda x: (-x["delta"], x["title"]))

            line1 = format_profile_freshness_line(s1, label=f"**{safe_u1}**", unlinked_live=not sub1)
            line2 = format_profile_freshness_line(s2, label=f"**{safe_u2}**", unlinked_live=not sub2)
            freshness_note = "\n".join(line for line in (line1, line2) if line) or None

            embed = create_compare_embed(
                user1=u1_name,
                user2=u2_name,
                match_score=match_pct,
                shared_loves=shared_loves,
                disagreements=disagreements,
                freshness_note=freshness_note,
            )
            view = LinkButtonView(label=f"View {u1_name}'s Profile", url=build_backloggd_user_url(u1_name))

            await send_public_followup(interaction, embed=embed, view=view)

        except _ProfileNotFoundSignal as missing:
            await send_private_followup(
                interaction,
                f"❌ Backloggd user **{escape_display_text(missing.username)}** could not be found.",
                log=logger,
            )
        except asyncio.TimeoutError:
            await send_private_followup(
                interaction,
                "⏳ Fetching Backloggd library data timed out. Please try again shortly.",
                log=logger,
            )
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.error("Error comparing %s and %s: %s", u1_name, u2_name, e, exc_info=True)
            await send_private_followup(
                interaction,
                f"❌ An error occurred while comparing **{safe_u1}** and **{safe_u2}**.",
                log=logger,
            )


    # =========================================================================
    # /taste-twins (Server Taste Twins & Nemesis Finder)
    # =========================================================================

    @app_commands.command(
        name="taste-twins",
        description="Find your closest gaming taste twins (and arch-nemesis) in this server"
    )
    @app_commands.describe(
        user="Optional server member or Backloggd user to analyze (defaults to you)"
    )
    async def taste_twins(
        self,
        interaction: discord.Interaction,
        user: Optional[str] = None
    ):
        await defer_interaction(interaction)

        if not interaction.guild:
            await send_private_followup(
                interaction,
                "❌ This command must be run inside a Discord server.",
                log=logger,
            )
            return

        # 1. Target resolution
        target_username = None
        target_discord_id = None
        target_avatar = None

        if user:
            member_id = parse_discord_member_id(user)
            if member_id is not None:
                target_discord_id = member_id
                linked = await self.bot.db.get_linked_user(target_discord_id)
                if not linked:
                    await send_private_followup(
                        interaction,
                        "❌ That user has not linked their Backloggd account yet.",
                        log=logger,
                    )
                    return
                target_username = linked["backloggd_username"]
                target_avatar = linked["avatar_url"]
            else:
                try:
                    clean_name = normalize_backloggd_username(user)
                except InvalidUsernameError as err:
                    await send_private_followup(interaction, f"❌ {err}", log=logger)
                    return
                profile = await self.bot.db.get_profile(clean_name)
                if profile:
                    target_username = profile["display_username"]
                    target_avatar = profile["avatar_url"]
                else:
                    target_username = clean_name
                    target_avatar = None
        else:
            target_discord_id = interaction.user.id
            linked = await self.bot.db.get_linked_user(target_discord_id)
            if not linked:
                await send_private_followup(
                    interaction,
                    "❌ You haven't linked your Backloggd account yet! Use `/link <username>` first.",
                    log=logger,
                )
                return
            target_username = linked["backloggd_username"]
            target_avatar = linked["avatar_url"]

        safe_target = escape_display_text(target_username)
        try:
            now = self.clock()
            target_key = target_username.lower()
            target_subscribed = bool(target_discord_id) or await self._is_subscribed(target_username)
            target_statuses = await ensure_profiles_fresh(
                self.bot,
                [(target_key, target_username)],
                now=now,
                require_subscribed=True,
            )
            target_status = target_statuses.get(target_key)

            # 2. Get target ratings
            if target_discord_id or target_subscribed:
                my_ratings = await self.bot.db.get_profile_ratings_dict(target_username)
            else:
                my_ratings = await self._get_or_fetch_ratings(target_username)

            if not my_ratings:
                if target_subscribed and target_status is not None and target_status.missing:
                    await send_private_followup(
                        interaction,
                        format_fetching_library_message(target_username, target_status),
                        log=logger,
                    )
                else:
                    is_caller = target_discord_id == interaction.user.id
                    has_synced_library = bool(
                        target_subscribed
                        or (target_status is not None and not target_status.missing)
                    )
                    if has_synced_library:
                        empty_message = (
                            "Your synced Backloggd library contains no logged games yet."
                            if is_caller
                            else f"**{safe_target}**'s synced Backloggd library contains no logged games yet."
                        )
                    elif is_caller:
                        empty_message = "Your live Backloggd library contains no logged games yet."
                    else:
                        empty_message = (
                            f"A live Backloggd fetch found no logged games in **{safe_target}**'s library."
                        )
                    await send_private_followup(
                        interaction,
                        f"❌ {empty_message}",
                        log=logger,
                    )
                return

            my_numeric_ratings = numeric_ratings(my_ratings)
            if not my_numeric_ratings:
                empty_ratings_message = (
                    "Your library has no numeric ratings yet. Unrated backlog and wishlist entries do not count toward similarity."
                    if target_discord_id == interaction.user.id
                    else f"**{safe_target}** has no numeric ratings yet. Unrated backlog and wishlist entries do not count toward similarity."
                )
                await send_private_followup(
                    interaction,
                    f"ℹ️ {empty_ratings_message}",
                    log=logger,
                )
                return

            # 3. Fetch distinct candidate profiles in this guild, excluding every link to the target.
            guild_member_ids = await resolve_guild_member_ids(
                self.bot, interaction.guild, exclude_member_id=target_discord_id
            )
            if guild_member_ids is None:
                await send_private_followup(
                    interaction,
                    GUILD_MEMBERSHIP_UNAVAILABLE_MESSAGE,
                    log=logger,
                )
                return
            other_linked = await self.bot.db.get_guild_linked_profiles(
                guild_member_ids, exclude_profile_key=target_key
            )

            if not other_linked:
                await send_private_followup(
                    interaction,
                    "ℹ️ No other distinct Backloggd profiles are linked in this server yet. Invite members to use `/link`.",
                    log=logger,
                )
                return

            other_statuses = await ensure_profiles_fresh(
                self.bot,
                other_linked,
                now=now,
                require_subscribed=True,
            )

            # 4. Batch fetch ratings once per candidate profile.
            other_profile_keys = [o["profile_key"] for o in other_linked]
            all_other_ratings = await self.bot.db.get_multi_profile_ratings_dict(other_profile_keys)

            candidates = []
            slugs_to_lookup = set()
            enough_rated_profiles = False

            for other in other_linked:
                other_profile_key = other["profile_key"]
                other_username = other["display_username"]
                other_avatar = other["avatar_url"]
                other_ratings = numeric_ratings(all_other_ratings.get(other_profile_key, {}))
                if len(other_ratings) >= 5:
                    enough_rated_profiles = True

                # Similarity requires at least two shared numeric ratings.
                common_slugs = list(set(my_numeric_ratings) & set(other_ratings))
                if len(common_slugs) < 2:
                    continue

                final_score, _, _ = calculate_similarity_raw(my_numeric_ratings, other_ratings)
                match_pct = int(round(final_score))

                # Find top shared love (both rated >= 3.5)
                shared_loves = [
                    (s, (my_numeric_ratings[s] + other_ratings[s]) / 2.0)
                    for s in common_slugs
                    if my_numeric_ratings[s] >= 3.5 and other_ratings[s] >= 3.5
                ]
                shared_loves.sort(key=lambda x: x[1], reverse=True)
                top_fav_slug = shared_loves[0][0] if shared_loves else None

                # Find biggest disagreement
                disagreements = [
                    (s, abs(my_numeric_ratings[s] - other_ratings[s]))
                    for s in common_slugs
                ]
                disagreements.sort(key=lambda x: x[1], reverse=True)
                top_disagree_slug, max_delta = (disagreements[0][0], disagreements[0][1]) if disagreements else (None, 0.0)

                if top_fav_slug:
                    slugs_to_lookup.add(top_fav_slug)
                if top_disagree_slug:
                    slugs_to_lookup.add(top_disagree_slug)

                candidates.append({
                    "profile_key": other_profile_key,
                    "username": other_username,
                    "avatar_url": other_avatar,
                    "match_pct": match_pct,
                    "shared_count": len(common_slugs),
                    "top_fav_slug": top_fav_slug,
                    "top_disagree_slug": top_disagree_slug,
                    "delta": max_delta,
                })

            if not candidates:
                if enough_rated_profiles:
                    reason = "No other profile shares at least two numeric ratings with this profile."
                else:
                    reason = "Other profiles need at least five numeric ratings before we can compare taste."
                await send_private_followup(
                    interaction,
                    f"ℹ️ {reason} Unrated backlog and wishlist entries do not count toward similarity.",
                    log=logger,
                )
                return

            # 5. Batch resolve authentic game titles from cache
            cached_meta = await self.bot.db.get_games_by_slugs(list(slugs_to_lookup)) if slugs_to_lookup else {}
            for c in candidates:
                if c["top_fav_slug"]:
                    meta = cached_meta.get(c["top_fav_slug"])
                    c["top_fav"] = meta["title"] if meta and meta["title"] else c["top_fav_slug"].replace("-", " ").title()
                else:
                    c["top_fav"] = None

                if c["top_disagree_slug"]:
                    meta = cached_meta.get(c["top_disagree_slug"])
                    c["top_disagree"] = meta["title"] if meta and meta["title"] else c["top_disagree_slug"].replace("-", " ").title()
                else:
                    c["top_disagree"] = None

            # 6. Sort candidates by similarity descending
            candidates.sort(key=lambda x: (x["match_pct"], x["shared_count"]), reverse=True)

            twins = candidates[:3]
            # Nemesis is lowest match candidate if score is divergent (< 65%)
            nemesis = candidates[-1] if len(candidates) > 1 and candidates[-1]["match_pct"] < 65 else None
            server_avg_match = sum(c["match_pct"] for c in candidates) / len(candidates)

            top_twin_name = twins[0]["username"] if twins else None

            # Interactive duel callback
            async def on_compare(btn_interaction: discord.Interaction):
                if btn_interaction.user.id != interaction.user.id:
                    await send_private_followup(
                        btn_interaction,
                        "❌ Only the member who initiated `/taste-twins` can duel with their twin!",
                        log=logger,
                    )
                    return
                try:
                    await defer_interaction(btn_interaction)
                    t1_ratings = my_numeric_ratings
                    t2_key = twins[0]["profile_key"]
                    t2_ratings = numeric_ratings(all_other_ratings.get(t2_key, {}))
                    final_s, _, _ = calculate_similarity_raw(my_numeric_ratings, t2_ratings)
                    c_slugs = set(t1_ratings.keys()) & set(t2_ratings.keys())
                    duel_meta = dict(cached_meta)
                    missing_slugs = sorted(s for s in c_slugs if s not in duel_meta)
                    slug_lookup = getattr(self.bot.db, "get_games_by_slugs", None)
                    if missing_slugs and callable(slug_lookup):
                        extra_meta = await slug_lookup(missing_slugs)
                        if extra_meta:
                            duel_meta.update(extra_meta)
                    c_loves = []
                    c_dis = []
                    for s in sorted(c_slugs):
                        r1 = my_numeric_ratings.get(s)
                        r2 = t2_ratings.get(s)
                        if r1 is None or r2 is None:
                            continue
                        d = abs(r1 - r2)
                        meta_lookup = duel_meta.get(s)
                        raw_title = meta_lookup["title"] if meta_lookup else None
                        t_title = (
                            raw_title.strip()
                            if isinstance(raw_title, str) and raw_title.strip()
                            else s.replace("-", " ").title()
                        )
                        if r1 >= 4.0 and r2 >= 4.0:
                            c_loves.append({"title": t_title, "r1": r1, "r2": r2, "avg": (r1 + r2) / 2})
                        if d >= 1.0:
                            c_dis.append({"title": t_title, "r1": r1, "r2": r2, "delta": d})

                    c_loves.sort(key=lambda x: (-x["avg"], x["title"]))
                    c_dis.sort(key=lambda x: (-x["delta"], x["title"]))
                    l1 = format_profile_freshness_line(target_status, label=f"**{safe_target}**", unlinked_live=not target_subscribed)
                    l2 = format_profile_freshness_line(other_statuses.get(t2_key), label=f"**{escape_display_text(top_twin_name)}**")
                    duel_freshness = "\n".join(line for line in (l1, l2) if line) or None
                    comp_embed = create_compare_embed(
                        user1=target_username,
                        user2=top_twin_name,
                        match_score=final_s / 100.0,
                        shared_loves=c_loves,
                        disagreements=c_dis,
                        freshness_note=duel_freshness,
                    )
                    comp_view = LinkButtonView(label=f"View {top_twin_name}'s Profile", url=build_backloggd_user_url(top_twin_name))
                    await send_public_followup(btn_interaction, embed=comp_embed, view=comp_view)
                except asyncio.CancelledError:
                    raise
                except Exception as error:
                    await handle_component_error(
                        btn_interaction,
                        error,
                        component_name="taste-twins duel",
                        user_message=(
                            f"❌ Could not complete the duel between **{safe_target}** and "
                            f"**{escape_display_text(top_twin_name)}**. Please try again later."
                        ),
                        log=logger,
                    )

            contributing = ([target_status] if (target_subscribed and target_status is not None) else []) + list(other_statuses.values())
            freshness_note = format_aggregate_freshness(summarize_aggregate_freshness(contributing))
            if not target_subscribed and freshness_note:
                freshness_note = f"{safe_target}: Fetched live · {freshness_note}"

            embed = create_taste_twins_embed(
                username=target_username,
                avatar_url=target_avatar,
                guild_name=interaction.guild.name,
                twins=twins,
                nemesis=nemesis,
                server_avg_match=server_avg_match,
                freshness_note=freshness_note,
            )
            view = TasteTwinsView(
                user_url=build_backloggd_user_url(target_username),
                top_twin_name=top_twin_name,
                on_compare=on_compare if top_twin_name else None
            )

            view.message = await send_public_followup(interaction, embed=embed, view=view, wait=True)

        except (UserNotFoundError, ResourceNotFoundError):
            await send_private_followup(
                interaction,
                f"❌ Backloggd user **{safe_target}** could not be found.",
                log=logger,
            )
        except asyncio.TimeoutError:
            await send_private_followup(
                interaction,
                "⏳ Fetching Backloggd library data timed out. Please try again shortly.",
                log=logger,
            )
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.error("Error running taste-twins for %s: %s", target_username, e, exc_info=True)
            await send_private_followup(
                interaction,
                f"❌ An error occurred while computing taste twins for **{safe_target}**.",
                log=logger,
            )

async def setup(bot):
    await bot.add_cog(SocialCog(bot))
