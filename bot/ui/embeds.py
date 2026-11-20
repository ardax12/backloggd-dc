"""Constructors for all Backloggd Discord Embeds."""

import re
from typing import Any, List, Optional

import discord

from bot.config import COLOR_ERROR, COLOR_GOLD, COLOR_PLAYED, COLOR_PLAYING, COLOR_PRIMARY
from bot.input_validation import escape_display_text
from bot.ui.helpers import (
    DEFAULT_BACKLOGGD_AVATAR,
    build_backloggd_game_url,
    build_backloggd_user_url,
    clamp_escaped_text,
    clean_image_url,
    format_number,
    format_status,
    rating_to_stars,
)


def append_freshness_note(embed: discord.Embed, note: Optional[str]) -> None:
    """Append a single subdued line, keeping relative timestamps renderable."""
    if not note:
        return
    line = clamp_escaped_text(" ".join(note.splitlines()).strip(), 1024, preserve_word_boundary=True)
    if not line:
        return
    line = f"*{line}*"
    description = (embed.description or "").rstrip()
    if description:
        description = clamp_escaped_text(description, 4096 - len(line) - 2)
        embed.description = f"{description}\n\n{line}"
    else:
        embed.description = line


def create_game_embed(
    game: Any,
    server_ratings: List[Any],
    guild_name: Optional[str] = "Server",
    freshness_note: Optional[str] = None,
) -> discord.Embed:
    """Construct embed for /game command with one rating row per server profile."""
    game_url = build_backloggd_game_url(game.slug)
    title = f"{game.title} ({game.release_year})" if game.release_year else game.title

    embed = discord.Embed(
        title=title[:256],
        url=game_url,
        color=COLOR_PRIMARY
    )

    cover = clean_image_url(getattr(game, "cover_url", None))
    if cover:
        embed.set_thumbnail(url=cover)

    # Community rating & metadata description
    desc_lines = []
    if game.community_rating:
        stars = rating_to_stars(game.community_rating)
        rating_count = getattr(game.stats, "total_ratings", None)
        count_str = f" ({format_number(rating_count)} ratings)" if rating_count else ""
        desc_lines.append(f"**Community Rating:** {stars} **{game.community_rating:.1f}** / 5.0{count_str}")
    else:
        desc_lines.append("**Community Rating:** ☆☆☆☆☆ *No ratings yet*")

    rated = [float(r["rating"]) for r in server_ratings if r["rating"] is not None]
    if rated:
        server_avg = sum(rated) / len(rated)
        server_stars = rating_to_stars(server_avg)
        rating_word = "rating" if len(rated) == 1 else "ratings"
        delta_str = ""
        if game.community_rating:
            delta = server_avg - float(game.community_rating)
            if abs(delta) >= 0.05:
                delta_str = f" ({delta:+.1f} vs Backloggd)"
        desc_lines.append(
            f"**Server Average:** {server_stars} **{server_avg:.1f}** / 5.0 "
            f"({len(rated)} {rating_word}){delta_str}"
        )

    if game.summary:
        clean_summary = re.sub(r"[\r\n]+", " ", str(game.summary)).strip()
        escaped_summary = escape_display_text(clean_summary)
        wrapped_summary = f"\n*{escaped_summary}*"
        desc_lines.append(clamp_escaped_text(wrapped_summary, 280, suffix="...*"))

    embed.description = clamp_escaped_text("\n".join(desc_lines), 4096)

    # Global Stats Field
    if hasattr(game, "stats") and game.stats:
        stats_parts = []
        plays = getattr(game.stats, "plays_count", None) or getattr(game.stats, "played_count", None)
        if plays is not None:
            stats_parts.append(f"🎮 **{format_number(plays)}** Played")

        playing = getattr(game.stats, "playing_count", None)
        if playing is not None:
            stats_parts.append(f"▶️ **{format_number(playing)}** Playing")

        backlogs = getattr(game.stats, "backlogs_count", None) or getattr(game.stats, "backlog_count", None)
        if backlogs is not None:
            stats_parts.append(f"⏳ **{format_number(backlogs)}** Backlog")

        wishlists = getattr(game.stats, "wishlists_count", None) or getattr(game.stats, "wishlist_count", None)
        if wishlists is not None:
            stats_parts.append(f"💖 **{format_number(wishlists)}** Wishlist")

        if stats_parts:
            embed.add_field(name="Global Stats", value="  •  ".join(stats_parts), inline=False)

    # Server profile ratings field
    if server_ratings:
        server_lines = []
        display_limit = 12  # Display up to 12 profiles cleanly
        for r in server_ratings[:display_limit]:
            username = escape_display_text(r["backloggd_username"])
            raw_status = str(r["status"]).lower().strip()
            if r["rating"] is not None:
                stars = rating_to_stars(r["rating"])
                # If status is unknown/played, keep it concise without redundant '- Unknown'
                if raw_status in ("unknown", "played", "none", ""):
                    server_lines.append(f"• **{username}**: {stars} (`{r['rating']:.1f}`)")
                else:
                    status_str = format_status(r["status"])
                    server_lines.append(f"• **{username}**: {stars} (`{r['rating']:.1f}`) — {status_str}")
            else:
                status_str = format_status(r["status"])
                server_lines.append(f"• **{username}**: {status_str}")

        if len(server_ratings) > display_limit:
            remaining = len(server_ratings) - display_limit
            server_lines.append(f"*+ {remaining} more profiles (click 'View All Ratings' below)*")

        field_name = f"👥 {guild_name} Profile Ratings ({len(server_ratings)})"[:256]
        embed.add_field(name=field_name, value=clamp_escaped_text("\n".join(server_lines), 1024), inline=False)
    else:
        embed.add_field(
            name=f"👥 {guild_name} Profile Ratings"[:256],
            value="*No cached library entries for this game from connected Backloggd profiles.*",
            inline=False
        )

    append_freshness_note(embed, freshness_note)

    embed.set_footer(text="Data from Backloggd • Use /link to connect your profile")
    return embed

def create_profile_embed(
    profile: Any,
    avg_rating: Optional[float] = None,
    rated_count: Optional[int] = None,
    recent_played: Optional[List[Any]] = None,
    latest_review: Optional[Any] = None,
    fallback_avatar_url: Optional[str] = None,
    freshness_note: Optional[str] = None,
    *,
    collection_counts: Optional[dict[str, int]] = None,
) -> discord.Embed:
    """Construct embed for /profile command with clean formatting and top-left avatar."""
    user_url = build_backloggd_user_url(profile.username)

    # Filter special role badges for title (VIP, Patron, Supporter, Founder, Mod)
    special_roles = {"vip": "VIP", "patron": "Patron", "supporter": "Supporter", "founder": "Founder", "mod": "Mod"}
    title_badges = []
    if profile.badges:
        for b in profile.badges:
            b_clean = str(b).lower().replace("-", "_")
            for role_key, role_label in special_roles.items():
                if role_key in b_clean and role_label not in title_badges:
                    title_badges.append(role_label)

    badge_suffix = f" [{ ' | '.join(title_badges) }]" if title_badges else ""

    # Normalize profile avatar URL or use fallback / default Backloggd avatar
    raw_avatar = getattr(profile, "avatar_url", None) or fallback_avatar_url or DEFAULT_BACKLOGGD_AVATAR
    avatar_url = clean_image_url(raw_avatar) or DEFAULT_BACKLOGGD_AVATAR

    embed = discord.Embed(
        title=f"{profile.username}{badge_suffix}"[:256],
        url=user_url,
        color=COLOR_PRIMARY
    )

    # Place profile image in the top-right thumbnail slot
    if avatar_url:
        embed.set_thumbnail(url=avatar_url)

    desc_lines = []
    bio = getattr(profile, "bio", None)
    if bio and str(bio).strip():
        # Replace newlines with spaces so words do not combine, and separate glued words
        clean_bio = re.sub(r"[\r\n]+", " ", str(bio))
        clean_bio = re.sub(r"([a-zA-Z0-9]),([a-zA-Z])", r"\1, \2", clean_bio)
        clean_bio = re.sub(r"([a-z0-9])([A-Z])", r"\1 \2", clean_bio)
        clean_bio = re.sub(r"\s+", " ", clean_bio).strip()
        escaped_bio = escape_display_text(clean_bio)
        quoted_bio = f"> {escaped_bio}"
        desc_lines.append(clamp_escaped_text(quoted_bio, 350, preserve_word_boundary=True))

    # Show badges count cleanly if any achievements earned
    if profile.badges:
        desc_lines.append(f"🏆 **{len(profile.badges)}** Badges Earned")

    if desc_lines:
        embed.description = clamp_escaped_text("\n\n".join(desc_lines), 4096)

    # Activity Counter Fields
    def display_count(collection: str) -> str:
        value = getattr(profile, f"{collection}_count", None)
        if value is None and collection_counts:
            value = collection_counts.get(collection)
        return "Unavailable" if value is None else format_number(value)

    embed.add_field(
        name="📊 Library Overview",
        value=(
            f"🎮 **{display_count('played')}** Played\n"
            f"▶️ **{display_count('playing')}** Playing\n"
            f"⏳ **{display_count('backlog')}** Backlog\n"
            f"💖 **{display_count('wishlist')}** Wishlist"
        ),
        inline=True
    )

    # Average rating if cached/computed
    if avg_rating is not None:
        count_text = f"\n*({rated_count} rated games)*" if rated_count else ""
        embed.add_field(
            name="⭐ Average Rating",
            value=f"{rating_to_stars(avg_rating)}\n**{avg_rating:.2f}** / 5.0{count_text}",
            inline=True
        )

    # 1. Pinned Favorite Games (up to 5)
    if profile.favorite_games:
        fav_lines = []
        for i, fav in enumerate(profile.favorite_games[:5], 1):
            if isinstance(fav, str):
                title = fav
            elif hasattr(fav, "title"):
                val = fav.title
                title = val() if callable(val) else val
            else:
                title = str(fav)
            safe_title = clamp_escaped_text(escape_display_text(title), 180)
            fav_lines.append(f"**{i}.** {safe_title}")
        embed.add_field(
            name="⭐ Favorite Games",
            value=clamp_escaped_text("\n".join(fav_lines), 1024),
            inline=False,
        )

    # 2. Recently Played (up to 5)
    if recent_played:
        recent_lines = []
        for entry in recent_played[:5]:
            if hasattr(entry, "keys"):
                from types import SimpleNamespace
                entry = SimpleNamespace(**dict(entry))
            title = getattr(entry, "game_title", None) or getattr(entry, "title", "Unknown Game")
            rating = getattr(entry, "rating", None)
            date_str = getattr(entry, "played_date", None)
            replay = " 🔄" if getattr(entry, "is_replay", False) else ""

            safe_title = clamp_escaped_text(escape_display_text(title), 140)
            stars = f" — {rating_to_stars(rating)}" if rating is not None else ""
            safe_date = clamp_escaped_text(escape_display_text(date_str), 40) if date_str else None
            date_suffix = f" *({safe_date})*" if safe_date else ""
            recent_lines.append(f"• **{safe_title}**{stars}{replay}{date_suffix}")
        if recent_lines:
            embed.add_field(
                name="🕹️ Recently Played",
                value=clamp_escaped_text("\n".join(recent_lines), 1024),
                inline=False,
            )

    # 3. Latest Review (if present)
    if latest_review and getattr(latest_review, "game_title", None):
        r_title = clamp_escaped_text(escape_display_text(latest_review.game_title), 256)
        r_rating = rating_to_stars(latest_review.rating) if latest_review.rating is not None else ""
        likes_count = getattr(latest_review, "like_count", 0)
        likes_text = f" • ❤️ {likes_count}" if likes_count > 0 else ""

        header = f"**{r_title}** ({r_rating}){likes_text}" if r_rating else f"**{r_title}**{likes_text}"

        review_text = getattr(latest_review, "review_text", "") or ""
        if review_text:
            # Replace newlines with spaces so words do not combine, and separate glued words
            clean_text = re.sub(r"[\r\n]+", " ", str(review_text))
            clean_text = re.sub(r"([a-zA-Z0-9]),([a-zA-Z])", r"\1, \2", clean_text)
            clean_text = re.sub(r"([a-z0-9])([A-Z])", r"\1 \2", clean_text)
            clean_text = re.sub(r"\s+", " ", clean_text).strip()
            escaped_body = escape_display_text(clean_text)

            has_spoilers = bool(getattr(latest_review, "has_spoilers", False))
            body_budget = max(80, min(320, 1024 - len(header) - 1))
            if has_spoilers:
                review_line = clamp_escaped_text(
                    f'|| > "{escaped_body}" ||',
                    body_budget,
                    suffix='..." ||',
                    preserve_word_boundary=True,
                )
            else:
                review_line = clamp_escaped_text(
                    f'> "{escaped_body}"',
                    body_budget,
                    suffix='..."',
                    preserve_word_boundary=True,
                )
            review_content = f"{header}\n{review_line}"
        else:
            review_content = clamp_escaped_text(header, 1024)

        embed.add_field(name="📝 Latest Review", value=review_content, inline=False)

    append_freshness_note(embed, freshness_note)

    embed.set_footer(text="Backloggd Profile Overview • Connect yours with /link")
    return embed

def create_compare_embed(
    user1: str,
    user2: str,
    match_score: float,
    shared_loves: List[dict],
    disagreements: List[dict],
    freshness_note: Optional[str] = None,
) -> discord.Embed:
    """Construct embed for /compare taste duel."""
    score_pct = int(round(match_score * 100))
    if score_pct >= 80:
        verdict = "✨ Very High Agreement (Taste Twins!)"
        color = COLOR_PLAYED
    elif score_pct >= 60:
        verdict = "🤝 High Compatibility"
        color = COLOR_PRIMARY
    elif score_pct >= 40:
        verdict = "⚖️ Moderate Overlap"
        color = COLOR_GOLD
    else:
        verdict = "⚡ Polar Opposites"
        color = COLOR_ERROR

    safe_u1 = escape_display_text(user1)
    safe_u2 = escape_display_text(user2)
    embed = discord.Embed(
        title=f"⚔️ Taste Duel: {user1} vs {user2}"[:256],
        description=f"## {score_pct}% Taste Match\n**{verdict}**",
        color=color
    )

    # Shared Loves
    if shared_loves:
        lines = []
        for g in shared_loves[:5]:
            lines.append(f"• **{escape_display_text(g['title'])}** ({safe_u1}: `{g['r1']:.1f}` | {safe_u2}: `{g['r2']:.1f}`)")
        embed.add_field(name="❤️ Shared Loves (Both Rated 4.0+)", value=clamp_escaped_text("\n".join(lines), 1024), inline=False)
    else:
        embed.add_field(name="❤️ Shared Loves", value="*No shared 4.0+ ratings found.*", inline=False)

    # Disagreements
    if disagreements:
        lines = []
        for g in disagreements[:5]:
            lines.append(f"• **{escape_display_text(g['title'])}** ({safe_u1}: `{g['r1']:.1f}` vs {safe_u2}: `{g['r2']:.1f}` — `Δ {g['delta']:.1f}`)")
        embed.add_field(name="⚡ Biggest Disagreements", value=clamp_escaped_text("\n".join(lines), 1024), inline=False)

    append_freshness_note(embed, freshness_note)

    embed.set_footer(text="Calculated using Backloggd library similarity metrics")
    return embed

def _read_field(item: Any, key: str, default: Any = None) -> Any:
    if isinstance(item, dict) or (hasattr(item, "keys") and callable(item.keys)):
        try:
            return item[key]
        except (KeyError, IndexError):
            return default
    return getattr(item, key, default)


def create_roulette_embed(
    username: str,
    game: Any,
    remaining_backlog: int,
    freshness_note: Optional[str] = None,
    *,
    pool: Optional[str] = None,
) -> discord.Embed:
    """Construct embed for /roulette unplayed-library pick."""
    raw_title = _read_field(game, "title", "Unknown Game")
    release_year = _read_field(game, "release_year")
    slug = _read_field(game, "slug", "")
    title = f"{raw_title} ({release_year})" if release_year else str(raw_title)
    game_url = build_backloggd_game_url(slug)

    embed = discord.Embed(
        title=f"🎲 Unplayed Game Roulette for {username}"[:256],
        url=game_url,
        color=COLOR_GOLD
    )

    cover = clean_image_url(_read_field(game, "cover_url"))
    if cover:
        embed.set_thumbnail(url=cover)

    desc_lines = [
        f"### [{escape_display_text(title)}]({game_url})"
    ]
    community_rating = _read_field(game, "community_rating")
    if community_rating:
        stars = rating_to_stars(community_rating)
        desc_lines.append(f"{stars} **{community_rating:.1f}** / 5.0")

    pool_suffix = f" in {pool}" if pool in ("Backlog", "Wishlist") else ""
    desc_lines.append(
        f"\n🎯 **Your Mission:** Pick one of these unrated games to play! ({remaining_backlog} games still waiting{pool_suffix})"
    )
    embed.description = clamp_escaped_text("\n".join(desc_lines), 4096)
    append_freshness_note(embed, freshness_note)
    embed.set_footer(text="Click [🔄 Reroll] below to spin again")
    return embed

def create_server_top_embed(
    title: str,
    entries: List[Any],
    mode: str,
    guild_name: str,
    freshness_note: Optional[str] = None,
    *,
    page: int = 1,
    per_page: int = 10,
    min_ratings: int = 2,
) -> discord.Embed:
    """Construct embed for /server-top leaderboards."""
    embed = discord.Embed(
        title=f"🏆 {guild_name} Leaderboard — {title}"[:256],
        color=COLOR_PRIMARY
    )

    show_min_ratings = mode in ("highest_rated", "polarizing") and min_ratings != 2

    if not entries:
        if show_min_ratings:
            noun = "rating" if min_ratings == 1 else "ratings"
            embed.description = (
                f"*Not enough server data to generate this leaderboard yet "
                f"(minimum {min_ratings} profile {noun} required).*"
            )
        else:
            embed.description = "*Not enough server data to generate this leaderboard yet.*"
        append_freshness_note(embed, freshness_note)
        return embed

    page_size = max(1, int(per_page))
    total_pages = max(1, (len(entries) + page_size - 1) // page_size)
    current_page = max(1, min(int(page), total_pages))
    start_idx = (current_page - 1) * page_size
    page_entries = entries[start_idx : start_idx + page_size]

    lines = []
    medals = ["🥇", "🥈", "🥉"]
    for offset, row in enumerate(page_entries):
        rank = start_idx + offset + 1
        prefix = medals[rank - 1] if rank <= 3 else f"`#{rank}`"
        game_title = escape_display_text(row["title"])
        slug = row["slug"]
        url = build_backloggd_game_url(slug)

        if mode == "highest_rated":
            avg = row["avg_rating"]
            count = row["rating_count"]
            lines.append(f"{prefix} **[{game_title}]({url})** — ★ **{avg:.2f}** *({count} profile ratings)*")
        elif mode == "most_played":
            count = row["play_count"]
            lines.append(f"{prefix} **[{game_title}]({url})** — 🎮 **{count}** profiles completed")
        elif mode == "shame_list":
            count = row["backlog_count"]
            lines.append(f"{prefix} **[{game_title}]({url})** — ⏳ in **{count}** profiles' backlogs")
        elif mode == "polarizing":
            delta = row["rating_delta"]
            avg = row["avg_rating"]
            lines.append(f"{prefix} **[{game_title}]({url})** — ⚡ **Δ {delta:.1f} gap** *(avg: ★ {avg:.2f})*")
        elif mode == "hidden_gems":
            avg = row["avg_rating"]
            count = row["rating_count"]
            lines.append(f"{prefix} **[{game_title}]({url})** — 💎 ★ **{avg:.2f}** *({count} profile{'s' if count != 1 else ''})*")
        else:
            lines.append(f"{prefix} **[{game_title}]({url})**")

    embed.description = clamp_escaped_text("\n".join(lines), 4096)
    append_freshness_note(embed, freshness_note)

    footer_parts: list[str] = []
    if total_pages > 1:
        footer_parts.append(f"Page {current_page}/{total_pages}")
    if show_min_ratings:
        noun = "rating" if min_ratings == 1 else "ratings"
        footer_parts.append(f"Min {min_ratings} {noun}")
    base_footer = f"Server rankings across distinct Backloggd profiles linked in {guild_name}"
    footer_text = f"{' · '.join(footer_parts)} · {base_footer}" if footer_parts else base_footer
    embed.set_footer(text=footer_text[:2048])
    return embed

def create_server_stats_embed(
    stats: dict,
    guild_name: str,
    freshness_note: Optional[str] = None,
) -> discord.Embed:
    """Construct embed for /server-stats overview."""
    embed = discord.Embed(
        title=f"📊 {guild_name} Gaming Stats"[:256],
        color=COLOR_PRIMARY
    )
    append_freshness_note(embed, freshness_note)

    avg_str = (
        f"⭐ **{stats['server_avg_rating']:.2f}** / 5.0"
        if stats.get("server_avg_rating") is not None
        else "*N/A*"
    )
    raw_top_game = stats.get("top_game")
    top_game = escape_display_text(raw_top_game) if raw_top_game else "*N/A*"

    embed.add_field(name="👥 Linked Members", value=f"**{stats['linked_members']}** members", inline=True)
    embed.add_field(
        name="Unique Backloggd Profiles",
        value=f"**{stats.get('profile_count', stats['linked_members'])}** profiles",
        inline=True,
    )
    missing_counts = stats.get("logged_profiles_missing", 0)
    stale_counts = stats.get("logged_profiles_stale", 0)
    if missing_counts:
        logged_value = (
            f"At least **{stats['total_logged']}** games"
            if stats.get("logged_profiles_counted", 0) else "Unavailable"
        )
        noun = "profile count" if missing_counts == 1 else "profile counts"
        logged_value += f"\n{missing_counts} {noun} unavailable"
        if stale_counts:
            stale_noun = "profile count" if stale_counts == 1 else "profile counts"
            logged_value += f"\n{stale_counts} {stale_noun} stale (>1h)"
    elif stale_counts:
        noun = "profile count" if stale_counts == 1 else "profile counts"
        logged_value = f"**{stats['total_logged']}** games\n{stale_counts} {noun} stale (>1h)"
    else:
        logged_value = f"**{stats['total_logged']}** games"
    embed.add_field(name="📚 Games Logged", value=logged_value, inline=True)
    embed.add_field(name="⭐ Server Avg Rating", value=avg_str, inline=True)
    embed.add_field(name="👑 Most Played Game", value=clamp_escaped_text(str(top_game), 1024), inline=True)

    embed.set_footer(text="Games Logged sums Played tabs per unique linked profile; profile counts cached up to 1 hour.")
    return embed


def create_taste_twins_embed(
    username: str,
    avatar_url: Optional[str],
    guild_name: str,
    twins: List[dict],
    nemesis: Optional[dict] = None,
    server_avg_match: Optional[float] = None,
    freshness_note: Optional[str] = None,
) -> discord.Embed:
    """Construct embed for /taste-twins command displaying closest taste twins and nemesis."""
    embed = discord.Embed(
        title=f"👥 {username}'s Server Taste Twins"[:256],
        url=build_backloggd_user_url(username),
        color=COLOR_PLAYED if twins and twins[0]["match_pct"] >= 75 else COLOR_PRIMARY
    )
    clean_avatar = clean_image_url(avatar_url) or DEFAULT_BACKLOGGD_AVATAR
    if clean_avatar:
        embed.set_thumbnail(url=clean_avatar)

    desc_lines = [f"Comparing gaming taste across **{escape_display_text(guild_name)}**."]
    if server_avg_match is not None:
        desc_lines.append(f"**Community Compatibility:** `{int(round(server_avg_match))}%` average agreement")
    embed.description = clamp_escaped_text("\n".join(desc_lines), 4096)

    # 1. Top Taste Twins
    if twins:
        twin_lines = []
        medals = ["🥇", "🥈", "🥉"]
        for i, t in enumerate(twins[:3]):
            medal = medals[i] if i < len(medals) else f"**#{i+1}**"
            fav_text = f" — *Loved: {escape_display_text(str(t['top_fav']))}*" if t.get("top_fav") else ""
            safe_twin = escape_display_text(str(t["username"]))
            twin_url = build_backloggd_user_url(t["username"])
            twin_lines.append(
                f"{medal} **[{safe_twin}]({twin_url})** "
                f"(`{t['match_pct']}%` match, {t['shared_count']} shared){fav_text}"
            )
        embed.add_field(name="✨ Top Taste Soulmates", value=clamp_escaped_text("\n".join(twin_lines), 1024), inline=False)
    else:
        embed.add_field(
            name="✨ Top Taste Soulmates",
            value="*Not enough overlap between distinct profiles with numeric ratings yet.*",
            inline=False
        )

    # 2. Taste Nemesis
    if nemesis:
        nem_fav = (
            f" — *Divergence on: {escape_display_text(str(nemesis['top_disagree']))}* (`Δ {nemesis['delta']:.1f}`)"
            if nemesis.get("top_disagree")
            else ""
        )
        safe_nem = escape_display_text(str(nemesis["username"]))
        nem_url = build_backloggd_user_url(nemesis["username"])
        nem_line = (
            f"⚡ **[{safe_nem}]({nem_url})** "
            f"(`{nemesis['match_pct']}%` match, {nemesis['shared_count']} shared){nem_fav}"
        )
        embed.add_field(name="⚡ #1 Taste Nemesis (Polar Opposite)", value=clamp_escaped_text(nem_line, 1024), inline=False)

    append_freshness_note(embed, freshness_note)

    embed.set_footer(text="Based on Backloggd library ratings • Use /compare to duel with any member")
    return embed


def create_all_ratings_embed(
    game_title: str,
    game_slug: str,
    guild_name: str,
    server_ratings: List[Any],
    page: int = 1,
    per_page: int = 15,
    *,
    freshness_note: Optional[str] = None,
) -> discord.Embed:
    """Construct a paginated embed showing linked profiles' rating and collection status."""
    total_ratings = len(server_ratings)
    total_pages = max(1, (total_ratings + per_page - 1) // per_page)
    current_page = max(1, min(page, total_pages))

    start_idx = (current_page - 1) * per_page
    end_idx = start_idx + per_page
    slice_ratings = server_ratings[start_idx:end_idx]

    embed = discord.Embed(
        title=f"👥 {game_title} — All {guild_name} Ratings"[:256],
        url=build_backloggd_game_url(game_slug),
        description=f"Showing **{start_idx + 1}–{min(end_idx, total_ratings)}** of **{total_ratings}** profiles with this game in their library:\n",
        color=COLOR_PRIMARY
    )

    append_freshness_note(embed, freshness_note)
    lines = []
    for r in slice_ratings:
        raw_username = r["backloggd_username"]
        safe_username = escape_display_text(raw_username)
        user_url = build_backloggd_user_url(raw_username)
        raw_status = str(r["status"]).lower().strip()
        if r["rating"] is not None:
            stars = rating_to_stars(r["rating"])
            if raw_status in ("unknown", "played", "none", ""):
                lines.append(f"• **[{safe_username}]({user_url})**: {stars} (`{r['rating']:.1f}`)")
            else:
                status_str = format_status(r["status"])
                lines.append(f"• **[{safe_username}]({user_url})**: {stars} (`{r['rating']:.1f}`) — {status_str}")
        else:
            status_str = format_status(r["status"])
            lines.append(f"• **[{safe_username}]({user_url})**: {status_str}")

    # Split at profile boundaries rather than cutting off ratings mid-line.
    chunk = ""
    for line in lines:
        line = clamp_escaped_text(line, 280)
        if chunk and len(chunk) + 1 + len(line) > 1024:
            embed.add_field(name="Profile Ratings & Status", value=chunk, inline=False)
            chunk = ""
        chunk = f"{chunk}\n{line}" if chunk else line
    embed.add_field(name="Profile Ratings & Status", value=chunk or "*No ratings*", inline=False)
    embed.set_footer(text=f"Page {current_page} of {total_pages} • Backloggd Discord Bot")
    return embed


def create_playing_embed(
    games_page: List[dict],
    *,
    guild_name: str,
    total_games: int,
    total_players: int,
    page: int = 1,
    total_pages: int = 1,
    freshness_note: Optional[str] = None,
    empty_message: Optional[str] = None,
) -> discord.Embed:
    """Construct a paginated embed for /playing showing what server members are currently playing."""
    embed = discord.Embed(
        title=f"🎮 Now Playing in {guild_name}"[:256],
        color=COLOR_PLAYING,
    )

    if not games_page:
        embed.description = empty_message or "*No one in this server currently has any games marked as Playing on Backloggd.*"
        append_freshness_note(embed, freshness_note)
        embed.set_footer(text="Based on synced Backloggd Playing lists • Use /refresh to update yours")
        return embed

    current_page = max(1, min(int(page), max(1, int(total_pages))))
    pages_total = max(1, int(total_pages))
    game_word = "game" if total_games == 1 else "games"
    player_word = "linked profile" if total_players == 1 else "linked profiles"
    lines = [f"**{total_games}** {game_word} currently being played across **{total_players}** {player_word}.\n"]

    max_players_per_line = 6
    for row in games_page:
        safe_title = escape_display_text(str(row["title"]))
        year = row.get("release_year")
        year_suffix = f" ({year})" if year else ""
        url = build_backloggd_game_url(row["slug"])
        raw_players = list(row.get("players") or [])
        visible_players = [escape_display_text(str(name)) for name in raw_players[:max_players_per_line]]
        players_text = ", ".join(visible_players)
        if len(raw_players) > max_players_per_line:
            players_text += f" *+{len(raw_players) - max_players_per_line} more*"
        lines.append(f"• **[{safe_title}{year_suffix}]({url})** — {players_text}")

    embed.description = clamp_escaped_text("\n".join(lines), 4096)
    append_freshness_note(embed, freshness_note)

    footer_parts: list[str] = []
    if pages_total > 1:
        footer_parts.append(f"Page {current_page}/{pages_total}")
    footer_parts.append("Based on synced Backloggd Playing lists • Use /refresh to update yours")
    embed.set_footer(text=" · ".join(footer_parts)[:2048])
    return embed


HELP_TOPICS: tuple[tuple[str, str, str], ...] = (
    ("Overview", "📖", "Quick start and command categories"),
    ("Getting Started", "🚀", "Linking, unlinking, and library sync"),
    ("Games & Profiles", "🎮", "Game lookup, profiles, /playing, and context menus"),
    ("Social & Discovery", "✨", "Comparisons, taste twins, roulette, and leaderboards"),
    ("Activity Feed & Admin", "⚙️", "Feed preferences, admin setup, and cooldowns"),
)
_VALID_HELP_TOPICS = {name for name, _, _ in HELP_TOPICS}


def create_help_embed(topic: str = "Overview") -> discord.Embed:
    """Construct an ephemeral categorized help embed for /help [topic]."""
    selected = topic if topic in _VALID_HELP_TOPICS else "Overview"

    if selected == "Getting Started":
        embed = discord.Embed(
            title="🚀 Backloggd Bot — Getting Started",
            description=(
                "Connect your public Backloggd profile once and the bot syncs your ratings, "
                "played games, backlog, and wishlist in the background."
            ),
            color=COLOR_PRIMARY,
        )
        embed.add_field(
            name="Account & Library Commands",
            value=(
                "• `/link <username>` — Connect your public Backloggd profile and join this server's activity feed.\n"
                "• `/unlink` — Disconnect your profile (past feed announcements stay as an archive).\n"
                "• `/refresh [member]` — Request a fresh library sync for yourself or another linked member.\n"
                "• `/help [topic]` — Open this guide privately in any server or DM."
            ),
            inline=False,
        )
        embed.add_field(
            name="How Syncing & Linking Work",
            value=(
                "• Library-powered commands (`/playing`, `/roulette`, `/recommend`, `/server-top`, `/taste-twins`) "
                "become available once your first sync finishes.\n"
                "• Linking in a DM connects your profile without enabling server feeds; run `/link` or `/feed on` in a server to join its feed.\n"
                "• Linking is a convenience feature for public profiles, not proof of account ownership."
            ),
            inline=False,
        )
    elif selected == "Games & Profiles":
        embed = discord.Embed(
            title="🎮 Backloggd Bot — Games & Profiles",
            description="Look up games, inspect Backloggd profiles, and see what your server is playing right now.",
            color=COLOR_PRIMARY,
        )
        embed.add_field(
            name="Slash Commands",
            value=(
                "• `/game <title>` — Search Backloggd for a game, compare Backloggd's community rating "
                "with your server's average, and see every linked member's score.\n"
                "• `/profile [user]` — View your profile or any public Backloggd profile (accepts a @mention or username) "
                "with stats, recent journal activity, latest review, and quick actions.\n"
                "• `/playing` — See what games linked server members currently have marked as **Playing**."
            ),
            inline=False,
        )
        embed.add_field(
            name="User Context Menus (Right-Click a Member → Apps)",
            value=(
                "• **View Backloggd Profile** — Open a server member's Backloggd profile card directly.\n"
                "• **Compare Backloggd Tastes** — Instantly duel your ratings against that server member."
            ),
            inline=False,
        )
    elif selected == "Social & Discovery":
        embed = discord.Embed(
            title="✨ Backloggd Bot — Social & Discovery",
            description="Compare gaming tastes, break backlog paralysis, and explore server rankings.",
            color=COLOR_PRIMARY,
        )
        embed.add_field(
            name="Taste Comparisons",
            value=(
                "• `/compare <user1> [user2]` — Duel two profiles' ratings, similarity score, shared favorites, "
                "and biggest disagreements.\n"
                "• `/taste-twins [member]` — Find your closest taste soulmates and #1 nemesis in this server."
            ),
            inline=False,
        )
        embed.add_field(
            name="Discovery & Server Rankings",
            value=(
                "• `/roulette [member] [pool]` — Spin a random unrated game from a member's Backlog, Wishlist, or both.\n"
                "• `/recommend [member] [pool]` — Get personalized server recommendations or suggest your favorites to a friend.\n"
                "• `/server-top [mode] [min_ratings]` — Browse top-50 server rankings across Highest Rated, Most Played, "
                "Most Polarizing, Hidden Gems, and Backlog Shame List.\n"
                "• `/server-stats` — See server-wide game totals, average rating, and most-played title.\n"
                "• `/server-recap [period]` — Summarize the last 7 or 30 days of server activity feed announcements."
            ),
            inline=False,
        )
    elif selected == "Activity Feed & Admin":
        embed = discord.Embed(
            title="⚙️ Backloggd Bot — Activity Feed & Admin",
            description="Share new ratings, reviews, and Started/Finished milestones automatically in a dedicated channel.",
            color=COLOR_PRIMARY,
        )
        embed.add_field(
            name="Member Feed Controls",
            value=(
                "• `/feed on` / `/feed off` — Opt in or out of this server's activity feed.\n"
                "• `/feed status` — Check your feed status and any sync or delivery issues privately."
            ),
            inline=False,
        )
        embed.add_field(
            name="Server Admin Commands (Requires Manage Channels)",
            value=(
                "• `/setchannel [channel]` — Choose where this server's activity feed posts.\n"
                "• `/unsetchannel` — Pause feed announcements without deleting past messages.\n"
                "• `/feed add <username>` / `/feed remove <username>` — Follow or unfollow any public Backloggd profile.\n"
                "• `/feed list` — Browse followed profiles and their feed health.\n"
                "• `/feed filter` — Toggle announced categories (Started, Finished, Replays, Ratings, Reviews) or wrap all review notes in `||spoilers||`.\n"
                "• `/feed test` — Verify channel permissions by posting a non-persisted sample announcement.\n"
                "• `/feed digest [cadence] [channel]` — Schedule automatic Weekly or Monthly server activity recaps."
            ),
            inline=False,
        )
        embed.add_field(
            name="Moderator Cooldown Controls (Administrator, Manage Server, Manage Channels, or Moderate Members)",
            value=(
                "• `/cooldown view` — Inspect current per-command cooldown durations in this server.\n"
                "• `/cooldown set <command> <seconds>` — Customize a command cooldown within safe bounds.\n"
                "• `/cooldown reset [command]` — Reset one or all server cooldowns back to defaults."
            ),
            inline=False,
        )
    else:
        embed = discord.Embed(
            title="📖 Backloggd Bot — Help & Command Guide",
            description=(
                "Bring Backloggd profiles, ratings, taste duels, and live activity feeds into Discord.\n\n"
                "**Quick 3-Step Setup:**\n"
                "1. Run `/link <username>` to connect your public Backloggd profile.\n"
                "2. *(Admins)* Run `/setchannel` to choose a channel for activity announcements.\n"
                "3. Try `/game`, `/profile`, `/playing`, or `/compare`!"
            ),
            color=COLOR_PRIMARY,
        )
        category_lines = [
            f"{emoji} **{name}** — {summary}"
            for name, emoji, summary in HELP_TOPICS
            if name != "Overview"
        ]
        embed.add_field(
            name="Browse Categories Below",
            value="\n".join(category_lines),
            inline=False,
        )

    embed.set_footer(text="Use the menu below to switch categories • This message is visible only to you")
    return embed


def create_server_recap_embed(
    recap: dict[str, Any],
    *,
    guild_name: str,
    period_label: str,
    empty_message: Optional[str] = None,
    is_digest: bool = False,
) -> discord.Embed:
    """Build an embed summarizing recent server activity feed announcements."""
    safe_guild = str(guild_name or "Server")
    title = f"🗓️ {safe_guild} — {period_label} Recap"
    total = int(recap.get("total_announcements", 0) or 0)

    if total <= 0:
        description = (
            empty_message
            or f"*No feed announcements were recorded in this server during {escape_display_text(period_label.lower())}.*"
        )
        embed = discord.Embed(
            title=title[:256],
            description=clamp_escaped_text(description, 4096),
            color=COLOR_PRIMARY,
        )
    else:
        finished = int(recap.get("finished_count", 0) or 0)
        started = int(recap.get("started_count", 0) or 0)
        rated = int(recap.get("rated_count", 0) or 0)
        reviewed = int(recap.get("reviewed_count", 0) or 0)
        ann_word = "announcement" if total == 1 else "announcements"
        summary_line = (
            f"**{total}** {ann_word} · "
            f"🏆 **{finished}** finished · "
            f"▶️ **{started}** started · "
            f"⭐ **{rated}** rated · "
            f"📝 **{reviewed}** reviewed"
        )
        embed = discord.Embed(
            title=title[:256],
            description=clamp_escaped_text(summary_line, 4096),
            color=COLOR_GOLD,
        )

        def _format_game_ref(item: dict[str, Any]) -> str:
            safe_title = escape_display_text(str(item.get("title") or "Unknown Game"))
            slug = str(item.get("slug") or "").strip()
            if slug:
                return f"[**{safe_title}**]({build_backloggd_game_url(slug)})"
            return f"**{safe_title}**"

        top_rated = recap.get("top_rated") or []
        if top_rated:
            rated_lines = []
            for item in top_rated[:5]:
                safe_user = escape_display_text(str(item.get("username") or "Unknown"))
                rating_val = float(item.get("rating", 0.0))
                rated_lines.append(
                    f"• {_format_game_ref(item)} — **{rating_val:.1f}** / 5.0 by **{safe_user}**"
                )
            embed.add_field(
                name="⭐ Top Newly Rated",
                value=clamp_escaped_text("\n".join(rated_lines), 1024),
                inline=False,
            )

        recently_finished = recap.get("recently_finished") or []
        if recently_finished:
            fin_lines = []
            for item in recently_finished[:5]:
                safe_user = escape_display_text(str(item.get("username") or "Unknown"))
                rating_raw = item.get("rating")
                rating_suffix = f" ({float(rating_raw):.1f}/5.0)" if rating_raw is not None else ""
                fin_lines.append(f"• {_format_game_ref(item)} — **{safe_user}**{rating_suffix}")
            embed.add_field(
                name="🏆 Recently Finished",
                value=clamp_escaped_text("\n".join(fin_lines), 1024),
                inline=False,
            )

        active_players = recap.get("most_active_players") or []
        if active_players:
            player_lines = []
            for item in active_players[:5]:
                safe_user = escape_display_text(str(item.get("username") or "Unknown"))
                count = int(item.get("count", 0) or 0)
                word = "announcement" if count == 1 else "announcements"
                player_lines.append(f"• **{safe_user}** — {count} {word}")
            embed.add_field(
                name="🔥 Most Active Players",
                value=clamp_escaped_text("\n".join(player_lines), 1024),
                inline=False,
            )

    if is_digest:
        embed.set_footer(text="Automated Server Digest • Configure with /feed digest")
    else:
        embed.set_footer(text="Based on delivered Backloggd activity feed announcements")
    return embed
