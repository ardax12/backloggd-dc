"""Constructors for all Backloggd Discord Embeds."""

import re
import discord
from typing import List, Optional, Any
from bot.config import (
    COLOR_PRIMARY,
    COLOR_GOLD,
    COLOR_PLAYED,
    COLOR_PLAYING,
    COLOR_WISHLIST,
    COLOR_MUTED,
    COLOR_ERROR
)
from bot.input_validation import escape_display_text
from bot.ui.helpers import (
    rating_to_stars,
    format_number,
    format_status,
    build_backloggd_game_url,
    build_backloggd_user_url,
    clean_image_url,
    DEFAULT_BACKLOGGD_AVATAR
)

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

    if game.summary:
        # Truncate summary if too long
        summary = game.summary if len(game.summary) <= 220 else game.summary[:217] + "..."
        desc_lines.append(f"\n*{summary}*")

    embed.description = "\n".join(desc_lines)

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
        embed.add_field(name=field_name, value="\n".join(server_lines)[:1024], inline=False)
    else:
        embed.add_field(
            name=f"👥 {guild_name} Profile Ratings"[:256],
            value="*No connected Backloggd profiles have rated or logged this game yet.*",
            inline=False
        )

    if freshness_note:
        embed.add_field(name="⏱ Cache Freshness", value=freshness_note[:1024], inline=False)

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
        # Enforce character limit for bio
        if len(clean_bio) > 250:
            clean_bio = clean_bio[:247].rsplit(" ", 1)[0] + "..."
        desc_lines.append(f"> {clean_bio}")

    # Show badges count cleanly if any achievements earned
    if profile.badges:
        desc_lines.append(f"🏆 **{len(profile.badges)}** Badges Earned")

    if desc_lines:
        embed.description = "\n\n".join(desc_lines)

    # Activity Counter Fields
    played = getattr(profile, "played_count", 0) or 0
    playing = getattr(profile, "playing_count", 0) or 0
    backlog = getattr(profile, "backlog_count", 0) or 0
    wishlist = getattr(profile, "wishlist_count", 0) or 0

    embed.add_field(
        name="📊 Library Overview",
        value=(
            f"🎮 **{format_number(played)}** Played\n"
            f"▶️ **{format_number(playing)}** Playing\n"
            f"⏳ **{format_number(backlog)}** Backlog\n"
            f"💖 **{format_number(wishlist)}** Wishlist"
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
            fav_lines.append(f"**{i}.** {title}")
        embed.add_field(name="⭐ Favorite Games", value="\n".join(fav_lines)[:1024], inline=False)

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

            stars = f" — {rating_to_stars(rating)}" if rating is not None else ""
            date_suffix = f" *({date_str})*" if date_str else ""
            recent_lines.append(f"• **{title}**{stars}{replay}{date_suffix}")
        if recent_lines:
            embed.add_field(name="🕹️ Recently Played", value="\n".join(recent_lines)[:1024], inline=False)

    # 3. Latest Review (if present)
    if latest_review and getattr(latest_review, "game_title", None):
        r_title = latest_review.game_title
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

            if len(clean_text) > 200:
                clean_text = clean_text[:197].rsplit(" ", 1)[0] + "..."

            if getattr(latest_review, "has_spoilers", False):
                review_content = f"{header}\n|| > \"{clean_text}\" ||"
            else:
                review_content = f"{header}\n> \"{clean_text}\""
        else:
            review_content = header

        embed.add_field(name="📝 Latest Review", value=review_content[:1024], inline=False)

    if freshness_note:
        embed.add_field(name="⏱ Cache Freshness", value=freshness_note[:1024], inline=False)

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
        embed.add_field(name="❤️ Shared Loves (Both Rated 4.0+)", value="\n".join(lines)[:1024], inline=False)
    else:
        embed.add_field(name="❤️ Shared Loves", value="*No shared 4.0+ ratings found.*", inline=False)

    # Disagreements
    if disagreements:
        lines = []
        for g in disagreements[:5]:
            lines.append(f"• **{escape_display_text(g['title'])}** ({safe_u1}: `{g['r1']:.1f}` vs {safe_u2}: `{g['r2']:.1f}` — `Δ {g['delta']:.1f}`)")
        embed.add_field(name="⚡ Biggest Disagreements", value="\n".join(lines)[:1024], inline=False)

    if freshness_note:
        embed.add_field(name="⏱ Cache Freshness", value=freshness_note[:1024], inline=False)

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
    
    desc_lines.append(f"\n🎯 **Your Mission:** Pick one of these unrated games to play! ({remaining_backlog} games still waiting)")
    embed.description = "\n".join(desc_lines)[:4096]
    if freshness_note:
        embed.add_field(name="⏱ Cache Freshness", value=freshness_note[:1024], inline=False)
    embed.set_footer(text="Click [🔄 Reroll] below to spin again")
    return embed

def create_server_top_embed(
    title: str,
    entries: List[Any],
    mode: str,
    guild_name: str,
    freshness_note: Optional[str] = None,
) -> discord.Embed:
    """Construct embed for /server-top leaderboards."""
    embed = discord.Embed(
        title=f"🏆 {guild_name} Leaderboard — {title}"[:256],
        color=COLOR_PRIMARY
    )

    if not entries:
        embed.description = "*Not enough server data to generate this leaderboard yet.*"
        if freshness_note:
            embed.add_field(name="⏱ Cache Freshness", value=freshness_note[:1024], inline=False)
        return embed

    lines = []
    medals = ["🥇", "🥈", "🥉"]
    for i, row in enumerate(entries, 1):
        prefix = medals[i - 1] if i <= 3 else f"`#{i}`"
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

    embed.description = "\n".join(lines)[:4096]
    if freshness_note:
        embed.add_field(name="⏱ Cache Freshness", value=freshness_note[:1024], inline=False)
    embed.set_footer(text=f"Server rankings across distinct Backloggd profiles linked in {guild_name}"[:2048])
    return embed

def create_server_stats_embed(
    stats: dict,
    guild_name: str,
    freshness_note: Optional[str] = None,
) -> discord.Embed:
    """Construct embed for /server-stats overview."""
    embed = discord.Embed(
        title=f"📊 {guild_name} Gaming Stats"[:256],
        description=f"⏱ {freshness_note}"[:4096] if freshness_note else None,
        color=COLOR_PRIMARY
    )

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
    if missing_counts:
        logged_value = (
            f"At least **{stats['total_logged']}** games"
            if stats.get("logged_profiles_counted", 0) else "Unavailable"
        )
        noun = "profile count" if missing_counts == 1 else "profile counts"
        logged_value += f"\n{missing_counts} {noun} unavailable"
    else:
        logged_value = f"**{stats['total_logged']}** games"
    embed.add_field(name="📚 Games Logged", value=logged_value, inline=True)
    embed.add_field(name="⭐ Server Avg Rating", value=avg_str, inline=True)
    embed.add_field(name="👑 Most Played Game", value=str(top_game)[:1024], inline=True)

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
    embed.description = "\n".join(desc_lines)[:4096]

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
        embed.add_field(name="✨ Top Taste Soulmates", value="\n".join(twin_lines)[:1024], inline=False)
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
        embed.add_field(name="⚡ #1 Taste Nemesis (Polar Opposite)", value=nem_line[:1024], inline=False)

    if freshness_note:
        embed.add_field(name="⏱ Cache Freshness", value=freshness_note[:1024], inline=False)

    embed.set_footer(text="Based on Backloggd library ratings • Use /compare to duel with any member")
    return embed


def create_all_ratings_embed(
    game_title: str,
    game_slug: str,
    guild_name: str,
    server_ratings: List[Any],
    page: int = 1,
    per_page: int = 15
) -> discord.Embed:
    """Construct a paginated embed showing each linked profile that logged/rated a game."""
    total_ratings = len(server_ratings)
    total_pages = max(1, (total_ratings + per_page - 1) // per_page)
    current_page = max(1, min(page, total_pages))

    start_idx = (current_page - 1) * per_page
    end_idx = start_idx + per_page
    slice_ratings = server_ratings[start_idx:end_idx]

    embed = discord.Embed(
        title=f"👥 {game_title} — All {guild_name} Ratings"[:256],
        url=build_backloggd_game_url(game_slug),
        description=f"Showing **{start_idx + 1}–{min(end_idx, total_ratings)}** of **{total_ratings}** profiles who logged this game:\n",
        color=COLOR_PRIMARY
    )

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
        line = line if len(line) <= 280 else line[:277] + "..."
        if chunk and len(chunk) + 1 + len(line) > 1024:
            embed.add_field(name="Profile Ratings & Status", value=chunk, inline=False)
            chunk = ""
        chunk = f"{chunk}\n{line}" if chunk else line
    embed.add_field(name="Profile Ratings & Status", value=chunk or "*No ratings*", inline=False)
    embed.set_footer(text=f"Page {current_page} of {total_pages} • Backloggd Discord Bot")
    return embed
