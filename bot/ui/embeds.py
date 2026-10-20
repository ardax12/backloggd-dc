"""Constructors for all Backloggd Discord Embeds."""

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
from bot.ui.helpers import (
    rating_to_stars,
    format_number,
    format_status,
    build_backloggd_game_url,
    build_backloggd_user_url
)

def create_game_embed(
    game: Any,
    server_ratings: List[Any],
    guild_name: Optional[str] = "Server"
) -> discord.Embed:
    """Construct embed for /game command with server member ratings shelf."""
    game_url = build_backloggd_game_url(game.slug)
    title = f"{game.title} ({game.release_year})" if game.release_year else game.title
    
    embed = discord.Embed(
        title=title,
        url=game_url,
        color=COLOR_PRIMARY
    )

    if game.cover_url:
        embed.set_thumbnail(url=game.cover_url)

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
        stats_text = (
            f"🎮 **{format_number(game.stats.played_count)}** Played  •  "
            f"▶️ **{format_number(game.stats.playing_count)}** Playing  •  "
            f"⏳ **{format_number(game.stats.backlog_count)}** Backlog  •  "
            f"💖 **{format_number(game.stats.wishlist_count)}** Wishlist"
        )
        embed.add_field(name="Global Stats", value=stats_text, inline=False)

    # Server Member Ratings Field
    if server_ratings:
        server_lines = []
        for r in server_ratings[:8]:  # Show top 8
            username = r["backloggd_username"]
            status_str = format_status(r["status"])
            if r["rating"] is not None:
                stars = rating_to_stars(r["rating"])
                server_lines.append(f"• **{username}**: {stars} (`{r['rating']:.1f}`) — {status_str}")
            else:
                server_lines.append(f"• **{username}**: {status_str}")

        if len(server_ratings) > 8:
            server_lines.append(f"*+ {len(server_ratings) - 8} more server members*")

        field_name = f"👥 {guild_name} Ratings ({len(server_ratings)})"
        embed.add_field(name=field_name, value="\n".join(server_lines), inline=False)
    else:
        embed.add_field(
            name=f"👥 {guild_name} Ratings",
            value="*No server members have rated or logged this game yet.*",
            inline=False
        )

    embed.set_footer(text="Data from Backloggd • Use /link to connect your profile")
    return embed

def create_profile_embed(profile: Any, stats: Optional[Any] = None) -> discord.Embed:
    """Construct embed for /profile command."""
    user_url = build_backloggd_user_url(profile.username)
    badge_str = f" [{' | '.join(profile.badges)}]" if profile.badges else ""

    embed = discord.Embed(
        title=f"{profile.username}{badge_str}",
        url=user_url,
        color=COLOR_PRIMARY
    )

    if profile.avatar_url:
        embed.set_thumbnail(url=profile.avatar_url)

    if profile.bio:
        embed.description = f"> {profile.bio}"

    # Activity Counter Fields
    played = profile.played_count or 0
    playing = profile.playing_count or 0
    backlog = profile.backlog_count or 0
    wishlist = profile.wishlist_count or 0

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

    # Average rating if computed or present in stats
    if stats and stats.average_rating is not None:
        embed.add_field(
            name="⭐ Average Rating",
            value=f"{rating_to_stars(stats.average_rating)}\n**{stats.average_rating:.2f}** / 5.0\n*({stats.rated_count} rated games)*",
            inline=True
        )

    # Pinned Favorite Games
    if profile.favorite_games:
        fav_lines = []
        for i, fav in enumerate(profile.favorite_games[:4], 1):
            title = getattr(fav, "title", str(fav))
            fav_lines.append(f"**{i}.** {title}")
        embed.add_field(name="⭐ Favorite Games", value="\n".join(fav_lines), inline=False)

    embed.set_footer(text="Backloggd Profile Overview")
    return embed

def create_compare_embed(
    user1: str,
    user2: str,
    match_score: float,
    shared_loves: List[dict],
    disagreements: List[dict]
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

    embed = discord.Embed(
        title=f"⚔️ Taste Duel: {user1} vs {user2}",
        description=f"## {score_pct}% Taste Match\n**{verdict}**",
        color=color
    )

    # Shared Loves
    if shared_loves:
        lines = []
        for g in shared_loves[:5]:
            lines.append(f"• **{g['title']}** ({user1}: `{g['r1']:.1f}` | {user2}: `{g['r2']:.1f}`)")
        embed.add_field(name="❤️ Shared Loves (Both Rated 4.0+)", value="\n".join(lines), inline=False)
    else:
        embed.add_field(name="❤️ Shared Loves", value="*No shared 4.0+ ratings found.*", inline=False)

    # Disagreements
    if disagreements:
        lines = []
        for g in disagreements[:5]:
            lines.append(f"• **{g['title']}** ({user1}: `{g['r1']:.1f}` vs {user2}: `{g['r2']:.1f}` — `Δ {g['delta']:.1f}`)")
        embed.add_field(name="⚡ Biggest Disagreements", value="\n".join(lines), inline=False)

    embed.set_footer(text="Calculated using Backloggd library similarity metrics")
    return embed

def create_roulette_embed(username: str, game: Any, remaining_backlog: int) -> discord.Embed:
    """Construct embed for /roulette backlog pick."""
    title = f"{game.title} ({game.release_year})" if getattr(game, "release_year", None) else game.title
    game_url = build_backloggd_game_url(game.slug)

    embed = discord.Embed(
        title=f"🎲 Backlog Roulette for {username}",
        url=game_url,
        color=COLOR_GOLD
    )

    if getattr(game, "cover_url", None):
        embed.set_thumbnail(url=game.cover_url)

    desc_lines = [
        f"### [{title}]({game_url})"
    ]
    if getattr(game, "community_rating", None):
        stars = rating_to_stars(game.community_rating)
        desc_lines.append(f"{stars} **{game.community_rating:.1f}** / 5.0")
    
    desc_lines.append(f"\n🎯 **Your Mission:** Time to take this off your backlog! ({remaining_backlog} games still waiting)")
    embed.description = "\n".join(desc_lines)
    embed.set_footer(text="Click [🔄 Reroll] below to spin again")
    return embed

def create_server_top_embed(
    title: str,
    entries: List[Any],
    mode: str,
    guild_name: str
) -> discord.Embed:
    """Construct embed for /server-top leaderboards."""
    embed = discord.Embed(
        title=f"🏆 {guild_name} Leaderboard — {title}",
        color=COLOR_PRIMARY
    )

    if not entries:
        embed.description = "*Not enough server data to generate this leaderboard yet.*"
        return embed

    lines = []
    medals = ["🥇", "🥈", "🥉"]
    for i, row in enumerate(entries, 1):
        prefix = medals[i - 1] if i <= 3 else f"`#{i}`"
        game_title = row["title"]
        slug = row["slug"]
        url = build_backloggd_game_url(slug)

        if mode == "highest_rated":
            avg = row["avg_rating"]
            count = row["rating_count"]
            lines.append(f"{prefix} **[{game_title}]({url})** — ★ **{avg:.2f}** *({count} server ratings)*")
        elif mode == "most_played":
            count = row["play_count"]
            lines.append(f"{prefix} **[{game_title}]({url})** — 🎮 **{count}** completed")
        elif mode == "shame_list":
            count = row["backlog_count"]
            lines.append(f"{prefix} **[{game_title}]({url})** — ⏳ in **{count}** members' backlogs")
        elif mode == "polarizing":
            delta = row["rating_delta"]
            avg = row["avg_rating"]
            lines.append(f"{prefix} **[{game_title}]({url})** — ⚡ **Δ {delta:.1f} gap** *(avg: ★ {avg:.2f})*")
        elif mode == "hidden_gems":
            avg = row["avg_rating"]
            count = row["rating_count"]
            lines.append(f"{prefix} **[{game_title}]({url})** — 💎 ★ **{avg:.2f}** *({count} server player{'s' if count > 1 else ''})*")
        else:
            lines.append(f"{prefix} **[{game_title}]({url})**")

    embed.description = "\n".join(lines)
    embed.set_footer(text=f"Server rankings across linked {guild_name} members")
    return embed

def create_server_stats_embed(stats: dict, guild_name: str) -> discord.Embed:
    """Construct embed for /server-stats overview."""
    embed = discord.Embed(
        title=f"📊 {guild_name} Gaming Stats",
        color=COLOR_PRIMARY
    )

    avg_str = f"⭐ **{stats['server_avg_rating']:.2f}** / 5.0" if stats.get("server_avg_rating") else "*N/A*"
    top_game = stats.get("top_game") or "*N/A*"

    embed.add_field(name="👥 Linked Members", value=f"**{stats['linked_members']}** members", inline=True)
    embed.add_field(name="📚 Games Logged", value=f"**{stats['total_logged']}** games", inline=True)
    embed.add_field(name="🎮 Games Completed", value=f"**{stats['total_played']}** played", inline=True)
    embed.add_field(name="⭐ Server Avg Rating", value=avg_str, inline=True)
    embed.add_field(name="👑 Most Played Game", value=top_game, inline=True)

    embed.set_footer(text="Community aggregate data from linked Backloggd profiles")
    return embed
