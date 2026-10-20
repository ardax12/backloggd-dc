"""Runtime HTML parser patches for backloggd-py to support rich profile metadata."""

import re
import logging
from typing import Optional
from bs4 import BeautifulSoup, Tag
from backloggd.models.game import GameEntry
from backloggd.models.user import UserProfile
from backloggd.models.review import Review
import backloggd.parsers.profile
import backloggd.parsers.reviews
import backloggd.client

logger = logging.getLogger("backloggd.patches")

def patched_parse_user_profile_page(html: str, username: str) -> UserProfile:
    """Enhanced parser for Backloggd user profile pages with comprehensive avatar and bio support."""
    try:
        soup = BeautifulSoup(html, "lxml")
    except Exception:
        soup = BeautifulSoup(html, "html.parser")

    # 1. Avatar URL: check OpenGraph/Twitter meta tags first (Backloggd standard), then HTML selectors
    avatar_url: Optional[str] = None
    og_image = soup.select_one("meta[property='og:image'], meta[name='og:image'], meta[property='twitter:image']")
    if og_image and og_image.get("content"):
        content_url = og_image["content"].strip()
        if content_url and not any(k in content_url for k in ["no_avatar.jpg", "no_avatar.png", "default_avatar"]):
            if content_url.startswith("//"):
                content_url = f"https:{content_url}"
            elif content_url.startswith("/"):
                content_url = f"https://backloggd.com{content_url}"
            avatar_url = content_url

    if not avatar_url:
        avatar_selectors = [
            "#profile-header img",
            "img.profile-avatar",
            ".profile-avatar img",
            ".user-avatar img",
            "img.user-avatar",
            ".avatar img",
            "img.avatar",
            "[class*='avatar'] img",
            "img[class*='avatar']",
            "img[src*='avatar']",
            "img[src*='avatars']",
            "img[src*='backloggd-avatars']",
            ".profile-pic img",
            "img.profile-pic",
            "img[alt*='avatar']",
            "img[alt*='profile']",
            ".user-details img"
        ]
        for selector in avatar_selectors:
            avatar_img = soup.select_one(selector)
            if avatar_img:
                src = avatar_img.get("src") or avatar_img.get("data-src") or avatar_img.get("data-original")
                if src and not any(k in src for k in ["no_avatar.jpg", "no_avatar.png", "default_avatar"]):
                    if src.startswith("//"):
                        src = f"https:{src}"
                    elif src.startswith("/"):
                        src = f"https://backloggd.com{src}"
                    avatar_url = src
                    break

        if not avatar_url:
            avatar_url = "https://backloggd.b-cdn.net/no_avatar.jpg"

    # 2. Bio text: check #bio-body first (Backloggd DOM container), then other profile bio selectors
    bio: Optional[str] = None
    bio_selectors = [
        "#bio-body",
        "#profile-bio",
        "#bio",
        ".profile-bio",
        ".user-bio",
        "p.bio",
        ".profile-description",
        "#profile-description",
        ".user-description",
        "#user-bio",
        ".bio-content",
        ".about-me",
        "#about-me",
        ".profile-about",
        "#profile-header p",
        ".user-details p",
        "div[class*='bio']",
        "p[class*='bio']"
    ]
    for selector in bio_selectors:
        bio_el = soup.select_one(selector)
        if bio_el:
            # Replace line breaks and paragraph tags with spaces so words never combine
            for br in bio_el.find_all(["br", "p", "div"]):
                br.insert_after(" ")
            raw_text = bio_el.get_text()
            if raw_text:
                # Normalize spaces and fix combined words
                cleaned = re.sub(r"[\r\n]+", " ", raw_text)
                cleaned = re.sub(r"([a-zA-Z0-9]),([a-zA-Z])", r"\1, \2", cleaned)
                cleaned = re.sub(r"([a-z0-9])([A-Z])", r"\1 \2", cleaned)
                cleaned = re.sub(r"\s+", " ", cleaned).strip()
                if cleaned and cleaned.lower() not in ["nothing here!", "nothing here."] and not any(k in cleaned.lower() for k in ["edit profile", "settings", "followers", "following"]):
                    bio = cleaned
                    break

    # 3. Badges
    badges: list[str] = []
    badge_imgs = soup.select("img[src*='badges'], .badge img, [class*='badge'] img")
    for b in badge_imgs:
        src = b.get("src", "")
        if "badge" in src:
            fname = src.split("/")[-1].split(".")[0].replace("replay-", "replay_").replace("goty-", "goty_")
            if fname and fname not in badges:
                badges.append(fname)

    # 4. Favorite Games
    favorite_games: list[GameEntry] = []
    fav_section = soup.select_one("#profile-favorites, .favorite-games, #favorites")
    if fav_section:
        covers = fav_section.select(".game-cover")
        for c in covers:
            img = c.select_one("img")
            title = img.get("alt", "").strip() if img else ""
            cover_url = img.get("src") if img else None
            game_id = c.get("game_id")

            link = c.find_parent("a", href=lambda h: bool(h and "/games/" in h)) or c.find("a", href=lambda h: bool(h and "/games/" in h))
            slug = ""
            if link and link.get("href"):
                slug = link["href"].strip("/").split("/")[-1]
            elif title:
                slug = title.lower().replace(" ", "-").replace(":", "").replace("'", "")

            if title and slug and slug not in {g.slug for g in favorite_games}:
                favorite_games.append(
                    GameEntry(
                        slug=slug,
                        title=title,
                        cover_url=cover_url,
                        game_id=game_id,
                    )
                )

    # 5. Quick Counts from header/nav
    played_count: Optional[int] = None
    playing_count: Optional[int] = None
    backlog_count: Optional[int] = None
    wishlist_count: Optional[int] = None

    clean_user = username.lower().strip()
    for a in soup.find_all("a", href=lambda h: bool(h and f"/u/{clean_user}/" in h.lower())):
        href = a.get("href", "").lower()
        num_match = re.search(r"(\d+[\d,]*)", a.get_text(separator=" ", strip=True))
        if not num_match:
            continue
        count_val = int(num_match.group(1).replace(",", ""))

        if "/playing" in href and playing_count is None:
            playing_count = count_val
        elif "/backlog" in href and backlog_count is None:
            backlog_count = count_val
        elif "/wishlist" in href and wishlist_count is None:
            wishlist_count = count_val
        elif "/games" in href and played_count is None and not any(k in href for k in ["backlog", "wishlist", "playing"]):
            played_count = count_val

    return UserProfile(
        username=username,
        avatar_url=avatar_url,
        bio=bio,
        favorite_games=favorite_games,
        badges=badges,
        short_url=None,
        played_count=played_count,
        playing_count=playing_count,
        backlog_count=backlog_count,
        wishlist_count=wishlist_count,
    )

def patched_parse_review_card(
    card: Tag,
    default_author: Optional[str] = None,
    default_game_slug: str = "",
    default_game_title: str = "",
) -> Optional[Review]:
    """Enhanced review card parser preserving whitespace and spacing between paragraphs and line breaks."""
    game_a = card.select_one(".game-name a, h3 a, a[href*='/games/']")
    if game_a:
        game_title = game_a.get_text(strip=True)
        slug = game_a.get("href", "").strip("/").split("/")[-1]
    else:
        game_title = default_game_title
        slug = default_game_slug

    if not slug and not game_title:
        return None

    author = default_author or ""
    author_a = card.find("a", href=lambda h: bool(h and "/u/" in h and "/review/" not in h))
    if author_a and author_a.get_text(strip=True):
        author = author_a.get_text(strip=True)

    review_link = card.find("a", href=lambda h: bool(h and "/review/" in h))
    review_id = ""
    if review_link and review_link.get("href"):
        match = re.search(r"/review/(\d+)/", review_link["href"])
        if match:
            review_id = match.group(1)

    rating: Optional[float] = None
    stars_top = card.select_one(".stars-top")
    if stars_top and "width:" in stars_top.get("style", ""):
        try:
            w = stars_top["style"].split("width:")[1].split("%")[0].strip()
            rating = float(w) / 20.0
        except (ValueError, IndexError):
            pass

    # Review written text with proper whitespace handling
    review_text = ""
    body_div = card.select_one(".review-text, .card-text, .body-text, .review-body")
    if body_div:
        for el in body_div.find_all(["br", "p", "div"]):
            el.insert_after(" ")
        raw = body_div.get_text()
        review_text = re.sub(r"[\r\n]+", " ", raw)
        review_text = re.sub(r"([a-zA-Z0-9]),([a-zA-Z])", r"\1, \2", review_text)
        review_text = re.sub(r"([a-z0-9])([A-Z])", r"\1 \2", review_text)
        review_text = re.sub(r"\s+", " ", review_text).strip()
    else:
        for p in card.find_all("p"):
            for el in p.find_all(["br"]):
                el.insert_after(" ")
            txt = p.get_text()
            txt = re.sub(r"[\r\n]+", " ", txt)
            txt = re.sub(r"([a-zA-Z0-9]),([a-zA-Z])", r"\1, \2", txt)
            txt = re.sub(r"([a-z0-9])([A-Z])", r"\1 \2", txt)
            txt = re.sub(r"\s+", " ", txt).strip()
            if len(txt) > 20 and not any(k in txt for k in ["Edit log", "Completed", "Likes"]):
                review_text = txt
                break

    like_count = 0
    like_el = card.find(string=re.compile(r"(\d+)\s+Like"))
    if like_el:
        match = re.search(r"(\d+)", like_el)
        if match:
            like_count = int(match.group(1))

    created_at = None
    date_el = card.find(string=re.compile(r"(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+\d{1,2},\s+\d{4}"))
    if date_el:
        created_at = date_el.strip()

    has_spoilers = bool(card.select_one(".spoiler, .has-spoilers, [class*='spoiler']"))

    return Review(
        review_id=review_id,
        game_title=game_title,
        game_slug=slug,
        author=author,
        rating=rating,
        review_text=review_text,
        created_at=created_at,
        like_count=like_count,
        has_spoilers=has_spoilers,
    )

def apply_patches():
    """Apply monkey-patches to backloggd-py parsers."""
    backloggd.parsers.profile.parse_user_profile_page = patched_parse_user_profile_page
    backloggd.client.parse_user_profile_page = patched_parse_user_profile_page
    backloggd.parsers.reviews.parse_review_card = patched_parse_review_card
    logger.info("Successfully applied backloggd-py parser enhancements.")
