from dataclasses import dataclass
import hashlib
import re
from typing import Optional
from bs4 import BeautifulSoup, Tag
from bot.ui.helpers import clean_image_url

@dataclass
class ActivityEntry:
    activity_id: str
    game_title: str
    game_slug: str
    cover_url: Optional[str]
    action_type: str  # "played", "completed", "rated", "reviewed", "backlog", "wishlist"
    action_text: str  # e.g. "played", "rated ★★★★½ (4.5)", "added to backlog"
    rating: Optional[float]
    review_notes: Optional[str]
    date_str: Optional[str]

def parse_user_activity_page(html: str, username: str) -> list[ActivityEntry]:
    """Parse Backloggd user activity page HTML and return only genuine game activities.
    
    Ignores non-game activities like badges, user follows, and likes.
    """
    try:
        soup = BeautifulSoup(html, "lxml")
    except Exception:
        soup = BeautifulSoup(html, "html.parser")

    # Target activity stream rows or cards
    rows = soup.select(".activity-row, .stream-entry, .stream-row, .card, .feed-item, .col-12")
    if not rows:
        # Fallback to any container that holds game links
        rows = soup.find_all("div", class_=lambda c: c and any(k in str(c).lower() for k in ["activity", "stream", "feed", "log"]))

    activities: list[ActivityEntry] = []
    seen_ids: set[str] = set()

    for row in rows:
        # 1. STRICT GAME OBJECT FILTER: Must contain a link to /games/{slug}/
        # This completely filters out badges (/badges/), users (/u/), follows, etc.
        game_link = row.find("a", href=lambda h: bool(h and "/games/" in h and not any(k in h for k in ["/popular", "/trending"])))
        if not game_link or not game_link.get("href"):
            continue

        raw_href = game_link["href"].strip("/")
        parts = raw_href.split("/")
        if "games" not in parts:
            continue
        slug_idx = parts.index("games") + 1
        if slug_idx >= len(parts):
            continue
        game_slug = parts[slug_idx]
        if not game_slug or game_slug in ("popular", "games", "lib"):
            continue

        # Extract Game Title
        game_title = game_link.get_text(strip=True)
        img = row.find("img")
        if not game_title and img and img.get("alt"):
            game_title = img["alt"].strip()
        if not game_title:
            title_el = row.select_one(".game-text-centered, .game-title, h3, h4")
            if title_el:
                game_title = title_el.get_text(strip=True)
        if not game_title:
            game_title = game_slug.replace("-", " ").title()

        cover_url = clean_image_url(img.get("src") if img else None)

        # 2. Extract Rating if present
        rating: Optional[float] = None
        rated_el = row.find(attrs={"data-rating": True})
        if rated_el and rated_el.get("data-rating"):
            try:
                rating = float(rated_el["data-rating"]) / 2.0
            except (ValueError, TypeError):
                pass
        if rating is None:
            stars_top = row.select_one(".stars-top")
            if stars_top and "width:" in stars_top.get("style", ""):
                try:
                    w = stars_top["style"].split("width:")[1].split("%")[0].strip()
                    rating = float(w) / 20.0
                except (ValueError, IndexError, TypeError):
                    pass

        # 3. Detect Action Type & Action Text
        row_text = row.get_text(separator=" ", strip=True).lower()
        action_type = "played"
        action_text = "played"

        if "review" in row_text or row.select_one(".review-text, .card-text"):
            action_type = "reviewed"
            action_text = "reviewed"
        elif "backlog" in row_text:
            action_type = "backlog"
            action_text = "added to backlog"
        elif "wishlist" in row_text:
            action_type = "wishlist"
            action_text = "wishlisted"
        elif "completed" in row_text or "finished" in row_text:
            action_type = "completed"
            action_text = "completed"
        elif "playing" in row_text:
            action_type = "playing"
            action_text = "started playing"
        elif rating is not None:
            action_type = "rated"
            action_text = f"rated {rating:.1f}★"

        # 4. Review text or notes if user wrote thoughts
        review_notes = None
        body_div = row.select_one(".review-text, .card-text, .body-text, .review-body")
        if body_div:
            review_notes = body_div.get_text(separator=" ", strip=True)

        # 5. Date / Relative time
        date_str = None
        time_el = row.find(["time", "span"], class_=lambda c: c and any(k in str(c).lower() for k in ["time", "date", "ago"]))
        if time_el:
            date_str = time_el.get_text(strip=True)

        # 6. Activity ID for Deduplication
        act_id = row.get("data-activity-id") or row.get("data-log-id") or row.get("id")
        if not act_id:
            # Deterministic hash to prevent duplicate deliveries
            hash_input = f"{username.lower()}:{game_slug}:{action_type}:{rating}:{date_str}"
            act_id = "act_" + hashlib.sha256(hash_input.encode("utf-8")).hexdigest()[:16]

        if act_id in seen_ids:
            continue
        seen_ids.add(act_id)

        activities.append(
            ActivityEntry(
                activity_id=act_id,
                game_title=game_title,
                game_slug=game_slug,
                cover_url=cover_url,
                action_type=action_type,
                action_text=action_text,
                rating=rating,
                review_notes=review_notes,
                date_str=date_str
            )
        )

    return activities
