# backloggd-dc

[![Discord.py](https://img.shields.io/badge/discord.py-v2.7.1-5865F2.svg)](https://github.com/Rapptz/discord.py)
[![Python 3.10+](https://img.shields.io/badge/python-3.10+-3776ab.svg)](https://www.python.org/)
[![Powered by backloggd-py](https://img.shields.io/badge/powered%20by-backloggd--py-8E61F2.svg)](https://github.com/ardax12/backloggd-matcher)
[![License: MIT](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)
[![Docker Ready](https://img.shields.io/badge/docker-ready-blue.svg)](Dockerfile)

A modern, high-performance Discord bot that integrates [Backloggd](https://backloggd.com) profiles and video game tracking into Discord servers.

Designed for gaming communities and friend groups: search games with live autocomplete, inspect your server's collective ratings, run taste-matching duels, break backlog paralysis with interactive roulette, and stream playthrough activity automatically (Filmlinkd-style).

---

## Highlights

- **Fast & Resource-Efficient**: Built with native Discord Embeds and `aiosqlite`. Zero headless browser overhead and zero GPU image generation dependencies. Idles at ~35MB RAM, ensuring compatibility with free-tier hosting platforms (Render, Fly.io, Railway, Oracle Cloud).
- **Collective Server Ratings**: When looking up a game with `/game`, view every linked member's individual score and play status directly within the server shelf.
- **Live Autocomplete Search**: Interactive search choices query Backloggd in real time as you type, complete with an in-memory debounce cache.
- **Taste Compatibility Duels**: Cross-references two user libraries using Bayesian correlation and overlap scoring to reveal match percentages, shared 4.0+ favorites, and largest rating disagreements.
- **Interactive Backlog Roulette**: Picks a random game from your unplayed backlog and includes an in-place `[ Reroll ]` button to re-spin without re-running commands.
- **Server-Sourced Recommendations**: Finds your taste matches across your Discord server and recommends games they rated 4.0+ that you have not played yet.
- **Specialty Leaderboards**: 5 distinct server leaderboard modes including *Highest Rated*, *Most Played*, *Most Polarizing*, *Hidden Gems*, and the *Server Backlog Shame List*.
- **Automated Activity Feed**: An automated background worker monitors linked journals and broadcasts newly logged playthroughs, star ratings, and review thoughts to your designated channel.

---

## Command Reference

| Command | Description |
| :--- | :--- |
| `/game <title>` | Search any game with live autocomplete. Displays community score, global statistics, distinct-profile Server Ratings Shelf, and aggregate cache freshness. |
| `/profile [user]` | View user profile card, bio quote, library metrics (*Played, Playing, Backlog, Wishlist*), 4 pinned favorites, and library cache age. |
| `/compare <user1> [user2]` | Compare video game tastes between two members (or yourself vs another member), using distinct profiles and serving cached data promptly with background refresh when stale. |
| `/roulette [user]` | Pick a random title from unrated `backlog` / `wishlist` titles with an interactive `[ Reroll ]` button and freshness disclosure. |
| `/recommend [user]` | Get game recommendations from distinct taste-matched profiles (numeric ratings determine similarity; `played`, `playing`, and rated titles are excluded). |
| `/server-top [mode]` | Distinct-profile server rankings: `Highest Rated`, `Most Played`, `Most Polarizing`, `Hidden Gems`, `Server Shame List`. |
| `/server-stats` | Community metrics and cache age. Games Logged sums Played-tab totals across distinct linked Backloggd profiles; duplicate links count once. |
| `/link <username>` | Connect your Discord account to a public Backloggd profile (nonexclusive; server-scoped duplicate notice only; never exposes other servers or DMs), schedule a shared sync, and enable local feed participation when run in a server. |
| `/unlink` | Disconnect only your personal Backloggd link and server enrollments without erasing shared caches, other connected members, or archived feed posts. |
| `/refresh [user]` | Request an immediate on-demand library sync (bypasses automatic cooldown/backoff and coalesces concurrent duplicate requests). |
| `/feed on` | Enable or re-enable activity feed announcements for your currently `/link`ed profile in this server only. |
| `/feed off` | Opt out of member feed announcements in this server (notes when another member or admin follow still keeps the profile active). |
| `/feed status` | Inspect your effective feed eligibility, active participation source (`member`, `admin`, `both`), channel, pending baseline, source health/gaps, and suppression counts (ephemeral; hides review text and other servers). |
| `/feed add <username>` | Follow any public Backloggd profile in this server's feed independently of personal `/link`s *(Manage Channels required)*. |
| `/feed remove <username>` | Remove an admin public-profile follow without altering personal `/link`s or other members' feed enrollments *(Manage Channels required)*. |
| `/feed list` | Paginated server list of enrolled and admin-followed feed profiles with participation sources and effective/pending states *(Manage Channels required)*. |
| `/setchannel <channel>` | Designate a channel for automatic playthrough journal broadcasts and initialize required baselines *(Manage Channels required)*. |
| `/unsetchannel` | Pause automatic journal broadcasts for the server while preserving subscriptions and archived message mappings *(Manage Channels required)*. |

---

## Profile Connections, Feeds & Freshness Behavior

- **Nonexclusive public profile connections**: There is no ownership verification. Multiple Discord members can `/link` to the same Backloggd username (normalized case-insensitively by `profile_key`). Duplicate notices mention only other connected members currently present in that server and never appear in DMs. Server ratings shelves, `/server-top`, `/server-stats`, `/taste-twins`, and `/recommend` count each distinct profile once, attribute profiles canonically, and never treat two Discord accounts sharing the same profile as taste twins.
- **Server Games Logged**: This is the sum of each present linked profile's Played-tab total, obtained through the SDK's public profile API, with profile counts cached for up to one hour and at most two concurrent profile fetches. It is not the number of cached library rows or dated sessions. The same game contributes once for each distinct profile that includes it. Missing counts are disclosed as unavailable or as a partial lower bound; they are never silently treated as zero. The separate Games Completed field has been removed.
- **Per-server feed enrollment & preference isolation**: Feed preferences are keyed by `(guild_id, discord_id)` and default to **off**. Running `/link` inside a server or running `/feed on` explicitly enrolls (or re-enrolls) that server only; linking in a DM, passively joining another server, or configuring a channel in another guild enrolls nobody. Preferences persist across `/unlink`, relinks, departures, and `/unsetchannel` without resetting other servers.
- **Independent admin follows (`/feed add|remove|list`)**: Server admins (`Manage Channels`) can follow any public Backloggd profile directly. Admin follows and member `/feed on` participation operate independently: a member running `/feed off` never deletes an admin follow, and `/feed remove` never removes member enrollments or personal links. Profiles followed solely via admin follow never enter server statistics, leaderboards, `/compare`, `/taste-twins`, or `/recommend`, and do not trigger unnecessary full-library syncs.
- **One-time legacy grandfathering exception**: Upgrades from pre-v3 schemas preserve existing feed participation in servers that already had a feed channel configured once Discord provides a complete initial member snapshot (respecting any explicit `/feed off` choice). New joins, later `/link`s in other guilds, or newly configured feed channels are never grandfathered automatically.
- **Seven-day cutoff & conservative edit reconciliation**: Newly discovered `Started`, `Finished`, or replay milestones older than 7 UTC calendar days at discovery (and future-dated or uncertain sessions) are suppressed, while new explicit ratings and written reviews remain eligible independently without being discarded alongside an old milestone. Ambiguous edits (such as same-game updates lacking strong persisted aliases or stable IDs) are held rather than duplicating announcements.
- **Archived Discord posts**: Removing entries on Backloggd, opting out, or unlinking does not delete already posted Discord announcements. Confirmed source updates edit existing posts in place; manual Discord deletion (`NotFound`) records permanent suppression for that event so future updates never repost it.
- **On-use library freshness & shared sync**: Social/discovery/stats commands (`/profile`, `/game`, `/compare`, `/taste-twins`, `/roulette`, `/recommend`, `/server-top`, `/server-stats`) treat cached libraries older than 1 hour as stale, serve cached results immediately with Discord `<t:unix:R>` timestamps (or disclose that a missing subscribed cache is currently being fetched instead of claiming an empty library), and coalesce background syncs (2 workers, 5-minute automatic debounce/failure backoff). Aggregates disclose the oldest contributing cache and stale/missing counts.
- **Bounded activity recovery & source limits**: Activity feeds poll once per distinct profile per cycle across servers and consume bounded snapshots (`max_pages=5`, stopping at known boundary IDs when supported by the SDK). With published `backloggd-py` `0.2.0`, activity polling falls back to `get_user_activity()` (`coverage="unsupported"`) covering only the latest activity page; bounded pagination (`boundary_reached` / `exhausted` / `limited`) applies when an updated SDK exposes `get_user_activity_snapshot()`. Unresolved source outage gaps are tracked independently from library cache freshness and never cleared by a newest-page success alone until continuity is re-established.

---

## Quickstart

### 1. Prerequisites
- Python 3.10+
- A Discord Bot Token from the [Discord Developer Portal](https://discord.com/developers/applications)

### 2. Configure Gateway Intents
In the Discord Developer Portal, navigate to your application -> **Bot** and enable:
- **Server Members Intent** *(needed to match server members)*
- **Message Content Intent**

### 3. Installation

```bash
# Clone the repository
git clone https://github.com/YOUR_USERNAME/backloggd-dc.git
cd backloggd-dc

# Create and activate virtual environment
python -m venv .venv
# On Windows:
.venv\Scripts\activate
# On Linux / macOS:
source .venv/bin/activate

# Install dependencies
pip install -r requirements.txt
```

### 4. Configuration

Copy `.env.example` to `.env`:
```bash
cp .env.example .env
```

Configure your secrets in `.env`:
```env
DISCORD_TOKEN=your_bot_token_here

# Optional: Add your test Discord server ID for instant slash command sync
DEV_GUILD_ID=123456789012345678

# Database path (default: data/backloggd.db)
DATABASE_PATH=data/backloggd.db
```

### 5. Run the Bot

```bash
python main.py
```

---

## Database Migrations, Backup & Rollback (Schema v1–v7)

Schema upgrades in `bot/db_migrations.py` run atomically inside a transaction on startup (supporting both local SQLite and Turso/libSQL via `schema_migrations`):

- **v1**: Normalizes `profiles` and `profile_ratings`, removes legacy unique-username constraints on `linked_users`, and exposes a compatibility `user_ratings` view.
- **v2**: Moves feed baselines, deliveries, and message mappings onto `(guild_id, profile_key)` (`profile_feed_sources`, `profile_feed_messages`, `profile_seen_journal_*`) while keeping legacy tables for audit.
- **v3**: Adds `guild_feed_preferences`, `guild_profile_feed_activity`, and one-time legacy reconciliation tables (`guild_feed_legacy_scope`, `guild_feed_legacy_migrations`).
- **v4**: Adds `guild_admin_follows` for independent admin public-profile follows.
- **v5**: Adds `profile_feed_decisions` to persist 7-day/future/uncertain/ambiguity decisions.
- **v6**: Adds `profile_feed_source_health` for separate source attempts, coverage, boundaries, and unresolved gaps.
- **v7**: Adds `last_sync_attempt_at` / `last_sync_error` on `profiles`, `unresolved_gap_until` on `profile_feed_source_health`, and `profile_activity_observations` / `profile_activity_observation_sources`.

### Backup & Rollback Procedure

1. **Always back up before upgrading** *(required for both SQLite and Turso)*:
   - **Local SQLite**: Stop the bot cleanly and copy the database file together with any `-wal` / `-shm` files:
     ```bash
     cp data/backloggd.db data/backloggd.db.bak
     ```
   - **Turso / libSQL**: Export a logical dump or create a database snapshot/branch before starting the upgraded bot binary.
2. **Rolling back to an earlier release**:
   - Because v1–v7 rebuild `linked_users` / `user_ratings` and add new normalized tables (`profiles`, `profile_ratings`, `guild_feed_*`, `profile_feed_*`), simply downgrading the application code while leaving a v7 database in place is **not** supported.
   - To roll back safely: stop the bot, restore the pre-upgrade SQLite backup file (or restore the Turso snapshot/branch from step 1), verify `SELECT MAX(version) FROM schema_migrations;` matches the target release, and restart the previous bot version.

---

## Docker Deployment

The bot includes a multi-stage, production-ready `Dockerfile` and `.dockerignore`:

```bash
# Build the Docker image
docker build -t backloggd-dc .

# Run with persistent SQLite volume
docker run -d \
  --name backloggd-dc \
  --restart always \
  -v $(pwd)/data:/app/data \
  -e DISCORD_TOKEN="your_bot_token_here" \
  backloggd-dc
```

---

## Architecture & Codebase Layout

```text
backloggd-dc/
├── bot/
│   ├── client.py          # Custom Discord Bot class, lifecycle hooks, shared LibrarySyncService
│   ├── config.py          # Bot settings and embed color constants
│   ├── db.py              # Asynchronous SQLite/libSQL database manager
│   ├── db_migrations.py   # Atomic versioned schema migrations (v1-v7)
│   ├── feed_models.py     # Normalized feed event models and parsing
│   ├── feed_rules.py      # Action classification, journal normalization, conservative merging
│   ├── feed_policy.py     # 7-day cutoff, future/uncertain handling, and ambiguity policy
│   ├── services/
│   │   ├── library_sync.py# Coalesced per-profile background library sync coordinator
│   │   └── freshness.py   # 1-hour cache freshness checks and Discord timestamp summaries
│   ├── ui/
│   │   ├── embeds.py      # Native Discord Embed builders with freshness disclosures & size clamps
│   │   ├── views.py       # Interactive buttons & paginators (Reroll, Ratings, FeedListView, Link)
│   │   ├── feed_embed.py  # Automated journal announcement embed
│   │   └── helpers.py     # Star formatters, number formatters, URLs
│   └── cogs/
│       ├── account.py     # /link, /unlink, /refresh
│       ├── games.py       # /game (autocomplete + distinct-profile ratings shelf)
│       ├── users.py       # /profile
│       ├── social.py      # /compare, /taste-twins
│       ├── discovery.py   # /roulette, /recommend
│       ├── stats.py       # /server-top, /server-stats
│       └── feed.py        # /feed on|off|status|add|remove|list, /setchannel, /unsetchannel, poller
├── tests/                 # Unit, migration, freshness, policy, feed pipeline, and integration tests
├── data/                  # SQLite database directory (gitignored)
├── Dockerfile             # Production container build
├── main.py                # Application entrypoint
├── requirements.txt       # Python package requirements
└── verify_all.py          # Comprehensive verification & smoke test script
```

---

## Verification & Testing

The repository includes an offline test suite covering migrations, nonexclusive profile deductions, feed eligibility/policies/recovery, stale-cache freshness, embed/UI regressions, architecture boundaries, and combined integration stories without requiring an active Discord token:

```bash
python verify_all.py
python -m unittest discover -s tests -v
```

Both checks run offline with temporary databases and injected mocks/clocks; verification does not use configured Turso credentials, live Discord sends, or live scraping.

Activity feeds read activity and journal independently. `Started`/`Finished` milestones, explicit ratings and rating changes, written reviews, and identifiable replays are announced. Ordinary daily sessions, backlog/wishlist changes, likes, follows, badges, comments, and lists are hidden. A rating displayed beside a completion is included in that announcement; it does not generate a second rating event.

Journal dates, `Started`/`Finished` labels, replay flags, and available platforms enrich matching announcements. A unique game/action/calendar-date pair can connect an activity milestone to a journal row; ambiguous same-day sessions remain separate. Reviews and standalone ratings combine with milestones only through an exact timestamp or numeric permalink. Source aliases and Discord message IDs persist so later verified information edits existing announcements across polls and restarts. Backdated milestones retain `Started`/`Finished` verbs in their titles and display their played date when within the 7-day discovery policy.

Public journal rows expose no unique session IDs, and activity rows may also lack immutable event IDs. Suppressing ambiguous edits and uncertain timestamps deliberately trades coverage for avoiding duplicate Discord posts.

The private "View All Ratings" response and `/feed list` paginate cleanly using timeout cleanup while clamping titles, descriptions, and fields to Discord embed limits.

---

## Acknowledgements

- Built on top of [backloggd-py](https://github.com/ardax12/backloggd-matcher) for Backloggd scraping and taste similarity algorithms.
- Inspired by Letterboxd Discord bots like *Filmlinkd*.

---

## License

This project is licensed under the [MIT License](LICENSE).

---

## Local SDK development

All Backloggd HTTP requests, security/layout validation, pagination, and HTML
parsing belong to `backloggd-py`. The bot calls only its public
`get_user_activity()`, `get_user_journal()`, `get_user_profile()`,
`get_user_library()`, `get_game()`, `search_games()`, and (when available)
`get_user_activity_snapshot()` APIs while keeping feed policy and announcement
state in `bot/feed_models.py` and `bot/feed_policy.py`. HTML fixtures and
parser regression tests live in the SDK repository; bot tests use typed SDK
samples.

The bot uses the published `backloggd-py` 0.2.0 API (`>=0.2.0,<0.3.0`) with an
honest fallback to `get_user_activity()` (`unsupported` pagination coverage)
when `get_user_activity_snapshot()` is unavailable. Install it with the other
requirements and run the bot's checks:

```shell
python -m pip install -r requirements.txt
python -m unittest discover -s tests -v
python verify_all.py
```

When developing both repositories together, replace the published SDK with an
editable install: `python -m pip install -e /path/to/backloggd-matcher`.
On this machine that checkout is at
`C:/Users/LENOVO/Documents/GitHub/backloggd-matcher`.
