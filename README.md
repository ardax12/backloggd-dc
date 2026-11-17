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
| `/game <title>` | Search any game with live autocomplete (using stable `slug:` choices, bounded temporary fallback tokens, and a ~2-second search budget that returns empty choices on timeout without poisoning caches). Displays community score, global statistics, distinct-profile Server Ratings Shelf, and aggregate cache freshness. |
| `/profile [user]` | View user profile card, bio quote, library metrics (*Played, Playing, Backlog, Wishlist*), 4 pinned favorites, and library cache age (accepts `@mention`, Backloggd username, or exact profile URL). |
| `/compare <user1> [user2]` | Compare video game tastes between two users (`@mention`, Backloggd username, or exact profile URL), using distinct profiles, cached game titles (with slug fallback), and coalesced/bounded live fetches for uncached profiles or prompt cached results with background refresh when stale. |
| `/taste-twins` | Find your closest distinct-profile taste matches across the server using Bayesian similarity on cached numeric ratings. |
| `/roulette [@member]` | Pick a random title from a member's unrated `backlog` / `wishlist` titles with an interactive `[ Reroll ]` button and freshness disclosure. |
| `/recommend [@member]` | Get game recommendations from distinct taste-matched profiles (or recommend your 4.0+ titles to a server member; `played`, `playing`, and rated titles are excluded). |
| `/server-top [mode]` | Distinct-profile server rankings: `Highest Rated`, `Most Played`, `Most Polarizing`, `Hidden Gems`, `Server Shame List`. |
| `/server-stats` | Community metrics and cache age. Games Logged sums Played-tab totals across distinct linked Backloggd profiles; duplicate links count once. |
| `/link <username>` | Connect your Discord account to a public Backloggd profile (nonexclusive; server-scoped duplicate notice only; never exposes other servers or DMs), schedule a shared sync, and enable local feed participation when run in a server. |
| `/unlink` | Disconnect only your personal Backloggd link and server enrollments without erasing shared caches, other connected members, or archived feed posts. |
| `/refresh [@member]` | Queue an immediate on-demand library sync (bypasses automatic cooldown/backoff, coalesces duplicate requests, confirms promptly, and updates the status message best-effort upon completion). |
| `/feed on` | Enable or re-enable activity feed announcements for your currently `/link`ed profile in this server only. |
| `/feed off` | Opt out of member feed announcements in this server (notes when another member or admin follow still keeps the profile active). |
| `/feed status` | Inspect your effective feed eligibility, active participation source (`member`, `admin`, `both`), channel/target availability, pending baseline, source health/gaps, and suppression counts (ephemeral; hides review text and other servers). |
| `/feed add <username>` | Follow any public Backloggd profile in this server's feed independently of personal `/link`s *(Manage Channels required)*. |
| `/feed remove <username>` | Remove an admin public-profile follow without altering personal `/link`s or other members' feed enrollments *(Manage Channels required)*. |
| `/feed list` | Paginated server list of enrolled and admin-followed feed profiles with participation sources and effective/pending states *(Manage Channels required)*. |
| `/setchannel <channel>` | Validate permissions, save the feed channel immediately, and let each source (`activity` / `journal`) initialize its baseline silently on its first successful poll *(Manage Channels required)*. |
| `/unsetchannel` | Defer privately, pause automatic journal broadcasts for the server while preserving subscriptions and archived message mappings *(Manage Channels required)*. |

---

## Profile Connections, Feeds & Freshness Behavior

- **Nonexclusive public profile connections**: There is no ownership verification. Multiple Discord members can `/link` to the same Backloggd username (normalized case-insensitively by `profile_key`). Duplicate notices mention only other connected members currently present in that server and never appear in DMs. Server ratings shelves, `/server-top`, `/server-stats`, `/taste-twins`, and `/recommend` count each distinct profile once, attribute profiles canonically, and never treat two Discord accounts sharing the same profile as taste twins.
- **Complete guild membership & bounded lookups**: Server-wide commands (`/game`, `/server-top`, `/server-stats`, `/taste-twins`, `/recommend`) require a complete Discord member snapshot (coalescing guild member chunks when incomplete) and refuse to fall back to partial member lists. Database lookups and atomic library replacements run in bounded SQL parameter chunks across both local SQLite and Turso/libSQL, while leaderboard/statistics aggregates combine chunk metrics globally in Python before filtering, sorting, or applying `LIMIT`.
- **Input validation & error boundaries**: Username inputs accept `@name` or exact `https://www.backloggd.com/u/<username>/` profile URLs, reject invalid paths, control characters, markdown injection, or inputs exceeding the bot's conservative 100-character limit before calling the SDK, and escape user-controlled display names in replies. Live `/game` autocomplete bounds upstream queries to a ~2-second budget without poisoning query/fallback caches or closing the shared SDK client on timeout. Both global application-command handlers and interactive component views (`ExpiringView` callbacks, serialized pagination controls, superseded-view cleanup on rerolls, and duel callbacks) log unexpected errors once, send a single safe ephemeral reply without replacing parent messages after message-update deferrals, and keep pagination state aligned on failed edits.
- **Server Games Logged**: This is the sum of each present linked profile's Played-tab total, obtained through the SDK's public profile API, with profile counts cached for up to one hour, at most two concurrent profile fetches, and a 15-second aggregate lookup deadline. If the deadline expires before every profile finishes, already completed valid counts are preserved as a partial lower bound and remaining profiles are disclosed as unavailable (or `Unavailable` if none completed). It is not the number of cached library rows or dated sessions. The same game contributes once for each distinct profile that includes it. Missing counts are disclosed as unavailable or as a partial lower bound; they are never silently treated as zero. The separate Games Completed field has been removed.
- **Multi-collection library sync & `0.2.1` Playing-page limit**: Library syncs fetch `Played` (`get_user_library`) alongside `Playing` (`get_user_playing`), `Backlog` (`get_user_backlog`), and `Wishlist` (`get_user_wishlist`) when exposed by `backloggd-py`, merge overlapping slugs with deterministic precedence (`played > playing > backlog > wishlist > unknown`) while preserving owner ratings/likes, and atomically replace the cached library only after every collection fetch succeeds. Because published `backloggd-py` `0.2.1` reads only page 1 (up to 70 entries) of `get_user_playing()` with no pagination/continuation flag, the bot treats 70+ `Playing` entries as potentially truncated, refuses to overwrite the cache or mark the sync complete, and records a sync error until SDK pagination support is available.
- **Per-server feed enrollment & preference isolation**: Feed preferences are keyed by `(guild_id, discord_id)` and default to **off**. Running `/link` inside a server or running `/feed on` explicitly enrolls (or re-enrolls) that server only; linking in a DM, passively joining another server, or configuring a channel in another guild enrolls nobody. Preferences persist across `/unlink`, relinks, departures, and `/unsetchannel` without resetting other servers.
- **Independent admin follows (`/feed add|remove|list`)**: Server admins (`Manage Channels`) can follow any public Backloggd profile directly. Admin follows and member `/feed on` participation operate independently: a member running `/feed off` never deletes an admin follow, and `/feed remove` never removes member enrollments or personal links. Profiles followed solely via admin follow never enter server statistics, leaderboards, `/compare`, `/taste-twins`, or `/recommend`, and do not trigger unnecessary full-library syncs.
- **One-time legacy grandfathering exception**: Upgrades from pre-v3 schemas preserve existing feed participation in servers that already had a feed channel configured once Discord provides a complete initial member snapshot (respecting any explicit `/feed off` choice). New joins, later `/link`s in other guilds, or newly configured feed channels are never grandfathered automatically.
- **Prompt setup, atomic baseline, seven-day cutoff & conservative edit reconciliation**: `/setchannel` validates permissions and saves immediately; each source (`activity` and `journal`) atomically commits its baseline messages, seen IDs, initial policy decisions, and source marker together in one transaction on its first successful poll so a temporary failure in one source does not reset or block the other. Persisted aliases take precedence over ambiguity heuristics without duplicating session history. Newly discovered `Started`, `Finished`, or replay milestones older than 7 UTC calendar days at discovery (and future-dated or uncertain sessions) are suppressed, while new explicit ratings and written reviews remain eligible independently without being discarded alongside an old milestone. Ambiguous edits (such as same-game updates lacking strong persisted aliases or stable IDs) are held rather than duplicating announcements.
- **Final alias decisions, pre-addition continuity & honest pending status**: Events that share persisted aliases are judged using the strictest final decision across the complete overlapping group (`suppressed_pre_activation`, `suppressed_old`, `held_ambiguous`, `held_future`, `held_uncertain`, `baseline`, then `eligible`). A baseline can preserve identity/history but never creates an announcement. An event is accepted only after its final persisted decision is known; sent, deleted, pending, and unknown delivery records remain protected during reconciliation. Before enabling a new member or admin follow, the bot inspects pre-addition sources transactionally so a silent prior-member departure does not bridge across the new source; when membership is incomplete, prior candidates are captured before mutation rather than letting the new addition mask a gap, and `/feed status` + `/feed list` remain read-only while disclosing `Pending continuity verification` or `Pending baseline (continuity unverified)` instead of reporting `Active`.
- **Discovery-time policy, identity resolution & rating chronology**: The first UTC observation time is retained across retries, and resolving an ambiguous identity passes through original discovery-time classification and activation/recovery context rather than assuming eligibility. First-baseline refreshes preserve canonical source histories cleanly for downstream deduplication and rating lookups without inventing prior-rating arrows from unsequenced or later journal entries; missing, later, or conflicting equal-time ratings omit the transition.
- **Source-gap extent, coalesced task ownership & cancellation-resilient shutdown**: Initial unsupported or limited source coverage without a committed boundary has unknown extent; event dates do not fabricate a gap start or end. A later source failure opens a gap from the last committed boundary, and a successful newest-page poll alone does not close it. Live comparisons (`SocialCog`) track coalesced fetch waiter counts per task generation so older unwinding waiters cannot corrupt newer fetch ownership. All cog close drains (`FeedCog`, `StatsCog`, `AccountCog`, `SocialCog`) and `BackloggdBot.close()` run inside shielded/idempotent teardown tasks that finish unwinding active work before propagating caller cancellation and continue remaining shutdown stages cleanly across shared SDK, database, and Discord resources even if one stage fails.
- **Unavailable target resilience, recipient-gap isolation, delivery ownership & archives**: Feed polling checks target availability (`View Channel`, `Send Messages`, `Embed Links`, cached guild/channel) before fetching, preserves subscriptions/archives while unavailable, logs transitions once, and recovers automatically when access returns. Consecutive successful activity snapshots can establish source continuity through overlapping event IDs even when an unavailable recipient pins an older delivery boundary; this observation evidence is conservative after restart or loss of overlap. The bot also observes profiles that were already active when every current recipient is unavailable, retaining eligible observations and pinning the shared activity continuity floor for recovery without inflicting `held_uncertain` onto unrelated healthy guilds. Skipped targets and active delivery failures record per-guild recipient gaps in `guild_profile_feed_recipient_gaps` separately from profile-level `profile_feed_source_health`, clearing only after that guild's pending/current delivery succeeds or is disabled. Observation alone never sends a Discord message: delivery waits for authoritative current eligibility, complete server membership, and configured channel permissions. Removing entries on Backloggd, opting out, or unlinking does not delete already posted Discord announcements. When a server moves its feed channel from A to B via `/setchannel`, tracked existing posts are edited in their original channel A while accessible in the same guild rather than reposted to B, while new announcements go to B; if A is unavailable or lacks permissions, newly learned aliases are retained alongside the prior confirmed payload for retry. Live sends hold attempt-scoped ownership tokens across the full `begin_feed_delivery` -> Discord send -> persistence window; definitive client HTTP send rejections release `pending` intent for retry, while cancellation or uncertain transport/DB failures finalize durable `sent` (when a message ID returned) or `unknown` protection in tracked cleanup tasks without leaving abandoned same-process intents skipped by recovery.
- **Supported DB decision policy adapter contract**: Production persistence uses `Database.get_feed_event_decisions` and atomic batched `Database.persist_feed_event_decisions`. Lightweight adapters implementing only the legacy single-row contract (`get_feed_event_decision`, `record_feed_event_decision`, `revise_unsent_eligible_feed_decision`) share the exact same pure planning, connected-component alias propagation, and final acceptance logic (handling `None` returns safely by rereading or retaining planned state), though cross-row atomicity under concurrent writers requires the bulk contract.
- **On-use library freshness & shared sync**: Social/discovery/stats commands (`/profile`, `/game`, `/compare`, `/taste-twins`, `/roulette`, `/recommend`, `/server-top`, `/server-stats`) treat cached libraries older than 1 hour as stale, serve cached results immediately with Discord `<t:unix:R>` timestamps (or disclose that a missing subscribed cache is currently being fetched instead of claiming an empty library), and coalesce background syncs (2 workers, 5-minute automatic debounce/failure backoff). Feed observation seeding avoids redundant initial library refreshes only when a completed library sync demonstrably covers the observed activity state. Aggregates disclose the oldest contributing cache and stale/missing counts.
- **Request-scoped freshness metadata**: A command reuses sync metadata supplied with its profile rows and batches only metadata that was not supplied. A supplied missing value remains missing; it is not silently replaced by a second per-profile read. Subscription evidence is batched and a refresh is scheduled only for a known subscribed profile (or known linked guild-member profile), not inferred from an admin follow or an unlinked name.
- **Bounded activity recovery & source limits**: Activity feeds poll once per distinct profile per cycle across servers using request-local context reuse and bounded bulk DB lookups, and consume bounded snapshots (`max_pages=5`, stopping at known boundary IDs when supported by the SDK). Published `backloggd-py` `0.2.1` exposes `get_user_activity_snapshot()` with explicit coverage. Current public activity pages expose no validated pagination controls, so snapshots cover the latest page and report `unsupported` unless a known boundary ID is reached. A page budget does not guarantee recoverable history; bounded pagination (`exhausted` / `limited`) requires source pagination support. Unresolved source outage gaps are tracked independently from library cache freshness and never cleared by a newest-page success alone until continuity is re-established. Health errors in `/feed status` are normalized, scrubbed of credential-like tokens, and rendered as escaped prose outside inline-code delimiters.

---

## Quickstart

### 1. Prerequisites
- Python 3.10+
- A Discord Bot Token from the [Discord Developer Portal](https://discord.com/developers/applications)

### 2. Configure Gateway Intents
In the Discord Developer Portal, navigate to your application -> **Bot** and enable:
- **Server Members Intent** *(needed to match server members)*

**Message Content Intent** is not needed because the bot uses slash commands only.

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

## Database Migrations, Backup & Rollback (Schema v1–v9)

Schema upgrades in `bot/db_migrations.py` run atomically inside a transaction on startup (supporting both local SQLite and Turso/libSQL via `schema_migrations`). Startup initialization checks `schema_migrations` so pre-v1 `user_ratings` table checks and pre-v2 legacy feed DDL/backfills run only when needed for unversioned or pre-v2 upgrades and are skipped on already-upgraded restarts:

- **v1**: Normalizes `profiles` and `profile_ratings`, removes legacy unique-username constraints on `linked_users`, and exposes a compatibility `user_ratings` view.
- **v2**: Moves feed baselines, deliveries, and message mappings onto `(guild_id, profile_key)` (`profile_feed_sources`, `profile_feed_messages`, `profile_seen_journal_*`) while keeping legacy tables for audit.
- **v3**: Adds `guild_feed_preferences`, `guild_profile_feed_activity`, and one-time legacy reconciliation tables (`guild_feed_legacy_scope`, `guild_feed_legacy_migrations`).
- **v4**: Adds `guild_admin_follows` for independent admin public-profile follows.
- **v5**: Adds `profile_feed_decisions` to persist 7-day/future/uncertain/ambiguity decisions.
- **v6**: Adds `profile_feed_source_health` for separate source attempts, coverage, boundaries, and unresolved gaps.
- **v7**: Adds `last_sync_attempt_at` / `last_sync_error` on `profiles`, `unresolved_gap_until` on `profile_feed_source_health`, and `profile_activity_observations` / `profile_activity_observation_sources`.
- **v8**: Adds `guild_profile_feed_reconciliation` to preserve uncertain source-removal state across restarts. Membership recovery resolves whether another member continuously kept the profile active before reenrollment or delivery.
- **v9**: Adds `guild_profile_feed_recipient_gaps` (indexed by `(profile_key, guild_id)`) so per-guild recipient unavailability and delivery failures are tracked separately from profile-level source uncertainty.

### Backup & Rollback Procedure

1. **Always back up before upgrading** *(required for both SQLite and Turso)*:
   - **Local SQLite**: Stop the bot cleanly and copy the database file together with any `-wal` / `-shm` files:
     ```bash
     cp data/backloggd.db data/backloggd.db.bak
     ```
   - **Turso / libSQL**: Export a logical dump or create a database snapshot/branch before starting the upgraded bot binary.
2. **Rolling back to an earlier release**:
   - The v1–v9 migration history includes rebuilds of `linked_users` / `user_ratings` and additions of normalized tables (`profiles`, `profile_ratings`, `guild_feed_*`, `profile_feed_*`). Simply downgrading the application code while leaving an upgraded v9 database in place is **not** supported.
   - v8 and v9 add tables without rebuilding existing tables, but older releases do not implement their pre-addition reconciliation or per-guild recipient-gap isolation semantics; an in-place downgrade is not a supported rollback. Offline migration tests do not establish compatibility with older binaries.
   - To roll back safely: stop the bot, restore the pre-upgrade SQLite backup file (or restore the Turso snapshot/branch from step 1), verify `SELECT MAX(version) FROM schema_migrations;` matches the target release, and restart the previous bot version.

---

## Docker Deployment

The repository includes a single-stage `Dockerfile` (`python:3.12-slim`) and `.dockerignore`. Dependencies are installed from the compiled `requirements.lock` using prebuilt Linux `x86_64` wheels (`--no-deps --only-binary=:all:`, no `gcc` required in the runtime image), and the container executes as non-root user `app` (UID `10001`, GID `10001`).

Regenerate `requirements.lock` for the Linux/Python 3.12 target without touching `requirements.txt`:

```bash
uv pip compile requirements.txt --python-version 3.12 --python-platform x86_64-unknown-linux-gnu -o requirements.lock
```

> [!NOTE]
> Local Windows / Python 3.14 testing uses `requirements.txt` and platform-specific environment markers (for example `audioop-lts` on Python 3.13+ and Windows wheels/packages such as `colorama`), whereas `requirements.lock` pins the exact transitive set for the Linux Python 3.12 container image while retaining `backloggd-py>=0.2.1,<0.3.0`.

```bash
# Build the Docker image
docker build -t backloggd-dc .

# Ensure an existing host SQLite directory is writable by container UID/GID 10001:10001
mkdir -p ./data
sudo chown -R 10001:10001 ./data

# Run with persistent SQLite volume (or set DATABASE_PATH / TURSO_DATABASE_URL)
docker run -d \
  --name backloggd-dc \
  --restart always \
  -v $(pwd)/data:/app/data \
  -e DISCORD_TOKEN="your_bot_token_here" \
  backloggd-dc
```

The SDK cache lives in `sdk-cache/backloggd_cache.db` beside the configured bot
database (`/app/data/sdk-cache/backloggd_cache.db` by default in Docker). This
keeps startup and cache writes inside the non-root user's writable data directory,
including when the bot database uses Turso. Developer SDK caches are excluded
from the Docker build context. Existing cache files are disposable; profile and
feed history remain in the bot database.

When another server can still poll a shared profile, eligible observations for
temporarily unavailable, previously active feed recipients are retained for
recovery. The shared activity continuity boundary stays pinned until those
recipients can be processed. Recovery still requires complete server membership
and the configured channel permissions; archived announcements are not replayed.

---

## Architecture & Codebase Layout

```text
backloggd-dc/
├── bot/
│   ├── client.py          # Custom Discord Bot class, lifecycle hooks, global error boundary
│   ├── command_errors.py  # Safe application-command error classification and ephemeral replies
│   ├── config.py          # Bot settings and embed color constants
│   ├── db.py              # Asynchronous SQLite/libSQL database manager
│   ├── db_adapter.py      # Shared SQLite/libSQL transaction, batch, and lock adapter
│   ├── db_migrations.py   # Atomic versioned schema migrations (v1-v9)
│   ├── feed_models.py     # Normalized feed event models and parsing
│   ├── feed_rules.py      # Action classification, journal normalization, conservative merging
│   ├── feed_policy.py     # 7-day cutoff, future/uncertain handling, and ambiguity policy
│   ├── input_validation.py# Mention resolution, Backloggd username/URL validation, markdown escaping
│   ├── services/
│   │   ├── guild_membership.py # Complete, coalesced Discord guild membership resolution
│   │   ├── library_sync.py# Coalesced per-profile background multi-collection sync coordinator
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
├── Dockerfile             # Non-root production container build from compiled lockfile
├── main.py                # Application entrypoint
├── requirements.txt       # Human-readable Python package requirements
├── requirements.lock      # Compiled transitive pins for Linux/Python 3.12 container builds
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
`get_user_library()`, `get_user_playing()`, `get_user_backlog()`,
`get_user_wishlist()`, `get_game()`, `search_games()`, and (when available)
`get_user_activity_snapshot()` APIs while keeping feed policy and announcement
state in `bot/feed_models.py` and `bot/feed_policy.py`. HTML fixtures and
parser regression tests live in the SDK repository; bot tests use typed SDK
samples.

The bot requires published `backloggd-py` 0.2.1 or newer (`>=0.2.1,<0.3.0`),
including profile-owner status parsing and the public
`get_user_activity_snapshot()` API. Snapshots report `unsupported` pagination
coverage when Backloggd exposes no validated pagination controls. The bot retains
a compatibility fallback to `get_user_activity()` for older client adapters.
For full-library refreshes, `bot/services/library_sync.py` composes
`get_user_library()` (Played) with `get_user_playing()`, `get_user_backlog()`,
and `get_user_wishlist()`; because published `0.2.1` `get_user_playing()` only
fetches page 1 (`70` entries max) without a continuation flag, 70+ returned
Playing items are treated as potentially incomplete so partial collections never
overwrite complete caches.
The SDK invalidates its old library caches automatically; library data copied
into the bot database is corrected on the next successful library refresh.
Install it with the other requirements and run the bot's checks:

```shell
python -m pip install -r requirements.txt
python -m unittest discover -s tests -v
python verify_all.py
```

When developing both repositories together, replace the published SDK with an
editable install: `python -m pip install -e /path/to/backloggd-matcher`.
On this machine that checkout is at
`C:/Users/LENOVO/Documents/GitHub/backloggd-matcher`.
