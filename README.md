# backloggd-dc

Bring your Backloggd profiles, ratings, and game activity into your Discord server. Compare tastes with friends, find something to play, and share new ratings and playthrough milestones automatically.

The bot uses [backloggd-py](https://github.com/ardax12/backloggd-py), my companion library for Backloggd scraping and taste comparisons.

**This is an unofficial project. It is not affiliated with, endorsed by, or operated by Backloggd.**

## What you can do

- Look up games and see how your server rated them.
- Share profiles, compare tastes, and find your closest taste twins.
- Get recommendations or roll a random game from your backlog or wishlist.
- Browse server leaderboards and community statistics.
- Follow new ratings, reviews, and Started/Finished milestones in a dedicated channel.

## Getting started in Discord

Once the bot is running and invited to your server:

1. Run `/link` with your Backloggd username. This connects your profile and starts loading your library.
2. An admin can run `/setchannel` to choose where announcements appear.
3. Try `/profile`, `/game`, or `/compare`. Library-based features become available after the first sync finishes.

Linking in a server enables your feed participation there. In another server, use `/link` again or `/feed on` to join that server's feed. Linking in a DM does not enable any server feeds. Use `/feed off` to opt out in the current server.

Multiple members may connect the same public Backloggd profile. The bot counts that profile once in server statistics and feeds. Linking is a convenience feature, not proof of account ownership.

## Commands

Parameters in `<angle brackets>` are required and parameters in `[square brackets]` are optional. Commands with only optional parameters (or no parameters) can be run without arguments — they default to your linked profile, current settings, or the default mode/period noted below.

| Command | What it does |
| --- | --- |
| `/help [topic]` | Browse commands, onboarding steps, and admin tips privately by category (defaults to `Overview`). |
| `/game <title>` | Search for a game with live autocomplete and see its details and server ratings. |
| `/profile [user]` | Show your linked profile (when run with no argument) or another public Backloggd profile (`@mention` or username). |
| `/playing` | See which games linked server members are currently playing on Backloggd. |
| `/compare <user1> [user2]` | Compare two profiles' ratings and shared favorites (omitting `user2` compares `user1` against your linked profile). |
| `/taste-twins [user]` | Find the closest taste twins and arch-nemesis in the server for yourself (no argument) or another `@mention` / Backloggd username. |
| `/roulette [user] [pool]` | Pick an unrated game from a linked member's backlog, wishlist, or both (defaults to you and `Both`; e.g. `/roulette pool:Backlog`). |
| `/recommend [user] [pool]` | With no `user`, finds unplayed games for you from similar-taste server members; with another `user`, suggests your 4.0+ favorites to them (`pool` defaults to `Any Unplayed`, or filter to `Backlog`, `Wishlist`, or `Backlog & Wishlist`). |
| `/server-top [mode] [min_ratings]` | Browse paginated server rankings (up to top 50) across Highest Rated, Most Played, Most Polarizing, Hidden Gems, and Server Backlog Shame List (defaults to `Highest Rated` with `min_ratings` `2`; `min_ratings` applies to Highest Rated and Most Polarizing). |
| `/server-stats` | See community totals and how recently the libraries were updated. |
| `/server-recap [period]` | Summarize delivered server activity feed announcements over the last `7 days` (default) or `30 days`. |
| `/link <username>` | Connect a Backloggd profile and join the current server's feed. |
| `/unlink` | Disconnect your profile. Existing announcements remain as an archive. |
| `/refresh [user]` | Request a library update for yourself (default) or another linked server member. |
| `/feed on` / `/feed off` | Change your feed participation in this server. |
| `/feed status` | Check your feed setup and any collection or delivery problems privately. |

You can also right-click any server member and choose **Apps → View Backloggd Profile** or **Apps → Compare Backloggd Tastes**.

Admins with **Manage Channels** can also use:

| Command | What it does |
| --- | --- |
| `/setchannel <channel>` | Choose the announcement channel. |
| `/unsetchannel` | Pause the server's feed without deleting old announcements. |
| `/feed add <username>` / `/feed remove <username>` | Follow or unfollow any public Backloggd profile. |
| `/feed list` | Browse followed profiles and their feed status. |
| `/feed filter` | Run with no options to view current settings, or toggle announced activity categories (`started`, `finished`, `replays`, `ratings`, `reviews`) and `spoiler_reviews` (wrap all review notes in `||spoilers||`). |
| `/feed test` | Post a non-persisted sample announcement to verify channel permissions and spoiler formatting. |
| `/feed digest [cadence] [channel]` | Run with no arguments (`View`) to check the current schedule, or enable (`Weekly` / `Monthly`, defaulting to the server's feed channel) or disable (`Off`) automatic scheduled server activity recaps. |

Moderators with **Administrator**, **Manage Server**, **Manage Channels**, or **Moderate Members** can also use:

| Command | What it does |
| --- | --- |
| `/cooldown view` | View current per-command cooldown durations and allowed ranges in this server. |
| `/cooldown set <command> <seconds>` | Set a custom server cooldown for a supported command within safe bounds (`/server-recap` shares the `/server-stats` cooldown). |
| `/cooldown reset [command]` | Reset one command's custom cooldown, or omit `command` to reset all custom server cooldowns back to defaults. |

Admin follows are separate from member preferences. `/feed off` does not remove an admin follow, and `/feed remove` does not unlink a member. Profiles followed only by an admin do not enter server statistics or library comparisons.

## Run your own bot

### 1. What you need

- Python 3.11+ (3.12 recommended), or Docker.
- A bot application and token from the [Discord Developer Portal](https://discord.com/developers/applications).
- A Backloggd account and its `_backloggd_session` cookie. **The cookie is required for scraping.**
- Somewhere the bot can stay running, with persistent storage for its database.

### 2. Configure Gateway Intents

In the Developer Portal, open your application's **Bot** page and enable:

- **Server Members Intent** (needed to identify linked members in each server).

**Message Content Intent** is not needed; the bot uses slash commands.

Invite the bot with the `bot` and `applications.commands` scopes. In the feed channel, allow it to **View Channel**, **Send Messages**, and **Embed Links**.

### 3. Install

```shell
git clone https://github.com/ardax12/backloggd-dc.git
cd backloggd-dc
python -m venv .venv
```

Activate the virtual environment:

```powershell
# Windows PowerShell
.venv\Scripts\Activate.ps1
```

```bash
# Linux / macOS
source .venv/bin/activate
```

Then install the dependencies:

```shell
python -m pip install -r requirements.txt
```

### 4. Add your credentials

Copy `.env.example` to `.env` (`Copy-Item .env.example .env` in PowerShell, or `cp .env.example .env` on Linux/macOS). Edit the new file:

```dotenv
DISCORD_TOKEN=your_bot_token_here
BACKLOGGD_SESSION=your_backloggd_session_cookie_here
DATABASE_PATH=data/backloggd.db
```

**Replace both placeholder values before starting.** You can also set these as environment variables in your hosting platform's settings.

To find your Backloggd cookie:

1. Sign in to Backloggd in your browser.
2. Open Developer Tools (usually F12), then **Application** or **Storage**.
3. Open **Cookies** for the Backloggd site and find `_backloggd_session`.
4. Copy only its value into `BACKLOGGD_SESSION`.

Treat the cookie like a password. Keep `.env` private and never include it in a commit or Docker image. Cookies can expire or become invalid after signing out; replace the value and restart the bot if that happens. Startup checks for a missing or example value, but cannot prove a supplied cookie is still valid.

For a test server, optionally set `DEV_GUILD_ID` to that server's ID. Otherwise commands are registered globally and may take time to appear.

For optional upstream session health alerts (fired once when Backloggd auth/challenge/rate-limit issues begin and once on recovery), you can also set `ALERT_WEBHOOK_URL` (a Discord or compatible webhook URL) and/or `ALERT_OWNER_DM=true` to DM the bot application owner.

For structured log collectors, set `LOG_FORMAT=json` to emit single-line JSON logs to stdout (default is plain text).

For larger multi-server deployments, optional operational tuning variables are also available: `FEED_DELIVERY_CONCURRENCY` (default `2`, valid range `1`–`5`; values above `5` are capped at `5` and values below `1` fall back to `2`), `FEED_ADAPTIVE_POLL_THRESHOLD` (default `40`, min `0`; active profile count at which dormant profiles switch to staggered polling), `HISTORY_RETENTION_DAYS` (default `90`, min `7`), and `MISSING_GUILD_PAUSE_HOURS` (default `24`, min `1`).

### 5. Start

```shell
python main.py
```

Keep this process running. If your host provides a numeric `PORT` environment variable, the bot serves HTTP checks at `/` and `/health`. It still needs a persistent background process to stay connected to Discord.

- `/` is a lightweight process liveness check (`200` while the HTTP server is up), for hosts that restart only when the process stops responding.
- `/health` reports local bot readiness (`ready`, `status`, `service`, and `revision` when `APP_REVISION` — or host-provided `RENDER_GIT_COMMIT` — is set). When upstream Backloggd scraping is degraded, it also includes `upstream: "degraded"` and `upstream_kind` (`auth`, `challenge`, or `rate_limit`) without changing HTTP readiness. A `200` response confirms that initial Discord startup completed and the database client is open (returning `reconnecting` with `ready: false` during a 60-second Discord reconnect grace window), and `503` reports `starting`, `unavailable`, or `stopping`. Remote database reachability and slash-command registration have separate failure and recovery paths. If a host configures `/health` as its restart probe, extended Discord outages can cause restarts.

## Run with Docker

Create and fill in `.env` as described above, then:

```shell
docker build -t backloggd-dc .
docker volume create backloggd-data
docker run -d --name backloggd-dc --restart unless-stopped --env-file .env -v backloggd-data:/app/data backloggd-dc
```

`--env-file .env` supplies **both** the Discord token and required Backloggd cookie. The named volume keeps the database across container replacements. Keep `DATABASE_PATH=data/backloggd.db` (or `/app/data/backloggd.db`) for this example.

The container runs as a non-root user. If you use a host directory instead of the named volume, make it writable by UID/GID `10001:10001` on Linux. Check startup messages with `docker logs backloggd-dc`.

The image includes a port-aware Docker `HEALTHCHECK`: when `PORT` is set to a numeric value, it probes `http://127.0.0.1:$PORT/health` every 30 seconds and reports healthy only on HTTP `200`. When `PORT` is unset (the default for headless worker deployments), the check passes immediately without starting an HTTP server.

## Database and backups

**Local SQLite is the default.** No hosted database account is needed. The bot creates `data/backloggd.db`; keep this file on persistent storage so links, preferences, and feed history survive restarts.

If you prefer a compatible remote libSQL database, set:

```dotenv
LIBSQL_DATABASE_URL=your_libsql_database_url
LIBSQL_AUTH_TOKEN=your_database_auth_token
```

Use the connection URL and access token supplied by your database host. The endpoint must support libSQL. Even with a remote database, keep the local data directory writable for the SDK's disposable cache.

**Back up before updating the bot.** For SQLite, stop the bot cleanly and copy the database together with any matching `-wal` and `-shm` files. With Docker, back up the database inside the mounted volume. For a remote database, use your host's snapshot or export tools.

Database upgrades run automatically on startup (currently schema versions 1-15). To return to an older release, stop the bot and restore its matching pre-upgrade database backup before restarting the old version. Downgrading the code alone is not a supported rollback. The migration details live in [bot/db_migrations.py](bot/db_migrations.py).

## How updates and announcements work

**Library updates:** Results that use library data include a short line showing when it was last updated. Libraries older than an hour or marked stale by feed activity are refreshed in the background when used; `/refresh` requests an update directly. If an update fails, the previous library stays available.

**Server totals:** Games Logged adds the Played-tab totals of distinct linked profiles currently in the server. A game owned by two different profiles contributes twice; two Discord accounts linked to the same profile contribute once. Authoritative Played-tab totals observed by `/profile` or `/server-stats` are cached per profile with an observation timestamp for up to one hour; missing, stale, or partial totals are disclosed.

**Feed setup:** The first successful activity and journal checks establish a starting point silently. By default, the bot polls every five minutes (reading up to five recent activity pages and journal page 1, including at setup and after an outage). When a deployment tracks `40` or more active profiles (`FEED_ADAPTIVE_POLL_THRESHOLD`), healthy profiles with no activity for 7+ days are polled every 15 minutes instead. Backdated entries, edits, or missed entries that stay on later pages may go unnoticed.

**What gets announced:** Started/Finished milestones, identifiable replays, explicit ratings, rating changes, and written reviews. Ordinary daily sessions, backlog/wishlist changes, likes, follows, comments, badges, and lists are hidden. Related events are combined where they can be matched safely. Server admins can use `/feed filter` to disable specific categories for new events (without affecting in-place edits of already-posted messages or backfilling history if re-enabled later), enable `spoiler_reviews` to wrap all announced review notes in Discord spoiler tags, test permissions with `/feed test`, or opt in to scheduled Weekly/Monthly recaps with `/feed digest` (empty windows are skipped quietly).

**Backdated entries:** Newly discovered playthrough milestones more than seven UTC calendar days old are suppressed. Recent backdated milestones keep Started/Finished in the title and show the played date. A new rating or review can still be announced independently of an older milestone.

**Edits and archives:** Recognized updates edit existing announcements, including in an earlier feed channel if it is still accessible. Removing an entry, unlinking, or opting out does not delete posted messages. Ambiguous updates may be held to avoid duplicate announcements.

**Outages:** Previously active profiles can still be observed while their Discord targets are unavailable, even if all recipients are affected. Delivery resumes only after membership and channel access are verified. One server's delivery problem does not make a healthy server's feed uncertain. Backloggd exposes limited activity history, so extended outages can still leave unrecoverable gaps; `/feed status` shows known problems.

## Troubleshooting

| Problem | What to check |
| --- | --- |
| Startup says a credential is missing | Replace the example Discord token and cookie in `.env`, or set both in your host's environment settings. |
| Backloggd requests fail | Check that the session cookie is current. Site changes or security checks can also interrupt scraping; `/feed status` surfaces active upstream warnings and retry timing. |
| Commands do not appear | Check the invite scopes and allow global commands time to register; use `DEV_GUILD_ID` for a test server. |
| Server commands cannot identify members | Enable Server Members Intent in the Developer Portal. |
| A feed is quiet | Check `/feed status`, your server's `/feed on` preference, `/feed filter` (in case categories are disabled), and `/feed test` to verify channel permissions. Existing history is intentionally skipped. |
| Comparisons or recommendations are empty | Wait for the initial library sync, check the shown cache age, or use `/refresh`. Server taste matching (`/taste-twins`, `/recommend`) also requires at least 5 numeric ratings per candidate profile and 2 shared rated games (unrated backlog/wishlist entries do not count). |
| A library refresh fails with 70+ Playing games | The SDK only reads the first Playing page and cannot confirm that collection is complete. The bot keeps the previous cache rather than replacing it with partial data. |
| Docker cannot write its database | Check volume placement and permissions for the container's non-root user. |

## Development

Scraping and HTML parsing belong in [backloggd-py](https://github.com/ardax12/backloggd-py). This repository handles Discord commands, cached libraries, and announcement rules. To work on both projects locally, install the SDK checkout with `python -m pip install -e /path/to/backloggd-py`.

Before opening a pull request, you can run the same static checks enforced in CI:

```shell
python -m pip check
python -m compileall -q bot main.py
ruff check bot main.py
pyright
```

Docker installs from `requirements.lock`, targeting Linux x86_64 and Python 3.12. After changing dependencies, regenerate it with:

```shell
uv pip compile requirements.txt --python-version 3.12 --python-platform x86_64-unknown-linux-gnu -o requirements.lock
```

## License

[MIT](LICENSE).
