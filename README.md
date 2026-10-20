# backloggd-dc 🎮

[![Discord.py](https://img.shields.io/badge/discord.py-v2.7.1-5865F2.svg?logo=discord&logoColor=white)](https://github.com/Rapptz/discord.py)
[![Python 3.10+](https://img.shields.io/badge/python-3.10+-3776ab.svg?logo=python&logoColor=white)](https://www.python.org/)
[![Powered by backloggd-py](https://img.shields.io/badge/powered%20by-backloggd--py-8E61F2.svg)](https://github.com/ardax12/backloggd-matcher)
[![License: MIT](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)
[![Docker Ready](https://img.shields.io/badge/docker-ready-blue.svg?logo=docker&logoColor=white)](Dockerfile)

A modern, high-performance Discord bot that integrates [Backloggd](https://backloggd.com) profiles and video game tracking into your Discord servers.

Designed specifically for gaming communities and friend groups: search games with live autocomplete, inspect your server's collective ratings, run taste-matching duels, break backlog paralysis with interactive roulette, and stream playthrough activity automatically (Filmlinkd-style).

---

## Highlights

- **⚡ Blazing Fast & Free-Tier Friendly**: Built with native Discord Embeds and `aiosqlite`. Zero headless browser overhead and zero heavy GPU image generation. Idles at **~35MB RAM**, making it 100% compatible with free hosting tiers (Render, Fly.io, Railway, Oracle Free VM).
- **👥 Collective Server Ratings**: When looking up a game with `/game`, view every linked member's individual score and play status directly within the server shelf.
- **🔍 Live Autocomplete Search**: Interactive search choices query Backloggd in real time as you type, complete with an in-memory debounce cache.
- **⚔️ Taste Compatibility Duels**: Cross-references two user libraries using Bayesian correlation and overlap scoring to reveal match percentages, shared 4.0+ favorites, and biggest rating disagreements.
- **🎲 Interactive Backlog Roulette**: Picks a random game from your unplayed backlog and includes an in-place **`[ 🔄 Reroll ]`** button to re-spin without re-running commands.
- **💡 Server-Sourced Recommendations**: Finds your "taste twins" across your Discord server and recommends games they rated 4.0+ that you haven't played yet.
- **🏆 Specialty Leaderboards**: 5 distinct server leaderboard modes including *Highest Rated*, *Most Played*, *Most Polarizing*, *Hidden Gems*, and the *Server Backlog Shame List*.
- **📡 Filmlinkd-Style Activity Feed**: An automated background worker monitors linked journals and broadcasts newly logged playthroughs, star ratings, and review thoughts to your designated channel.

---

## Command Reference

| Command | Description |
| :--- | :--- |
| **`/game <title>`** | Search any game with live autocomplete. Displays community score, global statistics, and **Server Ratings Shelf**. |
| **`/profile [user]`** | View user profile card, bio quote, library metrics (*Played, Playing, Backlog, Wishlist*), and 4 pinned favorites. |
| **`/compare <user1> [user2]`** | Compare video game tastes between two members (or yourself vs another member). |
| **`/roulette [user]`** | Pick a random title from your unplayed backlog with an interactive **`[ 🔄 Reroll ]`** button. |
| **`/recommend [user]`** | Get game recommendations from server members who have the highest taste agreement with you. |
| **`/server-top [mode]`** | Server rankings: `Highest Rated`, `Most Played`, `Most Polarizing`, `Hidden Gems`, `Server Shame List`. |
| **`/server-stats`** | Server community gaming metrics and most popular games in the guild. |
| **`/link <username>`** | Link your Discord account to your Backloggd profile and trigger background library sync. |
| **`/unlink`** | Disconnect your Backloggd account and purge cached ratings. |
| **`/refresh [user]`** | Force an on-demand sync of your Backloggd library and ratings. |
| **`/setchannel <channel>`** | Designate a channel for automatic playthrough journal broadcasts *(Manage Channels required)*. |
| **`/unsetchannel`** | Disable automatic journal broadcasts for the server *(Manage Channels required)*. |

---

## Quickstart

### 1. Prerequisites
- Python 3.10+
- A Discord Bot Token from the [Discord Developer Portal](https://discord.com/developers/applications)

### 2. Configure Gateway Intents
In the Discord Developer Portal, navigate to your application $\rightarrow$ **Bot** and enable:
- ✅ **Server Members Intent** *(needed to match server members)*
- ✅ **Message Content Intent**

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

## 🐳 Docker Deployment

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
│   ├── client.py        # Custom Discord Bot class & lifecycle hooks
│   ├── config.py        # Bot settings & Backloggd embed colors
│   ├── db.py            # Asynchronous SQLite database manager (aiosqlite)
│   ├── ui/
│   │   ├── embeds.py    # Native Discord Embed builders
│   │   ├── views.py     # Interactive buttons (Reroll, Link buttons)
│   │   ├── feed_embed.py# Filmlinkd-style journal announcement embed
│   │   └── helpers.py   # Star formatters (★★★★½), numbers (14.2k), URLs
│   └── cogs/
│       ├── account.py   # /link, /unlink, /refresh
│       ├── games.py     # /game (autocomplete + server ratings shelf)
│       ├── users.py     # /profile
│       ├── social.py    # /compare
│       ├── discovery.py # /roulette, /recommend
│       ├── stats.py     # /server-top, /server-stats
│       └── feed.py      # /setchannel, /unsetchannel, automated poller loop
├── data/                # SQLite database directory (gitignored)
├── Dockerfile           # Production container build
├── main.py              # Application entrypoint
├── requirements.txt     # Python package requirements
└── verify_all.py        # Comprehensive verification test suite
```

---

## Verification & Testing

The repository includes a standalone test suite covering database queries, formatting helpers, embed builders, and cog registrations without requiring an active Discord token:

```bash
python verify_all.py
```

---

## Acknowledgements

- Built on top of [backloggd-py](https://github.com/ardax12/backloggd-matcher) for Backloggd scraping and taste similarity algorithms.
- Inspired by Letterboxd Discord bots like *Filmlinkd*.

---

## License

This project is licensed under the [MIT License](LICENSE).
