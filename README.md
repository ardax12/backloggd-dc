# Backloggd Discord Bot 🎮

A feature-rich, high-performance Discord bot that connects Backloggd accounts with Discord servers. Features game search with server ratings, profile showcases, taste duels, backlog roulette, specialty server leaderboards, and Filmlinkd-style automatic playthrough feeds.

Built using [discord.py](https://github.com/Rapptz/discord.py) v2, [backloggd-py](https://github.com/farkasg/backloggd-py), and SQLite.

---

## ✨ Features

- **🎮 `/game <title>`**: Search games with real-time Discord autocomplete. Displays community rating, release metadata, global stats, and a **Server Ratings Shelf** showing ratings and statuses from all linked members in your Discord server.
- **👤 `/profile [user]`**: Displays user profile card, bio quote, activity stats (*Played, Playing, Backlog, Wishlist*), average rating, and 4 pinned favorite games.
- **⚔️ `/compare <user1> [user2]`**: Head-to-head taste duel calculating similarity percentage, **Shared Loves** (games both rated 4.0+), and **Biggest Disagreements** with rating deltas ($\Delta$).
- **🎲 `/roulette [user]`**: Solves the eternal "what should I play next?" dilemma with an interactive **`[ 🔄 Reroll ]`** button.
- **💡 `/recommend [user]`**: Recommends top-rated games from server members who have the highest taste agreement with you.
- **🏆 `/server-top [mode]`**: 5 distinct server leaderboard modes:
  - ⭐ *Highest Rated*
  - 🎮 *Most Played*
  - 🔥 *Most Polarizing / Divisive*
  - 💎 *Hidden Gems*
  - 🪦 *Server Backlog Shame List*
- **📊 `/server-stats`**: Server community gaming metrics and most popular games in the guild.
- **📡 Automatic Playthrough Feed (Filmlinkd-style)**: Automatically broadcasts new journal logs to a configured channel (`/setchannel`).

---

## 🚀 Quickstart & Setup

### 1. Prerequisites
- Python 3.10+ (or Docker)
- A Discord Bot Token from the [Discord Developer Portal](https://discord.com/developers/applications)

### 2. Configure Intents
In the Discord Developer Portal, navigate to your bot and enable:
- ✅ **Server Members Intent**
- ✅ **Message Content Intent**

### 3. Installation

Clone the repository and set up a virtual environment:
```bash
git clone https://github.com/YOUR_USERNAME/backloggd-dc.git
cd backloggd-dc

python -m venv .venv
# Windows:
.venv\Scripts\activate
# Linux/macOS:
source .venv/bin/activate

pip install -r requirements.txt
```

### 4. Configuration

Copy `.env.example` to `.env`:
```bash
cp .env.example .env
```

Edit `.env` and fill in your bot token:
```env
DISCORD_TOKEN=your_bot_token_here

# Optional: Add your private test server ID for instant slash command sync
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

You can run the bot as a lightweight container (~35MB RAM footprint):

```bash
docker build -t backloggd-bot .
docker run -d --name backloggd-bot --restart always \
  -v $(pwd)/data:/app/data \
  -e DISCORD_TOKEN="your_bot_token_here" \
  backloggd-bot
```

---

## 📁 Project Structure

```text
backloggd-dc/
├── bot/
│   ├── client.py        # Custom Discord Bot class & lifecycle hooks
│   ├── config.py        # Bot settings & embed colors
│   ├── db.py            # Asynchronous SQLite database manager
│   ├── ui/
│   │   ├── embeds.py    # Native Discord Embed builders
│   │   ├── views.py     # Interactive buttons (Reroll, Web links)
│   │   └── helpers.py   # Star formatters, number formatters, URL builders
│   └── cogs/
│       ├── account.py   # /link, /unlink, /refresh
│       ├── games.py     # /game (autocomplete + server ratings shelf)
│       ├── users.py     # /profile
│       ├── social.py    # /compare
│       ├── discovery.py # /roulette, /recommend
│       ├── stats.py     # /server-top, /server-stats
│       └── feed.py      # /setchannel, /unsetchannel, automatic feed loop
├── data/                # SQLite database directory (gitignored)
├── Dockerfile           # Production container build
├── main.py              # Bot bootstrap entrypoint
├── requirements.txt     # Python dependencies
└── verify_all.py        # Comprehensive verification test suite
```

---

## 📄 License
MIT License.
