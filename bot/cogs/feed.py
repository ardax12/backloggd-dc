"""FeedCog: Handles /setchannel, /unsetchannel, and the periodic playthrough journal feed loop."""

import discord
from discord import app_commands
from discord.ext import commands, tasks
import logging
import asyncio
import json
from dataclasses import asdict, replace
from bot.feed_rules import VISIBLE_ACTIONS, journal_events, merge_announcements, calendar_date
from bot.ui.feed_embed import create_journal_feed_embed
from bot.ui.views import LinkButtonView
from bot.ui.helpers import build_backloggd_game_url
from bot.feed_models import FeedEvent

logger = logging.getLogger("backloggd.cogs.feed")

class FeedCog(commands.Cog, name="Feed"):
    def __init__(self, bot):
        self.bot = bot

    async def cog_load(self):
        # Start loop when cog is loaded and bot event loop is active
        if not self.journal_feed_loop.is_running():
            self.journal_feed_loop.start()

    def cog_unload(self):
        if self.journal_feed_loop.is_running():
            self.journal_feed_loop.cancel()

    # =========================================================================
    # Channel Configuration Slash Commands (Admin Only)
    # =========================================================================


    async def _fetch_feed_sources(self, username: str) -> dict[str, list[FeedEvent]]:
        """Read sources independently; a blocked page is never an empty snapshot."""
        sources = {}
        try:
            entries = await self.bot.backloggd.get_user_activity(username)
            sources["activity"] = [FeedEvent.from_activity(entry) for entry in entries]
        except Exception as error:
            logger.warning("Could not read activity for %s: %s", username, error)
        try:
            entries = await self.bot.backloggd.get_user_journal(
                username, max_pages=100, require_complete=True
            )
            sources["journal"] = journal_events(entries, username)
        except Exception as error:
            logger.warning("Could not read journal for %s: %s", username, error)
        return sources

    async def _publish_sources(
        self, guild_id, profile_key, username, avatar_url, channel, sources
    ):
        rows = await self.bot.db.get_feed_messages(guild_id, profile_key)
        saved = {row["event_id"]: row for row in rows}
        historical = {}
        historical_journal = {}
        for row in rows:
            data = json.loads(row["payload"])
            data["event_ids"] = tuple(data["event_ids"])
            data["session_labels"] = tuple(data["session_labels"])
            event = FeedEvent(**data)
            if event.source == "activity":
                historical[event.activity_id] = event
            elif event.source == "journal":
                historical_journal[event.activity_id] = event
        for source, events in sources.items():
            # Empty successful sources are baselined too. A source absent from
            # this mapping failed and must be retried without changing state.
            ids = [e.activity_id for e in events]
            first = await self.bot.db.seed_feed_source(guild_id, profile_key, source + "-display-v1", ids)
            if first:
                for event in events:
                    payload = json.dumps(asdict(replace(event, event_ids=(event.activity_id,))), sort_keys=True)
                    await self.bot.db.seed_feed_message(guild_id, profile_key, (event.activity_id,), payload)
                # Refresh the lookup to include the baseline just written.
                saved = {r["event_id"]: r for r in await self.bot.db.get_feed_messages(guild_id, profile_key)}
        activity = {e.activity_id: e for e in historical.values()}
        for event in sources.get("activity", []):
            previous = historical.get(event.activity_id)
            if previous:
                preserved = {}
                if "journal" not in sources:
                    preserved = {
                        "played_date": event.played_date or previous.played_date,
                        "platform": event.platform or previous.platform,
                        "session_labels": event.session_labels or previous.session_labels,
                        "is_replay": event.is_replay or previous.is_replay,
                        "rating": event.rating if event.rating is not None else previous.rating,
                        "review_notes": event.review_notes or previous.review_notes,
                        "has_spoilers": event.has_spoilers or previous.has_spoilers,
                        "occurred_at": event.occurred_at or previous.occurred_at,
                        "date_str": event.date_str or previous.date_str,
                        "reference": event.reference or previous.reference,
                        "cover_url": event.cover_url or previous.cover_url,
                        "previous_rating": event.previous_rating if event.previous_rating is not None else previous.previous_rating,
                    }
                activity[event.activity_id] = replace(event, event_ids=previous.event_ids, **preserved)
            else:
                activity[event.activity_id] = event
        activities = [replace(e, event_ids=e.event_ids or (e.activity_id,)) for e in activity.values() if e.action_type in VISIBLE_ACTIONS]
        journal = {e.activity_id: e for e in historical_journal.values()}
        journal.update({e.activity_id: e for e in sources.get("journal", [])})
        logs = [e for e in journal.values() if e.action_type in VISIBLE_ACTIONS]
        events = merge_announcements(activities, logs)
        events.sort(key=lambda e: e.occurred_at or calendar_date(e.played_date) or "")
        known_ratings = {}
        for previous in sorted(historical.values(), key=lambda e: e.occurred_at or ""):
            if previous.source == "activity" and previous.rating is not None:
                known_ratings[previous.game_slug] = previous.rating
        for event in events:
            aliases = event.event_ids or (event.activity_id,)
            records = [saved[key] for key in aliases if key in saved]
            sent_records = [r for r in records if r["message_id"] is not None]
            record = min(sent_records, key=lambda row: row["message_id"]) if sent_records else (records[0] if records else None)
            if record is None:
                for alias in aliases:
                    if await self.bot.db.is_journal_entry_delivered(guild_id, profile_key, alias):
                        record = {
                            "payload": json.dumps(asdict(event)), "message_id": None,
                            "channel_id": None, "delivery_state": "baseline",
                        }
                        break
            if record:
                old = json.loads(record["payload"])
                previous_rating = old.get("previous_rating")
                if event.action_type == "rated" and old.get("rating") != event.rating:
                    previous_rating = old.get("rating")
                event = replace(event, previous_rating=previous_rating)
            elif event.action_type == "rated":
                old_rating = known_ratings.get(event.game_slug)
                if old_rating != event.rating:
                    event = replace(event, previous_rating=old_rating)
            payload = json.dumps(asdict(event), sort_keys=True)
            if record and payload == record["payload"] and all(alias in saved for alias in aliases):
                if event.source == "activity" and event.rating is not None:
                    known_ratings[event.game_slug] = event.rating
                continue
            message_id = record["message_id"] if record else None
            old_channel = record["channel_id"] if record else None
            state = record["delivery_state"] if record and "delivery_state" in record.keys() else None
            if state in {"pending", "unknown", "deleted"}:
                await self.bot.db.store_feed_message(
                    guild_id, profile_key, aliases, payload, message_id, old_channel,
                    delivery_state=state,
                )
                continue
            if record and message_id is None:
                # Historic baseline: remember aliases but never publish history.
                pass
            else:
                embed = create_journal_feed_embed(username, avatar_url, event)
                view = LinkButtonView(label="View on Backloggd", url=build_backloggd_game_url(event.game_slug))
                if message_id is not None:
                    if old_channel != channel.id or payload == record["payload"]:
                        continue
                    try:
                        await channel.get_partial_message(message_id).edit(embed=embed, view=view)
                    except discord.NotFound:
                        # A removed announcement stays removed; do not repost it.
                        await self.bot.db.mark_feed_message_deleted(
                            guild_id, profile_key, aliases, payload
                        )
                        continue
                else:
                    if not await self.bot.db.begin_feed_delivery(
                        guild_id, profile_key, aliases, payload, channel.id
                    ):
                        continue
                    try:
                        message = await channel.send(embed=embed, view=view)
                    except asyncio.CancelledError:
                        raise
                    except discord.HTTPException as error:
                        if 400 <= getattr(error, "status", 500) < 500:
                            await self.bot.db.release_feed_delivery(guild_id, profile_key, aliases)
                        else:
                            await self.bot.db.mark_feed_delivery_unknown(guild_id, profile_key, aliases)
                        raise
                    except Exception:
                        await self.bot.db.mark_feed_delivery_unknown(guild_id, profile_key, aliases)
                        raise
                    message_id, old_channel = message.id, channel.id
            await self.bot.db.store_feed_message(guild_id, profile_key, aliases, payload, message_id, old_channel)
            for alias in aliases:
                saved[alias] = {"payload": payload, "message_id": message_id, "channel_id": old_channel}
            if not record:
                await asyncio.sleep(1)
            if event.source == "activity" and event.rating is not None:
                known_ratings[event.game_slug] = event.rating

    @app_commands.command(name="setchannel", description="Set the channel where automatic Backloggd playthrough logs are posted")
    @app_commands.describe(channel="The text channel for game log announcements")
    @app_commands.checks.has_permissions(manage_channels=True)
    async def setchannel(self, interaction: discord.Interaction, channel: discord.TextChannel):
        await interaction.response.defer(ephemeral=True)

        if interaction.guild is None:
            await interaction.followup.send("❌ Choose a text channel in a server to set up the activity feed.", ephemeral=True)
            return

        permissions = channel.permissions_for(interaction.guild.me)
        missing = [
            name for attribute, name in (
                ("view_channel", "View Channel"),
                ("send_messages", "Send Messages"),
                ("embed_links", "Embed Links"),
            )
            if not getattr(permissions, attribute, False)
        ]
        if missing:
            await interaction.followup.send(
                "❌ I need **" + "**, **".join(missing) + "** permission in that channel before I can post activity.",
                ephemeral=True,
            )
            return

        # Seed each linked profile's current journal before enabling the feed,
        # so setup does not publish existing history as new activity.
        member_ids = [member.id for member in interaction.guild.members]
        linked_profiles = await self.bot.db.get_guild_linked_profiles(member_ids)
        for profile_row in linked_profiles:
            profile_key = profile_row["profile_key"]
            username = profile_row["display_username"]
            try:
                sources = await self._fetch_feed_sources(username)
                for source, entries in sources.items():
                    ids = [entry.activity_id for entry in entries]
                    await self.bot.db.seed_journal_entries_for_guild(interaction.guild_id, profile_key, ids)
                    await self.bot.db.seed_feed_source(interaction.guild_id, profile_key, source + "-display-v1", ids)
                    for event in entries:
                        payload = json.dumps(asdict(replace(event, event_ids=(event.activity_id,))), sort_keys=True)
                        await self.bot.db.seed_feed_message(interaction.guild_id, profile_key, (event.activity_id,), payload)
            except Exception as e:
                logger.warning("Could not seed current activity for %s: %s", username, e)

        await self.bot.db.set_feed_channel(interaction.guild_id, channel.id)
        await interaction.followup.send(
            f"✅ Set {channel.mention} as the Backloggd activity feed channel. Only future activity will be announced; existing history is skipped.",
            ephemeral=True
        )

    @app_commands.command(name="unsetchannel", description="Disable the automatic Backloggd activity feed for this server")
    @app_commands.checks.has_permissions(manage_channels=True)
    async def unsetchannel(self, interaction: discord.Interaction):
        removed = await self.bot.db.unset_feed_channel(interaction.guild_id)
        if removed:
            await interaction.response.send_message(
                "✅ Automatic activity feed has been disabled for this server.",
                ephemeral=True
            )
        else:
            await interaction.response.send_message(
                "ℹ️ No activity feed channel was currently set for this server.",
                ephemeral=True
            )

    @setchannel.error
    @unsetchannel.error
    async def channel_command_error(self, interaction: discord.Interaction, error: app_commands.AppCommandError):
        if isinstance(error, app_commands.MissingPermissions):
            await interaction.response.send_message(
                "❌ You need the **Manage Channels** permission to configure the activity feed channel.",
                ephemeral=True
            )

    # =========================================================================
    # Background Polling Task Loop (Filmlinkd-style)
    # =========================================================================

    @tasks.loop(minutes=5.0)
    async def journal_feed_loop(self):
        """Periodically check linked users' journals and broadcast new logs."""
        try:
            if not self.bot.db or not self.bot.db.is_connected:
                return

            guild_settings = await self.bot.db.get_all_guild_settings()
            if not guild_settings:
                return

            # Per-tick journal cache to avoid duplicate HTTP requests across multiple guilds
            user_journal_cache: dict[str, dict] = {}

            for setting in guild_settings:
                guild_id = setting["guild_id"]
                channel_id = setting["feed_channel_id"]

                guild = self.bot.get_guild(guild_id)
                if not guild:
                    continue

                channel = guild.get_channel(channel_id)
                if not channel or not isinstance(channel, discord.TextChannel):
                    continue

                # Check bot permissions in target channel
                permissions = channel.permissions_for(guild.me)
                if not permissions.send_messages or not permissions.embed_links:
                    logger.warning("Missing send_messages or embed_links permission in guild %d channel %d", guild_id, channel_id)
                    continue

                # Get distinct linked profiles in this guild.
                member_ids = [m.id for m in guild.members]
                linked_profiles = await self.bot.db.get_guild_linked_profiles(member_ids)

                for profile_row in linked_profiles:
                    profile_key = profile_row["profile_key"]
                    username = profile_row["display_username"]
                    avatar_url = profile_row["avatar_url"]

                    try:
                        if profile_key not in user_journal_cache:
                            user_journal_cache[profile_key] = await self._fetch_feed_sources(username)
                        await self._publish_sources(
                            guild_id, profile_key, username, avatar_url, channel, user_journal_cache[profile_key]
                        )

                    except Exception as e:
                        logger.warning("Error polling activity for %s: %s", username, e)

                    # Pause between users to stay respectful to Backloggd servers
                    await asyncio.sleep(1.5)

        except Exception as e:
            logger.error("Error in journal_feed_loop: %s", e, exc_info=True)

    @journal_feed_loop.before_loop
    async def before_feed_loop(self):
        """Wait until bot is fully logged in before running the background feed."""
        try:
            await self.bot.wait_until_ready()
        except RuntimeError:
            # Raised if testing offline without bot.start()
            pass

async def setup(bot):
    await bot.add_cog(FeedCog(bot))
