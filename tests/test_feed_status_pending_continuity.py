"""Task 8 regressions: honest pending-continuity status and list output (F8)."""

import os

os.environ["PYTHON_DOTENV_DISABLED"] = "1"
os.environ["TURSO_DATABASE_URL"] = ""
os.environ["TURSO_AUTH_TOKEN"] = ""
os.environ["DEV_GUILD_ID"] = ""

from datetime import datetime, timezone
import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import MagicMock

import discord

from bot.cogs.feed import FeedCog
from bot.db import Database
from bot.ui.views import FeedListView
from tests.test_feed_batch_writes import CountingTursoClient
from tests.test_interaction_visibility_protocol import StatefulDiscordInteraction


def _make_channel(channel_id: int = 100, *, can_send: bool = True) -> MagicMock:
    channel = MagicMock(spec=discord.TextChannel)
    channel.id = channel_id
    channel.mention = f"<#{channel_id}>"
    channel.permissions_for = lambda _me: discord.Permissions(
        view_channel=can_send,
        send_messages=can_send,
        embed_links=can_send,
    )
    return channel


def _make_guild(guild_id: int, member_ids: list[int], *, channel=None, chunked: bool = True):
    return SimpleNamespace(
        id=guild_id,
        name=f"Guild {guild_id}",
        me=SimpleNamespace(id=999),
        chunked=chunked,
        member_count=len(member_ids) if chunked else None,
        members=[SimpleNamespace(id=mid, display_name=f"User{mid}") for mid in member_ids],
        get_channel=lambda cid: channel if channel is not None and cid == channel.id else None,
    )


class FeedStatusPendingContinuityTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmpdir.cleanup)
        self.db_path = Path(self.tmpdir.name) / "status_continuity.db"
        self.db = Database(str(self.db_path), turso_url="")
        await self.db.connect()
        self.addAsyncCleanup(self.db.close)
        self.now = datetime(2026, 11, 17, 12, 0, tzinfo=timezone.utc)

    async def _seed_initialized_active(
        self,
        db: Database,
        *,
        username: str = "Millers",
        discord_id: int = 10,
        admin: bool = False,
        guild_id: int = 1,
        all_eligible: list[str] | None = None,
        all_members: list[int] | None = None,
    ):
        await db.set_feed_channel(guild_id, 100)
        if admin:
            await db.add_guild_admin_follow(guild_id, username, None, 99, member_ids=all_members or [discord_id])
        else:
            await db.link_user(discord_id, username)
            await db.set_guild_feed_preference(guild_id, discord_id, True, member_ids=all_members or [discord_id])
        await db.reconcile_guild_feed_activity(
            guild_id,
            all_eligible if all_eligible is not None else [username.lower()],
            now=self.now,
            present_member_ids=all_members if all_members is not None else [discord_id],
        )
        await db.seed_feed_source(guild_id, username, "activity-display-v1", ["a-1"])
        await db.seed_feed_source(guild_id, username, "journal-display-v1", ["j-1"])
        await db.record_feed_source_success(
            username, "activity", coverage="exhausted", pages_read=1, boundary_event_id="a-1", now=self.now,
        )
        await db.record_feed_source_success(
            username, "journal", coverage="exhausted", pages_read=1, now=self.now,
        )

    async def _run_feed_status(self, db: Database, *, guild, user_id: int = 10) -> dict:
        cog = FeedCog(SimpleNamespace(db=db, get_guild=lambda gid: guild if gid == guild.id else None), clock=lambda: self.now)
        inter = StatefulDiscordInteraction(name="feed", guild=guild, user_id=user_id, manage_channels=True)
        await FeedCog.feed_status.callback(cog, inter)
        ephem = inter.visible_ephemeral_messages()
        self.assertEqual(len(ephem), 1)
        return ephem[0]["embed"].to_dict()

    async def _run_feed_list(self, db: Database, *, guild, user_id: int = 99) -> tuple[dict, FeedListView]:
        cog = FeedCog(SimpleNamespace(db=db, get_guild=lambda gid: guild if gid == guild.id else None), clock=lambda: self.now)
        inter = StatefulDiscordInteraction(name="feed", guild=guild, user_id=user_id, manage_channels=True)
        await FeedCog.feed_list.callback(cog, inter)
        ephem = inter.visible_ephemeral_messages()
        self.assertEqual(len(ephem), 1)
        view = ephem[0]["view"]
        self.assertIsInstance(view, FeedListView)
        self.addCleanup(view.stop)
        return ephem[0]["embed"].to_dict(), view

    def test_describe_state_precedence_and_preserved_reasons_with_pending_continuity(self):
        base_health = [
            {"source": "activity", "last_error": None, "unresolved_gap_since": None, "coverage": "exhausted"},
            {"source": "journal", "last_error": None, "unresolved_gap_since": None, "coverage": "exhausted"},
        ]
        pending_base = {
            "active": True,
            "pending_reconciliation": True,
            "initialized_sources": ["activity-display-v1", "journal-display-v1"],
            "source_health": base_health,
            "recipient_gap": None,
            "has_attributed_recipient_gaps": False,
        }

        # Precedence 1-3: missing channel > inactive > unavailable target
        self.assertEqual(
            FeedCog._describe_profile_feed_state(
                channel_id=None, status=pending_base, is_candidate=True, target_issue="missing perms",
            ),
            "Pending channel",
        )
        self.assertEqual(
            FeedCog._describe_profile_feed_state(
                channel_id=100, status=pending_base, is_candidate=False, target_issue="missing perms",
            ),
            "Inactive",
        )
        self.assertEqual(
            FeedCog._describe_profile_feed_state(
                channel_id=100, status=pending_base, is_candidate=True, target_issue="missing perms",
            ),
            "Unavailable (missing perms)",
        )

        # Precedence 4: pending baseline preserves both baseline & unverified continuity, never says Active
        uninit = {**pending_base, "initialized_sources": []}
        uninit_state = FeedCog._describe_profile_feed_state(
            channel_id=100, status=uninit, is_candidate=True,
        )
        self.assertIn("Pending baseline", uninit_state)
        self.assertIn("continuity", uninit_state.lower())
        self.assertNotIn("Active", uninit_state)

        # Active initialized profile with pending evidence MUST NOT say Active!
        active_pending_state = FeedCog._describe_profile_feed_state(
            channel_id=100, status=pending_base, is_candidate=True,
        )
        self.assertNotIn("Active", active_pending_state)
        self.assertIn("continuity", active_pending_state.lower())

        # Preserves partial/degraded/recipient/coverage reasons while pending continuity
        partial = {**pending_base, "initialized_sources": ["activity-display-v1"]}
        partial_state = FeedCog._describe_profile_feed_state(channel_id=100, status=partial, is_candidate=True)
        self.assertNotIn("Active", partial_state)
        self.assertIn("continuity", partial_state.lower())
        self.assertIn("partially checked", partial_state.lower())

        errored = {
            **pending_base,
            "source_health": [
                {"source": "activity", "last_error": "503 error", "unresolved_gap_since": None, "coverage": "exhausted"},
                {"source": "journal", "last_error": None, "unresolved_gap_since": None, "coverage": "exhausted"},
            ],
        }
        errored_state = FeedCog._describe_profile_feed_state(channel_id=100, status=errored, is_candidate=True)
        self.assertNotIn("Active", errored_state)
        self.assertIn("continuity", errored_state.lower())
        self.assertIn("degraded", errored_state.lower())

        rec_gap = {**pending_base, "recipient_gap": {"reason": "unavailable_feed_target"}}
        rec_state = FeedCog._describe_profile_feed_state(channel_id=100, status=rec_gap, is_candidate=True)
        self.assertNotIn("Active", rec_state)
        self.assertIn("continuity", rec_state.lower())
        self.assertIn("recipient recovery pending", rec_state.lower())

        incomplete = {
            **pending_base,
            "source_health": [
                {"source": "activity", "last_error": None, "unresolved_gap_since": None, "coverage": "limited"},
                {"source": "journal", "last_error": None, "unresolved_gap_since": None, "coverage": "exhausted"},
            ],
        }
        inc_state = FeedCog._describe_profile_feed_state(channel_id=100, status=incomplete, is_candidate=True)
        self.assertNotIn("Active", inc_state)
        self.assertIn("continuity", inc_state.lower())
        self.assertIn("incomplete coverage", inc_state.lower())

    async def test_pending_candidates_nonempty_empty_and_prior_admin_in_status_and_list_readonly(self):
        ch = _make_channel(100)

        # Case 1: pending candidates NON-EMPTY (M2 enables while M1=10 is pending candidate under broken chunk)
        await self._seed_initialized_active(
            self.db, username="Millers", discord_id=10, all_eligible=["millers"], all_members=[10],
        )
        await self._seed_initialized_active(
            self.db, username="Beta", discord_id=30, all_eligible=["millers", "beta"], all_members=[10, 30],
        )
        await self._seed_initialized_active(
            self.db, username="Gamma", discord_id=50, all_eligible=["millers", "beta", "gamma"], all_members=[10, 30, 50],
        )
        await self.db.link_user(20, "Millers")
        await self.db.set_guild_feed_preference(1, 20, True, member_ids=None)

        # Case 2: pending candidates EMPTY (Beta had M1=30; M2=40 enables under broken chunk, then M1=30 unlinks -> candidate_member_ids='[]')
        await self.db.link_user(40, "Beta")
        await self.db.set_guild_feed_preference(1, 40, True, member_ids=None)
        await self.db.unlink_user(30)

        # Case 3: possible prior admin (had_admin_source = 1) alongside member candidate
        await self.db._execute(
            """INSERT INTO guild_profile_feed_reconciliation
               (guild_id, profile_key, had_admin_source, candidate_member_ids)
               VALUES (1, 'gamma', 1, '[50]')"""
        )

        # Even when guild is chunked for /feed list, reads MUST NOT mutate reconciliation rows!
        guild_chunked = _make_guild(1, [10, 20, 40, 50, 99], channel=ch, chunked=True)
        guild_unchunked = _make_guild(1, [], channel=ch, chunked=False)

        before_rows = await self.db._fetchall(
            "SELECT profile_key, had_admin_source, candidate_member_ids FROM guild_profile_feed_reconciliation WHERE guild_id = 1 ORDER BY profile_key"
        )
        self.assertEqual(
            [(r["profile_key"], r["had_admin_source"], json.loads(r["candidate_member_ids"])) for r in before_rows],
            [("beta", 0, []), ("gamma", 1, [50]), ("millers", 0, [10])],
        )

        for target_uid, expected_hint in (
            (20, "continuity"),
            (40, "continuity"),
            (50, "admin"),
        ):
            status_dict = await self._run_feed_status(self.db, guild=guild_unchunked, user_id=target_uid)
            desc = status_dict["description"]
            self.assertNotIn("**Effective state:** Active", desc)
            self.assertIn("continuity", desc.lower())
            self.assertIn(expected_hint, desc.lower())
            self.assertNotIn("reset has already", desc.lower())

        list_dict, view = await self._run_feed_list(self.db, guild=guild_chunked, user_id=99)
        page_desc = view.pages[0].description
        for uname in ("Millers", "Beta", "Gamma"):
            line = next(l for l in page_desc.splitlines() if f"**{uname}**" in l)
            self.assertNotIn("· Active", line)
            self.assertIn("continuity", line.lower())

        # Verify reads were strictly read-only (no reconciliation mutation!)
        after_rows = await self.db._fetchall(
            "SELECT profile_key, had_admin_source, candidate_member_ids FROM guild_profile_feed_reconciliation WHERE guild_id = 1 ORDER BY profile_key"
        )
        self.assertEqual(
            [(r["profile_key"], r["had_admin_source"], json.loads(r["candidate_member_ids"])) for r in after_rows],
            [(r["profile_key"], r["had_admin_source"], json.loads(r["candidate_member_ids"])) for r in before_rows],
        )

    async def test_precedence_and_resolved_continuous_vs_inactive_outcomes(self):
        ch = _make_channel(100)
        await self._seed_initialized_active(self.db, username="Millers", discord_id=10)
        await self.db.link_user(20, "Millers")
        await self.db.set_guild_feed_preference(1, 20, True, member_ids=None)

        # Unavailable channel overrides pending continuity
        bad_ch = _make_channel(100, can_send=False)
        unavail_guild = _make_guild(1, [20], channel=bad_ch, chunked=True)
        status_unavail = await self._run_feed_status(self.db, guild=unavail_guild, user_id=20)
        self.assertIn("**Effective state:** Unavailable", status_unavail["description"])
        list_unavail, _ = await self._run_feed_list(self.db, guild=unavail_guild, user_id=99)
        self.assertIn("Unavailable", list_unavail["description"])

        # Missing channel overrides pending continuity (verify in guild 2 without unsetting guild 1's active state)
        await self.db.set_guild_feed_preference(2, 20, True, member_ids=None)
        await self.db._execute(
            """INSERT OR IGNORE INTO guild_profile_feed_reconciliation
               (guild_id, profile_key, had_admin_source, candidate_member_ids)
               VALUES (2, 'millers', 0, '[10]')"""
        )
        no_ch_guild = _make_guild(2, [20], channel=None, chunked=True)
        status_noch = await self._run_feed_status(self.db, guild=no_ch_guild, user_id=20)
        self.assertIn("**Effective state:** Pending channel", status_noch["description"])

        # Resolved continuous outcome: both M1(10) and M2(20) present -> reconciles to Active!
        ok_guild = _make_guild(1, [10, 20], channel=ch, chunked=True)
        await self.db.reconcile_guild_feed_activity(1, ["millers"], now=self.now, present_member_ids=[10, 20])
        status_cont = await self._run_feed_status(self.db, guild=ok_guild, user_id=20)
        self.assertIn("**Effective state:** Active\n", status_cont["description"] + "\n")
        list_cont, _ = await self._run_feed_list(self.db, guild=ok_guild, user_id=99)
        self.assertIn("**Millers** — Member · Active", list_cont["description"])

        # Resolved inactive outcome: M1(30) leaves -> M2(31) enables under broken chunk -> snapshot [10, 20, 31] proves inactive gap -> Pending baseline!
        await self._seed_initialized_active(
            self.db,
            username="SoloLeave",
            discord_id=30,
            all_eligible=["millers", "sololeave"],
            all_members=[10, 20, 30],
        )
        await self.db.link_user(31, "SoloLeave")
        await self.db.set_guild_feed_preference(1, 31, True, member_ids=None)

        # Before snapshot: pending continuity
        status_before_res = await self._run_feed_status(self.db, guild=_make_guild(1, [], channel=ch, chunked=False), user_id=31)
        self.assertNotIn("**Effective state:** Active", status_before_res["description"])
        self.assertIn("continuity", status_before_res["description"].lower())

        # Resolve with [20, 31] (30 absent!): deactivated then re-activated -> Pending baseline (no pending_reconciliation)
        await self.db.reconcile_guild_feed_activity(1, ["millers", "sololeave"], now=self.now, present_member_ids=[10, 20, 31])
        status_resolved_inactive = await self._run_feed_status(self.db, guild=_make_guild(1, [10, 20, 31], channel=ch, chunked=True), user_id=31)
        self.assertIn("**Effective state:** Pending baseline", status_resolved_inactive["description"])
        list_resolved_inactive, _ = await self._run_feed_list(self.db, guild=_make_guild(1, [10, 20, 31], channel=ch, chunked=True), user_id=99)
        solo_line = next(l for l in list_resolved_inactive["description"].splitlines() if "**SoloLeave**" in l)
        self.assertIn("Pending baseline", solo_line)

        # Inactive (member opted out)
        await self.db.disable_guild_feed_preference_and_reconcile(1, 31, member_ids=[10, 20, 31])
        status_off = await self._run_feed_status(self.db, guild=_make_guild(1, [10, 20, 31], channel=ch, chunked=True), user_id=31)
        self.assertIn("**Effective state:** Inactive", status_off["description"])

    async def test_feed_list_with_many_pending_profiles_remains_bounded_across_sqlite_and_turso(self):
        turso_path = Path(self.tmpdir.name) / "list_pending_turso.db"
        seed = Database(str(turso_path), turso_url="")
        await seed.connect()
        await seed.close()
        turso_client = CountingTursoClient(str(turso_path))
        turso_db = Database(str(turso_path), turso_url="https://list-pending.turso.io")
        turso_db._client = turso_client
        self.addAsyncCleanup(turso_db.close)

        for label, db in (("sqlite", self.db), ("turso", turso_db)):
            with self.subTest(backend=label):
                await db.set_feed_channel(1, 100)
                for i in range(60):
                    await db.add_guild_admin_follow(1, f"Prof{i:03d}", None, 99, member_ids=[])
                await db.reconcile_guild_feed_activity(
                    1, [f"prof{i:03d}" for i in range(60)], now=self.now, present_member_ids=[],
                )
                for i in range(60):
                    await db.seed_feed_source(1, f"prof{i:03d}", "activity-display-v1", [f"a-{i}"])
                    await db.seed_feed_source(1, f"prof{i:03d}", "journal-display-v1", [f"j-{i}"])
                    if i % 2 == 0:
                        await db._execute(
                            """INSERT INTO guild_profile_feed_reconciliation
                               (guild_id, profile_key, had_admin_source, candidate_member_ids)
                               VALUES (1, ?, 1, '[]')""",
                            (f"prof{i:03d}",),
                        )

                ch = _make_channel(100)
                guild = _make_guild(1, [99], channel=ch, chunked=True)
                query_count = 0
                orig_fetchall = db._fetchall
                orig_fetchone = db._fetchone

                async def count_fetchall(*args, **kwargs):
                    nonlocal query_count
                    query_count += 1
                    return await orig_fetchall(*args, **kwargs)

                async def count_fetchone(*args, **kwargs):
                    nonlocal query_count
                    query_count += 1
                    return await orig_fetchone(*args, **kwargs)

                db._fetchall = count_fetchall
                db._fetchone = count_fetchone
                try:
                    _, view = await self._run_feed_list(db, guild=guild, user_id=99)
                finally:
                    db._fetchall = orig_fetchall
                    db._fetchone = orig_fetchone

                self.assertLessEqual(query_count, 15, f"backend={label} query_count={query_count}")
                lines = view.pages[0].description.splitlines()
                self.assertIn("continuity", lines[0].lower())
                self.assertNotIn("· Active", lines[0])
                self.assertIn("· Active", lines[1])


if __name__ == "__main__":
    unittest.main()
