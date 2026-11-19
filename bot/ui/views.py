"""Interactive Discord UI Views and Buttons for Backloggd Bot."""

import asyncio
import logging
import discord
from discord.ui import View, Button
from typing import Optional, Callable, Coroutine, Any, Sequence
from bot.command_errors import (
    GENERIC_COMMAND_ERROR_MESSAGE,
    defer_interaction,
    handle_component_error,
    send_private_followup,
)
from bot.input_validation import escape_display_text
from bot.ui.helpers import clamp_escaped_text, truncate_button_label

logger = logging.getLogger("backloggd.ui.views")

class LinkButtonView(View):
    """View with URL button(s) linking to Backloggd."""
    def __init__(self, label: str, url: str, extra_buttons: Optional[list[tuple[str, str]]] = None):
        super().__init__(timeout=None)
        self.add_item(Button(label=truncate_button_label(label), url=url, style=discord.ButtonStyle.link))
        if extra_buttons:
            for extra_label, extra_url in extra_buttons:
                self.add_item(
                    Button(label=truncate_button_label(extra_label), url=extra_url, style=discord.ButtonStyle.link)
                )

class ExpiringView(View):
    """Disable expired controls on the message that owns this view."""

    def __init__(self, *, timeout: float = 180):
        super().__init__(timeout=timeout)
        self.message = None
        self._retired = False

    def retire(self) -> None:
        """Stop this view cleanly and release its message handle when superseded."""
        self._retired = True
        self.message = None
        self.stop()

    async def on_timeout(self):
        # discord.py marks naturally timed-out views finished before this hook.
        if self._retired:
            return
        for item in self.children:
            if getattr(item, "url", None) is None:
                item.disabled = True
        message = self.message
        if message:
            try:
                await message.edit(view=self)
            except (discord.NotFound, discord.Forbidden, discord.HTTPException):
                logger.debug("Ignoring expected Discord error during expired view cleanup", exc_info=True)

    async def on_error(
        self,
        interaction: discord.Interaction,
        error: Exception,
        item: discord.ui.Item[Any],
        /,
    ) -> None:
        label = getattr(item, "label", None) or getattr(item, "custom_id", None) or item.__class__.__name__
        await handle_component_error(
            interaction,
            error,
            component_name=f"{self.__class__.__name__}:{label}",
            user_message=GENERIC_COMMAND_ERROR_MESSAGE,
            log=logger,
        )


class RouletteView(ExpiringView):
    """Interactive view for Backlog Roulette with a Reroll button."""
    def __init__(
        self,
        author_id: int,
        game_url: str,
        on_reroll: Callable[[discord.Interaction], Coroutine[Any, Any, None]]
    ):
        super().__init__()  # 3 minute interaction timeout
        self.author_id = author_id
        self.on_reroll = on_reroll

        # Backloggd Web Link Button
        self.add_item(Button(label="View on Backloggd", url=game_url, style=discord.ButtonStyle.link))

    @discord.ui.button(label="Reroll", emoji="🔄", style=discord.ButtonStyle.primary)
    async def reroll_button(self, interaction: discord.Interaction, button: Button):
        """Handle reroll button click."""
        if interaction.user.id != self.author_id:
            await interaction.response.send_message(
                "❌ Only the person who ran `/roulette` can spin again!",
                ephemeral=True
            )
            return

        if self.is_finished():
            return

        await self.on_reroll(interaction)


class TasteTwinsView(ExpiringView):
    """Interactive view for /taste-twins with link button and quick Duel button."""
    def __init__(
        self,
        user_url: str,
        top_twin_name: Optional[str] = None,
        on_compare: Optional[Callable[[discord.Interaction], Coroutine[Any, Any, None]]] = None
    ):
        super().__init__()
        self.add_item(Button(label="View Profile on Backloggd", url=user_url, style=discord.ButtonStyle.link))
        if top_twin_name and on_compare:
            compare_btn = Button(
                label=truncate_button_label(f"Duel with {top_twin_name}"),
                emoji="⚔️",
                style=discord.ButtonStyle.primary
            )
            compare_btn.callback = on_compare
            self.add_item(compare_btn)


class GameCardView(ExpiringView):
    """Interactive view for /game command with Backloggd link and optional 'View All Ratings' button."""
    def __init__(
        self,
        game_url: str,
        total_ratings: int,
        on_view_all: Optional[Callable[[discord.Interaction], Coroutine[Any, Any, None]]] = None
    ):
        super().__init__()
        self.add_item(Button(label="View on Backloggd", url=game_url, style=discord.ButtonStyle.link))
        if total_ratings > 12 and on_view_all:
            view_all_btn = Button(
                label=truncate_button_label(f"View All Ratings ({total_ratings})"),
                emoji="👥",
                style=discord.ButtonStyle.secondary
            )
            view_all_btn.callback = on_view_all
            self.add_item(view_all_btn)


class GamePickerView(ExpiringView):
    """Private, author-only select menu for disambiguating /game search results."""

    def __init__(
        self,
        author_id: int,
        candidates: Sequence[tuple[str, str, Optional[str]]],
        on_select: Callable[[discord.Interaction, str], Coroutine[Any, Any, bool]],
        *,
        timeout: float = 180,
    ):
        if not candidates or len(candidates) > 5:
            raise ValueError("GamePickerView requires between 1 and 5 candidates")
        super().__init__(timeout=timeout)
        self.author_id = author_id
        self.on_select = on_select
        self._accepted = False
        self._slug_by_value: dict[str, str] = {}
        self._label_by_value: dict[str, str] = {}

        select_options: list[discord.SelectOption] = []
        for index, (slug, label, description) in enumerate(candidates):
            option_value = str(index)
            clean_label = truncate_button_label(label or slug, max_length=100) or str(index + 1)
            clean_desc = truncate_button_label(description, max_length=100) if description else None
            self._slug_by_value[option_value] = slug
            self._label_by_value[option_value] = clean_label
            select_options.append(
                discord.SelectOption(
                    label=clean_label,
                    value=option_value,
                    description=clean_desc or None,
                )
            )

        self.select = discord.ui.Select(
            placeholder="Choose a game...",
            min_values=1,
            max_values=1,
            options=select_options,
        )
        self.select.callback = self._handle_select
        self.add_item(self.select)

    async def on_timeout(self):
        if self._accepted:
            return
        await super().on_timeout()

    async def _retire_picker_message(
        self,
        interaction: discord.Interaction,
        *,
        selected_label: Optional[str],
    ) -> None:
        edit_original = getattr(interaction, "edit_original_response", None)
        if not callable(edit_original):
            return
        edit_kwargs: dict[str, Any] = {"view": self}
        if selected_label is not None:
            edit_kwargs["content"] = clamp_escaped_text(
                f"✅ Selected **{escape_display_text(selected_label)}**.",
                2000,
            )
        try:
            await edit_original(**edit_kwargs)
        except (discord.NotFound, discord.Forbidden, discord.HTTPException):
            logger.debug(
                "Ignoring expected Discord error retiring game picker message",
                exc_info=True,
            )

    async def _handle_select(self, interaction: discord.Interaction) -> None:
        if interaction.user.id != self.author_id:
            await interaction.response.send_message(
                "❌ Only the person who ran `/game` can choose from this menu.",
                ephemeral=True,
            )
            return

        if self._accepted:
            if not interaction.response.is_done():
                await interaction.response.send_message(
                    "ℹ️ A game has already been selected from this menu.",
                    ephemeral=True,
                )
            return

        if self._retired or self.is_finished():
            if not interaction.response.is_done():
                await interaction.response.send_message(
                    "⏳ This game picker has expired. Please run `/game` again.",
                    ephemeral=True,
                )
            return

        values = list(self.select.values)
        if not values and isinstance(getattr(interaction, "data", None), dict):
            raw_values = interaction.data.get("values")
            if isinstance(raw_values, list):
                values = [str(v) for v in raw_values]

        if len(values) != 1 or values[0] not in self._slug_by_value:
            if not interaction.response.is_done():
                await interaction.response.send_message(
                    "❌ Invalid game selection. Please choose an option from the menu.",
                    ephemeral=True,
                )
            return

        selected_value = values[0]
        slug = self._slug_by_value[selected_value]
        selected_label = self._label_by_value[selected_value]

        # Claim single-shot selection before the first await so concurrent clicks
        # cannot both pass and trigger duplicate game loads.
        self._accepted = True
        self.select.disabled = True
        try:
            await defer_interaction(interaction)
            rendered = await self.on_select(interaction, slug)
        except Exception:
            self.retire()
            is_done = getattr(getattr(interaction, "response", None), "is_done", None)
            if callable(is_done) and is_done():
                await self._retire_picker_message(interaction, selected_label=None)
            raise
        except BaseException:
            self.retire()
            raise
        else:
            self.retire()
            await self._retire_picker_message(
                interaction,
                selected_label=selected_label if rendered else None,
            )


class RatingsView(ExpiringView):
    """Navigate the private ratings response without rerunning a game search."""

    def __init__(
        self, game_title: str, game_slug: str, guild_name: str, ratings: list, *, freshness_note: str | None = None
    ):
        super().__init__()
        self.game_title = game_title
        self.game_slug = game_slug
        self.guild_name = guild_name
        self.ratings = ratings
        self.freshness_note = freshness_note
        self.page = 1
        self.total_pages = max(1, (len(ratings) + 14) // 15)
        self._nav_lock = asyncio.Lock()
        self._update_buttons()

    def embed(self):
        from bot.ui.embeds import create_all_ratings_embed
        return create_all_ratings_embed(
            self.game_title, self.game_slug, self.guild_name, self.ratings,
            page=self.page, freshness_note=self.freshness_note,
        )

    def _update_buttons(self):
        self.previous_button.disabled = self.page <= 1
        self.next_button.disabled = self.page >= self.total_pages

    async def _move(self, interaction: discord.Interaction, offset: int):
        if not interaction.response.is_done():
            await defer_interaction(interaction)
        async with self._nav_lock:
            previous_page = self.page
            target_page = max(1, min(self.total_pages, previous_page + offset))
            self.page = target_page
            self._update_buttons()
            try:
                embed = self.embed()
                await interaction.edit_original_response(embed=embed, view=self)
            except BaseException:
                self.page = previous_page
                self._update_buttons()
                raise

    @discord.ui.button(label="Previous", style=discord.ButtonStyle.secondary)
    async def previous_button(self, interaction: discord.Interaction, button: Button):
        await self._move(interaction, -1)

    @discord.ui.button(label="Next", style=discord.ButtonStyle.secondary)
    async def next_button(self, interaction: discord.Interaction, button: Button):
        await self._move(interaction, 1)


class FeedListView(ExpiringView):
    """Page through server-local feed profiles in an ephemeral response."""

    def __init__(self, owner_id: int, pages: list[discord.Embed]):
        super().__init__()
        self.owner_id = owner_id
        self.pages = pages
        self.page = 0
        self._nav_lock = asyncio.Lock()
        self._update_buttons()

    def _update_buttons(self):
        self.previous_button.disabled = self.page <= 0
        self.next_button.disabled = self.page >= len(self.pages) - 1

    async def _move(self, interaction: discord.Interaction, offset: int):
        if interaction.user.id != self.owner_id:
            await interaction.response.send_message("Only the person who opened this feed list can change pages.", ephemeral=True)
            return
        if not interaction.response.is_done():
            await defer_interaction(interaction)
        async with self._nav_lock:
            previous_page = self.page
            target_page = max(0, min(len(self.pages) - 1, previous_page + offset))
            self.page = target_page
            self._update_buttons()
            try:
                await interaction.edit_original_response(embed=self.pages[target_page], view=self)
            except BaseException:
                self.page = previous_page
                self._update_buttons()
                raise

    @discord.ui.button(label="Previous", style=discord.ButtonStyle.secondary)
    async def previous_button(self, interaction: discord.Interaction, button: Button):
        await self._move(interaction, -1)

    @discord.ui.button(label="Next", style=discord.ButtonStyle.secondary)
    async def next_button(self, interaction: discord.Interaction, button: Button):
        await self._move(interaction, 1)


class ServerTopView(ExpiringView):
    """Author-only, expiring mode switcher for /server-top leaderboards."""

    def __init__(
        self,
        author_id: Optional[int],
        current_mode: str,
        modes: Sequence[tuple[str, Optional[str]]],
        on_mode_change: Callable[
            [discord.Interaction, str],
            Coroutine[Any, Any, tuple[Optional[discord.Embed], Optional[str]]],
        ],
        *,
        timeout: float = 180,
    ):
        if not modes or len(modes) > 25:
            raise ValueError("ServerTopView requires between 1 and 25 modes")
        super().__init__(timeout=timeout)
        self.author_id = author_id
        self.current_mode = current_mode
        self.on_mode_change = on_mode_change
        self._valid_modes = {mode_name for mode_name, _ in modes}
        self._generation = 0
        self._edit_lock = asyncio.Lock()

        options = [
            discord.SelectOption(
                label=mode_name,
                value=mode_name,
                emoji=emoji,
                default=(mode_name == current_mode),
            )
            for mode_name, emoji in modes
        ]
        self.select = discord.ui.Select(
            placeholder="Switch leaderboard mode...",
            min_values=1,
            max_values=1,
            options=options,
        )
        self.select.callback = self._handle_select
        self.add_item(self.select)

    def _set_active_mode(self, mode: str) -> None:
        self.current_mode = mode
        for option in self.select.options:
            option.default = option.value == mode

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        # Discord.py refreshes the view timeout only after this check passes.
        if self.author_id is not None and interaction.user.id == self.author_id:
            return True
        await interaction.response.send_message(
            "❌ Only the person who ran `/server-top` can switch leaderboard modes.",
            ephemeral=True,
        )
        return False

    async def on_timeout(self) -> None:
        # Apply the disabled controls after any in-flight selection edit.
        async with self._edit_lock:
            await super().on_timeout()

    async def _handle_select(self, interaction: discord.Interaction) -> None:
        if self._retired or self.is_finished():
            if not interaction.response.is_done():
                await interaction.response.send_message(
                    "⏳ This leaderboard menu has expired. Please run `/server-top` again.",
                    ephemeral=True,
                )
            return

        values = list(self.select.values)
        if not values and isinstance(getattr(interaction, "data", None), dict):
            raw_values = interaction.data.get("values")
            if isinstance(raw_values, list):
                values = [str(v) for v in raw_values]

        if len(values) != 1 or values[0] not in self._valid_modes:
            if not interaction.response.is_done():
                await interaction.response.send_message(
                    "❌ Invalid leaderboard mode selection. Please choose an option from the menu.",
                    ephemeral=True,
                )
            return

        requested_mode = values[0]
        # Claim latest selection generation before the first await so a slow earlier
        # request can never overwrite a newer selection.
        self._generation += 1
        generation = self._generation

        if not interaction.response.is_done():
            await defer_interaction(interaction)

        embed, error_message = await self.on_mode_change(interaction, requested_mode)

        async with self._edit_lock:
            if generation != self._generation or self._retired or self.is_finished():
                return

            if error_message is not None or embed is None:
                await send_private_followup(
                    interaction,
                    error_message or GENERIC_COMMAND_ERROR_MESSAGE,
                    log=logger,
                )
                return

            previous_mode = self.current_mode
            self._set_active_mode(requested_mode)
            try:
                edited_message = await interaction.edit_original_response(embed=embed, view=self)
            except BaseException:
                self._set_active_mode(previous_mode)
                raise
            if edited_message is not None:
                self.message = edited_message
