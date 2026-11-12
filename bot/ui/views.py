"""Interactive Discord UI Views and Buttons for Backloggd Bot."""

import discord
from discord.ui import View, Button
from typing import Optional, Callable, Coroutine, Any

class LinkButtonView(View):
    """View with URL button(s) linking to Backloggd."""
    def __init__(self, label: str, url: str, extra_buttons: Optional[list[tuple[str, str]]] = None):
        super().__init__(timeout=None)
        self.add_item(Button(label=label, url=url, style=discord.ButtonStyle.link))
        if extra_buttons:
            for extra_label, extra_url in extra_buttons:
                self.add_item(Button(label=extra_label, url=extra_url, style=discord.ButtonStyle.link))

class RouletteView(View):
    """Interactive view for Backlog Roulette with a Reroll button."""
    def __init__(
        self,
        author_id: int,
        game_url: str,
        on_reroll: Callable[[discord.Interaction], Coroutine[Any, Any, None]]
    ):
        super().__init__(timeout=180)  # 3 minute interaction timeout
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

        await self.on_reroll(interaction)


class TasteTwinsView(View):
    """Interactive view for /taste-twins with link button and quick Duel button."""
    def __init__(
        self,
        user_url: str,
        top_twin_name: Optional[str] = None,
        on_compare: Optional[Callable[[discord.Interaction], Coroutine[Any, Any, None]]] = None
    ):
        super().__init__(timeout=180)
        self.add_item(Button(label="View Profile on Backloggd", url=user_url, style=discord.ButtonStyle.link))
        if top_twin_name and on_compare:
            compare_btn = Button(
                label=f"Duel with {top_twin_name}",
                emoji="⚔️",
                style=discord.ButtonStyle.primary
            )
            compare_btn.callback = on_compare
            self.add_item(compare_btn)


class GameCardView(View):
    """Interactive view for /game command with Backloggd link and optional 'View All Ratings' button."""
    def __init__(
        self,
        game_url: str,
        total_ratings: int,
        on_view_all: Optional[Callable[[discord.Interaction], Coroutine[Any, Any, None]]] = None
    ):
        super().__init__(timeout=180)
        self.add_item(Button(label="View on Backloggd", url=game_url, style=discord.ButtonStyle.link))
        if total_ratings > 12 and on_view_all:
            view_all_btn = Button(
                label=f"View All Ratings ({total_ratings})",
                emoji="👥",
                style=discord.ButtonStyle.secondary
            )
            view_all_btn.callback = on_view_all
            self.add_item(view_all_btn)


class RatingsView(View):
    """Navigate the private ratings response without rerunning a game search."""

    def __init__(self, game_title: str, game_slug: str, guild_name: str, ratings: list):
        super().__init__(timeout=180)
        self.game_title = game_title
        self.game_slug = game_slug
        self.guild_name = guild_name
        self.ratings = ratings
        self.page = 1
        self.total_pages = max(1, (len(ratings) + 14) // 15)
        self._update_buttons()

    def embed(self):
        from bot.ui.embeds import create_all_ratings_embed
        return create_all_ratings_embed(
            self.game_title, self.game_slug, self.guild_name, self.ratings, page=self.page
        )

    def _update_buttons(self):
        self.previous_button.disabled = self.page <= 1
        self.next_button.disabled = self.page >= self.total_pages

    async def _move(self, interaction: discord.Interaction, offset: int):
        self.page = max(1, min(self.total_pages, self.page + offset))
        self._update_buttons()
        await interaction.response.edit_message(embed=self.embed(), view=self)

    @discord.ui.button(label="Previous", style=discord.ButtonStyle.secondary)
    async def previous_button(self, interaction: discord.Interaction, button: Button):
        await self._move(interaction, -1)

    @discord.ui.button(label="Next", style=discord.ButtonStyle.secondary)
    async def next_button(self, interaction: discord.Interaction, button: Button):
        await self._move(interaction, 1)
