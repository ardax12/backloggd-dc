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
