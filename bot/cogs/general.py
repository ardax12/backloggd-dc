"""GeneralCog: Handles zero-DB general utility commands such as /help."""

import logging
from typing import Any, Callable, Literal, Optional

import discord
from discord import app_commands
from discord.ext import commands

from bot.ui.embeds import create_help_embed
from bot.ui.views import HelpView

logger = logging.getLogger("backloggd.cogs.general")

HelpTopic = Literal[
    "Overview",
    "Getting Started",
    "Games & Profiles",
    "Social & Discovery",
    "Activity Feed & Admin",
]


class GeneralCog(commands.Cog, name="General"):
    def __init__(self, bot):
        self.bot = bot

    @app_commands.command(
        name="help",
        description="Browse Backloggd bot commands, setup steps, and tips by category",
    )
    @app_commands.describe(topic="Optional: Jump directly to a help category")
    async def help_command(
        self,
        interaction: discord.Interaction,
        topic: HelpTopic = "Overview",
    ):
        embed = create_help_embed(topic)
        author_id = getattr(getattr(interaction, "user", None), "id", None)
        view = HelpView(author_id=author_id, current_topic=topic)

        if not interaction.response.is_done():
            await interaction.response.send_message(embed=embed, view=view, ephemeral=True)
            original_response: Optional[Callable[..., Any]] = getattr(interaction, "original_response", None)
            if callable(original_response):
                try:
                    view.message = await original_response()
                except (discord.NotFound, discord.Forbidden, discord.HTTPException):
                    logger.debug("Could not fetch original_response for /help view", exc_info=True)
        else:
            view.message = await interaction.followup.send(
                embed=embed,
                view=view,
                ephemeral=True,
                wait=True,
            )


async def setup(bot):
    await bot.add_cog(GeneralCog(bot))
