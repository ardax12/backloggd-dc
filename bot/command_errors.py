"""Global and local application-command error handling helpers."""

import asyncio
import logging
import discord
from discord import app_commands

logger = logging.getLogger("backloggd.bot")

_HANDLED_ERROR_KEY = "_app_command_error_handled"
GENERIC_COMMAND_ERROR_MESSAGE = (
    "❌ Something went wrong while running this command. Please try again later."
)


def mark_interaction_error_handled(interaction) -> None:
    """Record that an interaction's error reply ownership has been claimed."""
    extras = getattr(interaction, "extras", None)
    if isinstance(extras, dict):
        extras[_HANDLED_ERROR_KEY] = True
        return
    try:
        setattr(interaction, _HANDLED_ERROR_KEY, True)
    except AttributeError:
        pass


def is_interaction_error_handled(interaction) -> bool:
    """Return True only when a local error handler explicitly claimed the interaction."""
    extras = getattr(interaction, "extras", None)
    if isinstance(extras, dict) and extras.get(_HANDLED_ERROR_KEY) is True:
        return True
    return getattr(interaction, _HANDLED_ERROR_KEY, False) is True


async def send_ephemeral_error(
    interaction: discord.Interaction,
    message: str,
    *,
    log: logging.Logger | None = None,
) -> bool:
    """Send an ephemeral error response without raising or recursing on Discord HTTP failures."""
    target_logger = log or logger
    command = getattr(interaction, "command", None) or getattr(interaction, "_cs_command", None)
    command_name = getattr(command, "qualified_name", None) or getattr(command, "name", "<unknown>")
    try:
        if interaction.response.is_done():
            await interaction.followup.send(message, ephemeral=True)
        else:
            try:
                await interaction.response.send_message(message, ephemeral=True)
            except discord.InteractionResponded:
                await interaction.followup.send(message, ephemeral=True)
        return True
    except discord.HTTPException as send_error:
        target_logger.warning(
            "Failed to send error reply for command %r: %s",
            command_name,
            send_error,
        )
        return False


def classify_app_command_error(error: app_commands.AppCommandError) -> tuple[bool, str]:
    """Classify an AppCommandError into (is_expected_user_error, safe_ephemeral_message)."""
    if isinstance(error, app_commands.MissingPermissions):
        return True, "❌ You do not have the required permissions to run this command."
    if isinstance(error, app_commands.BotMissingPermissions):
        return True, "❌ I am missing required permissions to complete this command."
    if isinstance(error, app_commands.NoPrivateMessage):
        return True, "This command can only be used in a server."
    if isinstance(error, (app_commands.MissingRole, app_commands.MissingAnyRole)):
        return True, "❌ You do not have the required role to run this command."
    if isinstance(error, app_commands.CommandOnCooldown):
        retry_after = max(1, round(getattr(error, "retry_after", 1) or 1))
        return True, f"⏳ This command is on cooldown. Try again in {retry_after}s."
    if isinstance(error, app_commands.CheckFailure):
        return True, "❌ You cannot use this command right now."
    if isinstance(error, app_commands.TransformerError):
        return True, "❌ Invalid command input. Please check your arguments and try again."
    return False, GENERIC_COMMAND_ERROR_MESSAGE


async def handle_app_command_error(
    interaction: discord.Interaction,
    error: app_commands.AppCommandError,
    *,
    log: logging.Logger | None = None,
) -> None:
    """Global CommandTree error boundary with explicit local-handler cooperation."""
    if is_interaction_error_handled(interaction):
        return

    original = getattr(error, "original", None)
    if isinstance(original, asyncio.CancelledError):
        raise original
    if isinstance(error, asyncio.CancelledError):
        raise error

    target_logger = log or logger
    expected, reply = classify_app_command_error(error)
    if not expected:
        command = (
            getattr(interaction, "command", None)
            or getattr(interaction, "_cs_command", None)
            or getattr(error, "command", None)
        )
        command_name = getattr(command, "qualified_name", None) or getattr(command, "name", "<unknown>")
        guild_id = getattr(interaction, "guild_id", None)
        user_id = getattr(getattr(interaction, "user", None), "id", None)
        target_logger.error(
            "Unhandled error in application command %r (guild=%s, user=%s)",
            command_name,
            guild_id,
            user_id,
            exc_info=error,
        )

    mark_interaction_error_handled(interaction)
    await send_ephemeral_error(interaction, reply, log=target_logger)
