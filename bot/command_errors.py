"""Global and local application-command error handling helpers."""

import asyncio
import logging
import discord
from discord import app_commands

logger = logging.getLogger("backloggd.bot")

_HANDLED_ERROR_KEY = "_app_command_error_handled"
_DEFER_VISIBILITY_KEY = "_interaction_defer_visibility"
GENERIC_COMMAND_ERROR_MESSAGE = (
    "❌ Something went wrong while running this command. Please try again later."
)
SAFE_PUBLIC_ERROR_PLACEHOLDER = (
    "⚠️ Unable to complete that request. Details were sent privately."
)


def _get_defer_state(interaction) -> dict | None:
    extras = getattr(interaction, "extras", None)
    if isinstance(extras, dict):
        state = extras.get(_DEFER_VISIBILITY_KEY)
        if isinstance(state, dict):
            return state
    state = getattr(interaction, _DEFER_VISIBILITY_KEY, None)
    return state if isinstance(state, dict) else None


def _set_defer_state(interaction, *, ephemeral: bool, original_resolved: bool) -> None:
    state = {"ephemeral": bool(ephemeral), "original_resolved": bool(original_resolved)}
    extras = getattr(interaction, "extras", None)
    if isinstance(extras, dict):
        extras[_DEFER_VISIBILITY_KEY] = state
        return
    try:
        setattr(interaction, _DEFER_VISIBILITY_KEY, state)
    except AttributeError:
        pass


def mark_original_response_resolved(interaction) -> None:
    """Record that @original has already been resolved so later followups are new messages."""
    state = _get_defer_state(interaction)
    if state is not None:
        state["original_resolved"] = True
    else:
        _set_defer_state(interaction, ephemeral=False, original_resolved=True)


async def defer_interaction(interaction, *, ephemeral: bool = False) -> None:
    """Defer an interaction and record whether @original is public or ephemeral."""
    if ephemeral:
        await interaction.response.defer(ephemeral=True)
    else:
        await interaction.response.defer()
    # Component update deferrals acknowledge an existing message; there is no
    # loading response to replace or delete when reporting an error.
    is_message_update = (
        getattr(interaction.response, "type", None) == discord.InteractionResponseType.deferred_message_update
    )
    _set_defer_state(interaction, ephemeral=ephemeral, original_resolved=is_message_update)


def _is_unresolved_public_defer(interaction) -> bool:
    state = _get_defer_state(interaction)
    if state is not None:
        return (not state["ephemeral"]) and (not state["original_resolved"])
    response = getattr(interaction, "response", None)
    resp_type = getattr(response, "type", None)
    deferred_ephemeral = getattr(interaction, "deferred_ephemeral", None)
    if (
        resp_type == discord.InteractionResponseType.deferred_channel_message
        and deferred_ephemeral is not True
    ):
        return not getattr(interaction, "original_consumed", False)
    return False


async def send_public_followup(interaction, *args, **kwargs):
    """Send a public followup/result and mark @original resolved."""
    result = await interaction.followup.send(*args, **kwargs)
    mark_original_response_resolved(interaction)
    return result


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


async def send_private_followup(
    interaction: discord.Interaction,
    message: str | None = None,
    *,
    embed: discord.Embed | None = None,
    allowed_mentions: discord.AllowedMentions | None = None,
    log: logging.Logger | None = None,
) -> bool:
    """Deliver a genuinely private error/hint without leaking via a public deferred original."""
    target_logger = log or logger
    command = getattr(interaction, "command", None) or getattr(interaction, "_cs_command", None)
    command_name = getattr(command, "qualified_name", None) or getattr(command, "name", "<unknown>")
    mentions = discord.AllowedMentions.none() if allowed_mentions is None else allowed_mentions

    send_args = (message,) if message is not None else ()
    send_kwargs: dict = {"ephemeral": True, "allowed_mentions": mentions}
    if embed is not None:
        send_kwargs["embed"] = embed

    response = getattr(interaction, "response", None)
    is_done_fn = getattr(response, "is_done", None)
    responded = bool(is_done_fn()) if callable(is_done_fn) else True

    if not responded:
        try:
            await response.send_message(*send_args, **send_kwargs)
            mark_original_response_resolved(interaction)
            return True
        except discord.InteractionResponded:
            responded = True
        except discord.HTTPException as send_error:
            target_logger.warning(
                "Failed to send private reply for command %r: %s",
                command_name,
                send_error,
            )
            return False

    edit_original = getattr(interaction, "edit_original_response", None)
    delete_original = getattr(interaction, "delete_original_response", None)
    resolved_public_placeholder = False

    if _is_unresolved_public_defer(interaction) and callable(edit_original):
        try:
            await edit_original(
                content=SAFE_PUBLIC_ERROR_PLACEHOLDER,
                allowed_mentions=discord.AllowedMentions.none(),
            )
            mark_original_response_resolved(interaction)
            resolved_public_placeholder = True
        except discord.HTTPException as edit_error:
            target_logger.warning(
                "Failed to resolve public deferred response for command %r: %s",
                command_name,
                edit_error,
            )
            if callable(delete_original):
                try:
                    await delete_original()
                except discord.HTTPException:
                    target_logger.debug(
                        "Best-effort delete after failed original edit also failed for %r",
                        command_name,
                        exc_info=True,
                    )
            return False

    try:
        await interaction.followup.send(*send_args, **send_kwargs)
        mark_original_response_resolved(interaction)
    except discord.HTTPException as send_error:
        target_logger.warning(
            "Failed to send error reply for command %r: %s",
            command_name,
            send_error,
        )
        return False

    if resolved_public_placeholder and callable(delete_original):
        try:
            await delete_original()
        except discord.HTTPException as delete_error:
            target_logger.info(
                "Could not delete generic placeholder response for %r: %s",
                command_name,
                delete_error,
            )
    return True


async def send_ephemeral_error(
    interaction: discord.Interaction,
    message: str,
    *,
    log: logging.Logger | None = None,
) -> bool:
    """Send an ephemeral error response without raising or recursing on Discord HTTP failures."""
    return await send_private_followup(interaction, message, log=log)



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


async def handle_component_error(
    interaction: discord.Interaction,
    error: BaseException,
    *,
    component_name: str,
    user_message: str = GENERIC_COMMAND_ERROR_MESSAGE,
    log: logging.Logger | None = None,
) -> bool:
    """Safely log an unexpected component callback error once and notify the user privately."""
    if isinstance(error, asyncio.CancelledError):
        raise error

    target_logger = log or logger
    guild_id = getattr(interaction, "guild_id", None)
    user_id = getattr(getattr(interaction, "user", None), "id", None)
    if isinstance(error, discord.HTTPException):
        target_logger.warning(
            "Discord HTTP failure in component %r (guild=%s, user=%s): %s",
            component_name,
            guild_id,
            user_id,
            error,
        )
    else:
        target_logger.error(
            "Unhandled error in component %r (guild=%s, user=%s)",
            component_name,
            guild_id,
            user_id,
            exc_info=error,
        )

    return await send_private_followup(interaction, user_message, log=target_logger)
