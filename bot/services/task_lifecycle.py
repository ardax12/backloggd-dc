"""Await an owned task without transferring caller cancellation to it."""

import asyncio


async def await_owned_task(task: asyncio.Task):
    """Finish shared cleanup before propagating cancellation of a waiting caller.

    The owner keeps the task reference and controls its lifetime. Concurrent close
    callers can join it; cancellation of one caller must not cancel the drain.
    """
    if task is asyncio.current_task():
        return None
    cancelled = None
    while not task.done():
        try:
            await asyncio.shield(task)
        except asyncio.CancelledError as error:
            cancelled = error
    if cancelled is not None:
        if not task.cancelled():
            task.exception()
        raise cancelled
    return task.result()
