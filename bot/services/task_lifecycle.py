"""Owned async task lifecycle helpers and shared shutdown drain coordination."""

import asyncio
import inspect
from collections.abc import Awaitable, Callable, Iterable


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


class TaskDrainGroup:
    """Coordinate idempotent, caller-cancellation-shielded async task draining."""

    def __init__(self) -> None:
        self.closing: bool = False
        self.tasks: set[asyncio.Task] = set()
        self._draining_task: asyncio.Task | None = None

    @staticmethod
    async def cancel_and_drain(
        tasks: Iterable[asyncio.Task] | Callable[[], Iterable[asyncio.Task]],
        *,
        exclude: Iterable[asyncio.Task | None] = (),
        repeat: bool = True,
    ) -> None:
        """Cancel pending tasks from a collection or callable and await completion."""
        excluded = {task for task in exclude if task is not None}
        current = asyncio.current_task()
        if current is not None:
            excluded.add(current)
        while True:
            source = tasks() if callable(tasks) else tasks
            pending = [
                task for task in list(source)
                if not task.done() and task not in excluded
            ]
            if not pending:
                break
            for task in pending:
                if not task.cancelling():
                    task.cancel()
            await asyncio.gather(*pending, return_exceptions=True)
            if not repeat:
                break

    async def close(
        self,
        *,
        task_source: Callable[[], Iterable[asyncio.Task]] | None = None,
        before_drain: Callable[[asyncio.Task | None], Awaitable[None]] | None = None,
        after_drain: Callable[[asyncio.Task | None], Awaitable[None] | None] | None = None,
        clear_tasks: bool = False,
    ) -> None:
        """Mark closing and run a single shielded drain task shared by concurrent callers."""
        self.closing = True
        caller_task = asyncio.current_task()
        draining = self._draining_task
        if draining is not None and not draining.done():
            await await_owned_task(draining)
            return

        async def _run_drain() -> None:
            if before_drain is not None:
                await before_drain(caller_task)
            source = task_source if task_source is not None else (lambda: self.tasks)
            await self.cancel_and_drain(source, exclude=(caller_task,), repeat=True)
            if clear_tasks:
                resolved = source()
                if isinstance(resolved, set):
                    resolved.clear()
            if after_drain is not None:
                result = after_drain(caller_task)
                if inspect.isawaitable(result):
                    await result

        draining = asyncio.create_task(_run_drain())
        self._draining_task = draining
        try:
            await await_owned_task(draining)
        finally:
            if self._draining_task is draining and draining.done():
                self._draining_task = None
