"""Narrow SQLite / libSQL transaction and batch adapters for Database."""

from __future__ import annotations

from contextlib import asynccontextmanager
import logging
from typing import Any, AsyncIterator, Iterable, Optional, Sequence

logger = logging.getLogger('backloggd.db')


class TransactionAdapter:
    """Uniform execute/fetch/executemany interface inside an active DB transaction."""

    def __init__(self, *, sqlite_conn: Any = None, turso_tx: Any = None) -> None:
        if (sqlite_conn is None) == (turso_tx is None):
            raise ValueError("TransactionAdapter requires exactly one backend handle")
        self._conn = sqlite_conn
        self._tx = turso_tx

    async def execute(self, query: str, params: Sequence[Any] = ()) -> int:
        if self._tx is not None:
            result = await self._tx.execute(query, list(params))
            return int(getattr(result, "rows_affected", 0) or 0)
        async with self._conn.execute(query, tuple(params)) as cursor:
            return int(cursor.rowcount)

    async def fetchone(self, query: str, params: Sequence[Any] = ()) -> Optional[Any]:
        if self._tx is not None:
            result = await self._tx.execute(query, list(params))
            return result.rows[0] if result.rows else None
        async with self._conn.execute(query, tuple(params)) as cursor:
            return await cursor.fetchone()

    async def fetchall(self, query: str, params: Sequence[Any] = ()) -> list[Any]:
        if self._tx is not None:
            result = await self._tx.execute(query, list(params))
            return list(result.rows)
        async with self._conn.execute(query, tuple(params)) as cursor:
            return list(await cursor.fetchall())

    async def executemany(self, query: str, rows: Iterable[Sequence[Any]]) -> None:
        materialized = list(rows)
        if not materialized:
            return
        if self._tx is not None:
            for row in materialized:
                await self._tx.execute(query, list(row))
            return
        await self._conn.executemany(query, materialized)


@asynccontextmanager
async def open_transaction(database: Any) -> AsyncIterator[TransactionAdapter]:
    """Acquire Database's non-reentrant operation lock once and wrap commit/rollback."""
    async with database._acquire_operation_lock():
        if database.is_turso:
            tx = database._client.transaction()
            try:
                yield TransactionAdapter(turso_tx=tx)
                await tx.commit()
            except BaseException:
                try:
                    await tx.rollback()
                except BaseException:
                    # A failed COMMIT may already have closed the remote stream.
                    logger.warning('Could not roll back failed Turso transaction', exc_info=True)
                raise
            finally:
                close = getattr(tx, 'close', None)
                if callable(close):
                    try:
                        close()
                    except Exception:
                        logger.warning('Could not close Turso transaction', exc_info=True)
            return

        conn = database._conn
        try:
            await conn.execute("BEGIN IMMEDIATE")
            yield TransactionAdapter(sqlite_conn=conn)
            await conn.commit()
        except BaseException:
            try:
                await conn.rollback()
            except BaseException:
                logger.warning('Could not roll back failed SQLite transaction', exc_info=True)
            raise


async def execute_batch_statements(
    database: Any, statements: Sequence[tuple[str, Sequence[Any]]]
) -> list[Any]:
    """Execute write/select statements atomically via Turso batch or guarded SQLite transaction."""
    if not statements:
        return []
    if database.is_turso:
        async with database._acquire_operation_lock():
            results = await database._client.batch(
                [(sql, list(args)) for sql, args in statements]
            )
            return list(results) if results is not None else []
    async with open_transaction(database) as tx:
        for sql, args in statements:
            await tx.execute(sql, args)
    return []
