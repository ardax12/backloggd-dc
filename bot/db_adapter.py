"""Narrow SQLite / libSQL transaction and batch adapters for Database."""

from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from typing import Any, AsyncIterator, Callable, Iterable, Optional, Sequence
from urllib.parse import urljoin, urlsplit

from libsql_client import LibsqlError
from libsql_client.hrana.convert import _result_set_from_proto, _stmt_to_proto
from libsql_client.http import HttpClient
from libsql_client.result import Row

logger = logging.getLogger('backloggd.db')


MAX_HTTP_PIPELINE_STATEMENTS = 100
MAX_HTTP_PIPELINE_BYTES = 56 * 1024


def _estimate_pipeline_stmt_bytes(sql_bytes: int, row: Sequence[Any]) -> int:
    """Conservatively estimate JSON payload size for one Hrana pipeline execute request."""
    total = sql_bytes + len(row) * 24
    for value in row:
        if value is None or isinstance(value, bool):
            total += 16
        elif isinstance(value, (int, float)):
            total += 32
        elif isinstance(value, (bytes, bytearray, memoryview)):
            total += (len(value) * 4) // 3 + 24
        else:
            total += len(str(value).encode("utf-8")) + 24
    return total


def normalize_libsql_row(row: Any) -> Any:
    """Expose real libSQL results as mappings for named-column consumers."""
    return row.asdict() if isinstance(row, Row) else row


class _HttpTransaction:
    """Keep an interactive transaction on one Hrana HTTP stream.

    libsql-client 0.3.1 lacks HTTP transactions. Reuse its authenticated
    transport and wire codecs while carrying the stream baton ourselves.
    """

    def __init__(
        self,
        client: HttpClient,
        url: str,
        *,
        max_pipeline_statements: int = MAX_HTTP_PIPELINE_STATEMENTS,
        max_pipeline_bytes: int = MAX_HTTP_PIPELINE_BYTES,
    ) -> None:
        self._client = client
        self._base_url = url
        self._baton: str | None = None
        self._max_pipeline_statements = max(1, int(max_pipeline_statements))
        self._max_pipeline_bytes = max(1024, int(max_pipeline_bytes))

    async def _send_pipeline(
        self,
        requests: list[dict[str, Any]],
        *,
        finish: bool = False,
    ) -> list[dict[str, Any]]:
        response = await self._client._send(
            "POST",
            urljoin(self._base_url.rstrip("/") + "/", "v3/pipeline"),
            {"baton": self._baton, "requests": requests},
        )
        self._baton = response["baton"]
        if base_url := response.get("base_url"):
            location = urlsplit(base_url)
            if (
                location.scheme != urlsplit(self._base_url).scheme
                or not location.hostname or location.username is not None
            ):
                self._baton = None
                raise LibsqlError("Invalid transaction stream URL", "PROTOCOL_ERROR")
            self._base_url = base_url
        results = response["results"]
        if len(results) != len(requests):
            raise LibsqlError("Incomplete transaction pipeline response", "PROTOCOL_ERROR")
        for result in results:
            if result["type"] == "error":
                error = result["error"]
                raise LibsqlError(error["message"], error.get("code") or "UNKNOWN")
        if not finish and not self._baton:
            raise LibsqlError("Transaction stream closed before commit", "PROTOCOL_ERROR")
        return results

    async def _execute(self, sql: str, args: Sequence[Any] = (), *, finish: bool = False):
        requests: list[dict[str, Any]] = [
            {"type": "execute", "stmt": _stmt_to_proto(sql, list(args))}
        ]
        if finish:
            requests.append({"type": "close"})
        results = await self._send_pipeline(requests, finish=finish)
        return _result_set_from_proto(results[0]["response"]["result"])

    async def execute(self, sql: str, args: Sequence[Any] = ()):
        return await self._execute(sql, args)

    async def executemany(self, sql: str, rows: Sequence[Sequence[Any]]) -> None:
        if not rows:
            return
        sql_bytes = len(sql.encode("utf-8")) + 64
        batch: list[dict[str, Any]] = []
        batch_bytes = 128
        for row in rows:
            stmt_bytes = _estimate_pipeline_stmt_bytes(sql_bytes, row)
            if batch and (
                len(batch) >= self._max_pipeline_statements
                or batch_bytes + stmt_bytes > self._max_pipeline_bytes
            ):
                await self._send_pipeline(batch)
                batch = []
                batch_bytes = 128
            batch.append({"type": "execute", "stmt": _stmt_to_proto(sql, list(row))})
            batch_bytes += stmt_bytes
        if batch:
            await self._send_pipeline(batch)

    async def commit(self) -> None:
        await self._execute("COMMIT", finish=True)

    async def rollback(self) -> None:
        if self._baton:
            await self._execute("ROLLBACK", finish=True)


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
            return normalize_libsql_row(result.rows[0]) if result.rows else None
        async with self._conn.execute(query, tuple(params)) as cursor:
            return await cursor.fetchone()

    async def fetchall(self, query: str, params: Sequence[Any] = ()) -> list[Any]:
        if self._tx is not None:
            result = await self._tx.execute(query, list(params))
            return [normalize_libsql_row(row) for row in result.rows]
        async with self._conn.execute(query, tuple(params)) as cursor:
            return list(await cursor.fetchall())

    async def executemany(self, query: str, rows: Iterable[Sequence[Any]]) -> None:
        materialized = list(rows)
        if not materialized:
            return
        if self._tx is not None:
            tx_executemany: Callable[..., Any] | None = getattr(self._tx, "executemany", None)
            if callable(tx_executemany):
                await tx_executemany(query, materialized)
                return
            for row in materialized:
                await self._tx.execute(query, list(row))
            return
        await self._conn.executemany(query, materialized)


@asynccontextmanager
async def open_transaction(database: Any) -> AsyncIterator[TransactionAdapter]:
    """Acquire Database's non-reentrant operation lock once and wrap commit/rollback."""
    async with database._acquire_operation_lock():
        if database.is_turso:
            client = database._client
            is_http = isinstance(client, HttpClient)
            tx = _HttpTransaction(client, database.turso_url) if is_http else client.transaction()
            try:
                if is_http:
                    await tx.execute("BEGIN IMMEDIATE")
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
