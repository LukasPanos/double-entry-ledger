"""Database access and transaction boundaries.

Every database transaction in this service is opened by `transaction()` or
`run_in_transaction()` below, and nothing else. Both issue literal BEGIN /
COMMIT / ROLLBACK statements against a connection in autocommit mode, which
means psycopg is not managing transactions on our behalf: the boundaries are
exactly the lines you can read here and nowhere else.

That is deliberate. The usual failure mode in ledger code is a write that turns
out to have committed in a different transaction than the check that authorised
it. If the only way to open a transaction is a context manager that prints its
own BEGIN, that class of bug becomes visible in review.
"""

from __future__ import annotations

import logging
import os
import random
import time
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from typing import Any, TypeVar

import psycopg
from psycopg import Cursor
from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool

from ledger.config import get_settings

log = logging.getLogger("ledger.db")

T = TypeVar("T")

# Isolation levels we actually use. READ COMMITTED for the pessimistic strategy
# (correctness comes from row locks), SERIALIZABLE for the optimistic one
# (correctness comes from the database aborting conflicting transactions).
READ_COMMITTED = "READ COMMITTED"
SERIALIZABLE = "SERIALIZABLE"

# SQLSTATEs worth retrying. 40001 serialization_failure, 40P01 deadlock_detected.
RETRYABLE_SQLSTATES = frozenset({"40001", "40P01"})

_pool: ConnectionPool | None = None


# --------------------------------------------------------------------- pool --


def _configure_connection(conn: psycopg.Connection) -> None:
    # autocommit=True hands transaction control to us. Without this, psycopg
    # opens an implicit transaction on first execute() and commits it when the
    # pool checks the connection back in -- an invisible boundary.
    conn.autocommit = True
    with conn.cursor() as cur:
        # Timestamps are hashed into the tamper-evidence chain, so the session
        # timezone must not vary between the writer and the verifier.
        cur.execute("SET TIME ZONE 'UTC'")


def init_pool(dsn: str | None = None) -> ConnectionPool:
    global _pool
    if _pool is not None:
        return _pool

    settings = get_settings()
    _pool = ConnectionPool(
        conninfo=dsn or settings.database_url,
        min_size=settings.pool_min_size,
        max_size=settings.pool_max_size,
        configure=_configure_connection,
        kwargs={"row_factory": dict_row},
        open=True,
        timeout=10.0,
    )
    _pool.wait(timeout=15.0)
    return _pool


def get_pool() -> ConnectionPool:
    if _pool is None:
        return init_pool()
    return _pool


def close_pool() -> None:
    global _pool
    if _pool is not None:
        _pool.close()
        _pool = None


# ------------------------------------------------------ transaction control --


@contextmanager
def transaction(
    *,
    isolation: str = READ_COMMITTED,
    read_only: bool = False,
) -> Iterator[Cursor]:
    """Open one database transaction and yield a cursor bound to it.

    Commits if the block returns normally, rolls back if it raises. There is no
    third outcome: the connection is returned to the pool with no transaction
    open either way.
    """
    mode = " READ ONLY" if read_only else " READ WRITE"

    with get_pool().connection() as conn:
        cur = conn.cursor()
        cur.execute(f"BEGIN ISOLATION LEVEL {isolation}{mode}")  # <-- BEGIN
        try:
            yield cur
        except BaseException:
            # Best effort: if the connection itself is gone there is nothing to
            # roll back, and the server has already discarded the transaction.
            try:
                cur.execute("ROLLBACK")  # <-- ROLLBACK
            except psycopg.Error:
                log.warning("ROLLBACK failed; connection is being discarded")
            raise
        else:
            cur.execute("COMMIT")  # <-- COMMIT
        finally:
            cur.close()


def is_retryable(exc: BaseException) -> bool:
    return (
        isinstance(exc, psycopg.Error)
        and getattr(exc, "sqlstate", None) in RETRYABLE_SQLSTATES
    )


def run_in_transaction(
    work: Callable[[Cursor], T],
    *,
    isolation: str = READ_COMMITTED,
    read_only: bool = False,
    max_retries: int | None = None,
) -> T:
    """Run `work` in a transaction, retrying the whole thing on 40001/40P01.

    The retry replays `work` from the top against a fresh transaction. `work`
    must therefore derive everything it writes from the cursor it is handed --
    if it closes over a value it read in an earlier attempt, that value may be
    from an aborted snapshot.
    """
    settings = get_settings()
    if max_retries is None:
        max_retries = settings.max_retries

    last: BaseException | None = None

    for attempt in range(max_retries + 1):
        try:
            with transaction(isolation=isolation, read_only=read_only) as cur:
                return work(cur)
        except BaseException as exc:  # noqa: BLE001 -- re-raised below
            if not is_retryable(exc) or attempt == max_retries:
                raise
            last = exc
            # Full jitter exponential backoff. Without jitter, two conflicting
            # writers retry in lockstep and keep colliding.
            delay = min(
                settings.retry_max_delay_seconds,
                settings.retry_base_delay_seconds * (2**attempt),
            )
            time.sleep(random.uniform(0, delay))
            TX_RETRIES.append(getattr(exc, "sqlstate", "?"))

    raise AssertionError("unreachable") from last


# Retry counter, read by the load test to report how much work the optimistic
# strategy threw away. A plain list because appends are atomic under the GIL and
# the load test only ever reads it after joining its threads.
TX_RETRIES: list[str] = []


# ---------------------------------------------------------------- migration --


def _migration_files(directory: str) -> list[tuple[str, str]]:
    names = sorted(f for f in os.listdir(directory) if f.endswith(".sql"))
    out = []
    for name in names:
        with open(os.path.join(directory, name), encoding="utf-8") as fh:
            out.append((name, fh.read()))
    return out


def migrate(directory: str = "migrations") -> list[str]:
    """Apply pending migrations. Each file runs in its own transaction."""
    with transaction() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS schema_migrations (
                version    text        PRIMARY KEY,
                applied_at timestamptz NOT NULL DEFAULT now()
            )
            """
        )

    with transaction(read_only=True) as cur:
        cur.execute("SELECT version FROM schema_migrations")
        applied = {row["version"] for row in cur.fetchall()}

    newly_applied = []
    for name, sql in _migration_files(directory):
        if name in applied:
            continue
        with transaction() as cur:
            cur.execute(sql)
            cur.execute(
                "INSERT INTO schema_migrations (version) VALUES (%s)", (name,)
            )
        newly_applied.append(name)
        log.info("applied migration %s", name)

    return newly_applied


def fetch_one(cur: Cursor, sql: str, params: Any = None) -> dict[str, Any] | None:
    cur.execute(sql, params)
    return cur.fetchone()


def fetch_all(cur: Cursor, sql: str, params: Any = None) -> list[dict[str, Any]]:
    cur.execute(sql, params)
    return cur.fetchall()
