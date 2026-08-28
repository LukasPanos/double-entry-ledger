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
import threading
import time
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from typing import Any, TypeVar

import psycopg
from psycopg import Cursor
from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool

from ledger.config import get_settings
from ledger.errors import RetriesExhausted

log = logging.getLogger("ledger.db")

T = TypeVar("T")

# Isolation levels we actually use. READ COMMITTED for the pessimistic strategy
# (correctness comes from row locks), SERIALIZABLE for the optimistic one
# (correctness comes from the database aborting conflicting transactions).
READ_COMMITTED = "READ COMMITTED"
REPEATABLE_READ = "REPEATABLE READ"
SERIALIZABLE = "SERIALIZABLE"

#: Which isolation level each concurrency strategy needs (Phase 4).
#:
#: The pessimistic strategy takes explicit row locks, so READ COMMITTED is
#: enough -- the locks, not the snapshot, are what serialise conflicting writers.
#: The optimistic strategy takes no locks at all, so it needs the database to
#: detect the conflicts for it, which means SERIALIZABLE and a retry loop.
STRATEGY_ISOLATION = {
    "pessimistic": READ_COMMITTED,
    "optimistic": SERIALIZABLE,
}

# Unique constraints whose violation means "another writer appended to the hash
# chain first". Retrying is the correct response; see conflict_kind() for why
# this is a narrow allowlist rather than "retry all unique violations".
CHAIN_CONFLICT_CONSTRAINTS = frozenset(
    {"transactions_prev_hash_key", "transactions_tx_hash_key"}
)

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


def init_pool(
    dsn: str | None = None,
    *,
    min_size: int | None = None,
    max_size: int | None = None,
) -> ConnectionPool:
    global _pool
    if _pool is not None:
        return _pool

    settings = get_settings()
    _pool = ConnectionPool(
        conninfo=dsn or settings.database_url,
        min_size=min_size if min_size is not None else settings.pool_min_size,
        max_size=max_size if max_size is not None else settings.pool_max_size,
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


def conflict_kind(exc: BaseException) -> str | None:
    """Classify an exception as a retryable conflict, or None if it is not.

    The return value is the *reason*, not just a boolean, because Phase 4 needs
    to attribute wasted work: an optimistic run that retries a lot is only
    interesting if you can say whether it was fighting over account balances or
    over the hash chain.
    """
    if not isinstance(exc, psycopg.Error):
        return None

    sqlstate = getattr(exc, "sqlstate", None)
    if sqlstate == "40001":
        return "serialization_failure"
    if sqlstate == "40P01":
        return "deadlock"
    if sqlstate == "23505":
        # A unique violation is normally a client error and must NOT be retried
        # -- retrying an idempotency-key collision would defeat the entire point
        # of Phase 2. Only the hash-chain constraints are retryable, because
        # losing that race means "somebody else appended first", which is
        # resolved by reading the new head and trying again.
        constraint = getattr(exc.diag, "constraint_name", None)
        if constraint in CHAIN_CONFLICT_CONSTRAINTS:
            return "chain_conflict"
    return None


def is_retryable(exc: BaseException) -> bool:
    return conflict_kind(exc) is not None


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

    for attempt in range(max_retries + 1):
        try:
            with transaction(isolation=isolation, read_only=read_only) as cur:
                return work(cur)
        except BaseException as exc:  # noqa: BLE001 -- re-raised below
            kind = conflict_kind(exc)
            if kind is None:
                raise
            if attempt == max_retries:
                RETRIES.record(f"{kind}:exhausted")
                raise RetriesExhausted(
                    f"gave up after {max_retries} retries on {kind}",
                    conflict=kind,
                    attempts=max_retries + 1,
                ) from exc
            RETRIES.record(kind)
            # Full jitter exponential backoff. Without jitter, conflicting
            # writers retry in lockstep and collide again on the same beat.
            delay = min(
                settings.retry_max_delay_seconds,
                settings.retry_base_delay_seconds * (2**attempt),
            )
            time.sleep(random.uniform(0, delay))

    raise AssertionError("unreachable")


class RetryCounter:
    """Tally of conflicts by reason, for the Phase 4 benchmark.

    Not metrics infrastructure -- just enough to answer "how much work did the
    optimistic strategy throw away, and what was it fighting over".
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._counts: dict[str, int] = {}

    def record(self, kind: str) -> None:
        with self._lock:
            self._counts[kind] = self._counts.get(kind, 0) + 1

    def snapshot(self) -> dict[str, int]:
        with self._lock:
            return dict(self._counts)

    def total(self) -> int:
        with self._lock:
            return sum(self._counts.values())

    def reset(self) -> None:
        with self._lock:
            self._counts.clear()


RETRIES = RetryCounter()


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
