"""Exactly-once semantics for write endpoints.

The whole mechanism is one transaction containing two things:

    BEGIN
      INSERT INTO idempotency_keys (key, request_hash) ... ON CONFLICT DO NOTHING
      -- if that inserted a row, we own the key: do the business write
      -- if it did not, someone else owns it: replay their stored response
      UPDATE idempotency_keys SET response_body = ..., status_code = ...
    COMMIT

Insert-first, not check-then-insert. A `SELECT ... WHERE key = ?` followed by an
`INSERT` has a window between them, and under concurrent retries both requests
see "no row" and both process. Making the *insert* the check closes the window,
because the unique index is the arbiter and only one insert can win.

The interesting part is what happens to the loser. Under READ COMMITTED,
`INSERT ... ON CONFLICT DO NOTHING` against a row inserted by an *uncommitted*
transaction blocks on that transaction's xid rather than returning immediately.
So the second request waits for the first to finish, and then:

  * first committed  -> the conflict is real, we read the stored response and
                        replay it. The client gets the original result.
  * first rolled back -> there is no conflicting row any more, our insert wins,
                        and we process normally.

That is exactly the behaviour you want, and it comes from the database's
concurrency control rather than from application coordination. There is no
polling, no advisory lock, and no state machine.

Because the reservation and the response live in the same transaction, a
committed row always has a response. There is no committed "in flight" state to
handle, and a request that fails rolls the reservation back with it -- so a
failed write does not consume its key. See docs/decisions.md 2.3 for why that is
the behaviour I chose.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any
from uuid import UUID

from psycopg import Cursor
from psycopg.types.json import Jsonb

from ledger.db import READ_COMMITTED, run_in_transaction
from ledger.errors import IdempotencyKeyInFlight, IdempotencyKeyReused
from ledger.schemas import IdempotentRequest


def canonical_json(value: Any) -> bytes:
    """Deterministic JSON bytes: sorted keys, no insignificant whitespace.

    Only used for fingerprinting requests, never for the hash chain -- the chain
    uses the bespoke format in ledger/hashing.py precisely because JSON has no
    single canonical encoding across implementations. Here both the writer and
    the reader are this process, on this Python version, so `sort_keys` is
    sufficient and much easier to read.
    """
    return json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=True
    ).encode("utf-8")


def fingerprint_hash(fingerprint: dict[str, Any]) -> bytes:
    return hashlib.sha256(canonical_json(fingerprint)).digest()


@dataclass(frozen=True, slots=True)
class Outcome:
    status_code: int
    body: dict[str, Any]
    replayed: bool


def execute_once(
    *,
    key: UUID,
    request: IdempotentRequest,
    status_code: int,
    work: Callable[[Cursor], dict[str, Any]],
    isolation: str = READ_COMMITTED,
) -> Outcome:
    """Run `work` at most once for `key`, in one transaction with the key claim.

    `work` receives a cursor already inside the transaction and must return a
    JSON-serialisable response body. It may be called more than once in total if
    the transaction is retried after a serialization failure, but it commits at
    most once -- which is the property that matters.
    """
    request_hash = fingerprint_hash(request.fingerprint())

    def run(cur: Cursor) -> Outcome:
        if _claim(cur, key, request_hash):
            body = work(cur)
            _store_response(cur, key, status_code=status_code, body=body)
            return Outcome(status_code=status_code, body=body, replayed=False)

        # Somebody else owns this key, and (under READ COMMITTED) `_claim`
        # already waited for them to commit before telling us so.
        return _replay(cur, key, request_hash)

    return run_in_transaction(run, isolation=isolation)


def _claim(cur: Cursor, key: UUID, request_hash: bytes) -> bool:
    cur.execute(
        """
        INSERT INTO idempotency_keys (key, request_hash)
        VALUES (%s, %s)
        ON CONFLICT (key) DO NOTHING
        """,
        (key, request_hash),
    )
    return cur.rowcount == 1


def _store_response(
    cur: Cursor, key: UUID, *, status_code: int, body: dict[str, Any]
) -> None:
    cur.execute(
        """
        UPDATE idempotency_keys
           SET response_body = %s,
               status_code   = %s
         WHERE key = %s
        """,
        (Jsonb(body), status_code, key),
    )
    if cur.rowcount != 1:
        raise AssertionError(
            f"idempotency key {key} vanished between claim and response store"
        )


def _replay(cur: Cursor, key: UUID, request_hash: bytes) -> Outcome:
    cur.execute(
        """
        SELECT request_hash, response_body, status_code
          FROM idempotency_keys
         WHERE key = %s
        """,
        (key,),
    )
    row = cur.fetchone()
    if row is None:
        # Only reachable if the owning transaction rolled back *and* another
        # request claimed and released the key in the gap. Retrying is correct.
        raise IdempotencyKeyInFlight(
            f"idempotency key {key} was released while replaying; retry",
            idempotency_key=str(key),
        )

    if bytes(row["request_hash"]) != request_hash:
        raise IdempotencyKeyReused(
            f"idempotency key {key} was already used for a different request; "
            f"a key identifies one specific operation and cannot be reused",
            idempotency_key=str(key),
        )

    if row["response_body"] is None or row["status_code"] is None:
        # No stored response: a backfilled or retention-pruned row. Re-executing
        # would risk a double write, so refuse instead of guessing.
        raise IdempotencyKeyInFlight(
            f"idempotency key {key} has no stored response and cannot be "
            f"replayed; use a new key",
            idempotency_key=str(key),
        )

    body = dict(row["response_body"])
    body["replayed"] = True
    return Outcome(status_code=row["status_code"], body=body, replayed=True)
