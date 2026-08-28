"""Hash-chain verification.

Walks `transactions` in `seq` order, recomputes each `tx_hash` from the rows in
`entries`, and checks that each `prev_hash` equals its predecessor's `tx_hash`.
Reports the first break.

Two things about the ordering, both of which took some thought:

**`seq` gaps are normal and are not a break.** `seq` is a `bigserial`, and a
transaction that rolls back still consumes its sequence value. Checking for
contiguity would report a failure every time a request was rejected, so the walk
only ever compares adjacent *committed* rows.

**`seq` order and chain order cannot disagree.** For transaction B to store A's
hash as its `prev_hash`, B must have read A's committed row, which means A
inserted (and therefore called `nextval`) before B did. So `A.seq < B.seq`
whenever B follows A in the chain, and sorting by `seq` reconstructs the chain
exactly. Two writers that read the same head both compute the same `prev_hash`
and one is rejected by `UNIQUE(prev_hash)`, so no fork survives to be walked.
"""

from __future__ import annotations

import time
from datetime import datetime, timezone
from typing import Any

from psycopg import Cursor

from ledger.db import REPEATABLE_READ, transaction
from ledger.hashing import GENESIS_PREV_HASH, HashableEntry, transaction_hash

# Rows pulled per round trip. The join below is ordered, so batching is just
# memory management -- it does not change what gets verified.
_BATCH = 2000


def _walk(cur: Cursor, *, stop_at_first_break: bool = True) -> dict[str, Any]:
    cur.execute(
        """
        SELECT t.seq,
               t.id,
               t.created_at,
               t.prev_hash,
               t.tx_hash,
               e.account_id,
               e.currency,
               e.amount_minor
          FROM transactions t
          JOIN entries e ON e.transaction_id = t.id
         ORDER BY t.seq, e.id
        """
    )

    expected_prev = GENESIS_PREV_HASH
    checked = 0
    breaks: list[dict[str, Any]] = []
    head_hash: bytes | None = None

    current: dict[str, Any] | None = None
    entries: list[HashableEntry] = []

    def finish(tx: dict[str, Any], tx_entries: list[HashableEntry]) -> bool:
        """Verify one transaction. Returns False if the walk should stop."""
        nonlocal expected_prev, checked, head_hash
        checked += 1

        stored_prev = bytes(tx["prev_hash"])
        stored_hash = bytes(tx["tx_hash"])

        if stored_prev != expected_prev:
            breaks.append(
                {
                    "reason": (
                        "genesis_mismatch"
                        if checked == 1
                        else "chain_break"
                    ),
                    "seq": tx["seq"],
                    "transaction_id": str(tx["id"]),
                    "detail": (
                        "prev_hash does not match the preceding transaction's "
                        "tx_hash"
                    ),
                    "expected_prev_hash": expected_prev.hex(),
                    "stored_prev_hash": stored_prev.hex(),
                }
            )
            return not stop_at_first_break

        recomputed = transaction_hash(
            transaction_id=tx["id"],
            created_at=tx["created_at"],
            entries=tx_entries,
            prev_hash=stored_prev,
        )
        if recomputed != stored_hash:
            breaks.append(
                {
                    "reason": "hash_mismatch",
                    "seq": tx["seq"],
                    "transaction_id": str(tx["id"]),
                    "detail": (
                        "recomputing the hash from this transaction's entries "
                        "does not reproduce the stored tx_hash, so a hashed "
                        "field was changed after the fact"
                    ),
                    "stored_tx_hash": stored_hash.hex(),
                    "recomputed_tx_hash": recomputed.hex(),
                }
            )
            return not stop_at_first_break

        expected_prev = stored_hash
        head_hash = stored_hash
        return True

    while True:
        rows = cur.fetchmany(_BATCH)
        if not rows:
            break
        for row in rows:
            if current is not None and row["seq"] != current["seq"]:
                if not finish(current, entries):
                    return _report(checked, breaks, head_hash)
                entries = []
            current = row
            entries.append(
                HashableEntry(
                    row["account_id"], row["currency"].strip(), row["amount_minor"]
                )
            )

    if current is not None:
        finish(current, entries)

    return _report(checked, breaks, head_hash)


def _report(
    checked: int, breaks: list[dict[str, Any]], head_hash: bytes | None
) -> dict[str, Any]:
    return {
        "transactions_checked": checked,
        "breaks": breaks,
        "head_hash": head_hash.hex() if head_hash else None,
    }


def verify_chain(*, stop_at_first_break: bool = True) -> dict[str, Any]:
    started = time.perf_counter()
    # One snapshot for the whole walk. Verifying across snapshots would report a
    # spurious break the moment a transaction committed mid-walk.
    with transaction(isolation=REPEATABLE_READ, read_only=True) as cur:
        result = _walk(cur, stop_at_first_break=stop_at_first_break)

    return {
        "ok": not result["breaks"],
        "transactions_checked": result["transactions_checked"],
        "first_break": result["breaks"][0] if result["breaks"] else None,
        "head_hash": result["head_hash"],
        "checked_at": datetime.now(timezone.utc),
        "duration_ms": round((time.perf_counter() - started) * 1000, 3),
    }


def chain_check_for_reconciliation(cur: Cursor) -> dict[str, Any]:
    """Same walk, but reusing a caller's snapshot.

    /reconciliation assembles every check inside one transaction so the whole
    report describes a single point in time; it cannot open its own.
    """
    return _walk(cur, stop_at_first_break=True)
