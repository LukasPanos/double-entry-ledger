"""Reconciliation: prove the ledger's invariants against the stored data.

Every check here is a query that should return zero rows. That framing is
deliberate -- a check that computes a number and compares it to another number
needs someone to decide what "close enough" means, whereas "this query returns
nothing" has exactly one passing state. When a check fails it returns the
offending rows, so the report says *which* account or transaction is wrong rather
than just that something is.

The whole report runs inside **one** REPEATABLE READ read-only transaction. This
matters more than it looks: if each check opened its own snapshot, a transaction
committing between check 1 and check 2 would make the global sum and the
per-account sums disagree, and the report would cry wolf on a perfectly healthy
ledger. Read-only at REPEATABLE READ also means the report can never abort and
never blocks a writer.
"""

from __future__ import annotations

import time
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Callable

from psycopg import Cursor

from ledger.db import REPEATABLE_READ, transaction
from ledger.services.integrity import chain_check_for_reconciliation

# How many offending rows to include per failed check. A broken ledger can be
# broken in a million places; the report needs to stay readable.
_MAX_FAILURES = 20


def _check(
    name: str, detail: str, cur: Cursor, sql: str, params: Any = None
) -> dict[str, Any]:
    """Run a query that must return no rows."""
    cur.execute(sql, params)
    rows = cur.fetchmany(_MAX_FAILURES + 1)
    truncated = len(rows) > _MAX_FAILURES
    rows = rows[:_MAX_FAILURES]
    return {
        "name": name,
        "passed": not rows,
        "detail": detail
        + (f" ({_MAX_FAILURES}+ failures, truncated)" if truncated else ""),
        "failures": [_stringify(row) for row in rows],
    }


def _stringify(row: dict[str, Any]) -> dict[str, Any]:
    """Make a failure row JSON-safe without turning numbers into strings.

    `SUM(bigint)` is `numeric` in Postgres, which arrives as a `Decimal`. The
    SQL deliberately does not cast it back to `bigint`: a sum over many rows can
    exceed int64 even when every row fits, and an overflow error inside the
    *reconciliation* path would take out the tool you use to diagnose problems.
    So the widening stays and the conversion happens here, where the values are
    known to be whole numbers of minor units.
    """
    out: dict[str, Any] = {}
    for key, value in row.items():
        if isinstance(value, bool) or value is None:
            out[key] = value
        elif isinstance(value, int):
            out[key] = value
        elif isinstance(value, Decimal):
            out[key] = int(value) if value == value.to_integral_value() else float(value)
        elif isinstance(value, float):
            out[key] = value
        elif isinstance(value, memoryview):
            out[key] = bytes(value).hex()
        elif isinstance(value, str):
            out[key] = value.strip()
        else:
            out[key] = str(value)
    return out


# ------------------------------------------------------------------- checks --


def global_zero_sum(cur: Cursor) -> dict[str, Any]:
    """The cheapest and most valuable check in the system.

    Money only enters through external_settlement, and every transaction is
    zero-sum per currency, so the sum over all entries in a currency must be
    exactly zero. One query, no parameters, and a nonzero result means money was
    created or destroyed somewhere. If only one check could be run, this is it.
    """
    return _check(
        "global_zero_sum",
        "SUM(entries.amount_minor) is zero for every currency",
        cur,
        """
        SELECT currency, SUM(amount_minor) AS total_minor
          FROM entries
         GROUP BY currency
        HAVING SUM(amount_minor) <> 0
        """,
    )


def cached_balances_match_entries(cur: Cursor) -> dict[str, Any]:
    """The cache is an optimization; this is what makes that claim checkable.

    A FULL OUTER JOIN rather than a plain one, so a missing cache row is a
    failure too -- otherwise an account with no cache row would silently pass.
    """
    return _check(
        "cached_balances_match_entries",
        "account_balances.balance_minor equals SUM(entries) for every account",
        cur,
        """
        WITH derived AS (
            SELECT a.id AS account_id,
                   COALESCE(SUM(e.amount_minor), 0) AS derived_minor,
                   COUNT(e.id)                      AS derived_count
              FROM accounts a
              LEFT JOIN entries e ON e.account_id = a.id
             GROUP BY a.id
        )
        SELECT d.account_id,
               d.derived_minor,
               d.derived_count,
               b.balance_minor AS cached_minor,
               b.entry_count   AS cached_count
          FROM derived d
          FULL OUTER JOIN account_balances b ON b.account_id = d.account_id
         WHERE b.account_id IS NULL
            OR d.account_id IS NULL
            OR b.balance_minor <> d.derived_minor
            OR b.entry_count   <> d.derived_count
        """,
    )


def every_transaction_balances(cur: Cursor) -> dict[str, Any]:
    """Re-derives what the DEFERRED constraint trigger enforces at write time.

    Not redundant: the trigger can be disabled by a table owner, and this check
    runs against whatever is actually in the table now.
    """
    return _check(
        "every_transaction_balances",
        "every transaction's entries sum to zero in every currency",
        cur,
        """
        SELECT transaction_id, currency, SUM(amount_minor) AS total_minor
          FROM entries
         GROUP BY transaction_id, currency
        HAVING SUM(amount_minor) <> 0
        """,
    )


def no_single_entry_transactions(cur: Cursor) -> dict[str, Any]:
    return _check(
        "no_single_entry_transactions",
        "every transaction has at least two entries",
        cur,
        """
        SELECT t.id AS transaction_id, count(e.id) AS entry_count
          FROM transactions t
          LEFT JOIN entries e ON e.transaction_id = t.id
         GROUP BY t.id
        HAVING count(e.id) < 2
        """,
    )


def no_orphaned_entries(cur: Cursor) -> dict[str, Any]:
    """Guarded by a foreign key, so this should be structurally impossible.

    Checked anyway, because "impossible" and "verified" are different words, and
    the check costs one indexed anti-join.
    """
    return _check(
        "no_orphaned_entries",
        "every entry belongs to an existing transaction and account",
        cur,
        """
        SELECT e.id AS entry_id, e.transaction_id, e.account_id
          FROM entries e
         WHERE NOT EXISTS (SELECT 1 FROM transactions t WHERE t.id = e.transaction_id)
            OR NOT EXISTS (SELECT 1 FROM accounts a WHERE a.id = e.account_id)
        """,
    )


def captured_holds_link_to_real_transactions(cur: Cursor) -> dict[str, Any]:
    """Both directions of the capture link, plus the amount.

    A `captured` hold must point at a transaction that exists, that actually
    debited the held account, and that debited no more than was authorized. The
    first is a foreign key; the last two are not expressible as constraints and
    are the part worth checking.
    """
    return _check(
        "captured_holds_link_to_real_transactions",
        "every captured hold links to a transaction that debited the held "
        "account by no more than the authorized amount",
        cur,
        """
        SELECT h.id AS hold_id,
               h.status::text,
               h.amount_minor AS authorized_minor,
               h.captured_transaction_id,
               (
                 SELECT -COALESCE(SUM(e.amount_minor), 0)
                   FROM entries e
                  WHERE e.transaction_id = h.captured_transaction_id
                    AND e.account_id = h.account_id
               ) AS captured_minor
          FROM holds h
         WHERE h.status = 'captured'
           AND (
                NOT EXISTS (
                    SELECT 1 FROM transactions t
                     WHERE t.id = h.captured_transaction_id
                )
                OR (
                    SELECT -COALESCE(SUM(e.amount_minor), 0)
                      FROM entries e
                     WHERE e.transaction_id = h.captured_transaction_id
                       AND e.account_id = h.account_id
                ) NOT BETWEEN 1 AND h.amount_minor
               )
        """,
    )


def non_captured_holds_have_no_transaction(cur: Cursor) -> dict[str, Any]:
    return _check(
        "non_captured_holds_have_no_transaction",
        "pending, voided and expired holds carry no transaction link",
        cur,
        """
        SELECT id AS hold_id, status::text, captured_transaction_id
          FROM holds
         WHERE status <> 'captured'
           AND captured_transaction_id IS NOT NULL
        """,
    )


def no_negative_available_balances(cur: Cursor) -> dict[str, Any]:
    """The overdraft invariant, checked after the fact.

    Only for account types that are not permitted to go negative -- settlement,
    revenue and liquidity accounts legitimately do, and settlement always does by
    exactly the amount users hold.
    """
    return _check(
        "no_negative_available_balances",
        "no user account has a negative actual or available balance",
        cur,
        """
        WITH figures AS (
            SELECT a.id AS account_id,
                   a.type::text,
                   COALESCE((
                       SELECT SUM(e.amount_minor) FROM entries e
                        WHERE e.account_id = a.id
                   ), 0) AS actual_minor,
                   COALESCE((
                       SELECT SUM(h.amount_minor) FROM holds h
                        WHERE h.account_id = a.id
                          AND h.status = 'pending'
                          AND h.expires_at > now()
                   ), 0) AS held_minor
              FROM accounts a
             WHERE a.type = 'user'
        )
        SELECT account_id, type, actual_minor, held_minor,
               actual_minor - held_minor AS available_minor
          FROM figures
         WHERE actual_minor < 0
            OR actual_minor - held_minor < 0
        """,
    )


def every_transaction_has_an_authorization(cur: Cursor) -> dict[str, Any]:
    return _check(
        "every_transaction_has_an_authorization",
        "every transaction references a recorded idempotency key",
        cur,
        """
        SELECT t.id AS transaction_id, t.idempotency_key
          FROM transactions t
         WHERE NOT EXISTS (
                SELECT 1 FROM idempotency_keys k WHERE k.key = t.idempotency_key
               )
        """,
    )


def every_transaction_has_an_outbox_event(cur: Cursor) -> dict[str, Any]:
    """Proves the dual write was actually closed.

    The outbox insert lives inside `append_transaction`, on the same cursor as
    the entries, so a committed transaction without an event is impossible. This
    check is what turns "impossible" into "verified" -- and it is the check that
    would catch someone helpfully refactoring the emit out into its own
    transaction.
    """
    return _check(
        "every_transaction_has_an_outbox_event",
        "every committed transaction produced a transaction.posted event",
        cur,
        """
        SELECT t.id AS transaction_id, t.seq
          FROM transactions t
         WHERE NOT EXISTS (
                SELECT 1 FROM outbox o
                 WHERE o.event_type = 'transaction.posted'
                   AND o.payload ->> 'transaction_id' = t.id::text
               )
        """,
    )


def hash_chain_intact(cur: Cursor) -> dict[str, Any]:
    result = chain_check_for_reconciliation(cur)
    return {
        "name": "hash_chain_intact",
        "passed": not result["breaks"],
        "detail": (
            f"walked {result['transactions_checked']} transaction(s); every "
            f"tx_hash recomputes and every prev_hash links"
        ),
        "failures": result["breaks"],
    }


#: Ordered cheapest-and-most-important first, so a human reading a failed report
#: sees the headline problem at the top.
CHECKS: list[Callable[[Cursor], dict[str, Any]]] = [
    global_zero_sum,
    every_transaction_balances,
    no_single_entry_transactions,
    cached_balances_match_entries,
    no_orphaned_entries,
    no_negative_available_balances,
    captured_holds_link_to_real_transactions,
    non_captured_holds_have_no_transaction,
    every_transaction_has_an_authorization,
    every_transaction_has_an_outbox_event,
    hash_chain_intact,
]


def reconcile() -> dict[str, Any]:
    started = time.perf_counter()
    with transaction(isolation=REPEATABLE_READ, read_only=True) as cur:
        checks = [check(cur) for check in CHECKS]

    return {
        "ok": all(c["passed"] for c in checks),
        "checked_at": datetime.now(timezone.utc),
        "duration_ms": round((time.perf_counter() - started) * 1000, 3),
        "checks": checks,
    }


def assert_reconciled() -> dict[str, Any]:
    """Raise if anything is off. Used by the load test and the chaos runner,
    which need reconciliation to be a hard gate rather than a report."""
    report = reconcile()
    if not report["ok"]:
        failed = [c for c in report["checks"] if not c["passed"]]
        lines = [f"  {c['name']}: {c['failures']}" for c in failed]
        raise AssertionError(
            "reconciliation failed:\n" + "\n".join(lines)
        )
    return report
