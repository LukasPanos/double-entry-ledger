"""The single write primitive.

Every entry this service ever writes goes through `append_transaction`. Plain
transfers, hold captures, FX conversions and reversals are all just different
sets of postings handed to the same function, so the zero-sum check, the hash
chain append and the balance-cache maintenance cannot be accidentally skipped by
one caller.

`append_transaction` does not open a transaction. It takes a cursor that is
already inside one, because its whole purpose is to be composable with the
idempotency-key insert (Phase 2) and the outbox insert (Phase 6) in a single
atomic unit. The transaction boundary lives in the caller, in `ledger/db.py`.
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Iterable
from uuid import UUID, uuid4

from psycopg import Cursor

from ledger.errors import (
    AccountNotFound,
    CurrencyMismatch,
    InsufficientFunds,
    UnbalancedTransaction,
)
from ledger.hashing import GENESIS_PREV_HASH, HashableEntry, transaction_hash
from ledger.money import validate_amount, validate_currency


@dataclass(frozen=True, slots=True)
class Posting:
    account_id: UUID
    amount_minor: int
    currency: str


@dataclass(frozen=True, slots=True)
class AccountRow:
    id: UUID
    name: str
    currency: str
    type: str


# ------------------------------------------------------------- validation ----


def validate_postings(postings: Iterable[Posting]) -> dict[str, int]:
    """Reject anything malformed or unbalanced *before* any write happens.

    Returns the per-currency totals (all zero) so callers can see which
    currencies a transaction touched.
    """
    postings = list(postings)

    if len(postings) < 2:
        raise UnbalancedTransaction(
            "a transaction needs at least two entries",
            entry_count=len(postings),
        )

    totals: dict[str, int] = defaultdict(int)
    for index, p in enumerate(postings):
        validate_currency(p.currency)
        validate_amount(p.amount_minor, field=f"entries[{index}].amount_minor")
        if p.amount_minor == 0:
            raise UnbalancedTransaction(
                f"entries[{index}].amount_minor is zero; an entry that moves "
                f"nothing should not be written",
                index=index,
            )
        totals[p.currency] += p.amount_minor

    unbalanced = {c: t for c, t in totals.items() if t != 0}
    if unbalanced:
        raise UnbalancedTransaction(
            "entries must sum to zero in every currency; "
            + ", ".join(f"{c} is off by {t}" for c, t in sorted(unbalanced.items())),
            imbalance=unbalanced,
        )

    # An int64 sum can overflow even when the net is zero, e.g. two postings of
    # +2^62 and one of -2^63. Postgres would raise; we would rather say why.
    for currency, _ in totals.items():
        gross = sum(
            abs(p.amount_minor) for p in postings if p.currency == currency
        )
        validate_amount(gross, field="gross amount")

    return dict(totals)


# ------------------------------------------------------------------ locking --


def lock_accounts(cur: Cursor, account_ids: Iterable[UUID]) -> dict[UUID, AccountRow]:
    """Take a row lock on each account's balance row, in ascending id order.

    Deterministic lock ordering is what prevents deadlock. If transaction A
    locks account 1 then 2 while transaction B locks 2 then 1, they deadlock and
    Postgres kills one of them. Sorting the ids means every writer in the system
    acquires locks in the same sequence, so a cycle cannot form.

    The ORDER BY is load-bearing, not cosmetic: Postgres puts the LockRows plan
    node *above* the Sort node, so rows are locked in the order the sort emits
    them. tests/test_phase4_locking.py asserts that shape via EXPLAIN, so a
    planner change that broke the guarantee would fail the build.
    """
    ids = sorted(set(account_ids))
    if not ids:
        return {}

    cur.execute(
        """
        SELECT a.id, a.name, a.currency, a.type
          FROM account_balances b
          JOIN accounts a ON a.id = b.account_id
         WHERE b.account_id = ANY(%s)
         ORDER BY b.account_id
           FOR UPDATE OF b
        """,
        (ids,),
    )
    rows = cur.fetchall()
    found = {
        row["id"]: AccountRow(
            id=row["id"],
            name=row["name"],
            currency=row["currency"].strip(),
            type=row["type"],
        )
        for row in rows
    }

    missing = [str(i) for i in ids if i not in found]
    if missing:
        raise AccountNotFound(
            f"account(s) not found: {', '.join(missing)}", account_ids=missing
        )
    return found


def load_accounts(cur: Cursor, account_ids: Iterable[UUID]) -> dict[UUID, AccountRow]:
    """Same as `lock_accounts` but without FOR UPDATE, for the optimistic
    strategy. Safety there comes from SERIALIZABLE plus retry, not from locks."""
    ids = sorted(set(account_ids))
    if not ids:
        return {}

    cur.execute(
        "SELECT id, name, currency, type FROM accounts WHERE id = ANY(%s) ORDER BY id",
        (ids,),
    )
    found = {
        row["id"]: AccountRow(
            id=row["id"],
            name=row["name"],
            currency=row["currency"].strip(),
            type=row["type"],
        )
        for row in cur.fetchall()
    }
    missing = [str(i) for i in ids if i not in found]
    if missing:
        raise AccountNotFound(
            f"account(s) not found: {', '.join(missing)}", account_ids=missing
        )
    return found


def assert_currencies_match(
    postings: Iterable[Posting], accounts: dict[UUID, AccountRow]
) -> None:
    """Redundant with the composite foreign key in 001_core.sql. Kept so the
    client gets a 422 naming the account instead of a foreign-key violation."""
    for p in postings:
        account = accounts[p.account_id]
        if account.currency != p.currency:
            raise CurrencyMismatch(
                f"account {p.account_id} is denominated in {account.currency}, "
                f"but the entry is in {p.currency}",
                account_id=str(p.account_id),
                account_currency=account.currency,
                entry_currency=p.currency,
            )


# ------------------------------------------------------- overdraft checking --

# Account types allowed to go negative. `external_settlement` is the mirror of
# all money in the system: when a user is funded, settlement goes negative by
# the same amount, which is what makes the global sum zero. `platform_revenue`
# and `liquidity` are internal and may run either way.
MAY_GO_NEGATIVE = frozenset({"external_settlement", "platform_revenue", "liquidity"})


def assert_no_overdraft(
    cur: Cursor,
    postings: Iterable[Posting],
    accounts: dict[UUID, AccountRow],
) -> None:
    """Check available balance for every account this transaction debits.

    Called after `lock_accounts`, inside the same transaction as the write, so
    there is no window between the check and the insert. That ordering is the
    entire point: a check-then-act split here is the classic double-spend.
    """
    net_by_account: dict[UUID, int] = defaultdict(int)
    for p in postings:
        net_by_account[p.account_id] += p.amount_minor

    debited = {
        account_id: delta
        for account_id, delta in net_by_account.items()
        if delta < 0 and accounts[account_id].type not in MAY_GO_NEGATIVE
    }
    if not debited:
        return

    cur.execute(
        """
        SELECT e.account_id,
               COALESCE(SUM(e.amount_minor), 0) AS actual_minor
          FROM entries e
         WHERE e.account_id = ANY(%s)
         GROUP BY e.account_id
        """,
        (sorted(debited),),
    )
    actual = {row["account_id"]: row["actual_minor"] for row in cur.fetchall()}

    cur.execute(
        """
        SELECT h.account_id,
               COALESCE(SUM(h.amount_minor), 0) AS held_minor
          FROM holds h
         WHERE h.account_id = ANY(%s)
           AND h.status = 'pending'
           AND h.expires_at > now()
         GROUP BY h.account_id
        """,
        (sorted(debited),),
    )
    held = {row["account_id"]: row["held_minor"] for row in cur.fetchall()}

    for account_id, delta in sorted(debited.items()):
        actual_minor = actual.get(account_id, 0)
        held_minor = held.get(account_id, 0)
        available = actual_minor - held_minor
        if available + delta < 0:
            raise InsufficientFunds(
                f"account {account_id} has {available} available "
                f"({actual_minor} actual - {held_minor} held) in "
                f"{accounts[account_id].currency}, but the transaction debits "
                f"{-delta}",
                account_id=str(account_id),
                available_minor=available,
                actual_minor=actual_minor,
                held_minor=held_minor,
                requested_minor=-delta,
            )


# ------------------------------------------------------------------- append --


def _chain_head(cur: Cursor) -> bytes:
    """Current tip of the hash chain, or the genesis sentinel if empty.

    No lock is taken here. Two writers can read the same head, and both will try
    to insert a row with the same `prev_hash` -- at which point the UNIQUE index
    on `transactions.prev_hash` rejects the loser with 23505. `ledger.db`
    classifies that specific constraint as retryable, so the loser replays
    against the new head.

    That makes the hash chain a global serialization point: at most one
    transaction can commit per chain position. It is the throughput ceiling of
    this service and Phase 4 measures it directly. The alternative -- an advisory
    lock around the append -- turns the retry into a queue but does not raise the
    ceiling, and it would mask the per-account contention the benchmark is
    trying to isolate.
    """
    cur.execute("SELECT tx_hash FROM transactions ORDER BY seq DESC LIMIT 1")
    row = cur.fetchone()
    return bytes(row["tx_hash"]) if row else GENESIS_PREV_HASH


def append_transaction(
    cur: Cursor,
    *,
    description: str,
    idempotency_key: UUID,
    postings: list[Posting],
    transaction_id: UUID | None = None,
    created_at: datetime | None = None,
) -> dict[str, Any]:
    """Insert one transaction and its entries. Assumes the caller has already
    validated, locked and authorised. Returns the transaction row plus entries.
    """
    transaction_id = transaction_id or uuid4()
    # Generated here rather than by DEFAULT now() because this exact value is
    # hashed into the chain, so the writer has to know it byte for byte.
    created_at = created_at or datetime.now(timezone.utc)

    prev_hash = _chain_head(cur)
    tx_hash = transaction_hash(
        transaction_id=transaction_id,
        created_at=created_at,
        entries=[
            HashableEntry(p.account_id, p.currency, p.amount_minor) for p in postings
        ],
        prev_hash=prev_hash,
    )

    cur.execute(
        """
        INSERT INTO transactions
            (id, idempotency_key, description, created_at, prev_hash, tx_hash)
        VALUES (%s, %s, %s, %s, %s, %s)
        RETURNING id, seq, description, created_at, prev_hash, tx_hash
        """,
        (
            transaction_id,
            idempotency_key,
            description,
            created_at,
            prev_hash,
            tx_hash,
        ),
    )
    tx_row = cur.fetchone()
    assert tx_row is not None

    # executemany with RETURNING is supported by psycopg 3.2 but the ordering
    # contract is clearer one row at a time, and entries per transaction is a
    # handful, not thousands.
    entries = []
    for p in postings:
        cur.execute(
            """
            INSERT INTO entries (transaction_id, account_id, amount_minor, currency)
            VALUES (%s, %s, %s, %s)
            RETURNING id, account_id, amount_minor, currency
            """,
            (transaction_id, p.account_id, p.amount_minor, p.currency),
        )
        row = cur.fetchone()
        assert row is not None
        row["currency"] = row["currency"].strip()
        entries.append(row)

    _bump_balance_cache(cur, postings)

    return {
        "id": tx_row["id"],
        "seq": tx_row["seq"],
        "description": tx_row["description"],
        "created_at": tx_row["created_at"],
        "prev_hash": bytes(tx_row["prev_hash"]).hex(),
        "tx_hash": bytes(tx_row["tx_hash"]).hex(),
        "entries": entries,
    }


def _bump_balance_cache(cur: Cursor, postings: list[Posting]) -> None:
    """Fold the postings into the cached balances.

    This is a cache, not state. It is written in the same transaction as the
    entries so it can never be stale *and* committed, and GET /reconciliation
    proves it against SUM(entries). Nothing in the write path ever reads it as
    an authority -- overdraft checks read the entries table directly.

    Deltas are aggregated per account first so that a transaction touching the
    same account twice issues one UPDATE, which keeps the lock ordering
    established by `lock_accounts` intact.
    """
    deltas: dict[UUID, tuple[int, int]] = defaultdict(lambda: (0, 0))
    for p in postings:
        amount, count = deltas[p.account_id]
        deltas[p.account_id] = (amount + p.amount_minor, count + 1)

    for account_id in sorted(deltas):
        amount, count = deltas[account_id]
        cur.execute(
            """
            UPDATE account_balances
               SET balance_minor = balance_minor + %s,
                   entry_count   = entry_count + %s,
                   updated_at    = now()
             WHERE account_id = %s
            """,
            (amount, count, account_id),
        )
        if cur.rowcount != 1:
            # Cannot happen: accounts and account_balances are created together
            # and account_balances has a foreign key to accounts.
            raise AccountNotFound(
                f"no balance row for account {account_id}",
                account_id=str(account_id),
            )
