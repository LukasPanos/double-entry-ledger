"""POST /transactions and the entry-history read path.

The write path is: validate before opening a transaction, then inside one
transaction claim the idempotency key, lock the accounts, append. The
switchable concurrency strategy lands in Phase 4.
"""

from __future__ import annotations

from typing import Any
from uuid import UUID

from psycopg import Cursor

from ledger.db import READ_COMMITTED, transaction
from ledger.errors import AccountNotFound, TransactionNotFound
from ledger.schemas import CreateTransactionRequest, TransactionResponse
from ledger.services.idempotency import Outcome, execute_once
from ledger.services.posting import (
    Posting,
    append_transaction,
    assert_currencies_match,
    assert_no_overdraft,
    lock_accounts,
    validate_postings,
)


def post_transaction(
    request: CreateTransactionRequest, idempotency_key: UUID
) -> Outcome:
    postings = [
        Posting(
            account_id=e.account_id,
            amount_minor=e.amount_minor,
            currency=e.currency,
        )
        for e in request.entries
    ]

    # Reject an unbalanced request here, before a transaction is even opened, so
    # the common client error never reaches the database. The DEFERRED constraint
    # trigger in 001_core.sql is the backstop for the case where this check is
    # wrong or a future code path forgets to call it.
    validate_postings(postings)

    def work(cur: Cursor) -> dict[str, Any]:
        # Lock, then check, then write -- all inside one transaction. The lock is
        # what makes the check meaningful: without it, two concurrent debits
        # could both read a sufficient available balance and both commit.
        accounts = lock_accounts(cur, [p.account_id for p in postings])
        assert_currencies_match(postings, accounts)
        assert_no_overdraft(cur, postings, accounts)
        tx = append_transaction(
            cur,
            description=request.description,
            idempotency_key=idempotency_key,
            postings=postings,
        )
        # Serialised here, inside the transaction, because this exact body is
        # what gets stored for replay. Building it later would risk the stored
        # response and the returned response drifting apart.
        return TransactionResponse.model_validate(tx).model_dump(mode="json")

    return execute_once(
        key=idempotency_key,
        fingerprint=request.fingerprint(),
        status_code=201,
        work=work,
        isolation=READ_COMMITTED,
    )


# ------------------------------------------------------------------- reads ----


def get_transaction(transaction_id: UUID) -> dict[str, Any]:
    with transaction(read_only=True) as cur:
        cur.execute(
            """
            SELECT id, seq, description, created_at, prev_hash, tx_hash
              FROM transactions
             WHERE id = %s
            """,
            (transaction_id,),
        )
        tx = cur.fetchone()
        if tx is None:
            raise TransactionNotFound(f"transaction {transaction_id} not found")

        cur.execute(
            """
            SELECT id, account_id, amount_minor, currency
              FROM entries
             WHERE transaction_id = %s
             ORDER BY id
            """,
            (transaction_id,),
        )
        entries = cur.fetchall()

    for e in entries:
        e["currency"] = e["currency"].strip()

    return {
        "id": tx["id"],
        "seq": tx["seq"],
        "description": tx["description"],
        "created_at": tx["created_at"],
        "prev_hash": bytes(tx["prev_hash"]).hex(),
        "tx_hash": bytes(tx["tx_hash"]).hex(),
        "entries": entries,
    }


def list_entries(
    account_id: UUID, *, limit: int = 50, cursor: int | None = None
) -> dict[str, Any]:
    """Keyset pagination on entries.id.

    Not OFFSET: entries is append-only and grows under the reader, and OFFSET
    would make a client walking history re-read or skip rows as new entries land.
    A cursor on a monotonic primary key gives a stable walk, and it stays O(log n)
    at page ten thousand.
    """
    with transaction(read_only=True) as cur:
        cur.execute("SELECT 1 FROM accounts WHERE id = %s", (account_id,))
        if cur.fetchone() is None:
            raise AccountNotFound(f"account {account_id} not found")

        cur.execute(
            """
            SELECT id, account_id, amount_minor, currency
              FROM entries
             WHERE account_id = %s
               AND (%s::bigint IS NULL OR id > %s::bigint)
             ORDER BY id
             LIMIT %s
            """,
            (account_id, cursor, cursor, limit + 1),
        )
        rows = cur.fetchall()

    has_more = len(rows) > limit
    rows = rows[:limit]
    for row in rows:
        row["currency"] = row["currency"].strip()

    return {
        "account_id": account_id,
        "entries": rows,
        "next_cursor": rows[-1]["id"] if (has_more and rows) else None,
    }
