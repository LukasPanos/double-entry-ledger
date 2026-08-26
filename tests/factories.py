"""Helpers for building ledger state in tests."""

from __future__ import annotations

from typing import Any
from uuid import UUID, uuid4

from ledger import db
from ledger.schemas import CreateAccountRequest, CreateTransactionRequest
from ledger.services import accounts as accounts_service
from ledger.services import transactions as transactions_service
from ledger.services.posting import Posting, append_transaction


def make_account(
    *, currency: str = "USD", type_: str = "user", name: str | None = None
) -> UUID:
    row = accounts_service.create_account(
        CreateAccountRequest(
            name=name or f"{type_} {currency} {uuid4().hex[:8]}",
            currency=currency,
            type=type_,
        )
    )
    return row["id"]


def settlement_account(currency: str = "USD") -> UUID:
    return make_account(
        currency=currency,
        type_="external_settlement",
        name=f"External Settlement {currency}",
    )


def revenue_account(currency: str = "USD") -> UUID:
    return make_account(
        currency=currency,
        type_="platform_revenue",
        name=f"Platform Revenue {currency}",
    )


def transaction_request(
    entries: list[tuple[UUID, int, str]], description: str = "test transaction"
) -> CreateTransactionRequest:
    return CreateTransactionRequest(
        description=description,
        entries=[
            {"account_id": a, "amount_minor": m, "currency": c} for a, m, c in entries
        ],
    )


def post_outcome(
    entries: list[tuple[UUID, int, str]],
    *,
    description: str = "test transaction",
    key: UUID | None = None,
):
    return transactions_service.post_transaction(
        transaction_request(entries, description), key or uuid4()
    )


def post(
    entries: list[tuple[UUID, int, str]],
    *,
    description: str = "test transaction",
    key: UUID | None = None,
) -> dict[str, Any]:
    """Post a transaction from (account_id, amount_minor, currency) tuples."""
    return post_outcome(entries, description=description, key=key).body


def fund(account_id: UUID, amount_minor: int, currency: str = "USD") -> dict[str, Any]:
    """Move `amount_minor` in from external settlement.

    Creates the settlement account on first use, since there is exactly one per
    currency and tests start from an empty database.
    """
    with db.transaction(read_only=True) as cur:
        cur.execute(
            """
            SELECT id FROM accounts
             WHERE type = 'external_settlement' AND currency = %s
            """,
            (currency,),
        )
        row = cur.fetchone()

    settlement = row["id"] if row else settlement_account(currency)
    return post(
        [
            (settlement, -amount_minor, currency),
            (account_id, amount_minor, currency),
        ],
        description=f"funding {amount_minor} {currency}",
    )


def derived_balance(account_id: UUID) -> int:
    with db.transaction(read_only=True) as cur:
        cur.execute(
            "SELECT COALESCE(SUM(amount_minor), 0) AS b FROM entries WHERE account_id = %s",
            (account_id,),
        )
        return cur.fetchone()["b"]


def cached_balance(account_id: UUID) -> int:
    with db.transaction(read_only=True) as cur:
        cur.execute(
            "SELECT balance_minor FROM account_balances WHERE account_id = %s",
            (account_id,),
        )
        return cur.fetchone()["balance_minor"]


def count_rows(table: str) -> int:
    with db.transaction(read_only=True) as cur:
        cur.execute(f'SELECT count(*) AS n FROM "{table}"')
        return cur.fetchone()["n"]


def raw_insert_transaction(
    cur: Any,
    *,
    transaction_id: UUID | None = None,
    prev_hash: bytes | None = None,
    tx_hash: bytes | None = None,
) -> UUID:
    """Insert a transactions row directly, bypassing the service layer.

    Used to prove the database-level guards hold even when application code is
    wrong. The hashes are arbitrary: nothing validates them at write time, which
    is exactly why GET /integrity exists.
    """
    from datetime import datetime, timezone

    transaction_id = transaction_id or uuid4()
    key = uuid4()
    # transactions.idempotency_key has a foreign key to idempotency_keys as of
    # migration 002, so even a deliberately raw insert needs the parent row.
    cur.execute(
        "INSERT INTO idempotency_keys (key, request_hash) VALUES (%s, %s)",
        (key, b"\x00" * 32),
    )
    cur.execute(
        """
        INSERT INTO transactions
            (id, idempotency_key, description, created_at, prev_hash, tx_hash)
        VALUES (%s, %s, %s, %s, %s, %s)
        """,
        (
            transaction_id,
            key,
            "raw insert",
            datetime.now(timezone.utc),
            prev_hash if prev_hash is not None else transaction_id.bytes * 2,
            tx_hash if tx_hash is not None else uuid4().bytes * 2,
        ),
    )
    return transaction_id
