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


def make_hold(
    account_id: UUID,
    amount_minor: int,
    *,
    currency: str = "USD",
    expires_in_seconds: int = 3600,
    key: UUID | None = None,
):
    from ledger.schemas import CreateHoldRequest
    from ledger.services import holds as holds_service

    return holds_service.create_hold(
        CreateHoldRequest(
            account_id=account_id,
            amount_minor=amount_minor,
            currency=currency,
            expires_in_seconds=expires_in_seconds,
        ),
        key or uuid4(),
    )


def capture(
    hold_id: UUID,
    credits: list[tuple[UUID, int]],
    *,
    amount_minor: int | None = None,
    key: UUID | None = None,
):
    from ledger.schemas import CaptureHoldRequest
    from ledger.services import holds as holds_service

    return holds_service.capture_hold(
        hold_id,
        CaptureHoldRequest(
            amount_minor=amount_minor,
            credits=[
                {"account_id": a, "amount_minor": m} for a, m in credits
            ],
        ),
        key or uuid4(),
    )


def void(hold_id: UUID, *, key: UUID | None = None):
    from ledger.schemas import VoidHoldRequest
    from ledger.services import holds as holds_service

    return holds_service.void_hold(hold_id, VoidHoldRequest(), key or uuid4())


def balance(account_id: UUID) -> dict[str, Any]:
    return accounts_service.get_balance(account_id)


def expire_hold_now(hold_id: UUID) -> None:
    """Rewind a hold's deadline into the past.

    `expires_at` is immutable through the service and the state-machine trigger
    forbids changing it -- which is the point. Tests need to reach the lapsed
    state without sleeping for an hour, so they suppress the trigger.

    `SET LOCAL session_replication_role` rather than `ALTER TABLE ... DISABLE
    TRIGGER`: the ALTER takes an ACCESS EXCLUSIVE lock on the whole table for the
    rest of the transaction, which would block every concurrent reader and made
    the sweeper-concurrency test fail for a reason that had nothing to do with
    the sweeper. This form is session-scoped and takes no table lock.
    """
    with db.transaction() as cur:
        cur.execute("SET LOCAL session_replication_role = 'replica'")
        cur.execute(
            "UPDATE holds SET expires_at = now() - interval '1 second' WHERE id = %s",
            (hold_id,),
        )


def corrupt(sql: str, params: Any = None) -> None:
    """Run a statement with every guard switched off.

    Used to manufacture the broken states that /reconciliation and /integrity
    are supposed to detect. A reconciliation suite that has never been shown to
    fail is not evidence of anything, so the tests forge damage on purpose.

    `session_replication_role = 'replica'` suppresses the append-only triggers,
    the deferred zero-sum constraint triggers and foreign key checks for this
    transaction only, and takes no table locks. This is the same door a database
    administrator has, which is exactly why tamper *evidence* exists alongside
    tamper *prevention*.
    """
    with db.transaction() as cur:
        cur.execute("SET LOCAL session_replication_role = 'replica'")
        cur.execute(sql, params)


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
