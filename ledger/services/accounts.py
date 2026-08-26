"""Account creation and balance reads."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any
from uuid import UUID, uuid4

import psycopg
from psycopg import Cursor

from ledger.db import transaction
from ledger.errors import AccountNotFound, ValidationFailed
from ledger.money import validate_currency
from ledger.schemas import CreateAccountRequest


def create_account(request: CreateAccountRequest) -> dict[str, Any]:
    validate_currency(request.currency)
    account_id = uuid4()

    try:
        with transaction() as cur:
            row = _insert_account(
                cur,
                account_id=account_id,
                name=request.name,
                currency=request.currency,
                type_=request.type,
            )
    except psycopg.errors.UniqueViolation as exc:
        if exc.diag.constraint_name == "accounts_one_system_account_per_currency":
            raise ValidationFailed(
                f"a {request.type} account already exists for {request.currency}; "
                f"there is exactly one per currency",
                type=request.type,
                currency=request.currency,
            ) from exc
        raise

    return row


def _insert_account(
    cur: Cursor,
    *,
    account_id: UUID,
    name: str,
    currency: str,
    type_: str,
) -> dict[str, Any]:
    """Insert the account and its balance-cache row together.

    They are created in the same statement pair inside one transaction so that
    `account_balances` has exactly one row per account, always. Every later code
    path (balance cache maintenance, pessimistic locking) relies on that row
    existing, and a foreign key alone would not guarantee it was created.
    """
    cur.execute(
        """
        INSERT INTO accounts (id, name, currency, type)
        VALUES (%s, %s, %s, %s)
        RETURNING id, name, currency, type, created_at
        """,
        (account_id, name, currency, type_),
    )
    row = cur.fetchone()
    assert row is not None
    row["currency"] = row["currency"].strip()

    cur.execute(
        """
        INSERT INTO account_balances (account_id, currency, balance_minor, entry_count)
        VALUES (%s, %s, 0, 0)
        """,
        (account_id, currency),
    )
    return row


def get_account(account_id: UUID) -> dict[str, Any]:
    with transaction(read_only=True) as cur:
        cur.execute(
            "SELECT id, name, currency, type, created_at FROM accounts WHERE id = %s",
            (account_id,),
        )
        row = cur.fetchone()
    if row is None:
        raise AccountNotFound(f"account {account_id} not found")
    row["currency"] = row["currency"].strip()
    return row


def get_balance(account_id: UUID) -> dict[str, Any]:
    """actual = SUM(entries). Always derived, never read from the cache.

    The cache exists to make this cheap, but reading it here would mean a bug in
    cache maintenance could authorise a payment. So the authoritative number is
    recomputed from entries, and /reconciliation is what proves the cache agrees.
    """
    with transaction(read_only=True) as cur:
        cur.execute("SELECT currency FROM accounts WHERE id = %s", (account_id,))
        account = cur.fetchone()
        if account is None:
            raise AccountNotFound(f"account {account_id} not found")

        cur.execute(
            """
            SELECT COALESCE(SUM(amount_minor), 0) AS actual_minor
              FROM entries
             WHERE account_id = %s
            """,
            (account_id,),
        )
        actual_minor = cur.fetchone()["actual_minor"]  # type: ignore[index]

        held_minor = _sum_active_holds(cur, account_id)

    return {
        "account_id": account_id,
        "currency": account["currency"].strip(),
        "actual_minor": actual_minor,
        "held_minor": held_minor,
        "available_minor": actual_minor - held_minor,
        "as_of": datetime.now(timezone.utc),
    }


def _sum_active_holds(cur: Cursor, account_id: UUID) -> int:
    """Sum of holds that are still reserving funds.

    The `expires_at > now()` predicate is why correctness does not depend on the
    expiry sweeper: a hold stops reserving funds the moment its deadline passes,
    not when a background job gets round to relabelling it.
    """
    cur.execute(
        """
        SELECT COALESCE(SUM(amount_minor), 0) AS held_minor
          FROM holds
         WHERE account_id = %s
           AND status = 'pending'
           AND expires_at > now()
        """,
        (account_id,),
    )
    return cur.fetchone()["held_minor"]  # type: ignore[index]
