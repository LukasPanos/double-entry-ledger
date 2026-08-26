#!/usr/bin/env python
"""Create the system accounts and an opening funding transaction.

    python -m scripts.seed

Re-running is safe: every id is a UUIDv5 derived from a stable name, so a second
run inserts nothing and posts nothing.

Why there is a funding transaction at all: money cannot appear in a double-entry
ledger. To give a user a balance, something has to be debited, and that
something is `external_settlement` -- the account representing the world outside
this service (the bank, the card network, the PSP). It goes negative by exactly
the amount users hold, which is why the global sum across all accounts is always
zero. A user balance is a liability of the platform, and settlement is the
mirror of that liability.
"""

from __future__ import annotations

import logging
import sys
import uuid
from typing import Any

from psycopg import Cursor

from ledger import db
from ledger.services.accounts import _insert_account
from ledger.services.posting import Posting, append_transaction, validate_postings

# Fixed namespace so seeded ids are reproducible across machines and reruns.
NAMESPACE = uuid.UUID("6ba7b812-9dad-11d1-80b4-00c04fd430c8")

CURRENCIES = ("USD", "CAD")

DEMO_FUNDING_MINOR = 1_000_000  # $10,000.00


def stable_id(name: str) -> uuid.UUID:
    return uuid.uuid5(NAMESPACE, name)


def _ensure_account(
    cur: Cursor, *, name: str, currency: str, type_: str
) -> uuid.UUID:
    account_id = stable_id(f"account:{type_}:{currency}:{name}")
    cur.execute("SELECT 1 FROM accounts WHERE id = %s", (account_id,))
    if cur.fetchone() is not None:
        return account_id
    _insert_account(
        cur, account_id=account_id, name=name, currency=currency, type_=type_
    )
    logging.info("created %-20s %s %s", type_, currency, account_id)
    return account_id


def seed(cur: Cursor) -> dict[str, Any]:
    created: dict[str, Any] = {}

    for currency in CURRENCIES:
        created[f"external_settlement:{currency}"] = _ensure_account(
            cur,
            name=f"External Settlement {currency}",
            currency=currency,
            type_="external_settlement",
        )
        created[f"platform_revenue:{currency}"] = _ensure_account(
            cur,
            name=f"Platform Revenue {currency}",
            currency=currency,
            type_="platform_revenue",
        )

    demo_user = _ensure_account(
        cur, name="Demo User USD", currency="USD", type_="user"
    )
    created["user:USD"] = demo_user

    # Opening funding. The idempotency key is derived, not random, so a second
    # `python -m scripts.seed` collides on transactions.idempotency_key rather
    # than funding the demo user twice.
    funding_key = stable_id("transaction:opening-funding:USD")
    cur.execute(
        "SELECT id FROM transactions WHERE idempotency_key = %s", (funding_key,)
    )
    existing = cur.fetchone()
    if existing is not None:
        created["funding_transaction"] = existing["id"]
        logging.info("opening funding already posted")
        return created

    # The seed writes through `append_transaction` directly rather than through
    # the HTTP layer, so it has to record its own idempotency key -- the same
    # authorization record any client request would leave behind. `response_body`
    # stays NULL because there was no HTTP response to store, which means a
    # replay of this key is refused rather than re-executed.
    cur.execute(
        """
        INSERT INTO idempotency_keys (key, request_hash)
        VALUES (%s, sha256(%s))
        """,
        (funding_key, b"scripts.seed:opening-funding:USD"),
    )

    postings = [
        Posting(
            account_id=created["external_settlement:USD"],
            amount_minor=-DEMO_FUNDING_MINOR,
            currency="USD",
        ),
        Posting(account_id=demo_user, amount_minor=DEMO_FUNDING_MINOR, currency="USD"),
    ]
    validate_postings(postings)
    tx = append_transaction(
        cur,
        description="opening funding for demo user",
        idempotency_key=funding_key,
        postings=postings,
    )
    created["funding_transaction"] = tx["id"]
    logging.info("posted opening funding %s", tx["id"])
    return created


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    db.init_pool()
    try:
        # One transaction for the whole seed: either the system accounts and the
        # opening balance all exist, or none of them do.
        with db.transaction() as cur:
            created = seed(cur)
    finally:
        db.close_pool()

    for key, value in created.items():
        print(f"{key:36} {value}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
