"""Holds: authorizations that reserve spending power without moving money.

A hold writes no entries. It reduces what an account may spend without changing
what the account has, which is the whole reason balances can stay derived from
`entries` alone -- a hold is a fact about the *future*, and the ledger only
records the past.

    available = SUM(entries) - SUM(live holds)

"Live" means `status = 'pending' AND expires_at > now()`. The `expires_at` half
of that predicate is load-bearing: **correctness does not depend on the expiry
sweeper running.** A hold whose deadline has passed stops reserving funds the
instant it lapses, whether or not any background job has noticed. The sweeper
exists to keep the table tidy and the partial indexes small, not to make the
numbers right. If it were the thing that released funds, an outage in a
background worker would silently freeze customer money.

Lock ordering, which is what keeps this deadlock-free: **holds before
accounts.** Capture locks its hold row, then locks account rows in ascending id
order (see posting.lock_accounts). Plain transfers lock only accounts. Voids lock
only a hold. No cycle can form.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Any
from uuid import UUID, uuid4

from psycopg import Cursor

from ledger.db import READ_COMMITTED, transaction
from ledger.errors import (
    CaptureExceedsHold,
    HoldNotFound,
    HoldNotPending,
    UnbalancedTransaction,
    ValidationFailed,
)
from ledger.schemas import (
    CaptureHoldRequest,
    CreateHoldRequest,
    HoldResponse,
    VoidHoldRequest,
)
from ledger.services import outbox
from ledger.services.idempotency import Outcome, execute_once
from ledger.services.posting import (
    Posting,
    append_transaction,
    assert_currencies_match,
    assert_no_overdraft,
    lock_accounts,
    validate_postings,
)

log = logging.getLogger("ledger.holds")


# The captured amount is derived from the capture transaction's entries on the
# held account, rather than stored in a column. Same principle as balances: one
# source of truth, and a reconciliation check that proves the derivation.
_HOLD_SELECT = """
    SELECT h.id,
           h.account_id,
           h.amount_minor,
           h.currency,
           h.status,
           h.expires_at,
           h.captured_transaction_id,
           h.created_at,
           (h.expires_at <= now()) AS lapsed,
           CASE WHEN h.captured_transaction_id IS NULL THEN NULL ELSE (
               SELECT -COALESCE(SUM(e.amount_minor), 0)
                 FROM entries e
                WHERE e.transaction_id = h.captured_transaction_id
                  AND e.account_id = h.account_id
           ) END AS captured_amount_minor
      FROM holds h
     WHERE h.id = %s
"""


def _serialise(row: dict[str, Any]) -> dict[str, Any]:
    captured = row["captured_amount_minor"]
    return HoldResponse.model_validate(
        {
            "id": row["id"],
            "account_id": row["account_id"],
            "amount_minor": row["amount_minor"],
            "currency": row["currency"].strip(),
            "status": row["status"],
            "expires_at": row["expires_at"],
            "captured_transaction_id": row["captured_transaction_id"],
            "captured_amount_minor": captured,
            "released_amount_minor": (
                None if captured is None else row["amount_minor"] - captured
            ),
            "created_at": row["created_at"],
        }
    ).model_dump(mode="json")


# ------------------------------------------------------------------ authorize --


def create_hold(request: CreateHoldRequest, idempotency_key: UUID) -> Outcome:
    def work(cur: Cursor) -> dict[str, Any]:
        # Lock the account first: the available-balance check below and the hold
        # insert have to be atomic with respect to every other writer touching
        # this account, or two concurrent holds could each see enough funds.
        accounts = lock_accounts(cur, [request.account_id])
        account = accounts[request.account_id]
        if account.currency != request.currency:
            raise ValidationFailed(
                f"account {request.account_id} is denominated in "
                f"{account.currency}, not {request.currency}",
                account_currency=account.currency,
                requested_currency=request.currency,
            )

        # A hold is modelled as a prospective debit for authorization purposes:
        # it must pass exactly the same available-balance test a real debit would.
        # Reusing `assert_no_overdraft` rather than writing a second check means
        # there is only one definition of "can this account afford this".
        assert_no_overdraft(
            cur,
            [Posting(request.account_id, -request.amount_minor, request.currency)],
            accounts,
        )

        hold_id = uuid4()
        expires_at = datetime.now(timezone.utc) + timedelta(
            seconds=request.expires_in_seconds
        )
        cur.execute(
            """
            INSERT INTO holds (id, account_id, amount_minor, currency, expires_at)
            VALUES (%s, %s, %s, %s, %s)
            """,
            (
                hold_id,
                request.account_id,
                request.amount_minor,
                request.currency,
                expires_at,
            ),
        )
        cur.execute(_HOLD_SELECT, (hold_id,))
        body = _serialise(cur.fetchone())  # type: ignore[arg-type]
        outbox.emit(cur, outbox.EVENT_HOLD_CREATED, body)
        return body

    return execute_once(
        key=idempotency_key,
        fingerprint=request.fingerprint(),
        status_code=201,
        work=work,
        isolation=READ_COMMITTED,
    )


# -------------------------------------------------------------------- capture --


def capture_hold(
    hold_id: UUID, request: CaptureHoldRequest, idempotency_key: UUID
) -> Outcome:
    def work(cur: Cursor) -> dict[str, Any]:
        hold = _lock_pending_hold(cur, hold_id)
        currency = hold["currency"].strip()

        amount = request.amount_minor
        if amount is None:
            amount = hold["amount_minor"]
        if amount > hold["amount_minor"]:
            raise CaptureExceedsHold(
                f"cannot capture {amount} against a hold authorized for "
                f"{hold['amount_minor']}",
                hold_id=str(hold_id),
                authorized_minor=hold["amount_minor"],
                requested_minor=amount,
            )

        credit_total = sum(c.amount_minor for c in request.credits)
        if credit_total != amount:
            raise UnbalancedTransaction(
                f"capture credits sum to {credit_total} but the capture amount "
                f"is {amount}",
                capture_amount_minor=amount,
                credit_total_minor=credit_total,
            )

        if any(c.account_id == hold["account_id"] for c in request.credits):
            # Otherwise the captured amount could not be derived unambiguously
            # from the transaction's entries on the held account, since the
            # account would appear as both a debit and a credit.
            raise ValidationFailed(
                "a capture cannot credit the account the hold is against",
                account_id=str(hold["account_id"]),
            )

        postings = [Posting(hold["account_id"], -amount, currency)] + [
            Posting(c.account_id, c.amount_minor, currency) for c in request.credits
        ]
        validate_postings(postings)

        # Retire the hold *before* writing the entries. The overdraft check
        # inside the write reads live holds, and if this hold were still pending
        # it would be counted against an account that is about to be debited for
        # the very amount it reserves -- the same money subtracted twice.
        #
        # Ordering it this way also makes the check a genuine safety net: with
        # the hold retired, available rises by the full authorized amount, and
        # since capture <= authorized the check provably cannot fail for a valid
        # capture. If it ever does fire, an invariant is broken somewhere else.
        transaction_id = uuid4()
        _transition(
            cur,
            hold_id,
            new_status="captured",
            captured_transaction_id=transaction_id,
        )

        accounts = lock_accounts(cur, [p.account_id for p in postings])
        assert_currencies_match(postings, accounts)
        assert_no_overdraft(cur, postings, accounts)
        append_transaction(
            cur,
            description=request.description
            or f"capture of hold {hold_id}",
            idempotency_key=idempotency_key,
            postings=postings,
            transaction_id=transaction_id,
        )

        cur.execute(_HOLD_SELECT, (hold_id,))
        body = _serialise(cur.fetchone())  # type: ignore[arg-type]
        outbox.emit(cur, outbox.EVENT_HOLD_CAPTURED, body)
        return body

    return execute_once(
        key=idempotency_key,
        # The hold id is part of the request identity, so one key cannot be
        # replayed against a different hold.
        fingerprint={**request.fingerprint(), "hold_id": str(hold_id)},
        status_code=200,
        work=work,
        isolation=READ_COMMITTED,
    )


# ----------------------------------------------------------------------- void --


def void_hold(
    hold_id: UUID, request: VoidHoldRequest, idempotency_key: UUID
) -> Outcome:
    def work(cur: Cursor) -> dict[str, Any]:
        # Voiding a lapsed-but-still-pending hold is allowed: both outcomes are
        # terminal and neither reserves funds, so refusing would be pedantry.
        _lock_pending_hold(cur, hold_id, allow_lapsed=True)
        _transition(cur, hold_id, new_status="voided")
        cur.execute(_HOLD_SELECT, (hold_id,))
        body = _serialise(cur.fetchone())  # type: ignore[arg-type]
        outbox.emit(cur, outbox.EVENT_HOLD_VOIDED, {**body, "reason": request.reason})
        return body

    return execute_once(
        key=idempotency_key,
        fingerprint={**request.fingerprint(), "hold_id": str(hold_id)},
        status_code=200,
        work=work,
        isolation=READ_COMMITTED,
    )


# ----------------------------------------------------------- state machine ----


def _lock_pending_hold(
    cur: Cursor, hold_id: UUID, *, allow_lapsed: bool = False
) -> dict[str, Any]:
    """Take a row lock on the hold and assert it is still capturable.

    FOR UPDATE, not a bare SELECT. Two concurrent captures of the same hold must
    not both read `pending`: the second one blocks here, and when it wakes up
    Postgres re-evaluates the row against the committed version, so it sees
    `captured` and is refused.
    """
    cur.execute(
        """
        SELECT id, account_id, amount_minor, currency, status, expires_at,
               (expires_at <= now()) AS lapsed
          FROM holds
         WHERE id = %s
           FOR UPDATE
        """,
        (hold_id,),
    )
    hold = cur.fetchone()
    if hold is None:
        raise HoldNotFound(f"hold {hold_id} not found", hold_id=str(hold_id))

    if hold["status"] != "pending":
        raise HoldNotPending(
            f"hold {hold_id} is {hold['status']}; only a pending hold can be "
            f"captured or voided",
            hold_id=str(hold_id),
            status=hold["status"],
        )

    if hold["lapsed"] and not allow_lapsed:
        raise HoldNotPending(
            f"hold {hold_id} expired at {hold['expires_at'].isoformat()} and can "
            f"no longer be captured",
            hold_id=str(hold_id),
            status="expired",
            expires_at=hold["expires_at"].isoformat(),
        )

    return hold


def _transition(
    cur: Cursor,
    hold_id: UUID,
    *,
    new_status: str,
    captured_transaction_id: UUID | None = None,
) -> None:
    """Compare-and-swap out of `pending`.

    The `AND status = 'pending'` predicate is redundant given the FOR UPDATE in
    `_lock_pending_hold`, and it stays anyway: it costs nothing and means this
    statement is safe even if a future caller forgets to lock first.
    """
    cur.execute(
        """
        UPDATE holds
           SET status = %s,
               captured_transaction_id = %s
         WHERE id = %s
           AND status = 'pending'
        """,
        (new_status, captured_transaction_id, hold_id),
    )
    if cur.rowcount != 1:
        raise HoldNotPending(
            f"hold {hold_id} left the pending state concurrently",
            hold_id=str(hold_id),
        )


# --------------------------------------------------------------------- reads ---


def get_hold(hold_id: UUID) -> dict[str, Any]:
    with transaction(read_only=True) as cur:
        cur.execute(_HOLD_SELECT, (hold_id,))
        row = cur.fetchone()
    if row is None:
        raise HoldNotFound(f"hold {hold_id} not found", hold_id=str(hold_id))
    return _serialise(row)


def list_holds(account_id: UUID, *, status: str | None = None) -> list[dict[str, Any]]:
    with transaction(read_only=True) as cur:
        cur.execute(
            """
            SELECT h.id FROM holds h
             WHERE h.account_id = %s
               AND (%s::text IS NULL OR h.status::text = %s::text)
             ORDER BY h.created_at DESC, h.id
             LIMIT 500
            """,
            (account_id, status, status),
        )
        ids = [row["id"] for row in cur.fetchall()]
        out = []
        for hold_id in ids:
            cur.execute(_HOLD_SELECT, (hold_id,))
            out.append(_serialise(cur.fetchone()))  # type: ignore[arg-type]
    return out


# ------------------------------------------------------------ expiry sweeper ---


def sweep_expired_holds(batch_size: int = 1000) -> int:
    """Mark lapsed holds as `expired`. Returns how many were swept.

    This is bookkeeping, not correctness. `available` already excludes holds past
    their deadline, so a hold releases its funds at the instant it lapses whether
    or not this ever runs. What sweeping buys is a smaller partial index and a
    hold table whose `status` column means what it says.

    SKIP LOCKED so the sweeper never blocks a capture that is mid-flight on the
    same row -- it just leaves that hold for the next pass. Batched so one pass
    cannot hold locks on an unbounded number of rows.
    """
    with transaction() as cur:
        cur.execute(
            """
            UPDATE holds
               SET status = 'expired'
             WHERE id IN (
                     SELECT id
                       FROM holds
                      WHERE status = 'pending'
                        AND expires_at <= now()
                      ORDER BY expires_at
                      LIMIT %s
                        FOR UPDATE SKIP LOCKED
                   )
            RETURNING id, account_id, amount_minor, currency, status,
                      expires_at, captured_transaction_id, created_at
            """,
            (batch_size,),
        )
        expired = cur.fetchall()

        # Events for the sweep go in the same transaction as the sweep itself,
        # for the same reason ledger events do: a background job is not exempt
        # from the dual-write problem.
        for row in expired:
            outbox.emit(
                cur,
                outbox.EVENT_HOLD_EXPIRED,
                _serialise({**row, "captured_amount_minor": None}),
            )

        return len(expired)
