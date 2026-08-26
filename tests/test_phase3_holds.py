"""Phase 3: holds, partial capture, expiry, and overdraft prevention.

The claims under test:

  1. A hold moves no money. It only changes `available`.
  2. `available = SUM(entries) - SUM(live holds)`, and "live" is a function of
     time, not of whether a background job has run.
  3. A debit is authorised against `available`, atomically with the write.
  4. Terminal hold states are terminal, under concurrency.
  5. A capture can be partial, and the remainder is released.
"""

from __future__ import annotations

import threading
from concurrent.futures import ThreadPoolExecutor
from typing import Any
from uuid import uuid4

import psycopg
import pytest

from ledger import db
from ledger.errors import (
    CaptureExceedsHold,
    HoldNotFound,
    HoldNotPending,
    InsufficientFunds,
    UnbalancedTransaction,
    ValidationFailed,
)
from ledger.services import holds as holds_service
from tests import factories as f


# ------------------------------------------------- a hold moves no money -----


def test_a_hold_writes_no_entries() -> None:
    alice = f.make_account()
    f.fund(alice, 10_000)
    entries_before = f.count_rows("entries")

    f.make_hold(alice, 2_500)

    assert f.count_rows("entries") == entries_before
    assert f.derived_balance(alice) == 10_000  # actual is untouched


def test_a_hold_reduces_available_not_actual() -> None:
    alice = f.make_account()
    f.fund(alice, 10_000)
    f.make_hold(alice, 2_500)

    b = f.balance(alice)
    assert b["actual_minor"] == 10_000
    assert b["held_minor"] == 2_500
    assert b["available_minor"] == 7_500


def test_holds_accumulate() -> None:
    alice = f.make_account()
    f.fund(alice, 10_000)
    f.make_hold(alice, 1_000)
    f.make_hold(alice, 2_000)
    f.make_hold(alice, 500)

    b = f.balance(alice)
    assert b["held_minor"] == 3_500
    assert b["available_minor"] == 6_500


# --------------------------------------------------- overdraft prevention ----


def test_a_debit_beyond_available_is_refused() -> None:
    alice = f.make_account()
    bob = f.make_account()
    f.fund(alice, 10_000)
    f.make_hold(alice, 8_000)

    with pytest.raises(InsufficientFunds) as exc:
        f.post([(alice, -3_000, "USD"), (bob, 3_000, "USD")])

    assert exc.value.details["available_minor"] == 2_000
    assert exc.value.details["actual_minor"] == 10_000
    assert exc.value.details["held_minor"] == 8_000
    # Nothing moved.
    assert f.derived_balance(alice) == 10_000


def test_a_debit_within_available_succeeds() -> None:
    alice = f.make_account()
    bob = f.make_account()
    f.fund(alice, 10_000)
    f.make_hold(alice, 8_000)

    f.post([(alice, -2_000, "USD"), (bob, 2_000, "USD")])
    assert f.balance(alice)["available_minor"] == 0


def test_a_hold_beyond_available_is_refused() -> None:
    alice = f.make_account()
    f.fund(alice, 1_000)
    f.make_hold(alice, 800)

    with pytest.raises(InsufficientFunds):
        f.make_hold(alice, 300)

    assert f.balance(alice)["held_minor"] == 800


def test_system_accounts_may_go_negative() -> None:
    """external_settlement is the mirror of every balance in the system, so it
    must be allowed below zero -- otherwise the first funding transaction is
    impossible."""
    alice = f.make_account()
    f.fund(alice, 10_000)
    settlement = None
    with db.transaction(read_only=True) as cur:
        cur.execute("SELECT id FROM accounts WHERE type = 'external_settlement'")
        settlement = cur.fetchone()["id"]
    assert f.derived_balance(settlement) == -10_000


def test_a_user_account_cannot_be_pushed_negative() -> None:
    alice = f.make_account()
    bob = f.make_account()
    f.fund(alice, 100)

    with pytest.raises(InsufficientFunds):
        f.post([(alice, -101, "USD"), (bob, 101, "USD")])


# ------------------------------------------------------------- full capture --


def test_full_capture_moves_the_money_and_closes_the_hold() -> None:
    alice = f.make_account()
    merchant = f.make_account()
    f.fund(alice, 10_000)
    hold = f.make_hold(alice, 2_500).body

    result = f.capture(hold["id"], [(merchant, 2_500)]).body

    assert result["status"] == "captured"
    assert result["captured_amount_minor"] == 2_500
    assert result["released_amount_minor"] == 0
    assert result["captured_transaction_id"] is not None

    assert f.derived_balance(alice) == 7_500
    assert f.derived_balance(merchant) == 2_500
    b = f.balance(alice)
    assert b["held_minor"] == 0
    assert b["available_minor"] == 7_500


def test_capture_can_split_between_merchant_and_revenue() -> None:
    alice = f.make_account()
    merchant = f.make_account()
    revenue = f.revenue_account("USD")
    f.fund(alice, 10_000)
    hold = f.make_hold(alice, 1_000).body

    f.capture(hold["id"], [(merchant, 971), (revenue, 29)])

    assert f.derived_balance(alice) == 9_000
    assert f.derived_balance(merchant) == 971
    assert f.derived_balance(revenue) == 29


# ---------------------------------------------------------- partial capture --


def test_partial_capture_releases_the_remainder() -> None:
    alice = f.make_account()
    merchant = f.make_account()
    f.fund(alice, 10_000)
    hold = f.make_hold(alice, 5_000).body
    assert f.balance(alice)["available_minor"] == 5_000

    result = f.capture(hold["id"], [(merchant, 3_000)], amount_minor=3_000).body

    assert result["status"] == "captured"
    assert result["captured_amount_minor"] == 3_000
    assert result["released_amount_minor"] == 2_000

    b = f.balance(alice)
    assert b["actual_minor"] == 7_000  # only the captured part left the account
    assert b["held_minor"] == 0  # the remaining 2,000 is released, not held
    assert b["available_minor"] == 7_000


def test_partial_capture_is_terminal_no_second_bite() -> None:
    """A hold authorizes at most one capture. Capturing 3,000 of a 5,000 hold
    does not leave a 2,000 hold behind -- the authorization is spent."""
    alice = f.make_account()
    merchant = f.make_account()
    f.fund(alice, 10_000)
    hold = f.make_hold(alice, 5_000).body

    f.capture(hold["id"], [(merchant, 3_000)], amount_minor=3_000)
    with pytest.raises(HoldNotPending):
        f.capture(hold["id"], [(merchant, 2_000)], amount_minor=2_000)


def test_capture_exceeding_the_hold_is_refused() -> None:
    alice = f.make_account()
    merchant = f.make_account()
    f.fund(alice, 10_000)
    hold = f.make_hold(alice, 1_000).body

    with pytest.raises(CaptureExceedsHold) as exc:
        f.capture(hold["id"], [(merchant, 1_001)], amount_minor=1_001)
    assert exc.value.details["authorized_minor"] == 1_000
    assert exc.value.status == 422
    assert f.derived_balance(alice) == 10_000


def test_capture_credits_must_sum_to_the_capture_amount() -> None:
    alice = f.make_account()
    merchant = f.make_account()
    f.fund(alice, 10_000)
    hold = f.make_hold(alice, 1_000).body

    with pytest.raises(UnbalancedTransaction):
        f.capture(hold["id"], [(merchant, 900)], amount_minor=1_000)


def test_capture_cannot_credit_the_held_account() -> None:
    alice = f.make_account()
    f.fund(alice, 10_000)
    hold = f.make_hold(alice, 1_000).body

    with pytest.raises(ValidationFailed):
        f.capture(hold["id"], [(alice, 1_000)])


def test_capture_provably_cannot_overdraft() -> None:
    """The strongest guarantee holds give: once authorized, capture cannot fail
    for lack of funds. Here every last cent is held, available is zero, and the
    full capture still succeeds -- because retiring the hold returns its
    authorized amount to available before the debit is checked."""
    alice = f.make_account()
    merchant = f.make_account()
    f.fund(alice, 1_000)
    hold = f.make_hold(alice, 1_000).body
    assert f.balance(alice)["available_minor"] == 0

    f.capture(hold["id"], [(merchant, 1_000)])
    assert f.derived_balance(alice) == 0
    assert f.derived_balance(merchant) == 1_000


# -------------------------------------------------------------------- void ---


def test_void_releases_the_hold_without_writing_entries() -> None:
    alice = f.make_account()
    f.fund(alice, 10_000)
    hold = f.make_hold(alice, 4_000).body
    entries_before = f.count_rows("entries")

    result = f.void(hold["id"]).body

    assert result["status"] == "voided"
    assert result["captured_transaction_id"] is None
    assert result["captured_amount_minor"] is None
    assert f.count_rows("entries") == entries_before
    assert f.balance(alice)["available_minor"] == 10_000


# ----------------------------------------------------------- state machine ---


def test_capture_after_void_is_refused() -> None:
    alice = f.make_account()
    merchant = f.make_account()
    f.fund(alice, 10_000)
    hold = f.make_hold(alice, 1_000).body
    f.void(hold["id"])

    with pytest.raises(HoldNotPending) as exc:
        f.capture(hold["id"], [(merchant, 1_000)])
    assert exc.value.details["status"] == "voided"


def test_void_after_capture_is_refused() -> None:
    alice = f.make_account()
    merchant = f.make_account()
    f.fund(alice, 10_000)
    hold = f.make_hold(alice, 1_000).body
    f.capture(hold["id"], [(merchant, 1_000)])

    with pytest.raises(HoldNotPending):
        f.void(hold["id"])


def test_double_void_is_refused() -> None:
    alice = f.make_account()
    f.fund(alice, 10_000)
    hold = f.make_hold(alice, 1_000).body
    f.void(hold["id"])
    with pytest.raises(HoldNotPending):
        f.void(hold["id"], key=uuid4())


def test_unknown_hold_is_404() -> None:
    with pytest.raises(HoldNotFound):
        f.void(uuid4())
    with pytest.raises(HoldNotFound):
        holds_service.get_hold(uuid4())


# ----------------------------------------------------------------- expiry ----


def test_a_lapsed_hold_stops_reserving_funds_before_any_sweep() -> None:
    """The point of putting `expires_at > now()` in the availability query:
    money is released the instant the authorization lapses, not when a
    background worker gets round to it. A dead worker must not freeze funds."""
    alice = f.make_account()
    f.fund(alice, 10_000)
    hold = f.make_hold(alice, 4_000).body
    assert f.balance(alice)["available_minor"] == 6_000

    f.expire_hold_now(hold["id"])

    # No sweep has run: the row still says 'pending'.
    assert holds_service.get_hold(hold["id"])["status"] == "pending"
    # And yet the funds are already free.
    assert f.balance(alice)["available_minor"] == 10_000


def test_a_lapsed_hold_cannot_be_captured() -> None:
    alice = f.make_account()
    merchant = f.make_account()
    f.fund(alice, 10_000)
    hold = f.make_hold(alice, 1_000).body
    f.expire_hold_now(hold["id"])

    with pytest.raises(HoldNotPending) as exc:
        f.capture(hold["id"], [(merchant, 1_000)])
    assert exc.value.details["status"] == "expired"
    assert f.derived_balance(alice) == 10_000


def test_a_lapsed_hold_can_still_be_voided() -> None:
    alice = f.make_account()
    f.fund(alice, 10_000)
    hold = f.make_hold(alice, 1_000).body
    f.expire_hold_now(hold["id"])

    assert f.void(hold["id"]).body["status"] == "voided"


def test_the_sweeper_relabels_lapsed_holds() -> None:
    alice = f.make_account()
    f.fund(alice, 10_000)
    live = f.make_hold(alice, 1_000).body
    lapsed = [f.make_hold(alice, 500).body for _ in range(3)]
    for hold in lapsed:
        f.expire_hold_now(hold["id"])

    assert holds_service.sweep_expired_holds() == 3
    assert holds_service.sweep_expired_holds() == 0  # idempotent

    for hold in lapsed:
        assert holds_service.get_hold(hold["id"])["status"] == "expired"
    assert holds_service.get_hold(live["id"])["status"] == "pending"
    # The sweep changed no numbers, only labels.
    assert f.balance(alice)["held_minor"] == 1_000


def test_the_sweeper_writes_no_entries() -> None:
    alice = f.make_account()
    f.fund(alice, 10_000)
    hold = f.make_hold(alice, 1_000).body
    f.expire_hold_now(hold["id"])
    entries_before = f.count_rows("entries")

    holds_service.sweep_expired_holds()
    assert f.count_rows("entries") == entries_before


# ------------------------------------------------------ database-level guards --


def test_terminal_states_are_terminal_at_the_database_level() -> None:
    alice = f.make_account()
    f.fund(alice, 10_000)
    hold = f.make_hold(alice, 1_000).body
    f.void(hold["id"])

    with pytest.raises(psycopg.errors.CheckViolation) as exc:
        with db.transaction() as cur:
            cur.execute(
                "UPDATE holds SET status = 'pending' WHERE id = %s", (hold["id"],)
            )
    assert "invalid_hold_transition" in str(exc.value)


def test_the_authorized_amount_is_immutable() -> None:
    """The hold equivalent of editing a signed cheque."""
    alice = f.make_account()
    f.fund(alice, 10_000)
    hold = f.make_hold(alice, 1_000).body

    with pytest.raises(psycopg.errors.CheckViolation) as exc:
        with db.transaction() as cur:
            cur.execute(
                "UPDATE holds SET amount_minor = 9_999 WHERE id = %s", (hold["id"],)
            )
    assert "invalid_hold_mutation" in str(exc.value)


def test_expires_at_is_immutable() -> None:
    alice = f.make_account()
    f.fund(alice, 10_000)
    hold = f.make_hold(alice, 1_000).body

    with pytest.raises(psycopg.errors.CheckViolation):
        with db.transaction() as cur:
            cur.execute(
                "UPDATE holds SET expires_at = now() + interval '1 year' WHERE id = %s",
                (hold["id"],),
            )


def test_captured_status_requires_a_transaction_link() -> None:
    alice = f.make_account()
    f.fund(alice, 10_000)
    hold = f.make_hold(alice, 1_000).body

    with pytest.raises(psycopg.errors.CheckViolation) as exc:
        with db.transaction() as cur:
            cur.execute(
                "UPDATE holds SET status = 'captured' WHERE id = %s", (hold["id"],)
            )
    assert "holds_capture_link" in str(exc.value)


def test_a_voided_hold_cannot_link_to_a_transaction() -> None:
    """The other half of the biconditional: a movement of money on a hold nobody
    captured."""
    alice = f.make_account()
    bob = f.make_account()
    f.fund(alice, 10_000)
    tx = f.post([(alice, -1, "USD"), (bob, 1, "USD")])
    hold = f.make_hold(alice, 1_000).body

    with pytest.raises(psycopg.errors.CheckViolation) as exc:
        with db.transaction() as cur:
            cur.execute(
                "UPDATE holds SET status = 'voided', captured_transaction_id = %s"
                " WHERE id = %s",
                (tx["id"], hold["id"]),
            )
    assert "holds_capture_link" in str(exc.value)


def test_a_lapsed_hold_cannot_be_captured_even_by_raw_sql() -> None:
    alice = f.make_account()
    bob = f.make_account()
    f.fund(alice, 10_000)
    tx = f.post([(alice, -1, "USD"), (bob, 1, "USD")])
    hold = f.make_hold(alice, 1_000).body
    f.expire_hold_now(hold["id"])

    with pytest.raises(psycopg.errors.CheckViolation) as exc:
        with db.transaction() as cur:
            cur.execute(
                "UPDATE holds SET status = 'captured', captured_transaction_id = %s"
                " WHERE id = %s",
                (tx["id"], hold["id"]),
            )
    assert "expired" in str(exc.value)


def test_holds_cannot_be_deleted() -> None:
    alice = f.make_account()
    f.fund(alice, 10_000)
    f.make_hold(alice, 1_000)

    with pytest.raises(psycopg.errors.CheckViolation):
        with db.transaction() as cur:
            cur.execute("DELETE FROM holds")


def test_a_hold_cannot_sit_on_a_foreign_currency_account() -> None:
    cad = f.make_account(currency="CAD")
    with pytest.raises(psycopg.errors.ForeignKeyViolation):
        with db.transaction() as cur:
            cur.execute(
                """
                INSERT INTO holds (id, account_id, amount_minor, currency, expires_at)
                VALUES (%s, %s, %s, %s, now() + interval '1 hour')
                """,
                (uuid4(), cad, 100, "USD"),
            )


def test_hold_currency_must_match_the_account() -> None:
    usd = f.make_account(currency="USD")
    f.fund(usd, 1_000)
    with pytest.raises(ValidationFailed):
        f.make_hold(usd, 100, currency="CAD")


# ------------------------------------------------------------- concurrency ---


def test_concurrent_captures_of_one_hold_only_one_wins() -> None:
    alice = f.make_account()
    merchant = f.make_account()
    f.fund(alice, 10_000)
    hold = f.make_hold(alice, 1_000).body

    start = threading.Barrier(2)
    results: list[Any] = []

    def fire() -> None:
        start.wait(timeout=10)
        try:
            results.append(f.capture(hold["id"], [(merchant, 1_000)], key=uuid4()))
        except Exception as exc:  # noqa: BLE001
            results.append(exc)

    threads = [threading.Thread(target=fire) for _ in range(2)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=20)
        assert not t.is_alive()

    succeeded = [r for r in results if not isinstance(r, Exception)]
    failed = [r for r in results if isinstance(r, Exception)]
    assert len(succeeded) == 1, results
    assert len(failed) == 1
    assert isinstance(failed[0], HoldNotPending)

    # The money moved exactly once.
    assert f.derived_balance(merchant) == 1_000
    assert f.derived_balance(alice) == 9_000


def test_concurrent_holds_cannot_over_reserve() -> None:
    """The check-then-act race, tested directly.

    Twenty threads each try to hold 100 against an account with 1,000. If the
    available-balance check were not atomic with the insert, more than ten would
    succeed and the account would be committed to more money than it has.
    """
    alice = f.make_account()
    f.fund(alice, 1_000)
    attempts = 20
    start = threading.Barrier(attempts)

    def fire() -> str:
        start.wait(timeout=15)
        try:
            f.make_hold(alice, 100)
            return "ok"
        except InsufficientFunds:
            return "refused"

    with ThreadPoolExecutor(max_workers=attempts) as pool:
        outcomes = [fut.result(timeout=60) for fut in
                    [pool.submit(fire) for _ in range(attempts)]]

    assert outcomes.count("ok") == 10, outcomes
    assert outcomes.count("refused") == 10

    b = f.balance(alice)
    assert b["held_minor"] == 1_000
    assert b["available_minor"] == 0


def test_concurrent_debits_cannot_overdraw() -> None:
    alice = f.make_account()
    bob = f.make_account()
    f.fund(alice, 1_000)
    attempts = 20
    start = threading.Barrier(attempts)

    def fire() -> str:
        start.wait(timeout=15)
        try:
            f.post([(alice, -100, "USD"), (bob, 100, "USD")])
            return "ok"
        except InsufficientFunds:
            return "refused"

    with ThreadPoolExecutor(max_workers=attempts) as pool:
        outcomes = [fut.result(timeout=60) for fut in
                    [pool.submit(fire) for _ in range(attempts)]]

    assert outcomes.count("ok") == 10, outcomes
    assert f.derived_balance(alice) == 0
    assert f.derived_balance(bob) == 1_000


def test_concurrent_mix_of_holds_and_debits_never_goes_negative() -> None:
    """Holds and transfers contend on the same account. Whatever interleaving
    occurs, available must never end up below zero."""
    alice = f.make_account()
    bob = f.make_account()
    f.fund(alice, 1_000)
    attempts = 24
    start = threading.Barrier(attempts)

    def fire(i: int) -> str:
        start.wait(timeout=15)
        try:
            if i % 2 == 0:
                f.make_hold(alice, 100)
            else:
                f.post([(alice, -100, "USD"), (bob, 100, "USD")])
            return "ok"
        except InsufficientFunds:
            return "refused"

    with ThreadPoolExecutor(max_workers=attempts) as pool:
        outcomes = [fut.result(timeout=60) for fut in
                    [pool.submit(fire, i) for i in range(attempts)]]

    assert outcomes.count("ok") == 10, outcomes
    b = f.balance(alice)
    assert b["available_minor"] == 0
    assert b["actual_minor"] >= 0
    assert b["actual_minor"] - b["held_minor"] == b["available_minor"]


def test_the_sweeper_does_not_block_a_concurrent_capture() -> None:
    """SKIP LOCKED: the sweeper steps around a row a capture is holding rather
    than waiting on it."""
    alice = f.make_account()
    merchant = f.make_account()
    f.fund(alice, 10_000)
    hold = f.make_hold(alice, 1_000).body

    f.expire_hold_now(hold["id"])

    swept: list[int] = []
    with db.transaction() as locker:
        # Stand in for a capture that is mid-flight on this row.
        locker.execute("SELECT id FROM holds WHERE id = %s FOR UPDATE", (hold["id"],))

        def sweep() -> None:
            swept.append(holds_service.sweep_expired_holds())

        t = threading.Thread(target=sweep)
        t.start()
        t.join(timeout=5)
        assert not t.is_alive(), "the sweeper blocked on a locked row"

    assert swept == [0]


# --------------------------------------------------------------- idempotency --


def test_hold_creation_is_idempotent() -> None:
    alice = f.make_account()
    f.fund(alice, 10_000)
    key = uuid4()

    first = f.make_hold(alice, 1_000, key=key)
    second = f.make_hold(alice, 1_000, key=key)

    assert second.replayed is True
    assert second.body["id"] == first.body["id"]
    assert f.count_rows("holds") == 1
    assert f.balance(alice)["held_minor"] == 1_000


def test_capture_is_idempotent() -> None:
    alice = f.make_account()
    merchant = f.make_account()
    f.fund(alice, 10_000)
    hold = f.make_hold(alice, 1_000).body
    key = uuid4()

    first = f.capture(hold["id"], [(merchant, 1_000)], key=key)
    second = f.capture(hold["id"], [(merchant, 1_000)], key=key)

    assert second.replayed is True
    assert second.body["captured_transaction_id"] == first.body["captured_transaction_id"]
    assert f.derived_balance(merchant) == 1_000  # not 2,000


def test_void_is_idempotent() -> None:
    alice = f.make_account()
    f.fund(alice, 10_000)
    hold = f.make_hold(alice, 1_000).body
    key = uuid4()

    f.void(hold["id"], key=key)
    replay = f.void(hold["id"], key=key)
    assert replay.replayed is True
    assert replay.body["status"] == "voided"


def test_a_capture_key_cannot_be_replayed_against_another_hold() -> None:
    from ledger.errors import IdempotencyKeyReused

    alice = f.make_account()
    merchant = f.make_account()
    f.fund(alice, 10_000)
    first_hold = f.make_hold(alice, 1_000).body
    second_hold = f.make_hold(alice, 1_000).body
    key = uuid4()

    f.capture(first_hold["id"], [(merchant, 1_000)], key=key)
    with pytest.raises(IdempotencyKeyReused):
        f.capture(second_hold["id"], [(merchant, 1_000)], key=key)


# --------------------------------------------------------------- http layer --


def test_hold_lifecycle_over_http(client: Any) -> None:
    alice = client.post("/accounts", json={"name": "alice", "currency": "USD"}).json()["id"]
    merchant = client.post("/accounts", json={"name": "m", "currency": "USD"}).json()["id"]
    settlement = client.post(
        "/accounts",
        json={"name": "s", "currency": "USD", "type": "external_settlement"},
    ).json()["id"]

    client.post(
        "/transactions",
        headers={"Idempotency-Key": str(uuid4())},
        json={
            "description": "funding",
            "entries": [
                {"account_id": settlement, "amount_minor": -10_000, "currency": "USD"},
                {"account_id": alice, "amount_minor": 10_000, "currency": "USD"},
            ],
        },
    )

    created = client.post(
        "/holds",
        headers={"Idempotency-Key": str(uuid4())},
        json={"account_id": alice, "amount_minor": 5_000, "currency": "USD"},
    )
    assert created.status_code == 201, created.text
    hold_id = created.json()["id"]
    assert created.json()["status"] == "pending"

    balance = client.get(f"/accounts/{alice}/balance").json()
    assert (balance["actual_minor"], balance["held_minor"], balance["available_minor"]) == (
        10_000,
        5_000,
        5_000,
    )

    captured = client.post(
        f"/holds/{hold_id}/capture",
        headers={"Idempotency-Key": str(uuid4())},
        json={"amount_minor": 3_000, "credits": [{"account_id": merchant, "amount_minor": 3_000}]},
    )
    assert captured.status_code == 200, captured.text
    assert captured.json()["captured_amount_minor"] == 3_000
    assert captured.json()["released_amount_minor"] == 2_000

    balance = client.get(f"/accounts/{alice}/balance").json()
    assert (balance["actual_minor"], balance["held_minor"], balance["available_minor"]) == (
        7_000,
        0,
        7_000,
    )

    # Voiding a spent hold is a 409.
    voided = client.post(
        f"/holds/{hold_id}/void", headers={"Idempotency-Key": str(uuid4())}, json={}
    )
    assert voided.status_code == 409
    assert voided.json()["error"]["code"] == "hold_not_pending"


def test_hold_requires_an_idempotency_key(client: Any) -> None:
    alice = client.post("/accounts", json={"name": "alice", "currency": "USD"}).json()["id"]
    response = client.post(
        "/holds", json={"account_id": alice, "amount_minor": 100, "currency": "USD"}
    )
    assert response.status_code == 400


def test_void_requires_an_idempotency_key(client: Any) -> None:
    response = client.post(f"/holds/{uuid4()}/void", json={})
    assert response.status_code == 400


def test_insufficient_funds_over_http(client: Any) -> None:
    alice = client.post("/accounts", json={"name": "alice", "currency": "USD"}).json()["id"]
    response = client.post(
        "/holds",
        headers={"Idempotency-Key": str(uuid4())},
        json={"account_id": alice, "amount_minor": 100, "currency": "USD"},
    )
    assert response.status_code == 422
    assert response.json()["error"]["code"] == "insufficient_funds"
