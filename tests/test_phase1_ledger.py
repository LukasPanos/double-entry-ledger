"""Phase 1: the application-layer ledger behaviour."""

from __future__ import annotations

from uuid import uuid4

import pytest
from pydantic import ValidationError

from ledger import db
from ledger.errors import (
    AccountNotFound,
    CurrencyMismatch,
    UnbalancedTransaction,
)
from ledger.hashing import GENESIS_PREV_HASH, HashableEntry, transaction_hash
from ledger.schemas import CreateTransactionRequest
from ledger.services import accounts as accounts_service
from ledger.services import transactions as transactions_service
from tests import factories as f


# ------------------------------------------------------------------ posting --


def test_balanced_transfer_moves_money() -> None:
    alice = f.make_account(name="alice")
    bob = f.make_account(name="bob")
    f.fund(alice, 10_000)

    f.post([(alice, -2_500, "USD"), (bob, 2_500, "USD")], description="alice pays bob")

    assert accounts_service.get_balance(alice)["actual_minor"] == 7_500
    assert accounts_service.get_balance(bob)["actual_minor"] == 2_500


def test_balance_is_derived_from_entries_not_the_cache() -> None:
    alice = f.make_account()
    f.fund(alice, 1_000)

    # Corrupt the cache behind the service's back.
    with db.transaction() as cur:
        cur.execute(
            "UPDATE account_balances SET balance_minor = 999_999 WHERE account_id = %s",
            (alice,),
        )

    # The reported balance is unaffected, because it is SUM(entries).
    assert accounts_service.get_balance(alice)["actual_minor"] == 1_000
    assert f.cached_balance(alice) == 999_999  # the drift is real...
    assert f.derived_balance(alice) == 1_000  # ...and the truth is elsewhere


def test_cache_tracks_entries_under_normal_operation() -> None:
    alice = f.make_account()
    bob = f.make_account()
    f.fund(alice, 5_000)
    for _ in range(5):
        f.post([(alice, -100, "USD"), (bob, 100, "USD")])

    for account in (alice, bob):
        assert f.cached_balance(account) == f.derived_balance(account)


def test_multi_leg_transaction_with_a_fee() -> None:
    """Three legs, still zero-sum: the payer is debited once and the money is
    split between the payee and platform revenue."""
    alice = f.make_account()
    bob = f.make_account()
    revenue = f.revenue_account("USD")
    f.fund(alice, 10_000)

    f.post(
        [
            (alice, -1_000, "USD"),
            (bob, -0 + 971, "USD"),
            (revenue, 29, "USD"),
        ],
        description="payment with 2.9% fee",
    )

    assert f.derived_balance(alice) == 9_000
    assert f.derived_balance(bob) == 971
    assert f.derived_balance(revenue) == 29


def test_one_transaction_may_span_currencies_if_each_balances() -> None:
    usd_a = f.make_account(currency="USD")
    usd_b = f.make_account(currency="USD")
    cad_a = f.make_account(currency="CAD")
    cad_b = f.make_account(currency="CAD")
    f.fund(usd_a, 1_000, "USD")
    f.fund(cad_a, 2_000, "CAD")

    f.post(
        [
            (usd_a, -500, "USD"),
            (usd_b, 500, "USD"),
            (cad_a, -700, "CAD"),
            (cad_b, 700, "CAD"),
        ],
        description="two independent legs in one transaction",
    )

    assert f.derived_balance(usd_b) == 500
    assert f.derived_balance(cad_b) == 700


# --------------------------------------------------------------- rejections --


def test_unbalanced_transaction_writes_nothing() -> None:
    alice = f.make_account()
    bob = f.make_account()
    f.fund(alice, 10_000)
    entries_before = f.count_rows("entries")

    with pytest.raises(UnbalancedTransaction) as exc:
        f.post([(alice, -100, "USD"), (bob, 50, "USD")])

    assert exc.value.details["imbalance"] == {"USD": -50}
    # Rejected before the transaction was opened, so nothing to roll back.
    assert f.count_rows("entries") == entries_before


def test_currency_imbalance_is_detected_per_currency() -> None:
    usd = f.make_account(currency="USD")
    cad = f.make_account(currency="CAD")

    with pytest.raises(UnbalancedTransaction) as exc:
        f.post([(usd, 100, "USD"), (cad, -100, "CAD")])

    assert exc.value.details["imbalance"] == {"USD": 100, "CAD": -100}


def test_entry_currency_must_match_account() -> None:
    usd = f.make_account(currency="USD")
    other_usd = f.make_account(currency="USD")

    # Balanced in CAD, but both accounts are USD accounts.
    with pytest.raises(CurrencyMismatch) as exc:
        f.post([(usd, -100, "CAD"), (other_usd, 100, "CAD")])
    assert exc.value.details["account_currency"] == "USD"


def test_unknown_account_is_rejected() -> None:
    real = f.make_account()
    with pytest.raises(AccountNotFound):
        f.post([(real, -100, "USD"), (uuid4(), 100, "USD")])


def test_single_entry_request_is_rejected_by_the_schema() -> None:
    with pytest.raises(ValidationError):
        CreateTransactionRequest(
            description="x",
            entries=[{"account_id": uuid4(), "amount_minor": 1, "currency": "USD"}],
        )


@pytest.mark.parametrize("amount", [100.0, "100", 100.5, None, True])
def test_non_integer_amounts_are_rejected(amount: object) -> None:
    """No floats in the money path. `100.0` is not accepted and quietly
    truncated; it is refused."""
    with pytest.raises(ValidationError):
        CreateTransactionRequest(
            description="x",
            entries=[
                {"account_id": uuid4(), "amount_minor": amount, "currency": "USD"},
                {"account_id": uuid4(), "amount_minor": -1, "currency": "USD"},
            ],
        )


def test_zero_amount_entry_is_rejected_by_the_schema() -> None:
    with pytest.raises(ValidationError):
        CreateTransactionRequest(
            description="x",
            entries=[
                {"account_id": uuid4(), "amount_minor": 0, "currency": "USD"},
                {"account_id": uuid4(), "amount_minor": 0, "currency": "USD"},
            ],
        )


def test_amount_beyond_int64_is_rejected() -> None:
    with pytest.raises(ValidationError):
        CreateTransactionRequest(
            description="x",
            entries=[
                {"account_id": uuid4(), "amount_minor": 2**63, "currency": "USD"},
                {"account_id": uuid4(), "amount_minor": -(2**63), "currency": "USD"},
            ],
        )


# ------------------------------------------------------------- global sum ----


def test_global_sum_stays_zero_per_currency() -> None:
    """The invariant that makes the whole design work: because money only ever
    enters through external_settlement, and every transaction is zero-sum, the
    sum over all accounts is zero. Any nonzero total means money was created."""
    alice = f.make_account()
    bob = f.make_account()
    revenue = f.revenue_account("USD")
    f.fund(alice, 50_000)
    f.post([(alice, -300, "USD"), (bob, 280, "USD"), (revenue, 20, "USD")])
    f.post([(bob, -100, "USD"), (alice, 100, "USD")])

    with db.transaction(read_only=True) as cur:
        cur.execute(
            "SELECT currency, SUM(amount_minor) AS total FROM entries GROUP BY currency"
        )
        totals = {row["currency"].strip(): row["total"] for row in cur.fetchall()}

    assert totals == {"USD": 0}


# ---------------------------------------------------------------- history ----


def test_entry_history_is_paginated_by_keyset() -> None:
    alice = f.make_account()
    bob = f.make_account()
    f.fund(alice, 10_000)
    for i in range(7):
        f.post([(alice, -(i + 1), "USD"), (bob, i + 1, "USD")])

    seen: list[int] = []
    cursor = None
    pages = 0
    while True:
        page = transactions_service.list_entries(alice, limit=3, cursor=cursor)
        seen += [e["id"] for e in page["entries"]]
        pages += 1
        cursor = page["next_cursor"]
        if cursor is None:
            break

    # 1 funding entry + 7 payments.
    assert len(seen) == 8
    assert seen == sorted(seen)
    assert len(set(seen)) == 8
    assert pages == 3


def test_entry_history_for_unknown_account() -> None:
    with pytest.raises(AccountNotFound):
        transactions_service.list_entries(uuid4())


# ------------------------------------------------------------ hash chain -----


def test_first_transaction_links_to_genesis() -> None:
    alice = f.make_account()
    tx = f.fund(alice, 100)
    assert tx["prev_hash"] == GENESIS_PREV_HASH.hex()


def test_each_transaction_links_to_its_predecessor() -> None:
    alice = f.make_account()
    bob = f.make_account()
    first = f.fund(alice, 1_000)
    second = f.post([(alice, -10, "USD"), (bob, 10, "USD")])
    third = f.post([(alice, -20, "USD"), (bob, 20, "USD")])

    assert second["prev_hash"] == first["tx_hash"]
    assert third["prev_hash"] == second["tx_hash"]


def test_stored_hash_matches_a_recomputation() -> None:
    alice = f.make_account()
    bob = f.make_account()
    f.fund(alice, 1_000)
    tx = f.post([(alice, -250, "USD"), (bob, 250, "USD")])

    stored = transactions_service.get_transaction(tx["id"])
    recomputed = transaction_hash(
        transaction_id=stored["id"],
        created_at=stored["created_at"],
        entries=[
            HashableEntry(e["account_id"], e["currency"], e["amount_minor"])
            for e in stored["entries"]
        ],
        prev_hash=bytes.fromhex(stored["prev_hash"]),
    )
    assert recomputed.hex() == stored["tx_hash"]


def test_hash_is_independent_of_entry_order() -> None:
    """The canonical form sorts entries, so the same economic transaction hashes
    the same regardless of the order the client listed its legs in."""
    a, b = sorted([uuid4(), uuid4()])
    from datetime import datetime, timezone

    now = datetime.now(timezone.utc)
    tx_id = uuid4()
    forward = transaction_hash(
        transaction_id=tx_id,
        created_at=now,
        entries=[HashableEntry(a, "USD", -5), HashableEntry(b, "USD", 5)],
        prev_hash=GENESIS_PREV_HASH,
    )
    reversed_ = transaction_hash(
        transaction_id=tx_id,
        created_at=now,
        entries=[HashableEntry(b, "USD", 5), HashableEntry(a, "USD", -5)],
        prev_hash=GENESIS_PREV_HASH,
    )
    assert forward == reversed_


def test_changing_any_hashed_field_changes_the_hash() -> None:
    from datetime import datetime, timezone

    base = {
        "transaction_id": uuid4(),
        "created_at": datetime.now(timezone.utc),
        "entries": [
            HashableEntry(uuid4(), "USD", -5),
            HashableEntry(uuid4(), "USD", 5),
        ],
        "prev_hash": GENESIS_PREV_HASH,
    }
    original = transaction_hash(**base)  # type: ignore[arg-type]

    mutated_amount = dict(base)
    mutated_amount["entries"] = [
        HashableEntry(base["entries"][0].account_id, "USD", -6),  # type: ignore[index]
        HashableEntry(base["entries"][1].account_id, "USD", 6),  # type: ignore[index]
    ]
    assert transaction_hash(**mutated_amount) != original  # type: ignore[arg-type]

    mutated_prev = dict(base)
    mutated_prev["prev_hash"] = b"\xff" * 32
    assert transaction_hash(**mutated_prev) != original  # type: ignore[arg-type]

    mutated_time = dict(base)
    mutated_time["created_at"] = base["created_at"].replace(microsecond=0)  # type: ignore[union-attr]
    assert transaction_hash(**mutated_time) != original  # type: ignore[arg-type]
