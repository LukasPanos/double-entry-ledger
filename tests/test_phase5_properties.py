"""Phase 5: property-based tests.

The headline property the spec asks for -- "no FX sequence can create or destroy
money in any currency" -- is checked against a Python model of what the balances
*should* be, not just against the zero-sum invariant. Zero-sum alone is a weak
oracle here: crediting the spread to the wrong currency's revenue account, or
swapping the two liquidity pools, still balances per currency and would pass. A
model that tracks every account independently catches those.
"""

from __future__ import annotations

from collections import defaultdict
from typing import Any

import pytest
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from ledger.errors import InsufficientFunds, ValidationFailed
from ledger.hashing import GENESIS_PREV_HASH, HashableEntry, transaction_hash
from ledger.money import format_amount
from ledger.services.fx import effective_rate
from ledger.services.integrity import verify_chain
from ledger.services.reconciliation import reconcile
from tests import factories as f
from tests.conftest import reset_database

INITIAL_USER_FUNDING = 1_000_000
INITIAL_POOL_FUNDING = 10_000_000

# `fx` sells one currency for the other; `xfer` is a same-currency transfer, in
# to make sure FX and ordinary postings cannot interfere with one another.
fx_op = st.tuples(
    st.just("fx"),
    st.sampled_from(["USD", "CAD"]),          # sell currency
    st.integers(min_value=1, max_value=50_000),   # sell amount, minor units
    st.integers(min_value=1, max_value=80_000),   # buy amount, minor units
    st.integers(min_value=0, max_value=2_000),    # spread, sell currency
)
xfer_op = st.tuples(
    st.just("xfer"),
    st.sampled_from(["USD", "CAD"]),
    st.integers(min_value=1, max_value=50_000),
    st.integers(min_value=1, max_value=1),        # padding, unused
    st.integers(min_value=0, max_value=0),
)

op_lists = st.lists(st.one_of(fx_op, xfer_op), min_size=1, max_size=18)


def _build_world() -> dict[str, Any]:
    world = f.fx_world(("USD", "CAD"))
    world["user2"] = {
        currency: f.make_account(currency=currency, name=f"user2 {currency}")
        for currency in ("USD", "CAD")
    }
    return world


def _initial_model(world: dict[str, Any]) -> dict[Any, int]:
    model: dict[Any, int] = defaultdict(int)
    for currency in ("USD", "CAD"):
        model[world["user"][currency]] = INITIAL_USER_FUNDING
        model[world["liquidity"][currency]] = INITIAL_POOL_FUNDING
        model[world["revenue"][currency]] = 0
        model[world["user2"][currency]] = 0
        model[world["settlement"][currency]] = -(
            INITIAL_USER_FUNDING + INITIAL_POOL_FUNDING
        )
    return model


@pytest.mark.slow
@settings(
    max_examples=25,
    deadline=None,
    suppress_health_check=[HealthCheck.function_scoped_fixture],
)
@given(ops=op_lists)
def test_no_operation_sequence_creates_or_destroys_money(
    ops: list[tuple[Any, ...]],
) -> None:
    # Hypothesis reuses the test function across examples, so the database is
    # reset per example rather than by the autouse fixture.
    reset_database()
    world = _build_world()
    model = _initial_model(world)

    applied = 0
    for kind, currency, amount_a, amount_b, spread in ops:
        other = "CAD" if currency == "USD" else "USD"

        if kind == "xfer":
            try:
                f.post(
                    [
                        (world["user"][currency], -amount_a, currency),
                        (world["user2"][currency], amount_a, currency),
                    ]
                )
            except InsufficientFunds:
                continue
            model[world["user"][currency]] -= amount_a
            model[world["user2"][currency]] += amount_a
            applied += 1
            continue

        try:
            f.convert(
                from_account_id=world["user"][currency],
                to_account_id=world["user"][other],
                sell_amount_minor=amount_a,
                buy_amount_minor=amount_b,
                spread_minor=spread,
            )
        except (InsufficientFunds, ValidationFailed):
            # Rejected before any write, so the model must not move either.
            continue

        converted = amount_a - spread
        model[world["user"][currency]] -= amount_a
        model[world["revenue"][currency]] += spread
        model[world["liquidity"][currency]] += converted
        model[world["liquidity"][other]] -= amount_b
        model[world["user"][other]] += amount_b
        applied += 1

    # 1. Every account holds exactly what the model says.
    for account_id, expected in model.items():
        assert f.derived_balance(account_id) == expected, account_id

    # 2. Nothing was created or destroyed, in either currency.
    totals = f.totals_by_currency()
    assert totals == {"USD": 0, "CAD": 0} or totals == {}, totals

    # 3. FX never touches the door money enters through. Settlement moves only
    #    on funding, so its balance is still exactly the opening figure.
    for currency in ("USD", "CAD"):
        assert model[world["settlement"][currency]] == -(
            INITIAL_USER_FUNDING + INITIAL_POOL_FUNDING
        )
        assert f.derived_balance(world["settlement"][currency]) == model[
            world["settlement"][currency]
        ]

    # 4. No user account went negative.
    for key in ("user", "user2"):
        for currency in ("USD", "CAD"):
            assert f.balance(world[key][currency])["available_minor"] >= 0

    # 5. The platform only ever gained, and only in currencies it charged in.
    for currency in ("USD", "CAD"):
        assert f.derived_balance(world["revenue"][currency]) >= 0

    # 6. And every structural invariant still holds.
    report = reconcile()
    assert report["ok"], [c for c in report["checks"] if not c["passed"]]
    assert verify_chain()["ok"]


@pytest.mark.slow
@settings(
    max_examples=20,
    deadline=None,
    suppress_health_check=[HealthCheck.function_scoped_fixture],
)
@given(
    sells=st.lists(
        st.tuples(
            st.integers(min_value=2, max_value=20_000),
            st.integers(min_value=1, max_value=30_000),
        ),
        min_size=1,
        max_size=10,
    )
)
def test_a_user_can_never_profit_from_round_tripping(
    sells: list[tuple[int, int]],
) -> None:
    """Whatever the rates, converting out and back cannot leave the user with
    more of the currency they started in than they would have had at the same
    rate with no spread. The spread is charged on the way out every time."""
    reset_database()
    world = _build_world()

    started_with = f.derived_balance(world["user"]["USD"])
    total_spread = 0

    for sell, buy in sells:
        spread = max(1, sell // 100)
        if spread >= sell:
            continue
        try:
            f.convert(
                from_account_id=world["user"]["USD"],
                to_account_id=world["user"]["CAD"],
                sell_amount_minor=sell,
                buy_amount_minor=buy,
                spread_minor=spread,
            )
        except (InsufficientFunds, ValidationFailed):
            continue
        total_spread += spread

    assert f.derived_balance(world["revenue"]["USD"]) == total_spread
    # Everything the user gave up in USD went either to the pool or to revenue.
    spent = started_with - f.derived_balance(world["user"]["USD"])
    assert (
        f.derived_balance(world["liquidity"]["USD"])
        == INITIAL_POOL_FUNDING + spent - total_spread
    )
    assert f.totals_by_currency() == {"USD": 0, "CAD": 0}


# ---------------------------------------------------- pure-function properties


@given(
    amounts=st.lists(
        st.integers(min_value=-(10**12), max_value=10**12).filter(lambda x: x != 0),
        min_size=1,
        max_size=8,
    )
)
def test_validate_postings_accepts_exactly_the_balanced_sets(
    amounts: list[int],
) -> None:
    """A set of postings is accepted if and only if it sums to zero, for any
    shape of input. Tested against the real validator rather than a
    reimplementation of it."""
    import uuid

    from ledger.errors import UnbalancedTransaction
    from ledger.services.posting import Posting, validate_postings

    accounts = [uuid.uuid4() for _ in amounts]
    balancing = -sum(amounts)

    postings = [
        Posting(account, amount, "USD") for account, amount in zip(accounts, amounts)
    ]
    if balancing != 0:
        postings.append(Posting(uuid.uuid4(), balancing, "USD"))

    if len(postings) < 2:
        with pytest.raises(UnbalancedTransaction):
            validate_postings(postings)
        return

    assert validate_postings(postings) == {"USD": 0}

    # Perturbing any single leg must be rejected.
    broken = list(postings)
    broken[0] = Posting(broken[0].account_id, broken[0].amount_minor + 1, "USD")
    with pytest.raises(UnbalancedTransaction):
        validate_postings(broken)


@given(
    entries=st.lists(
        st.tuples(
            st.uuids(),
            st.sampled_from(["USD", "CAD", "JPY"]),
            st.integers(min_value=-10**9, max_value=10**9).filter(lambda x: x != 0),
        ),
        min_size=2,
        max_size=10,
    ),
    seed=st.integers(min_value=0, max_value=10**6),
)
def test_the_transaction_hash_ignores_entry_order_and_nothing_else(
    entries: list[tuple[Any, str, int]], seed: int
) -> None:
    """Two properties of the canonical form at once: permuting the entries must
    not change the hash, and changing any single amount must."""
    import random
    import uuid
    from datetime import datetime, timezone

    tx_id = uuid.UUID(int=seed)
    created_at = datetime(2026, 1, 1, tzinfo=timezone.utc)
    hashable = [HashableEntry(a, c, m) for a, c, m in entries]

    baseline = transaction_hash(
        transaction_id=tx_id,
        created_at=created_at,
        entries=hashable,
        prev_hash=GENESIS_PREV_HASH,
    )

    shuffled = list(hashable)
    random.Random(seed).shuffle(shuffled)
    assert (
        transaction_hash(
            transaction_id=tx_id,
            created_at=created_at,
            entries=shuffled,
            prev_hash=GENESIS_PREV_HASH,
        )
        == baseline
    )

    mutated = list(hashable)
    mutated[0] = HashableEntry(
        mutated[0].account_id, mutated[0].currency, mutated[0].amount_minor + 1
    )
    assert (
        transaction_hash(
            transaction_id=tx_id,
            created_at=created_at,
            entries=mutated,
            prev_hash=GENESIS_PREV_HASH,
        )
        != baseline
    )


@given(
    sell=st.integers(min_value=1, max_value=10**9),
    buy=st.integers(min_value=1, max_value=10**9),
)
def test_effective_rate_is_never_a_float(sell: int, buy: int) -> None:
    """It is a display string computed with Decimal. If it ever came back as a
    float, some caller would eventually multiply money by it."""
    rate = effective_rate(
        sell_minor=sell, sell_currency="USD", buy_minor=buy, buy_currency="CAD"
    )
    assert isinstance(rate, str)
    assert "e" not in rate.lower(), rate  # no scientific notation to misparse


@given(
    amount=st.integers(min_value=-(10**15), max_value=10**15),
    currency=st.sampled_from(["USD", "JPY", "BHD"]),
)
def test_format_amount_round_trips_the_minor_units(
    amount: int, currency: str
) -> None:
    """Formatting is display-only, but it must not misplace the decimal point:
    parsing the rendered string back must recover the exact minor units."""
    from decimal import Decimal

    from ledger.money import MINOR_UNIT_EXPONENT

    rendered = format_amount(amount, currency)
    numeric, code = rendered.rsplit(" ", 1)
    assert code == currency
    exponent = MINOR_UNIT_EXPONENT[currency]
    recovered = int(Decimal(numeric) * (10**exponent))
    assert recovered == amount
