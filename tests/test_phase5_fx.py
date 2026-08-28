"""Phase 5: multi-currency and FX.

The claim: money never crosses the currency boundary. A conversion is two
independent single-currency movements that happen to share a transaction, and no
line of code ever adds a USD amount to a CAD amount.
"""

from __future__ import annotations

from uuid import uuid4

import pytest
from fastapi.testclient import TestClient

from ledger import db
from ledger.errors import (
    AccountNotFound,
    IdempotencyKeyReused,
    InsufficientFunds,
    ValidationFailed,
)
from ledger.services.fx import effective_rate
from ledger.services.integrity import verify_chain
from ledger.services.reconciliation import reconcile
from tests import factories as f


# ------------------------------------------------------------- structure -----


def test_zero_spread_conversion_writes_exactly_four_entries() -> None:
    """The four-entry structure is the spread-free case."""
    w = f.fx_world()

    result = f.convert(
        from_account_id=w["user"]["USD"],
        to_account_id=w["user"]["CAD"],
        sell_amount_minor=10_000,
        buy_amount_minor=13_500,
    ).body

    entries = result["entries"]
    assert len(entries) == 4
    assert result["revenue_account_id"] is None

    by_account = {e["account_id"]: e for e in entries}
    assert by_account[str(w["user"]["USD"])]["amount_minor"] == -10_000
    assert by_account[str(w["liquidity"]["USD"])]["amount_minor"] == 10_000
    assert by_account[str(w["liquidity"]["CAD"])]["amount_minor"] == -13_500
    assert by_account[str(w["user"]["CAD"])]["amount_minor"] == 13_500


def test_conversion_with_a_spread_writes_five_entries() -> None:
    w = f.fx_world()

    result = f.convert(
        from_account_id=w["user"]["USD"],
        to_account_id=w["user"]["CAD"],
        sell_amount_minor=10_000,
        buy_amount_minor=13_365,
        spread_minor=100,
    ).body

    entries = result["entries"]
    assert len(entries) == 5
    assert result["revenue_account_id"] == str(w["revenue"]["USD"])
    assert result["converted_amount_minor"] == 9_900

    by_account = {e["account_id"]: e for e in entries}
    assert by_account[str(w["user"]["USD"])]["amount_minor"] == -10_000
    assert by_account[str(w["revenue"]["USD"])]["amount_minor"] == 100
    assert by_account[str(w["liquidity"]["USD"])]["amount_minor"] == 9_900
    assert by_account[str(w["liquidity"]["CAD"])]["amount_minor"] == -13_365
    assert by_account[str(w["user"]["CAD"])]["amount_minor"] == 13_365


def test_the_spread_is_denominated_in_the_sell_currency() -> None:
    """Selling CAD means the fee is CAD, and it lands in the CAD revenue
    account -- not the USD one. Getting this backwards would still balance per
    currency, which is exactly why it needs its own test."""
    w = f.fx_world()

    result = f.convert(
        from_account_id=w["user"]["CAD"],
        to_account_id=w["user"]["USD"],
        sell_amount_minor=13_500,
        buy_amount_minor=9_900,
        spread_minor=135,
    ).body

    assert result["sell_currency"] == "CAD"
    assert result["revenue_account_id"] == str(w["revenue"]["CAD"])
    assert f.derived_balance(w["revenue"]["CAD"]) == 135
    assert f.derived_balance(w["revenue"]["USD"]) == 0

    spread_entry = next(
        e for e in result["entries"] if e["account_id"] == str(w["revenue"]["CAD"])
    )
    assert spread_entry["currency"] == "CAD"


def test_every_entry_currency_matches_its_account() -> None:
    w = f.fx_world()
    result = f.convert(
        from_account_id=w["user"]["USD"],
        to_account_id=w["user"]["CAD"],
        sell_amount_minor=5_000,
        buy_amount_minor=6_700,
        spread_minor=50,
    ).body

    with db.transaction(read_only=True) as cur:
        cur.execute(
            """
            SELECT e.currency AS entry_currency, a.currency AS account_currency
              FROM entries e JOIN accounts a ON a.id = e.account_id
             WHERE e.transaction_id = %s
            """,
            (result["transaction_id"],),
        )
        rows = cur.fetchall()

    assert len(rows) == 5
    for row in rows:
        assert row["entry_currency"] == row["account_currency"]


# -------------------------------------------------------------- balances -----


def test_balances_after_a_conversion() -> None:
    w = f.fx_world()
    usd_before = f.derived_balance(w["user"]["USD"])
    cad_before = f.derived_balance(w["user"]["CAD"])
    pool_usd_before = f.derived_balance(w["liquidity"]["USD"])
    pool_cad_before = f.derived_balance(w["liquidity"]["CAD"])

    f.convert(
        from_account_id=w["user"]["USD"],
        to_account_id=w["user"]["CAD"],
        sell_amount_minor=10_000,
        buy_amount_minor=13_365,
        spread_minor=100,
    )

    assert f.derived_balance(w["user"]["USD"]) == usd_before - 10_000
    assert f.derived_balance(w["user"]["CAD"]) == cad_before + 13_365
    assert f.derived_balance(w["liquidity"]["USD"]) == pool_usd_before + 9_900
    assert f.derived_balance(w["liquidity"]["CAD"]) == pool_cad_before - 13_365
    assert f.derived_balance(w["revenue"]["USD"]) == 100


def test_global_sum_stays_zero_per_currency() -> None:
    w = f.fx_world()
    for i in range(6):
        f.convert(
            from_account_id=w["user"]["USD"],
            to_account_id=w["user"]["CAD"],
            sell_amount_minor=1_000 + i,
            buy_amount_minor=1_350 + i,
            spread_minor=i,
        )
        f.convert(
            from_account_id=w["user"]["CAD"],
            to_account_id=w["user"]["USD"],
            sell_amount_minor=500 + i,
            buy_amount_minor=360 + i,
            spread_minor=i,
        )

    assert f.totals_by_currency() == {"USD": 0, "CAD": 0}
    assert reconcile()["ok"]
    assert verify_chain()["ok"]


def test_a_round_trip_costs_the_user_the_spread() -> None:
    """The user cannot come out ahead. Converting out and straight back leaves
    them down by exactly the two spreads, and the platform up by exactly the
    same, denominated where it was charged."""
    w = f.fx_world()
    usd_before = f.derived_balance(w["user"]["USD"])

    # USD -> CAD, 100 USD spread.
    f.convert(
        from_account_id=w["user"]["USD"],
        to_account_id=w["user"]["CAD"],
        sell_amount_minor=10_000,
        buy_amount_minor=13_365,
        spread_minor=100,
    )
    # CAD -> USD at the same rate, 135 CAD spread.
    f.convert(
        from_account_id=w["user"]["CAD"],
        to_account_id=w["user"]["USD"],
        sell_amount_minor=13_365,
        buy_amount_minor=9_800,
        spread_minor=135,
    )

    assert f.derived_balance(w["user"]["USD"]) == usd_before - 200
    assert f.derived_balance(w["revenue"]["USD"]) == 100
    assert f.derived_balance(w["revenue"]["CAD"]) == 135
    assert f.totals_by_currency() == {"USD": 0, "CAD": 0}


def test_the_liquidity_pool_may_go_negative() -> None:
    """A pool going short is the platform holding a funded position in that
    currency, which is a real thing a treasury does. It is one of the account
    types permitted below zero; a user account is not.

    Left unbounded on purpose: capping pool inventory is a treasury policy
    decision, not a ledger invariant. See docs/decisions.md 5.5.
    """
    w = f.fx_world()
    f.post(
        [
            (w["liquidity"]["CAD"], -10_000_000, "CAD"),
            (w["settlement"]["CAD"], 10_000_000, "CAD"),
        ],
        description="drain the CAD pool",
    )
    assert f.derived_balance(w["liquidity"]["CAD"]) == 0

    f.convert(
        from_account_id=w["user"]["USD"],
        to_account_id=w["user"]["CAD"],
        sell_amount_minor=1_000,
        buy_amount_minor=1_350,
    )
    assert f.derived_balance(w["liquidity"]["CAD"]) == -1_350
    assert reconcile()["ok"]


# ------------------------------------------------------------ rejections -----


def test_same_currency_conversion_is_rejected() -> None:
    w = f.fx_world()
    other_usd = f.make_account(currency="USD")

    with pytest.raises(ValidationFailed) as exc:
        f.convert(
            from_account_id=w["user"]["USD"],
            to_account_id=other_usd,
            sell_amount_minor=100,
            buy_amount_minor=100,
        )
    assert "same-currency transfer" in str(exc.value)


def test_converting_to_the_same_account_is_rejected() -> None:
    w = f.fx_world()
    with pytest.raises(ValidationFailed):
        f.convert(
            from_account_id=w["user"]["USD"],
            to_account_id=w["user"]["USD"],
            sell_amount_minor=100,
            buy_amount_minor=100,
        )


def test_a_spread_swallowing_the_whole_amount_is_rejected() -> None:
    w = f.fx_world()
    with pytest.raises(ValidationFailed) as exc:
        f.convert(
            from_account_id=w["user"]["USD"],
            to_account_id=w["user"]["CAD"],
            sell_amount_minor=1_000,
            buy_amount_minor=1,
            spread_minor=1_000,
        )
    assert "converts nothing" in str(exc.value)


def test_conversion_beyond_available_balance_is_rejected() -> None:
    w = f.fx_world()
    f.make_hold(w["user"]["USD"], 999_000)

    with pytest.raises(InsufficientFunds):
        f.convert(
            from_account_id=w["user"]["USD"],
            to_account_id=w["user"]["CAD"],
            sell_amount_minor=10_000,
            buy_amount_minor=13_500,
        )
    assert f.totals_by_currency()["USD"] == 0


def test_a_missing_liquidity_pool_is_a_404() -> None:
    """Converting into a currency with no pool must fail loudly rather than
    inventing an account."""
    f.settlement_account("USD")
    f.settlement_account("EUR")
    f.liquidity_account("USD")
    usd = f.make_account(currency="USD")
    eur = f.make_account(currency="EUR")
    f.fund(usd, 100_000, "USD")

    with pytest.raises(AccountNotFound) as exc:
        f.convert(
            from_account_id=usd,
            to_account_id=eur,
            sell_amount_minor=1_000,
            buy_amount_minor=900,
        )
    assert exc.value.details["currency"] == "EUR"
    assert exc.value.details["account_type"] == "liquidity"


def test_a_missing_revenue_account_only_matters_with_a_spread() -> None:
    """The revenue account is resolved lazily: a zero-spread conversion writes no
    revenue entry, so it must not require the account to exist."""
    f.settlement_account("USD")
    f.settlement_account("CAD")
    f.liquidity_account("USD")
    cad_pool = f.liquidity_account("CAD")
    usd = f.make_account(currency="USD")
    cad = f.make_account(currency="CAD")
    f.fund(usd, 100_000, "USD")
    f.fund(cad_pool, 100_000, "CAD")

    # No platform_revenue account exists anywhere.
    with db.transaction(read_only=True) as cur:
        cur.execute("SELECT count(*) AS n FROM accounts WHERE type = 'platform_revenue'")
        assert cur.fetchone()["n"] == 0

    f.convert(
        from_account_id=usd, to_account_id=cad, sell_amount_minor=100, buy_amount_minor=135
    )

    with pytest.raises(AccountNotFound) as exc:
        f.convert(
            from_account_id=usd,
            to_account_id=cad,
            sell_amount_minor=100,
            buy_amount_minor=134,
            spread_minor=1,
        )
    assert exc.value.details["account_type"] == "platform_revenue"


# --------------------------------------------------- differing minor units ---


def test_conversion_between_currencies_with_different_exponents() -> None:
    """USD has two decimal places, JPY has none. Because every amount is stated
    in its own minor units and nothing is ever multiplied, the mismatch needs no
    special handling in the ledger at all."""
    for currency in ("USD", "JPY"):
        f.settlement_account(currency)
        f.liquidity_account(currency)
    usd = f.make_account(currency="USD")
    jpy = f.make_account(currency="JPY")
    f.fund(usd, 100_000, "USD")
    with db.transaction(read_only=True) as cur:
        cur.execute("SELECT id FROM accounts WHERE type='liquidity' AND currency='JPY'")
        jpy_pool = cur.fetchone()["id"]
    f.fund(jpy_pool, 10_000_000, "JPY")

    # 100.00 USD -> 15,000 JPY (rate 150.00)
    result = f.convert(
        from_account_id=usd,
        to_account_id=jpy,
        sell_amount_minor=10_000,
        buy_amount_minor=15_000,
    ).body

    assert f.derived_balance(jpy) == 15_000
    assert result["effective_rate"] == "150"
    assert f.totals_by_currency() == {"USD": 0, "JPY": 0}


def test_effective_rate_accounts_for_minor_unit_exponents() -> None:
    # 100.00 USD -> 135.00 CAD is a rate of 1.35, not 1.
    assert effective_rate(
        sell_minor=10_000, sell_currency="USD", buy_minor=13_500, buy_currency="CAD"
    ) == "1.35"
    # 100.00 USD -> 15000 JPY is 150, not 1.5.
    assert effective_rate(
        sell_minor=10_000, sell_currency="USD", buy_minor=15_000, buy_currency="JPY"
    ) == "150"
    # And back the other way.
    assert effective_rate(
        sell_minor=15_000, sell_currency="JPY", buy_minor=10_000, buy_currency="USD"
    ) == "0.00666667"


# ------------------------------------------------------------ idempotency ----


def test_conversion_is_idempotent() -> None:
    w = f.fx_world()
    key = uuid4()

    first = f.convert(
        from_account_id=w["user"]["USD"],
        to_account_id=w["user"]["CAD"],
        sell_amount_minor=10_000,
        buy_amount_minor=13_365,
        spread_minor=100,
        key=key,
    )
    second = f.convert(
        from_account_id=w["user"]["USD"],
        to_account_id=w["user"]["CAD"],
        sell_amount_minor=10_000,
        buy_amount_minor=13_365,
        spread_minor=100,
        key=key,
    )

    assert second.replayed is True
    assert second.body["transaction_id"] == first.body["transaction_id"]
    assert f.derived_balance(w["user"]["CAD"]) == 1_000_000 + 13_365


def test_a_different_rate_under_the_same_key_is_409() -> None:
    """The dangerous retry: same key, better rate. Must be refused, not
    replayed and not re-executed."""
    w = f.fx_world()
    key = uuid4()

    f.convert(
        from_account_id=w["user"]["USD"],
        to_account_id=w["user"]["CAD"],
        sell_amount_minor=10_000,
        buy_amount_minor=13_365,
        key=key,
    )
    with pytest.raises(IdempotencyKeyReused):
        f.convert(
            from_account_id=w["user"]["USD"],
            to_account_id=w["user"]["CAD"],
            sell_amount_minor=10_000,
            buy_amount_minor=99_999,
            key=key,
        )
    assert f.derived_balance(w["user"]["CAD"]) == 1_000_000 + 13_365


# --------------------------------------------------------------- http layer --


def test_fx_convert_over_http(client: TestClient) -> None:
    w = f.fx_world()

    response = client.post(
        "/fx/convert",
        headers={"Idempotency-Key": str(uuid4())},
        json={
            "from_account_id": str(w["user"]["USD"]),
            "to_account_id": str(w["user"]["CAD"]),
            "sell_amount_minor": 10_000,
            "buy_amount_minor": 13_365,
            "spread_minor": 100,
        },
    )
    assert response.status_code == 201, response.text
    body = response.json()
    assert body["sell_currency"] == "USD"
    assert body["buy_currency"] == "CAD"
    assert body["converted_amount_minor"] == 9_900
    assert body["effective_rate"] == "1.3365"
    assert len(body["entries"]) == 5
    assert body["replayed"] is False

    balance = client.get(f"/accounts/{w['user']['CAD']}/balance").json()
    assert balance["actual_minor"] == 1_000_000 + 13_365

    assert client.get("/reconciliation").json()["ok"] is True
    assert client.get("/integrity").json()["ok"] is True


def test_fx_convert_requires_an_idempotency_key(client: TestClient) -> None:
    w = f.fx_world()
    response = client.post(
        "/fx/convert",
        json={
            "from_account_id": str(w["user"]["USD"]),
            "to_account_id": str(w["user"]["CAD"]),
            "sell_amount_minor": 100,
            "buy_amount_minor": 135,
        },
    )
    assert response.status_code == 400


def test_fx_convert_rejects_float_amounts(client: TestClient) -> None:
    w = f.fx_world()
    response = client.post(
        "/fx/convert",
        headers={"Idempotency-Key": str(uuid4())},
        json={
            "from_account_id": str(w["user"]["USD"]),
            "to_account_id": str(w["user"]["CAD"]),
            "sell_amount_minor": 100.5,
            "buy_amount_minor": 135,
        },
    )
    assert response.status_code == 422
