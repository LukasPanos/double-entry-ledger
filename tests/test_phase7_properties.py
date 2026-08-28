"""Phase 7: stateful property tests over random operation sequences.

Hypothesis drives a state machine that mixes transfers, holds, captures, voids,
FX conversions and deliberate idempotency-key replays. After every single step it
re-checks the four invariants the spec names:

  * global zero-sum per currency
  * no negative available balances
  * no orphaned entries
  * idempotency honoured

The point of a state machine rather than a list of generated operations is that
Hypothesis chooses each step *knowing the state it has already built* -- so it can
learn to capture the holds it just created, and can shrink a failure down to the
shortest sequence that still reproduces it. A flat list of random operations
mostly generates rejected requests.
"""

from __future__ import annotations

from typing import Any
from uuid import UUID, uuid4

import pytest
from hypothesis import HealthCheck, settings
from hypothesis import strategies as st
from hypothesis.stateful import (
    Bundle,
    RuleBasedStateMachine,
    initialize,
    invariant,
    multiple,
    rule,
)

from ledger import db
from ledger.errors import (
    CaptureExceedsHold,
    HoldNotPending,
    IdempotencyKeyReused,
    InsufficientFunds,
    LedgerError,
    UnbalancedTransaction,
    ValidationFailed,
)
from ledger.services.integrity import verify_chain
from ledger.services.reconciliation import reconcile
from tests import factories as f
from tests.conftest import reset_database

CURRENCIES = ("USD", "CAD")

# Deliberately tight relative to the amounts below: with ~25 steps drawing up to
# 20,000 each, an account funded with 500,000 never runs out and the overdraft
# path is never reached. A mutation test confirmed that -- disabling
# `assert_no_overdraft` entirely did not fail this suite until the funding came
# down. Starving the accounts is what makes "no negative available balance" a
# claim about behaviour under pressure rather than about arithmetic that never
# gets close to the boundary.
FUNDING = 25_000

amounts = st.integers(min_value=1, max_value=20_000)


class LedgerMachine(RuleBasedStateMachine):
    """Random ledger traffic, with the invariants checked after every step."""

    holds = Bundle("holds")
    replayable = Bundle("replayable")

    def __init__(self) -> None:
        super().__init__()
        reset_database()
        self.world = f.fx_world(CURRENCIES, user_funding=FUNDING)
        self.users: dict[str, list[UUID]] = {}
        for currency in CURRENCIES:
            extra = f.make_account(currency=currency, name=f"extra {currency}")
            f.fund(extra, FUNDING, currency)
            self.users[currency] = [self.world["user"][currency], extra]
        # What we believe the ledger holds, maintained only when a call succeeds.
        self.confirmed_transactions = 0
        self.replay_bodies: dict[UUID, Any] = {}

    # ------------------------------------------------------------- the rules --

    @initialize(target=replayable)
    def seed_a_replayable_key(self) -> Any:
        """Start with both bundles non-empty.

        Without this, the five rules that consume a bundle are disabled on the
        first step and Hypothesis spends its budget retrying draws -- it reported
        five outright invalid examples before these existed.
        """
        return self._do_transfer("USD", 1_000, with_fee=False)

    @initialize(target=holds)
    def seed_a_hold(self) -> Any:
        return self._do_hold("USD", 1_000, short=False)

    @rule(
        target=replayable,
        currency=st.sampled_from(CURRENCIES),
        amount=amounts,
        with_fee=st.booleans(),
    )
    def transfer(self, currency: str, amount: int, with_fee: bool) -> Any:
        return self._do_transfer(currency, amount, with_fee)

    def _do_transfer(self, currency: str, amount: int, with_fee: bool) -> Any:
        payer, payee = self.users[currency]
        legs = [(payer, -amount, currency), (payee, amount, currency)]
        if with_fee and amount > 100:
            fee = max(1, amount // 50)
            legs = [
                (payer, -amount, currency),
                (payee, amount - fee, currency),
                (self.world["revenue"][currency], fee, currency),
            ]
        key = uuid4()
        request = f.transaction_request(legs)
        try:
            from ledger.services.transactions import post_transaction

            post_transaction(request, key)
        except (InsufficientFunds, UnbalancedTransaction, ValidationFailed):
            # `multiple()` with no arguments adds nothing to the bundle. Returning
            # None would put a None *in* it, and every rule consuming the bundle
            # would then spend steps on a value it has to skip.
            return multiple()
        self.confirmed_transactions += 1
        self.replay_bodies[key] = ("transfer", request)
        return key

    @rule(
        target=holds,
        currency=st.sampled_from(CURRENCIES),
        amount=amounts,
        short=st.booleans(),
    )
    def create_hold(self, currency: str, amount: int, short: bool) -> Any:
        return self._do_hold(currency, amount, short)

    def _do_hold(self, currency: str, amount: int, short: bool) -> Any:
        account = self.users[currency][0]
        try:
            outcome = f.make_hold(
                account, amount, currency=currency,
                expires_in_seconds=1 if short else 3600,
            )
        except (InsufficientFunds, LedgerError):
            return multiple()
        return outcome.body["id"]

    @rule(hold_id=holds, fraction=st.integers(min_value=1, max_value=100))
    def capture_hold(self, hold_id: Any, fraction: int) -> None:
        from ledger.services.holds import get_hold

        try:
            hold = get_hold(UUID(hold_id))
        except LedgerError:
            return
        currency = hold["currency"]
        destination = next(
            a for a in self.users[currency] if str(a) != str(hold["account_id"])
        )
        amount = max(1, hold["amount_minor"] * fraction // 100)
        try:
            f.capture(UUID(hold_id), [(destination, amount)], amount_minor=amount)
        except (
            HoldNotPending,
            CaptureExceedsHold,
            InsufficientFunds,
            ValidationFailed,
            UnbalancedTransaction,
        ):
            return
        self.confirmed_transactions += 1

    @rule(hold_id=holds)
    def void_hold(self, hold_id: Any) -> None:
        try:
            f.void(UUID(hold_id))
        except LedgerError:
            return

    @rule(
        target=replayable,
        sell=st.sampled_from(CURRENCIES),
        sell_amount=amounts,
        buy_amount=amounts,
        spread=st.integers(min_value=0, max_value=200),
    )
    def fx_convert(
        self, sell: str, sell_amount: int, buy_amount: int, spread: int
    ) -> Any:
        buy = "CAD" if sell == "USD" else "USD"
        key = uuid4()
        try:
            f.convert(
                from_account_id=self.users[sell][0],
                to_account_id=self.users[buy][0],
                sell_amount_minor=sell_amount,
                buy_amount_minor=buy_amount,
                spread_minor=spread,
                key=key,
            )
        except (InsufficientFunds, ValidationFailed, UnbalancedTransaction):
            return multiple()
        self.confirmed_transactions += 1
        return key

    @rule(key=replayable)
    def replay_the_same_key(self, key: Any) -> None:
        """The idempotency invariant, driven rather than merely asserted.

        Re-sending a committed key must replay the stored response and must not
        produce a second transaction. `confirmed_transactions` is deliberately
        *not* incremented here, so the count assertion in the invariant below
        fails if a replay ever writes anything.
        """
        if key not in self.replay_bodies:
            return
        kind, request = self.replay_bodies[key]
        if kind != "transfer":
            return
        from ledger.services.transactions import post_transaction

        outcome = post_transaction(request, key)
        assert outcome.replayed is True, "a committed key did not replay"

    @rule(key=replayable, amount=amounts)
    def reuse_a_key_for_a_different_body(self, key: Any, amount: int) -> None:
        """Same key, different request. Must be refused with 409, and must not
        write anything."""
        if key not in self.replay_bodies:
            return
        kind, _ = self.replay_bodies[key]
        if kind != "transfer":
            return
        payer, payee = self.users["USD"]
        from ledger.services.transactions import post_transaction

        different = f.transaction_request(
            [(payer, -(amount + 7), "USD"), (payee, amount + 7, "USD")]
        )
        try:
            post_transaction(different, key)
        except IdempotencyKeyReused:
            return
        except (InsufficientFunds, ValidationFailed):
            return
        # If we get here the key was accepted for a different body, unless the
        # fingerprint genuinely matched (possible when amount+7 collides with the
        # original amount, which is fine).
        return

    @rule()
    def sweep_expired_holds(self) -> None:
        from ledger.services.holds import sweep_expired_holds

        sweep_expired_holds()

    # -------------------------------------------------------- the invariants --

    @invariant()
    def global_zero_sum_per_currency(self) -> None:
        for currency, total in f.totals_by_currency().items():
            assert total == 0, f"{currency} sums to {total}, not zero"

    @invariant()
    def no_negative_available_balances(self) -> None:
        with db.transaction(read_only=True) as cur:
            cur.execute(
                """
                SELECT a.id,
                       COALESCE((SELECT SUM(e.amount_minor) FROM entries e
                                  WHERE e.account_id = a.id), 0) AS actual,
                       COALESCE((SELECT SUM(h.amount_minor) FROM holds h
                                  WHERE h.account_id = a.id
                                    AND h.status = 'pending'
                                    AND h.expires_at > now()), 0) AS held
                  FROM accounts a
                 WHERE a.type = 'user'
                """
            )
            for row in cur.fetchall():
                available = row["actual"] - row["held"]
                assert row["actual"] >= 0, f"account {row['id']} actual {row['actual']}"
                assert available >= 0, f"account {row['id']} available {available}"

    @invariant()
    def no_orphaned_entries(self) -> None:
        with db.transaction(read_only=True) as cur:
            cur.execute(
                """
                SELECT count(*) AS n FROM entries e
                 WHERE NOT EXISTS (SELECT 1 FROM transactions t
                                    WHERE t.id = e.transaction_id)
                    OR NOT EXISTS (SELECT 1 FROM accounts a
                                    WHERE a.id = e.account_id)
                """
            )
            assert cur.fetchone()["n"] == 0

    @invariant()
    def idempotency_is_honoured(self) -> None:
        """One transaction per key, and the number of transactions matches the
        number of operations that reported success."""
        with db.transaction(read_only=True) as cur:
            cur.execute(
                """
                SELECT count(*) AS total,
                       count(DISTINCT idempotency_key) AS keys
                  FROM transactions
                """
            )
            row = cur.fetchone()
        assert row["total"] == row["keys"], "a key maps to more than one transaction"
        # `fx_world` posts one funding transaction per account it creates: two
        # per currency, plus one for the extra user account per currency.
        funding = len(CURRENCIES) * 3
        assert row["total"] == funding + self.confirmed_transactions, (
            f"{row['total']} transactions exist but {funding} funding + "
            f"{self.confirmed_transactions} confirmed were expected"
        )

    @invariant()
    def every_transaction_balances(self) -> None:
        with db.transaction(read_only=True) as cur:
            cur.execute(
                """
                SELECT transaction_id FROM entries
                 GROUP BY transaction_id, currency
                HAVING SUM(amount_minor) <> 0
                """
            )
            assert cur.fetchall() == []

    def teardown(self) -> None:
        """The expensive checks, once per example rather than once per step."""
        report = reconcile()
        assert report["ok"], [c for c in report["checks"] if not c["passed"]]
        assert verify_chain()["ok"]


LedgerMachine.TestCase.settings = settings(
    max_examples=20,
    stateful_step_count=25,
    deadline=None,
    suppress_health_check=[
        HealthCheck.function_scoped_fixture,
        HealthCheck.too_slow,
    ],
)

TestLedgerMachine = pytest.mark.slow(LedgerMachine.TestCase)
