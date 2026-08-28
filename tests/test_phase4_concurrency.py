"""Phase 4: the two concurrency strategies, and the proof they are equivalent.

The benchmark in scripts/loadtest.py answers "which is faster". These tests
answer the prior question: **are they both correct**. A performance comparison
between a correct implementation and a subtly broken one is worthless, so every
invariant that matters is asserted under both strategies.
"""

from __future__ import annotations

import threading
from concurrent.futures import ThreadPoolExecutor
from typing import Any
from uuid import uuid4

import psycopg
import pytest

from ledger import db
from ledger.config import get_settings
from ledger.errors import InsufficientFunds, RetriesExhausted
from ledger.services import transactions as transactions_service
from ledger.services.reconciliation import reconcile
from tests import factories as f

STRATEGIES = ["pessimistic", "optimistic"]


@pytest.fixture(params=STRATEGIES)
def strategy(request: Any) -> str:
    return request.param


def post(entries: list[tuple[Any, int, str]], strategy: str, key: Any = None) -> Any:
    return transactions_service.post_transaction(
        f.transaction_request(entries), key or uuid4(), strategy=strategy
    )


# ------------------------------------------------- both strategies agree -----


def test_both_strategies_post_transactions(strategy: str) -> None:
    alice = f.make_account()
    bob = f.make_account()
    f.fund(alice, 10_000)

    post([(alice, -2_500, "USD"), (bob, 2_500, "USD")], strategy)

    assert f.derived_balance(alice) == 7_500
    assert f.derived_balance(bob) == 2_500
    assert reconcile()["ok"]


def test_both_strategies_reject_unbalanced(strategy: str) -> None:
    alice = f.make_account()
    bob = f.make_account()
    f.fund(alice, 10_000)
    from ledger.errors import UnbalancedTransaction

    with pytest.raises(UnbalancedTransaction):
        post([(alice, -100, "USD"), (bob, 50, "USD")], strategy)


def test_both_strategies_enforce_available_balance(strategy: str) -> None:
    alice = f.make_account()
    bob = f.make_account()
    f.fund(alice, 1_000)
    f.make_hold(alice, 800)

    with pytest.raises(InsufficientFunds):
        post([(alice, -300, "USD"), (bob, 300, "USD")], strategy)


def test_both_strategies_are_idempotent(strategy: str) -> None:
    alice = f.make_account()
    bob = f.make_account()
    f.fund(alice, 10_000)
    key = uuid4()

    first = post([(alice, -100, "USD"), (bob, 100, "USD")], strategy, key)
    second = post([(alice, -100, "USD"), (bob, 100, "USD")], strategy, key)

    assert first.replayed is False
    assert second.replayed is True
    assert f.derived_balance(bob) == 100


def test_both_strategies_maintain_the_hash_chain(strategy: str) -> None:
    alice = f.make_account()
    bob = f.make_account()
    f.fund(alice, 10_000)
    for i in range(5):
        post([(alice, -(i + 1), "USD"), (bob, i + 1, "USD")], strategy)

    from ledger.services.integrity import verify_chain

    report = verify_chain()
    assert report["ok"]
    assert report["transactions_checked"] == 6  # funding + 5


# ---------------------------------------------- concurrency under both -------


def test_no_overdraft_under_concurrency(strategy: str) -> None:
    """The core safety property, under both strategies.

    Pessimistic gets this from row locks. Optimistic gets it from SERIALIZABLE
    detecting the read-write conflict and aborting one side, plus the retry loop
    replaying it against a fresh snapshot. Neither may let more than ten 100-cent
    debits through against a 1,000-cent balance.
    """
    alice = f.make_account()
    bob = f.make_account()
    f.fund(alice, 1_000)
    attempts = 20
    start = threading.Barrier(attempts)

    def fire() -> str:
        start.wait(timeout=15)
        try:
            post([(alice, -100, "USD"), (bob, 100, "USD")], strategy)
            return "ok"
        except InsufficientFunds:
            return "refused"

    with ThreadPoolExecutor(max_workers=attempts) as pool:
        outcomes = [
            fut.result(timeout=60) for fut in [pool.submit(fire) for _ in range(attempts)]
        ]

    assert outcomes.count("ok") == 10, outcomes
    assert f.derived_balance(alice) == 0
    assert f.derived_balance(bob) == 1_000
    assert reconcile()["ok"]


def test_hot_account_stays_consistent_under_both_strategies(strategy: str) -> None:
    """The benchmark workload in miniature: every transaction credits one shared
    fee account. Whatever the interleaving, the fee account's balance must equal
    the number of transactions times the fee."""
    revenue = f.revenue_account("USD")
    payers = [f.make_account() for _ in range(4)]
    merchants = [f.make_account() for _ in range(4)]
    for payer in payers:
        f.fund(payer, 100_000)

    per_worker = 15

    def fire(index: int) -> None:
        for _ in range(per_worker):
            post(
                [
                    (payers[index], -100, "USD"),
                    (merchants[index], 97, "USD"),
                    (revenue, 3, "USD"),
                ],
                strategy,
            )

    with ThreadPoolExecutor(max_workers=4) as pool:
        for fut in [pool.submit(fire, i) for i in range(4)]:
            fut.result(timeout=120)

    assert f.derived_balance(revenue) == 4 * per_worker * 3
    for i in range(4):
        assert f.derived_balance(merchants[i]) == per_worker * 97
    assert reconcile()["ok"]


def test_opposite_direction_transfers_do_not_deadlock() -> None:
    """Deterministic lock ordering, tested by trying to provoke a cycle.

    Half the threads move money A -> B and half move it B -> A. If
    `lock_accounts` locked in the order the entries arrived rather than sorted
    order, this is the shape that deadlocks: one holds A and wants B while the
    other holds B and wants A. Because ids are sorted first, every writer takes
    them in the same sequence and no cycle can form.
    """
    a = f.make_account()
    b = f.make_account()
    f.fund(a, 50_000)
    f.fund(b, 50_000)
    db.RETRIES.reset()

    def fire(index: int) -> None:
        for _ in range(10):
            if index % 2 == 0:
                post([(a, -10, "USD"), (b, 10, "USD")], "pessimistic")
            else:
                post([(b, -10, "USD"), (a, 10, "USD")], "pessimistic")

    with ThreadPoolExecutor(max_workers=8) as pool:
        for fut in [pool.submit(fire, i) for i in range(8)]:
            fut.result(timeout=120)

    assert "deadlock" not in db.RETRIES.snapshot()
    assert f.derived_balance(a) + f.derived_balance(b) == 100_000
    assert reconcile()["ok"]


# -------------------------------------------------------- lock ordering ------


def test_the_lock_query_locks_rows_in_sorted_order() -> None:
    """Asserts the plan *shape*, not just the SQL text.

    `lock_accounts` relies on the LockRows node sitting at the *top* of the plan,
    above whatever establishes the ordering. That is what makes `ORDER BY
    b.account_id` a deadlock-prevention mechanism rather than a cosmetic detail:
    rows are locked in the order the plan emits them, and the plan emits them
    sorted.

    Two plan shapes satisfy this, and which one appears depends on table
    statistics -- the first version of this test asserted a `Sort` node and broke
    the moment the test database grew enough rows for the planner to prefer an
    ordered index scan instead. Both are correct; what must not change is
    LockRows being the root.
    """
    accounts = sorted([f.make_account(), f.make_account(), f.make_account()])

    with db.transaction(read_only=True) as cur:
        cur.execute(
            """
            EXPLAIN
            SELECT a.id, a.name, a.currency, a.type
              FROM account_balances b
              JOIN accounts a ON a.id = b.account_id
             WHERE b.account_id = ANY(%s)
             ORDER BY b.account_id
               FOR UPDATE OF b
            """,
            (accounts,),
        )
        plan = [row["QUERY PLAN"] for row in cur.fetchall()]

    rendered = "\n".join(plan)

    # LockRows is the root: nothing reorders rows after they are locked.
    assert plan[0].startswith("LockRows"), rendered
    assert not any(
        line.startswith("LockRows") for line in plan[1:]
    ), rendered

    # And the ordering really is established below it, by one of the two shapes.
    orders_by_sort = any("Sort Key: " in line for line in plan)
    orders_by_index = any(
        "Index Scan using account_balances_pkey" in line for line in plan
    )
    assert orders_by_sort or orders_by_index, rendered


# ------------------------------------------------- conflict classification ---


def test_idempotency_collisions_are_never_retried() -> None:
    """The most dangerous possible bug in the retry logic.

    23505 means "unique violation". Retrying one blindly would mean retrying an
    idempotency-key collision, which would defeat Phase 2 entirely -- the retry
    would find the key already claimed and could double-process. Only the
    hash-chain constraints are classified as retryable.
    """
    from ledger.db import conflict_kind

    assert conflict_kind(RuntimeError("not a database error")) is None

    alice = f.make_account()
    bob = f.make_account()
    f.fund(alice, 1_000)
    key = uuid4()
    post([(alice, -10, "USD"), (bob, 10, "USD")], "pessimistic", key)

    # Provoke a real collision on the idempotency key rather than a synthesised
    # exception, so the test exercises the same diag.constraint_name the
    # classifier reads in production.
    with pytest.raises(psycopg.errors.UniqueViolation) as raised:
        with db.transaction() as cur:
            cur.execute(
                "INSERT INTO idempotency_keys (key, request_hash) VALUES (%s, %s)",
                (key, b"\x00" * 32),
            )

    assert raised.value.diag.constraint_name == "idempotency_keys_pkey"
    assert conflict_kind(raised.value) is None, (
        "an idempotency-key collision was classified as retryable; retrying one "
        "would defeat exactly-once semantics"
    )


def test_chain_conflicts_are_retryable() -> None:
    from ledger.db import conflict_kind

    a = f.make_account()
    b = f.make_account()
    f.fund(a, 1_000)

    with db.transaction(read_only=True) as cur:
        cur.execute("SELECT prev_hash FROM transactions ORDER BY seq LIMIT 1")
        taken_prev = bytes(cur.fetchone()["prev_hash"])

    try:
        with db.transaction() as cur:
            tx_id = f.raw_insert_transaction(
                cur, prev_hash=taken_prev, tx_hash=b"\x7f" * 32
            )
            cur.execute(
                "INSERT INTO entries (transaction_id, account_id, amount_minor, currency)"
                " VALUES (%s, %s, %s, %s)",
                (tx_id, a, 5, "USD"),
            )
            cur.execute(
                "INSERT INTO entries (transaction_id, account_id, amount_minor, currency)"
                " VALUES (%s, %s, %s, %s)",
                (tx_id, b, -5, "USD"),
            )
    except psycopg.errors.UniqueViolation as exc:
        assert exc.diag.constraint_name == "transactions_prev_hash_key"
        assert conflict_kind(exc) == "chain_conflict"
    else:
        pytest.fail("expected a chain conflict")


def test_retries_are_counted_by_kind() -> None:
    revenue = f.revenue_account("USD")
    payers = [f.make_account() for _ in range(6)]
    for payer in payers:
        f.fund(payer, 10_000)
    db.RETRIES.reset()

    def fire(index: int) -> None:
        for _ in range(8):
            post(
                [(payers[index], -10, "USD"), (revenue, 10, "USD")],
                "optimistic",
            )

    with ThreadPoolExecutor(max_workers=6) as pool:
        for fut in [pool.submit(fire, i) for i in range(6)]:
            fut.result(timeout=120)

    snapshot = db.RETRIES.snapshot()
    # Six writers all crediting one revenue account under SERIALIZABLE: there
    # must have been conflicts, and they must be attributed.
    assert snapshot, "expected the optimistic strategy to record conflicts"
    assert set(snapshot) <= {
        "serialization_failure",
        "deadlock",
        "chain_conflict",
    }, snapshot
    assert reconcile()["ok"]


def test_retries_exhausted_becomes_a_typed_503() -> None:
    """A conflict that outlives the retry budget must surface as a typed
    LedgerError, not as a raw psycopg exception leaking out of the API.

    Provoked deterministically rather than by racing threads: an open
    transaction claims the chain slot that the next append will want, so the
    appending request is guaranteed to collide. With `max_retries = 0` it gets
    exactly one attempt.
    """
    settings = get_settings()
    original = settings.max_retries
    settings.max_retries = 0

    a = f.make_account()
    b = f.make_account()
    f.fund(a, 1_000)
    outcome: list[Any] = []

    try:
        with db.transaction() as blocker:
            blocker.execute(
                "SELECT tx_hash FROM transactions ORDER BY seq DESC LIMIT 1"
            )
            head = bytes(blocker.fetchone()["tx_hash"])

            # Take the `prev_hash = head` slot, and hold it uncommitted.
            tx_id = f.raw_insert_transaction(
                blocker, prev_hash=head, tx_hash=b"\x5a" * 32
            )
            for account, amount in ((a, 1), (b, -1)):
                blocker.execute(
                    "INSERT INTO entries (transaction_id, account_id, amount_minor,"
                    " currency) VALUES (%s, %s, %s, %s)",
                    (tx_id, account, amount, "USD"),
                )

            def contend() -> None:
                try:
                    post([(a, -1, "USD"), (b, 1, "USD")], "optimistic")
                    outcome.append("committed")
                except Exception as exc:  # noqa: BLE001
                    outcome.append(exc)

            contender = threading.Thread(target=contend)
            contender.start()
            # It reads the same head we did, tries to insert the same prev_hash,
            # and blocks on the unique index waiting for this transaction.
            contender.join(timeout=1.0)
            assert contender.is_alive(), "contender did not block on the chain slot"

        # Committing here releases it into a guaranteed conflict.
        contender.join(timeout=15)
        assert not contender.is_alive()
    finally:
        settings.max_retries = original

    assert outcome and isinstance(outcome[0], RetriesExhausted), outcome
    assert outcome[0].status == 503
    assert outcome[0].details["conflict"] in {
        "chain_conflict",
        "serialization_failure",
    }
