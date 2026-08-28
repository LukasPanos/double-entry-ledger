"""Phase 4: reconciliation and integrity.

A reconciliation suite that has only ever been run against a healthy ledger is
not evidence of anything -- it might be ten queries that can never fail. So every
check here is shown failing against a deliberately corrupted database, and the
corruption is applied with the guards switched off, through the same door a
database administrator would have.
"""

from __future__ import annotations

import threading
import time
from typing import Any
from uuid import uuid4

from fastapi.testclient import TestClient

from ledger.services import transactions as transactions_service
from ledger.services.integrity import verify_chain
from ledger.services.reconciliation import reconcile
from tests import factories as f


def check(report: dict[str, Any], name: str) -> dict[str, Any]:
    return next(c for c in report["checks"] if c["name"] == name)


def healthy_ledger() -> dict[str, Any]:
    alice = f.make_account(name="alice")
    bob = f.make_account(name="bob")
    revenue = f.revenue_account("USD")
    f.fund(alice, 50_000)
    f.post([(alice, -1_000, "USD"), (bob, 971, "USD"), (revenue, 29, "USD")])
    hold = f.make_hold(alice, 5_000).body
    f.capture(hold["id"], [(bob, 3_000)], amount_minor=3_000)
    f.post([(bob, -500, "USD"), (alice, 500, "USD")])
    f.make_hold(alice, 1_000)
    voided = f.make_hold(alice, 500).body
    f.void(voided["id"])
    # Four transactions: funding, the fee split, the capture, the refund. Holds
    # write none of their own.
    return {"alice": alice, "bob": bob, "revenue": revenue}


# ------------------------------------------------------------- happy path ----


def test_a_healthy_ledger_passes_every_check() -> None:
    healthy_ledger()
    report = reconcile()
    failed = [c["name"] for c in report["checks"] if not c["passed"]]
    assert failed == [], failed
    assert report["ok"]


def test_an_empty_ledger_passes_every_check() -> None:
    report = reconcile()
    assert report["ok"], [c for c in report["checks"] if not c["passed"]]


def test_every_check_actually_runs() -> None:
    """Guards against a check silently disappearing from the CHECKS list."""
    report = reconcile()
    names = {c["name"] for c in report["checks"]}
    assert names == {
        "global_zero_sum",
        "every_transaction_balances",
        "no_single_entry_transactions",
        "cached_balances_match_entries",
        "no_orphaned_entries",
        "no_negative_available_balances",
        "captured_holds_link_to_real_transactions",
        "non_captured_holds_have_no_transaction",
        "every_transaction_has_an_authorization",
        "every_transaction_has_an_outbox_event",
        "hash_chain_intact",
    }


# ----------------------------------------------------- each check can fail ---


def test_cache_drift_is_detected() -> None:
    state = healthy_ledger()
    f.corrupt(
        "UPDATE account_balances SET balance_minor = balance_minor + 1 WHERE account_id = %s",
        (state["alice"],),
    )

    report = reconcile()
    assert not report["ok"]
    failure = check(report, "cached_balances_match_entries")
    assert not failure["passed"]
    assert failure["failures"][0]["account_id"] == str(state["alice"])
    assert (
        failure["failures"][0]["cached_minor"]
        == failure["failures"][0]["derived_minor"] + 1
    )
    # And nothing else is confused by it.
    assert check(report, "global_zero_sum")["passed"]


def test_a_missing_cache_row_is_detected() -> None:
    """A FULL OUTER JOIN, not an inner one: an account with no cache row at all
    must fail rather than being skipped."""
    state = healthy_ledger()
    f.corrupt(
        "DELETE FROM account_balances WHERE account_id = %s", (state["bob"],)
    )

    failure = check(reconcile(), "cached_balances_match_entries")
    assert not failure["passed"]
    assert failure["failures"][0]["cached_minor"] is None


def test_stale_entry_count_is_detected() -> None:
    state = healthy_ledger()
    f.corrupt(
        "UPDATE account_balances SET entry_count = entry_count + 5 WHERE account_id = %s",
        (state["bob"],),
    )
    assert not check(reconcile(), "cached_balances_match_entries")["passed"]


def test_money_created_out_of_nothing_is_detected() -> None:
    """The headline check. An entry with no counterpart is money appearing."""
    state = healthy_ledger()
    with_tx = None
    import ledger.db as db_module

    with db_module.transaction(read_only=True) as cur:
        cur.execute("SELECT id FROM transactions ORDER BY seq LIMIT 1")
        with_tx = cur.fetchone()["id"]

    f.corrupt(
        """
        INSERT INTO entries (transaction_id, account_id, amount_minor, currency)
        VALUES (%s, %s, %s, %s)
        """,
        (with_tx, state["bob"], 1_000_000, "USD"),
    )

    report = reconcile()
    assert not report["ok"]
    zero_sum = check(report, "global_zero_sum")
    assert not zero_sum["passed"]
    assert zero_sum["failures"][0]["currency"] == "USD"
    assert zero_sum["failures"][0]["total_minor"] == 1_000_000
    # The transaction it was smuggled into no longer balances either.
    assert not check(report, "every_transaction_balances")["passed"]


def test_an_unbalanced_transaction_is_detected() -> None:
    state = healthy_ledger()
    f.corrupt(
        """
        UPDATE entries SET amount_minor = amount_minor - 7
         WHERE id = (SELECT min(id) FROM entries WHERE account_id = %s)
        """,
        (state["bob"],),
    )
    report = reconcile()
    assert not check(report, "every_transaction_balances")["passed"]
    assert not check(report, "global_zero_sum")["passed"]


def test_an_orphaned_entry_is_detected() -> None:
    state = healthy_ledger()
    f.corrupt(
        """
        INSERT INTO entries (transaction_id, account_id, amount_minor, currency)
        VALUES (%s, %s, %s, %s)
        """,
        (uuid4(), state["bob"], 10, "USD"),
    )
    assert not check(reconcile(), "no_orphaned_entries")["passed"]


def test_a_single_entry_transaction_is_detected() -> None:
    state = healthy_ledger()
    f.corrupt(
        """
        INSERT INTO transactions
            (id, idempotency_key, description, created_at, prev_hash, tx_hash)
        VALUES (%s, %s, 'forged', now(), %s, %s)
        """,
        (uuid4(), uuid4(), b"\x33" * 32, b"\x44" * 32),
    )
    report = reconcile()
    assert not check(report, "no_single_entry_transactions")["passed"]
    # It also has no authorization record and breaks the chain.
    assert not check(report, "every_transaction_has_an_authorization")["passed"]


def test_a_negative_available_balance_is_detected() -> None:
    alice = f.make_account()
    bob = f.make_account()
    f.fund(alice, 1_000)
    # Take more out of alice than she has, bypassing the overdraft check.
    tx = f.post([(alice, -100, "USD"), (bob, 100, "USD")])
    f.corrupt(
        """
        UPDATE entries SET amount_minor = -5_000
         WHERE transaction_id = %s AND account_id = %s
        """,
        (tx["id"], alice),
    )

    failure = check(reconcile(), "no_negative_available_balances")
    assert not failure["passed"]
    assert failure["failures"][0]["account_id"] == str(alice)
    assert failure["failures"][0]["available_minor"] < 0


def test_a_hold_over_reserving_is_detected() -> None:
    alice = f.make_account()
    f.fund(alice, 1_000)
    f.make_hold(alice, 900)
    # A second hold that should never have been permitted.
    f.corrupt(
        """
        INSERT INTO holds (id, account_id, amount_minor, currency, expires_at)
        VALUES (%s, %s, %s, %s, now() + interval '1 hour')
        """,
        (uuid4(), alice, 900, "USD"),
    )
    failure = check(reconcile(), "no_negative_available_balances")
    assert not failure["passed"]
    assert failure["failures"][0]["held_minor"] == 1_800


def test_a_captured_hold_with_a_bogus_transaction_is_detected() -> None:
    alice = f.make_account()
    bob = f.make_account()
    f.fund(alice, 10_000)
    unrelated = f.post([(alice, -5, "USD"), (bob, 5, "USD")])
    hold = f.make_hold(alice, 1_000).body

    # Point the hold at a transaction that captured 5, not 1000... which is
    # within range, so instead point it at one that never touched alice at all.
    other = f.make_account()
    third = f.make_account()
    f.fund(other, 100)
    elsewhere = f.post([(other, -50, "USD"), (third, 50, "USD")])
    f.corrupt(
        """
        UPDATE holds SET status = 'captured', captured_transaction_id = %s
         WHERE id = %s
        """,
        (elsewhere["id"], hold["id"]),
    )

    failure = check(reconcile(), "captured_holds_link_to_real_transactions")
    assert not failure["passed"]
    assert failure["failures"][0]["captured_minor"] == 0


def test_a_capture_exceeding_its_authorization_is_detected() -> None:
    alice = f.make_account()
    merchant = f.make_account()
    f.fund(alice, 10_000)
    hold = f.make_hold(alice, 1_000).body
    f.capture(hold["id"], [(merchant, 1_000)])

    # Inflate the captured amount past what was authorized.
    f.corrupt(
        """
        UPDATE entries SET amount_minor = -9_000
         WHERE transaction_id = (
                SELECT captured_transaction_id FROM holds WHERE id = %s
               )
           AND account_id = %s
        """,
        (hold["id"], alice),
    )
    failure = check(reconcile(), "captured_holds_link_to_real_transactions")
    assert not failure["passed"]
    assert failure["failures"][0]["captured_minor"] == 9_000
    assert failure["failures"][0]["authorized_minor"] == 1_000


def test_a_voided_hold_cannot_be_given_a_transaction_link_at_all() -> None:
    """This one cannot be forged, and that is the finding.

    `session_replication_role = 'replica'` suppresses triggers -- including
    foreign key and the deferred zero-sum triggers -- but a CHECK constraint is
    not a trigger and still applies. So `holds_capture_link` refuses the write
    even with every guard the harness can switch off already switched off.

    That makes `non_captured_holds_have_no_transaction` a check that should never
    be able to fire. It is kept anyway: it costs one indexed scan, and if a
    future migration ever relaxes the CHECK, the reconciliation report is where
    that regression should show up.
    """
    import psycopg
    import pytest as _pytest

    alice = f.make_account()
    bob = f.make_account()
    f.fund(alice, 10_000)
    tx = f.post([(alice, -5, "USD"), (bob, 5, "USD")])
    hold = f.make_hold(alice, 1_000).body
    f.void(hold["id"])

    with _pytest.raises(psycopg.errors.CheckViolation) as raised:
        f.corrupt(
            "UPDATE holds SET captured_transaction_id = %s WHERE id = %s",
            (tx["id"], hold["id"]),
        )
    assert "holds_capture_link" in str(raised.value)

    # The ledger is untouched, so the check still passes.
    assert check(reconcile(), "non_captured_holds_have_no_transaction")["passed"]


def test_a_transaction_without_an_authorization_is_detected() -> None:
    healthy_ledger()
    f.corrupt(
        """
        DELETE FROM idempotency_keys
         WHERE key = (SELECT idempotency_key FROM transactions ORDER BY seq LIMIT 1)
        """
    )
    assert not check(reconcile(), "every_transaction_has_an_authorization")["passed"]


# ------------------------------------------------------------- integrity -----


def test_the_chain_verifies_on_a_healthy_ledger() -> None:
    healthy_ledger()
    report = verify_chain()
    assert report["ok"]
    assert report["first_break"] is None
    assert report["transactions_checked"] > 0
    assert len(report["head_hash"]) == 64


def test_editing_a_hashed_field_breaks_the_chain() -> None:
    """`created_at` is hashed but affects no balance, so this isolates the chain
    check: every other reconciliation check still passes, and only the hash
    fails. That is exactly the attack tamper evidence exists to catch -- a change
    that leaves the books adding up."""
    healthy_ledger()
    import ledger.db as db_module

    with db_module.transaction(read_only=True) as cur:
        cur.execute("SELECT id, seq FROM transactions ORDER BY seq OFFSET 1 LIMIT 1")
        target = cur.fetchone()

    f.corrupt(
        "UPDATE transactions SET created_at = created_at + interval '1 day' WHERE id = %s",
        (target["id"],),
    )

    report = verify_chain()
    assert not report["ok"]
    assert report["first_break"]["reason"] == "hash_mismatch"
    assert report["first_break"]["seq"] == target["seq"]
    assert report["first_break"]["transaction_id"] == str(target["id"])

    # Every other check still passes: the books balance, only the evidence
    # disagrees.
    recon = reconcile()
    assert [c["name"] for c in recon["checks"] if not c["passed"]] == [
        "hash_chain_intact"
    ]


def test_editing_an_amount_breaks_the_chain() -> None:
    healthy_ledger()
    f.corrupt("UPDATE entries SET amount_minor = amount_minor + 1 WHERE id = 1")

    report = verify_chain()
    assert not report["ok"]
    assert report["first_break"]["reason"] == "hash_mismatch"
    assert report["first_break"]["seq"] == 1


def test_a_relinked_chain_is_detected() -> None:
    healthy_ledger()
    f.corrupt(
        """
        UPDATE transactions SET prev_hash = %s
         WHERE seq = (SELECT max(seq) FROM transactions)
        """,
        (b"\x99" * 32,),
    )
    report = verify_chain()
    assert report["first_break"]["reason"] == "chain_break"


def test_the_first_break_is_reported_not_the_last() -> None:
    healthy_ledger()
    import ledger.db as db_module

    with db_module.transaction(read_only=True) as cur:
        cur.execute("SELECT seq, id FROM transactions ORDER BY seq")
        rows = cur.fetchall()
    assert len(rows) >= 4

    earlier, later = rows[1], rows[3]
    for row in (later, earlier):
        f.corrupt(
            "UPDATE transactions SET created_at = created_at + interval '1 day'"
            " WHERE id = %s",
            (row["id"],),
        )

    report = verify_chain()
    assert report["first_break"]["seq"] == earlier["seq"]


def test_deleting_the_genesis_transaction_is_detected() -> None:
    healthy_ledger()
    f.corrupt(
        "DELETE FROM entries WHERE transaction_id ="
        " (SELECT id FROM transactions ORDER BY seq LIMIT 1)"
    )
    f.corrupt("DELETE FROM transactions WHERE seq = (SELECT min(seq) FROM transactions)")

    report = verify_chain()
    assert not report["ok"]
    assert report["first_break"]["reason"] == "genesis_mismatch"


# ---------------------------------------------------- snapshot consistency ---


def test_reconciliation_does_not_cry_wolf_during_concurrent_writes() -> None:
    """The whole report runs in one REPEATABLE READ snapshot.

    Without that, a transaction committing between the global-sum check and the
    per-account check would make the two disagree and the report would fail on a
    perfectly healthy ledger. Here a writer hammers the ledger while
    reconciliation runs repeatedly; every report must pass.
    """
    alice = f.make_account()
    bob = f.make_account()
    revenue = f.revenue_account("USD")
    f.fund(alice, 500_000)

    stop = threading.Event()
    writer_error: list[BaseException] = []

    def writer() -> None:
        try:
            while not stop.is_set():
                transactions_service.post_transaction(
                    f.transaction_request(
                        [
                            (alice, -100, "USD"),
                            (bob, 97, "USD"),
                            (revenue, 3, "USD"),
                        ]
                    ),
                    uuid4(),
                )
        except BaseException as exc:  # noqa: BLE001
            writer_error.append(exc)

    thread = threading.Thread(target=writer, daemon=True)
    thread.start()
    try:
        reports = []
        deadline = time.monotonic() + 3.0
        while time.monotonic() < deadline:
            reports.append(reconcile())
        assert len(reports) >= 5
        for report in reports:
            failed = [c["name"] for c in report["checks"] if not c["passed"]]
            assert failed == [], failed
    finally:
        stop.set()
        thread.join(timeout=15)

    assert not writer_error, writer_error
    # And the writer really was writing.
    assert f.derived_balance(revenue) > 0


# --------------------------------------------------------------- http layer --


def test_reconciliation_endpoint(client: TestClient) -> None:
    healthy_ledger()
    response = client.get("/reconciliation")
    assert response.status_code == 200
    from ledger.services.reconciliation import CHECKS

    body = response.json()
    assert body["ok"] is True
    # The endpoint must report every check the module defines, not a subset.
    assert [c["name"] for c in body["checks"]] == [c.__name__ for c in CHECKS]
    assert all(c["passed"] for c in body["checks"])


def test_reconciliation_endpoint_reports_failure_with_200(client: TestClient) -> None:
    """The request succeeded; `ok` carries the answer. A 500 would mean the check
    itself broke, which is a different problem from the ledger being wrong."""
    state = healthy_ledger()
    f.corrupt(
        "UPDATE account_balances SET balance_minor = 1 WHERE account_id = %s",
        (state["alice"],),
    )
    response = client.get("/reconciliation")
    assert response.status_code == 200
    assert response.json()["ok"] is False


def test_integrity_endpoint(client: TestClient) -> None:
    healthy_ledger()
    response = client.get("/integrity")
    assert response.status_code == 200
    body = response.json()
    assert body["ok"] is True
    assert body["first_break"] is None
    assert body["transactions_checked"] >= 4
