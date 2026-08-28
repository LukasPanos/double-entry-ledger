"""Phase 6: the transactional outbox and webhook delivery.

The claim, in two halves:

  1. **Atomicity.** An event exists if and only if the ledger write committed.
     Not "almost always" -- there is no interleaving in which one exists without
     the other, because they are the same transaction.
  2. **Exactly-once, jointly.** The relay guarantees at-least-once. The receiver
     completes it by discarding event ids it has already processed. The test
     below asserts duplicates genuinely occurred, so the dedup is doing work
     rather than passing vacuously.
"""

from __future__ import annotations

import threading
from typing import Any
from uuid import uuid4

import httpx
import psycopg
import pytest

from ledger import db
from ledger.config import get_settings
from ledger.services import outbox
from ledger.services.reconciliation import reconcile
from scripts.receiver import ReceiverServer
from tests import factories as f


@pytest.fixture(autouse=True)
def fast_backoff() -> Any:
    """Real backoff is seconds; tests cannot wait for that.

    Only the delays change, not the retry *logic* -- the same code path decides
    when to retry and when to dead-letter.
    """
    settings = get_settings()
    saved = (
        settings.outbox_backoff_base_seconds,
        settings.outbox_backoff_cap_seconds,
        settings.outbox_max_attempts,
        settings.outbox_http_timeout_seconds,
        settings.outbox_batch_size,
        settings.webhook_secret,
    )
    settings.outbox_backoff_base_seconds = 0.001
    settings.outbox_backoff_cap_seconds = 0.01
    settings.outbox_max_attempts = 25
    settings.outbox_http_timeout_seconds = 2.0
    settings.outbox_batch_size = 8
    yield
    (
        settings.outbox_backoff_base_seconds,
        settings.outbox_backoff_cap_seconds,
        settings.outbox_max_attempts,
        settings.outbox_http_timeout_seconds,
        settings.outbox_batch_size,
        settings.webhook_secret,
    ) = saved


def outbox_rows(event_type: str | None = None) -> list[dict[str, Any]]:
    with db.transaction(read_only=True) as cur:
        cur.execute(
            """
            SELECT id, event_type, payload, status::text AS status, attempts,
                   delivered_at
              FROM outbox
             WHERE (%s::text IS NULL OR event_type = %s::text)
             ORDER BY id
            """,
            (event_type, event_type),
        )
        return cur.fetchall()


# ----------------------------------------------------------- atomicity -------


def test_an_event_is_written_with_the_ledger_entries() -> None:
    alice = f.make_account()
    bob = f.make_account()
    f.fund(alice, 10_000)
    before = len(outbox_rows("transaction.posted"))

    tx = f.post([(alice, -250, "USD"), (bob, 250, "USD")], description="a payment")

    events = outbox_rows("transaction.posted")
    assert len(events) == before + 1
    event = events[-1]
    assert event["status"] == "pending"
    assert event["attempts"] == 0
    assert event["delivered_at"] is None
    assert event["payload"]["transaction_id"] == tx["id"]
    assert event["payload"]["description"] == "a payment"
    assert event["payload"]["tx_hash"] == tx["tx_hash"]
    # The event carries the full entry set, so a consumer never has to call back.
    assert sorted(e["amount_minor"] for e in event["payload"]["entries"]) == [-250, 250]


def test_a_rolled_back_write_leaves_no_event() -> None:
    """The half of atomicity that matters most: a failed transaction must not
    announce itself. Rolling back takes the event with it because it is the same
    transaction."""
    alice = f.make_account()
    bob = f.make_account()
    f.fund(alice, 100)
    before = len(outbox_rows())

    from ledger.errors import InsufficientFunds

    with pytest.raises(InsufficientFunds):
        f.post([(alice, -100_000, "USD"), (bob, 100_000, "USD")])

    assert len(outbox_rows()) == before


def test_a_failure_after_the_emit_still_leaves_no_event() -> None:
    """Stronger version: blow up *after* the outbox row was inserted, and prove
    it does not survive the rollback."""
    alice = f.make_account()
    bob = f.make_account()
    f.fund(alice, 10_000)
    before = len(outbox_rows())

    from ledger.services.idempotency import execute_once
    from ledger.services.posting import Posting, append_transaction

    key = uuid4()

    def work(cur: Any) -> dict[str, Any]:
        append_transaction(
            cur,
            description="doomed",
            # Must be the key `execute_once` just claimed: transactions has a
            # foreign key into idempotency_keys.
            idempotency_key=key,
            postings=[
                Posting(alice, -10, "USD"),
                Posting(bob, 10, "USD"),
            ],
        )
        cur.execute("SELECT count(*) AS n FROM outbox")
        assert cur.fetchone()["n"] == before + 1  # visible in our own snapshot
        raise RuntimeError("crash after emit")

    with pytest.raises(RuntimeError):
        execute_once(
            key=key,
            fingerprint={"op": "probe"},
            status_code=201,
            work=work,
        )

    assert len(outbox_rows()) == before


def test_holds_emit_events_too() -> None:
    alice = f.make_account()
    merchant = f.make_account()
    f.fund(alice, 10_000)

    hold = f.make_hold(alice, 1_000).body
    f.capture(hold["id"], [(merchant, 600)], amount_minor=600)
    second = f.make_hold(alice, 500).body
    f.void(second["id"])
    third = f.make_hold(alice, 200).body
    f.expire_hold_now(third["id"])
    from ledger.services.holds import sweep_expired_holds

    assert sweep_expired_holds() == 1

    types = [row["event_type"] for row in outbox_rows()]
    assert types.count("hold.created") == 3
    assert types.count("hold.captured") == 1
    assert types.count("hold.voided") == 1
    assert types.count("hold.expired") == 1


def test_the_expiry_sweep_emits_inside_its_own_transaction() -> None:
    """A background job is not exempt from the dual-write problem: the sweep's
    events commit with the sweep."""
    alice = f.make_account()
    f.fund(alice, 10_000)
    holds = [f.make_hold(alice, 100).body for _ in range(4)]
    for hold in holds:
        f.expire_hold_now(hold["id"])

    from ledger.services.holds import sweep_expired_holds

    assert sweep_expired_holds() == 4
    expired_events = outbox_rows("hold.expired")
    assert len(expired_events) == 4
    assert {e["payload"]["id"] for e in expired_events} == {h["id"] for h in holds}


def test_reconciliation_notices_a_transaction_with_no_event() -> None:
    """The check that would catch someone refactoring the emit out of
    `append_transaction` into its own transaction."""
    alice = f.make_account()
    bob = f.make_account()
    f.fund(alice, 10_000)
    tx = f.post([(alice, -10, "USD"), (bob, 10, "USD")])

    f.corrupt(
        "DELETE FROM outbox WHERE payload ->> 'transaction_id' = %s", (tx["id"],)
    )

    report = reconcile()
    failed = [c for c in report["checks"] if not c["passed"]]
    assert [c["name"] for c in failed] == ["every_transaction_has_an_outbox_event"]
    assert failed[0]["failures"][0]["transaction_id"] == tx["id"]


# ----------------------------------------------------------- delivery --------


def test_delivery_against_a_reliable_receiver() -> None:
    alice = f.make_account()
    bob = f.make_account()
    f.fund(alice, 100_000)
    for i in range(10):
        f.post([(alice, -(i + 1), "USD"), (bob, i + 1, "USD")])

    expected = {str(row["id"]) for row in outbox_rows()}

    with ReceiverServer(fail_rate=0.0, seed=1) as receiver:
        stats = outbox.drain(receiver.url)
        snapshot = receiver.snapshot()

    assert outbox.pending_count() == 0
    assert stats.dead == 0
    assert snapshot["duplicates"] == 0
    assert set(snapshot["event_ids"]) == expected
    assert all(row["status"] == "delivered" for row in outbox_rows())
    assert all(row["attempts"] == 1 for row in outbox_rows())


def test_every_committed_transaction_is_delivered_exactly_once() -> None:
    """The headline test for this phase.

    The receiver fails 30% of requests *after* recording the event, so every
    injected failure produces a redelivery. The relay is at-least-once; the
    receiver's dedup set is what makes the joint result exactly-once.

    The assertions that matter are the last three: duplicates must have actually
    happened (otherwise dedup is untested), the unique set must match the outbox
    exactly, and the request count must exceed the event count (proving retries
    occurred rather than the failure injection silently doing nothing).
    """
    alice = f.make_account()
    bob = f.make_account()
    revenue = f.revenue_account("USD")
    f.fund(alice, 1_000_000)

    transaction_ids = []
    for i in range(40):
        tx = f.post(
            [
                (alice, -100, "USD"),
                (bob, 97, "USD"),
                (revenue, 3, "USD"),
            ],
            description=f"payment {i}",
        )
        transaction_ids.append(tx["id"])

    all_events = outbox_rows()
    expected_ids = {str(row["id"]) for row in all_events}
    assert len(expected_ids) == 41  # 40 payments + the funding transaction

    with ReceiverServer(fail_rate=0.3, seed=7) as receiver:
        stats = outbox.drain(receiver.url, max_passes=500)
        snapshot = receiver.snapshot()

    # Nothing left behind, nothing dead-lettered.
    assert outbox.pending_count() == 0, outbox.stats()
    assert stats.dead == 0
    assert all(row["status"] == "delivered" for row in outbox_rows())

    # Exactly once, from the consumer's point of view.
    assert set(snapshot["event_ids"]) == expected_ids
    assert snapshot["unique_events"] == 41

    # And the guarantee was genuinely exercised rather than trivially satisfied.
    assert snapshot["failures_injected"] > 0, "failure injection did nothing"
    assert snapshot["duplicates"] > 0, "no duplicate was ever delivered"
    assert snapshot["request_count"] > snapshot["unique_events"]
    assert snapshot["max_attempts_for_one_event"] > 1

    # Every transaction is represented.
    delivered_transaction_ids = {
        row["payload"]["transaction_id"]
        for row in outbox_rows("transaction.posted")
    }
    assert set(transaction_ids) <= delivered_transaction_ids
    assert reconcile()["ok"]


def test_attempts_are_counted_and_backoff_grows() -> None:
    alice = f.make_account()
    f.fund(alice, 1_000)

    assert outbox.backoff_seconds(1) == pytest.approx(0.001)
    assert outbox.backoff_seconds(2) == pytest.approx(0.002)
    assert outbox.backoff_seconds(3) == pytest.approx(0.004)
    # Capped.
    assert outbox.backoff_seconds(50) == pytest.approx(0.01)


def test_events_are_dead_lettered_after_max_attempts() -> None:
    settings = get_settings()
    settings.outbox_max_attempts = 3

    alice = f.make_account()
    f.fund(alice, 1_000)
    assert len(outbox_rows()) == 1

    with ReceiverServer(fail_rate=1.0, seed=3) as receiver:
        stats = outbox.drain(receiver.url, max_passes=50)

    rows = outbox_rows()
    assert [row["status"] for row in rows] == ["dead"]
    assert rows[0]["attempts"] == 3
    assert stats.dead == 1
    assert outbox.pending_count() == 0

    # A dead event is not retried again.
    with ReceiverServer(fail_rate=0.0, seed=4) as receiver:
        again = outbox.relay_once(receiver.url)
    assert again.claimed == 0
    assert outbox_rows()[0]["status"] == "dead"


def test_an_unreachable_endpoint_retries_then_dead_letters() -> None:
    """No server at all, as opposed to a server returning 500."""
    settings = get_settings()
    settings.outbox_max_attempts = 3
    settings.outbox_http_timeout_seconds = 0.2

    alice = f.make_account()
    f.fund(alice, 1_000)

    # Port 1 on loopback: connection refused, fast.
    stats = outbox.drain("http://127.0.0.1:1/webhook", max_passes=50)

    assert stats.dead == 1
    assert outbox_rows()[0]["status"] == "dead"
    assert any("Connect" in e or "connect" in e for e in stats.errors), stats.errors
    # The ledger is entirely unaffected by the endpoint being down.
    assert reconcile()["ok"]


def test_the_ledger_is_unaffected_by_delivery_failure() -> None:
    alice = f.make_account()
    bob = f.make_account()
    f.fund(alice, 10_000)
    f.post([(alice, -250, "USD"), (bob, 250, "USD")])

    get_settings().outbox_max_attempts = 2
    outbox.drain("http://127.0.0.1:1/webhook", max_passes=20)

    assert f.derived_balance(alice) == 9_750
    assert f.derived_balance(bob) == 250
    report = reconcile()
    assert report["ok"], [c for c in report["checks"] if not c["passed"]]


# ------------------------------------------------------------- ordering ------


def test_events_are_claimed_in_id_order() -> None:
    alice = f.make_account()
    bob = f.make_account()
    f.fund(alice, 100_000)
    for i in range(12):
        f.post([(alice, -(i + 1), "USD"), (bob, i + 1, "USD")])

    get_settings().outbox_batch_size = 5
    claimed = outbox.claim_due(5, 30.0)
    assert [row["id"] for row in claimed] == sorted(row["id"] for row in claimed)
    assert len(claimed) == 5
    # And the batch is the oldest five, not an arbitrary five.
    all_ids = [row["id"] for row in outbox_rows()]
    assert [row["id"] for row in claimed] == all_ids[:5]


def test_a_claim_is_a_lease_that_expires() -> None:
    """The reason redelivery can happen at all, and therefore the reason the
    receiver must dedup.

    Claiming pushes `next_attempt_at` forward and commits. If the relay dies
    before recording an outcome, the event stays `pending` and becomes due again
    once the lease lapses -- so it is delivered a second time rather than lost.
    """
    alice = f.make_account()
    f.fund(alice, 1_000)

    claimed = outbox.claim_due(10, lease_seconds=30.0)
    assert len(claimed) == 1
    event_id = claimed[0]["id"]

    # Simulate the relay dying here: no mark_delivered, no mark_failed.
    row = outbox_rows()[0]
    assert row["status"] == "pending"
    assert row["attempts"] == 1

    # Still leased, so a second relay pass sees nothing.
    assert outbox.claim_due(10, 30.0) == []

    # Once the lease lapses it becomes claimable again.
    f.corrupt(
        "UPDATE outbox SET next_attempt_at = now() - interval '1 second' WHERE id = %s",
        (event_id,),
    )
    reclaimed = outbox.claim_due(10, 30.0)
    assert [r["id"] for r in reclaimed] == [event_id]
    assert reclaimed[0]["attempts"] == 2


def test_two_relays_do_not_deliver_the_same_event() -> None:
    """FOR UPDATE SKIP LOCKED partitions the work instead of duplicating it."""
    alice = f.make_account()
    bob = f.make_account()
    f.fund(alice, 100_000)
    for i in range(20):
        f.post([(alice, -(i + 1), "USD"), (bob, i + 1, "USD")])

    results: list[list[int]] = []
    lock = threading.Lock()
    start = threading.Barrier(4)

    def relay() -> None:
        start.wait(timeout=10)
        claimed = outbox.claim_due(30, 30.0)
        with lock:
            results.append([row["id"] for row in claimed])

    threads = [threading.Thread(target=relay) for _ in range(4)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=20)

    claimed_ids = [event_id for batch in results for event_id in batch]
    assert len(claimed_ids) == len(set(claimed_ids)), "an event was claimed twice"


# ------------------------------------------------------------ signature ------


def test_events_are_hmac_signed_when_a_secret_is_configured() -> None:
    settings = get_settings()
    settings.webhook_secret = "hunter2"

    alice = f.make_account()
    f.fund(alice, 1_000)

    with ReceiverServer(fail_rate=0.0, secret="hunter2", seed=1) as receiver:
        outbox.drain(receiver.url)
        snapshot = receiver.snapshot()

    assert snapshot["unique_events"] == 1
    assert snapshot["rejected_signatures"] == 0


def test_a_receiver_expecting_a_signature_rejects_unsigned_events() -> None:
    settings = get_settings()
    settings.webhook_secret = None  # relay sends no signature
    settings.outbox_max_attempts = 2

    alice = f.make_account()
    f.fund(alice, 1_000)

    with ReceiverServer(fail_rate=0.0, secret="hunter2", seed=1) as receiver:
        outbox.drain(receiver.url, max_passes=20)
        snapshot = receiver.snapshot()

    assert snapshot["unique_events"] == 0
    assert snapshot["rejected_signatures"] >= 1
    assert outbox_rows()[0]["status"] == "dead"


def test_a_wrong_secret_is_rejected() -> None:
    settings = get_settings()
    settings.webhook_secret = "wrong"
    settings.outbox_max_attempts = 2

    alice = f.make_account()
    f.fund(alice, 1_000)

    with ReceiverServer(fail_rate=0.0, secret="hunter2", seed=1) as receiver:
        outbox.drain(receiver.url, max_passes=20)
        snapshot = receiver.snapshot()

    assert snapshot["rejected_signatures"] >= 1
    assert snapshot["unique_events"] == 0


def test_the_signature_covers_the_exact_bytes_sent() -> None:
    body = b'{"a":1}'
    assert outbox.sign(body, "s") == outbox.sign(b'{"a":1}', "s")
    assert outbox.sign(body, "s") != outbox.sign(b'{"a": 1}', "s")
    assert outbox.sign(body, "s") != outbox.sign(body, "t")
    assert outbox.sign(body, "s").startswith("sha256=")


# ------------------------------------------------------------- schema --------


def test_delivered_status_requires_a_timestamp() -> None:
    alice = f.make_account()
    f.fund(alice, 1_000)
    event_id = outbox_rows()[0]["id"]

    with pytest.raises(psycopg.errors.CheckViolation) as exc:
        f.corrupt(
            "UPDATE outbox SET status = 'delivered' WHERE id = %s", (event_id,)
        )
    assert "outbox_delivered_at_matches_status" in str(exc.value)


def test_a_pending_event_cannot_carry_a_delivery_timestamp() -> None:
    alice = f.make_account()
    f.fund(alice, 1_000)
    event_id = outbox_rows()[0]["id"]

    with pytest.raises(psycopg.errors.CheckViolation):
        f.corrupt(
            "UPDATE outbox SET delivered_at = now() WHERE id = %s", (event_id,)
        )


# ----------------------------------------------------------------- stats -----


def test_outbox_stats_endpoint(client: Any) -> None:
    alice = client.post("/accounts", json={"name": "a", "currency": "USD"}).json()["id"]
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
                {"account_id": settlement, "amount_minor": -100, "currency": "USD"},
                {"account_id": alice, "amount_minor": 100, "currency": "USD"},
            ],
        },
    )

    stats = client.get("/outbox/stats").json()
    assert stats["pending"] == 1
    assert stats["delivered"] == 0
    assert stats["dead"] == 0
    assert stats["oldest_pending_age_seconds"] is not None

    with ReceiverServer(fail_rate=0.0, seed=1) as receiver:
        outbox.drain(receiver.url)

    stats = client.get("/outbox/stats").json()
    assert stats["pending"] == 0
    assert stats["delivered"] == 1
    assert stats["oldest_pending_age_seconds"] is None


def test_the_receiver_reports_its_own_state_over_http() -> None:
    """The receiver is a standalone program; a test that only used it in-process
    would not notice it being broken as one."""
    alice = f.make_account()
    f.fund(alice, 1_000)

    with ReceiverServer(fail_rate=0.0, seed=1) as receiver:
        outbox.drain(receiver.url)
        response = httpx.get(f"http://127.0.0.1:{receiver.port}/events", timeout=5)

    assert response.status_code == 200
    assert response.json()["unique_events"] == 1
    assert response.json()["by_type"] == {"transaction.posted": 1}
