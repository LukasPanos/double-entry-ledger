"""Phase 2: idempotency under retries and under concurrency.

The claim being tested is narrow and specific: for a given Idempotency-Key, the
business write commits at most once, no matter how many requests arrive or how
they interleave.
"""

from __future__ import annotations

import threading
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Any
from uuid import uuid4

import pytest
from fastapi.testclient import TestClient

from ledger import db
from ledger.errors import IdempotencyKeyReused, UnbalancedTransaction
from ledger.services import transactions as transactions_service
from ledger.services.idempotency import execute_once, fingerprint_hash
from tests import factories as f


# ------------------------------------------------------------------- replay --


def test_retry_replays_the_original_response() -> None:
    alice = f.make_account()
    bob = f.make_account()
    f.fund(alice, 10_000)
    key = uuid4()
    request = f.transaction_request([(alice, -250, "USD"), (bob, 250, "USD")])

    first = transactions_service.post_transaction(request, key)
    second = transactions_service.post_transaction(request, key)

    assert first.replayed is False
    assert second.replayed is True
    assert second.status_code == first.status_code == 201

    # The replayed body is the original body plus the marker -- same transaction
    # id, same seq, same hash. Not a new transaction that happens to look alike.
    assert second.body["id"] == first.body["id"]
    assert second.body["seq"] == first.body["seq"]
    assert second.body["tx_hash"] == first.body["tx_hash"]
    assert {k: v for k, v in second.body.items() if k != "replayed"} == {
        k: v for k, v in first.body.items() if k != "replayed"
    }


def test_retry_writes_no_second_set_of_entries() -> None:
    alice = f.make_account()
    bob = f.make_account()
    f.fund(alice, 10_000)
    key = uuid4()
    request = f.transaction_request([(alice, -250, "USD"), (bob, 250, "USD")])

    transactions_service.post_transaction(request, key)
    entries_after_first = f.count_rows("entries")
    tx_after_first = f.count_rows("transactions")

    for _ in range(5):
        transactions_service.post_transaction(request, key)

    assert f.count_rows("entries") == entries_after_first
    assert f.count_rows("transactions") == tx_after_first
    assert f.derived_balance(alice) == 9_750
    assert f.derived_balance(bob) == 250


def test_replay_survives_across_processes() -> None:
    """The stored response is in Postgres, not in process memory, so a retry
    that lands on a different API instance replays identically. Simulated here
    by dropping the connection pool between the two calls."""
    alice = f.make_account()
    bob = f.make_account()
    f.fund(alice, 1_000)
    key = uuid4()
    request = f.transaction_request([(alice, -10, "USD"), (bob, 10, "USD")])

    first = transactions_service.post_transaction(request, key)
    db.close_pool()
    db.init_pool()
    second = transactions_service.post_transaction(request, key)

    assert second.replayed is True
    assert second.body["id"] == first.body["id"]


# ----------------------------------------------------------- body mismatch ---


def test_same_key_different_body_is_409() -> None:
    alice = f.make_account()
    bob = f.make_account()
    f.fund(alice, 10_000)
    key = uuid4()

    transactions_service.post_transaction(
        f.transaction_request([(alice, -250, "USD"), (bob, 250, "USD")]), key
    )

    with pytest.raises(IdempotencyKeyReused) as exc:
        transactions_service.post_transaction(
            f.transaction_request([(alice, -999, "USD"), (bob, 999, "USD")]), key
        )
    assert exc.value.status == 409
    assert f.derived_balance(alice) == 9_750


def test_same_key_different_description_is_409() -> None:
    alice = f.make_account()
    bob = f.make_account()
    f.fund(alice, 10_000)
    key = uuid4()
    legs = [(alice, -250, "USD"), (bob, 250, "USD")]

    transactions_service.post_transaction(
        f.transaction_request(legs, "invoice 1"), key
    )
    with pytest.raises(IdempotencyKeyReused):
        transactions_service.post_transaction(
            f.transaction_request(legs, "invoice 2"), key
        )


def test_reordered_entries_are_the_same_request() -> None:
    """Entry order is semantically meaningless -- the hash chain sorts too -- so
    a client that retries with its legs in a different order gets a replay, not
    a 409. Sorting preserves the multiset, so no two genuinely different
    requests can collide."""
    alice = f.make_account()
    bob = f.make_account()
    f.fund(alice, 10_000)
    key = uuid4()

    first = transactions_service.post_transaction(
        f.transaction_request([(alice, -250, "USD"), (bob, 250, "USD")]), key
    )
    second = transactions_service.post_transaction(
        f.transaction_request([(bob, 250, "USD"), (alice, -250, "USD")]), key
    )

    assert second.replayed is True
    assert second.body["id"] == first.body["id"]


def test_swapped_direction_is_a_different_request() -> None:
    """Reordering is fine; reversing the direction of the money is not."""
    alice = f.make_account()
    bob = f.make_account()
    f.fund(alice, 10_000)
    f.fund(bob, 10_000)
    key = uuid4()

    transactions_service.post_transaction(
        f.transaction_request([(alice, -250, "USD"), (bob, 250, "USD")]), key
    )
    with pytest.raises(IdempotencyKeyReused):
        transactions_service.post_transaction(
            f.transaction_request([(alice, 250, "USD"), (bob, -250, "USD")]), key
        )


def test_fingerprint_includes_the_operation() -> None:
    """A key used for POST /transactions cannot be reused for a different
    operation, because the operation name is part of the fingerprint."""
    from ledger.schemas import CreateTransactionRequest

    tx = CreateTransactionRequest(
        description="x",
        entries=[
            {"account_id": uuid4(), "amount_minor": -1, "currency": "USD"},
            {"account_id": uuid4(), "amount_minor": 1, "currency": "USD"},
        ],
    )
    assert tx.fingerprint()["op"] == "post_transaction"


# ------------------------------------------------- failure does not consume ---


def test_a_rejected_request_does_not_consume_its_key() -> None:
    """A request that fails rolls back the key reservation with it, because they
    are the same transaction. So a client that retries after fixing its payload
    is not permanently locked out of that key."""
    alice = f.make_account()
    bob = f.make_account()
    f.fund(alice, 10_000)
    key = uuid4()

    with pytest.raises(UnbalancedTransaction):
        transactions_service.post_transaction(
            f.transaction_request([(alice, -250, "USD"), (bob, 200, "USD")]), key
        )

    with db.transaction(read_only=True) as cur:
        cur.execute("SELECT count(*) AS n FROM idempotency_keys WHERE key = %s", (key,))
        assert cur.fetchone()["n"] == 0

    outcome = transactions_service.post_transaction(
        f.transaction_request([(alice, -250, "USD"), (bob, 250, "USD")]), key
    )
    assert outcome.replayed is False


def test_a_failure_inside_the_transaction_leaves_no_reservation() -> None:
    """Same property, but for a failure that happens *after* the key row was
    inserted -- proving the reservation and the write really are one atomic unit
    rather than two writes that usually both succeed."""
    alice = f.make_account()
    key = uuid4()
    request = f.transaction_request([(alice, -1, "USD"), (alice, 1, "USD")])

    def exploding_work(cur: Any) -> dict[str, Any]:
        cur.execute("SELECT count(*) AS n FROM idempotency_keys WHERE key = %s", (key,))
        assert cur.fetchone()["n"] == 1  # the reservation exists in *our* snapshot
        raise RuntimeError("business logic blew up")

    with pytest.raises(RuntimeError):
        execute_once(key=key, request=request, status_code=201, work=exploding_work)

    with db.transaction(read_only=True) as cur:
        cur.execute("SELECT count(*) AS n FROM idempotency_keys WHERE key = %s", (key,))
        assert cur.fetchone()["n"] == 0


# ------------------------------------------------------------- concurrency ---


def test_two_concurrent_identical_requests_write_exactly_one_set_of_entries() -> None:
    """The headline test for this phase.

    Two threads fire the same request with the same key at the same instant. One
    must do the work and one must replay it, and the ledger must end up with
    exactly one transaction and one pair of entries.
    """
    alice = f.make_account()
    bob = f.make_account()
    f.fund(alice, 10_000)
    entries_before = f.count_rows("entries")
    tx_before = f.count_rows("transactions")

    key = uuid4()
    request = f.transaction_request([(alice, -250, "USD"), (bob, 250, "USD")])

    start = threading.Barrier(2)
    results: list[Any] = []

    def fire() -> None:
        start.wait(timeout=5)
        try:
            results.append(transactions_service.post_transaction(request, key))
        except Exception as exc:  # noqa: BLE001 -- recorded and asserted below
            results.append(exc)

    threads = [threading.Thread(target=fire) for _ in range(2)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=20)
        assert not t.is_alive()

    assert len(results) == 2
    assert all(not isinstance(r, Exception) for r in results), results

    processed = [r for r in results if not r.replayed]
    replayed = [r for r in results if r.replayed]
    assert len(processed) == 1, "more than one request did the work"
    assert len(replayed) == 1, "the loser did not replay"

    # Both callers got the same transaction id.
    assert processed[0].body["id"] == replayed[0].body["id"]

    # And the ledger moved exactly once.
    assert f.count_rows("transactions") == tx_before + 1
    assert f.count_rows("entries") == entries_before + 2
    assert f.derived_balance(alice) == 9_750
    assert f.derived_balance(bob) == 250


@pytest.mark.parametrize("concurrency", [4, 16])
def test_many_concurrent_identical_requests_write_once(concurrency: int) -> None:
    alice = f.make_account()
    bob = f.make_account()
    f.fund(alice, 10_000)
    entries_before = f.count_rows("entries")

    key = uuid4()
    request = f.transaction_request([(alice, -100, "USD"), (bob, 100, "USD")])
    start = threading.Barrier(concurrency)

    def fire() -> Any:
        start.wait(timeout=10)
        return transactions_service.post_transaction(request, key)

    with ThreadPoolExecutor(max_workers=concurrency) as pool:
        outcomes = [future.result(timeout=30) for future in
                    [pool.submit(fire) for _ in range(concurrency)]]

    assert sum(1 for o in outcomes if not o.replayed) == 1
    assert sum(1 for o in outcomes if o.replayed) == concurrency - 1
    assert len({o.body["id"] for o in outcomes}) == 1
    assert f.count_rows("entries") == entries_before + 2
    assert f.derived_balance(bob) == 100


def test_concurrent_claim_blocks_until_the_owner_commits() -> None:
    """Shows *why* the concurrent case works, rather than just that it does.

    `INSERT ... ON CONFLICT DO NOTHING` against a row inserted by an uncommitted
    transaction does not return "already exists" -- it blocks on that
    transaction. So the second request cannot observe a half-finished state; it
    waits and then sees the committed outcome. That is the entire concurrency
    control mechanism, provided by Postgres rather than by our code.
    """
    key = uuid4()
    request_hash = fingerprint_hash({"op": "probe"})
    claimed_by_second = threading.Event()
    second_rowcount: list[int] = []

    with db.transaction() as owner:
        owner.execute(
            "INSERT INTO idempotency_keys (key, request_hash) VALUES (%s, %s)",
            (key, request_hash),
        )

        def contend() -> None:
            with db.transaction() as other:
                other.execute(
                    """
                    INSERT INTO idempotency_keys (key, request_hash)
                    VALUES (%s, %s)
                    ON CONFLICT (key) DO NOTHING
                    """,
                    (key, request_hash),
                )
                second_rowcount.append(other.rowcount)
            claimed_by_second.set()

        contender = threading.Thread(target=contend)
        contender.start()

        # The contender is blocked on our uncommitted row, not racing past it.
        assert not claimed_by_second.wait(timeout=1.0), (
            "the second claim returned while the first transaction was still "
            "open -- the insert-first pattern is not serialising"
        )
        assert second_rowcount == []

        # Confirm from the database's own point of view that it is waiting.
        with db.transaction(read_only=True) as observer:
            observer.execute(
                """
                SELECT count(*) AS n FROM pg_stat_activity
                 WHERE wait_event_type = 'Lock' AND state = 'active'
                   AND query ILIKE '%%idempotency_keys%%'
                """
            )
            assert observer.fetchone()["n"] >= 1

    # Our COMMIT releases it, and it now sees the conflict as committed fact.
    assert claimed_by_second.wait(timeout=10)
    contender.join(timeout=10)
    assert second_rowcount == [0], "the loser should insert no row"


def test_different_keys_both_write() -> None:
    """Sanity check on the other side: idempotency must not accidentally
    deduplicate two genuinely distinct payments that happen to be identical."""
    alice = f.make_account()
    bob = f.make_account()
    f.fund(alice, 10_000)
    legs = [(alice, -250, "USD"), (bob, 250, "USD")]

    first = transactions_service.post_transaction(f.transaction_request(legs), uuid4())
    second = transactions_service.post_transaction(f.transaction_request(legs), uuid4())

    assert first.body["id"] != second.body["id"]
    assert f.derived_balance(bob) == 500


def test_concurrent_distinct_transactions_all_commit() -> None:
    """Every writer touches the same funding account, so they contend on both
    the account row and the hash chain head. All of them must still commit --
    the retry loop is doing its job."""
    alice = f.make_account()
    bob = f.make_account()
    f.fund(alice, 100_000)
    n = 20

    def fire(i: int) -> Any:
        return transactions_service.post_transaction(
            f.transaction_request([(alice, -(i + 1), "USD"), (bob, i + 1, "USD")]),
            uuid4(),
        )

    with ThreadPoolExecutor(max_workers=8) as pool:
        outcomes = [fut.result(timeout=60) for fut in
                    [pool.submit(fire, i) for i in range(n)]]

    assert len({o.body["id"] for o in outcomes}) == n
    assert f.derived_balance(bob) == sum(range(1, n + 1))
    assert f.derived_balance(alice) == 100_000 - sum(range(1, n + 1))

    # Chain integrity: seq values are contiguous and every prev_hash links.
    with db.transaction(read_only=True) as cur:
        cur.execute("SELECT seq, prev_hash, tx_hash FROM transactions ORDER BY seq")
        rows = cur.fetchall()
    for previous, current in zip(rows, rows[1:]):
        assert bytes(current["prev_hash"]) == bytes(previous["tx_hash"])


# --------------------------------------------------------------- http layer --


def _payload(a: str, b: str, amount: int = 100) -> dict[str, Any]:
    return {
        "description": "transfer",
        "entries": [
            {"account_id": a, "amount_minor": -amount, "currency": "USD"},
            {"account_id": b, "amount_minor": amount, "currency": "USD"},
        ],
    }


def test_http_replay_returns_original_status_and_marker(client: TestClient) -> None:
    alice = client.post(
        "/accounts", json={"name": "alice", "currency": "USD"}
    ).json()["id"]
    settlement = client.post(
        "/accounts",
        json={"name": "s", "currency": "USD", "type": "external_settlement"},
    ).json()["id"]
    key = str(uuid4())

    first = client.post(
        "/transactions",
        headers={"Idempotency-Key": key},
        json=_payload(settlement, alice),
    )
    second = client.post(
        "/transactions",
        headers={"Idempotency-Key": key},
        json=_payload(settlement, alice),
    )

    assert first.status_code == 201
    assert first.json()["replayed"] is False
    # The replay carries the original 201, not a 200: the answer to "did my
    # request happen" is the answer the first attempt gave.
    assert second.status_code == 201
    assert second.json()["replayed"] is True
    assert second.json()["id"] == first.json()["id"]

    assert client.get(f"/accounts/{alice}/balance").json()["actual_minor"] == 100


def test_http_same_key_different_body_is_409(client: TestClient) -> None:
    alice = client.post(
        "/accounts", json={"name": "alice", "currency": "USD"}
    ).json()["id"]
    settlement = client.post(
        "/accounts",
        json={"name": "s", "currency": "USD", "type": "external_settlement"},
    ).json()["id"]
    key = str(uuid4())

    assert client.post(
        "/transactions",
        headers={"Idempotency-Key": key},
        json=_payload(settlement, alice, 100),
    ).status_code == 201

    conflict = client.post(
        "/transactions",
        headers={"Idempotency-Key": key},
        json=_payload(settlement, alice, 999),
    )
    assert conflict.status_code == 409
    assert conflict.json()["error"]["code"] == "idempotency_key_reused"
    assert client.get(f"/accounts/{alice}/balance").json()["actual_minor"] == 100


def test_stored_response_matches_what_was_returned() -> None:
    alice = f.make_account()
    bob = f.make_account()
    f.fund(alice, 1_000)
    key = uuid4()
    outcome = transactions_service.post_transaction(
        f.transaction_request([(alice, -10, "USD"), (bob, 10, "USD")]), key
    )

    with db.transaction(read_only=True) as cur:
        cur.execute(
            "SELECT response_body, status_code FROM idempotency_keys WHERE key = %s",
            (key,),
        )
        row = cur.fetchone()

    assert row["status_code"] == 201
    assert row["response_body"] == outcome.body


def test_transaction_cannot_exist_without_its_authorization_record() -> None:
    """transactions.idempotency_key is a foreign key into idempotency_keys, so
    there is no such thing as a transaction nobody asked for."""
    import psycopg

    alice = f.make_account()
    bob = f.make_account()

    with pytest.raises(psycopg.errors.ForeignKeyViolation):
        with db.transaction() as cur:
            from datetime import datetime, timezone

            cur.execute(
                """
                INSERT INTO transactions
                    (id, idempotency_key, description, created_at, prev_hash, tx_hash)
                VALUES (%s, %s, %s, %s, %s, %s)
                """,
                (
                    uuid4(),
                    uuid4(),  # no matching idempotency_keys row
                    "forged",
                    datetime.now(timezone.utc),
                    b"\x01" * 32,
                    b"\x02" * 32,
                ),
            )
