"""Phase 1: the HTTP contract."""

from __future__ import annotations

from uuid import uuid4

from fastapi.testclient import TestClient

from tests import factories as f


def _create_account(client: TestClient, currency: str = "USD", type_: str = "user"):
    response = client.post(
        "/accounts",
        json={"name": f"acct {uuid4().hex[:6]}", "currency": currency, "type": type_},
    )
    assert response.status_code == 201, response.text
    return response.json()["id"]


def test_create_account(client: TestClient) -> None:
    response = client.post(
        "/accounts", json={"name": "Alice", "currency": "USD", "type": "user"}
    )
    assert response.status_code == 201
    body = response.json()
    assert body["currency"] == "USD"
    assert body["type"] == "user"


def test_create_account_rejects_bad_currency(client: TestClient) -> None:
    assert client.post("/accounts", json={"name": "x", "currency": "usd"}).status_code == 422
    assert client.post("/accounts", json={"name": "x", "currency": "DOLLAR"}).status_code == 422

    # Well-formed but not configured: a 422 with a domain error code rather than
    # a schema error.
    response = client.post("/accounts", json={"name": "x", "currency": "XYZ"})
    assert response.status_code == 422
    assert response.json()["error"]["code"] == "unsupported_currency"


def test_create_account_rejects_unknown_fields(client: TestClient) -> None:
    response = client.post(
        "/accounts",
        json={"name": "x", "currency": "USD", "balance_minor": 100_000},
    )
    assert response.status_code == 422


def test_post_transaction_requires_idempotency_key(client: TestClient) -> None:
    alice = _create_account(client)
    settlement = _create_account(client, type_="external_settlement")

    response = client.post(
        "/transactions",
        json={
            "description": "funding",
            "entries": [
                {"account_id": settlement, "amount_minor": -100, "currency": "USD"},
                {"account_id": alice, "amount_minor": 100, "currency": "USD"},
            ],
        },
    )
    assert response.status_code == 400
    assert response.json()["error"]["code"] == "validation_failed"
    assert "Idempotency-Key" in response.json()["error"]["message"]


def test_idempotency_key_must_be_a_uuid(client: TestClient) -> None:
    response = client.post(
        "/transactions",
        headers={"Idempotency-Key": "not-a-uuid"},
        json={"description": "x", "entries": []},
    )
    assert response.status_code == 400
    assert response.json()["error"]["code"] == "validation_failed"


def test_post_transaction_and_read_balance(client: TestClient) -> None:
    alice = _create_account(client)
    settlement = _create_account(client, type_="external_settlement")

    response = client.post(
        "/transactions",
        headers={"Idempotency-Key": str(uuid4())},
        json={
            "description": "funding alice",
            "entries": [
                {"account_id": settlement, "amount_minor": -25_000, "currency": "USD"},
                {"account_id": alice, "amount_minor": 25_000, "currency": "USD"},
            ],
        },
    )
    assert response.status_code == 201, response.text
    body = response.json()
    assert body["seq"] == 1
    assert len(body["entries"]) == 2
    assert len(body["tx_hash"]) == 64

    balance = client.get(f"/accounts/{alice}/balance").json()
    assert balance == {
        "account_id": alice,
        "currency": "USD",
        "actual_minor": 25_000,
        "held_minor": 0,
        "available_minor": 25_000,
        "as_of": balance["as_of"],
    }

    settlement_balance = client.get(f"/accounts/{settlement}/balance").json()
    assert settlement_balance["actual_minor"] == -25_000


def test_unbalanced_transaction_returns_422(client: TestClient) -> None:
    alice = _create_account(client)
    bob = _create_account(client)

    response = client.post(
        "/transactions",
        headers={"Idempotency-Key": str(uuid4())},
        json={
            "description": "bad",
            "entries": [
                {"account_id": alice, "amount_minor": -100, "currency": "USD"},
                {"account_id": bob, "amount_minor": 90, "currency": "USD"},
            ],
        },
    )
    assert response.status_code == 422
    assert response.json()["error"]["code"] == "unbalanced_transaction"
    assert response.json()["error"]["details"]["imbalance"] == {"USD": -10}


def test_float_amount_returns_422(client: TestClient) -> None:
    alice = _create_account(client)
    bob = _create_account(client)
    response = client.post(
        "/transactions",
        headers={"Idempotency-Key": str(uuid4())},
        json={
            "description": "bad",
            "entries": [
                {"account_id": alice, "amount_minor": -100.5, "currency": "USD"},
                {"account_id": bob, "amount_minor": 100.5, "currency": "USD"},
            ],
        },
    )
    assert response.status_code == 422


def test_entries_endpoint_paginates(client: TestClient) -> None:
    alice = _create_account(client)
    bob = _create_account(client)
    settlement = _create_account(client, type_="external_settlement")

    client.post(
        "/transactions",
        headers={"Idempotency-Key": str(uuid4())},
        json={
            "description": "funding",
            "entries": [
                {"account_id": settlement, "amount_minor": -1_000, "currency": "USD"},
                {"account_id": alice, "amount_minor": 1_000, "currency": "USD"},
            ],
        },
    )
    for i in range(4):
        client.post(
            "/transactions",
            headers={"Idempotency-Key": str(uuid4())},
            json={
                "description": f"payment {i}",
                "entries": [
                    {"account_id": alice, "amount_minor": -10, "currency": "USD"},
                    {"account_id": bob, "amount_minor": 10, "currency": "USD"},
                ],
            },
        )

    page = client.get(f"/accounts/{alice}/entries?limit=2").json()
    assert len(page["entries"]) == 2
    assert page["next_cursor"] is not None

    page2 = client.get(
        f"/accounts/{alice}/entries?limit=2&cursor={page['next_cursor']}"
    ).json()
    assert len(page2["entries"]) == 2
    assert page2["entries"][0]["id"] > page["entries"][-1]["id"]


def test_unknown_account_returns_404(client: TestClient) -> None:
    assert client.get(f"/accounts/{uuid4()}/balance").status_code == 404
    assert client.get(f"/accounts/{uuid4()}/entries").status_code == 404
    assert client.get(f"/transactions/{uuid4()}").status_code == 404


def test_second_settlement_account_for_same_currency_is_refused(
    client: TestClient,
) -> None:
    _create_account(client, type_="external_settlement")
    response = client.post(
        "/accounts",
        json={"name": "second", "currency": "USD", "type": "external_settlement"},
    )
    assert response.status_code == 400
    assert response.json()["error"]["code"] == "validation_failed"


def test_health(client: TestClient) -> None:
    assert client.get("/health").json() == {"status": "ok"}
