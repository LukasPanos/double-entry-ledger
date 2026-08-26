"""HTTP surface.

The route functions are thin on purpose: parse, delegate, serialise. All of the
interesting reasoning lives in ledger/services/, which knows nothing about HTTP
and can therefore be driven directly by property tests.
"""

from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from typing import Annotated, Any
from uuid import UUID

from fastapi import Depends, FastAPI, Header, Query, Request
from fastapi.responses import JSONResponse

from ledger import db
from ledger.errors import LedgerError, ValidationFailed
from ledger.schemas import (
    AccountResponse,
    BalanceResponse,
    CreateAccountRequest,
    CreateTransactionRequest,
    EntriesPage,
    TransactionResponse,
)
from ledger.services import accounts as accounts_service
from ledger.services import transactions as transactions_service
from ledger.services.idempotency import Outcome

log = logging.getLogger("ledger.api")


@asynccontextmanager
async def lifespan(app: FastAPI):
    db.init_pool()
    try:
        yield
    finally:
        db.close_pool()


app = FastAPI(
    title="Ledger",
    version="0.1.0",
    summary="Double-entry ledger with derived balances and append-only entries",
    lifespan=lifespan,
)


# ----------------------------------------------------------- error handling --


@app.exception_handler(LedgerError)
async def _ledger_error_handler(_: Request, exc: LedgerError) -> JSONResponse:
    return JSONResponse(status_code=exc.status, content=exc.to_dict())


# ------------------------------------------------------------- dependencies --


def idempotency_key(
    key: Annotated[str | None, Header(alias="Idempotency-Key")] = None,
) -> UUID:
    """Every write endpoint requires a client-supplied UUID.

    Required rather than optional-with-a-generated-default: if the server
    invents the key, a client retry gets a fresh one and the write happens
    twice, which is the exact failure this header exists to prevent.
    """
    if key is None:
        raise ValidationFailed(
            "the Idempotency-Key header is required on all write endpoints"
        )
    try:
        return UUID(key)
    except ValueError as exc:
        raise ValidationFailed(
            f"Idempotency-Key must be a UUID, got {key!r}"
        ) from exc


IdempotencyKey = Annotated[UUID, Depends(idempotency_key)]


def _idempotent(outcome: Outcome) -> JSONResponse:
    """Return the outcome of an idempotent write.

    A replay carries the *original* status code, not 200: the client asked "did
    my request happen", and the honest answer is the answer the first attempt
    gave, plus `replayed: true` so the caller can tell the difference. Returning
    a raw JSONResponse means the stored body is echoed byte-for-byte rather than
    being re-serialised through the response model, which is what makes replay
    faithful even if the model later gains a field.
    """
    return JSONResponse(status_code=outcome.status_code, content=outcome.body)


# ---------------------------------------------------------------- accounts ---


@app.post("/accounts", response_model=AccountResponse, status_code=201)
def create_account(request: CreateAccountRequest) -> Any:
    return accounts_service.create_account(request)


@app.get("/accounts/{account_id}", response_model=AccountResponse)
def get_account(account_id: UUID) -> Any:
    return accounts_service.get_account(account_id)


@app.get("/accounts/{account_id}/balance", response_model=BalanceResponse)
def get_balance(account_id: UUID) -> Any:
    return accounts_service.get_balance(account_id)


@app.get("/accounts/{account_id}/entries", response_model=EntriesPage)
def list_entries(
    account_id: UUID,
    limit: Annotated[int, Query(ge=1, le=500)] = 50,
    cursor: Annotated[int | None, Query(ge=0)] = None,
) -> Any:
    return transactions_service.list_entries(account_id, limit=limit, cursor=cursor)


# ------------------------------------------------------------ transactions ---


@app.post("/transactions", response_model=TransactionResponse, status_code=201)
def post_transaction(request: CreateTransactionRequest, key: IdempotencyKey) -> Any:
    return _idempotent(transactions_service.post_transaction(request, key))


@app.get("/transactions/{transaction_id}", response_model=TransactionResponse)
def get_transaction(transaction_id: UUID) -> Any:
    return transactions_service.get_transaction(transaction_id)


# ------------------------------------------------------------------ health ---


@app.get("/health")
def health() -> dict[str, str]:
    with db.transaction(read_only=True) as cur:
        cur.execute("SELECT 1")
    return {"status": "ok"}
