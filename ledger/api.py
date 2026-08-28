"""HTTP surface.

The route functions are thin on purpose: parse, delegate, serialise. All of the
interesting reasoning lives in ledger/services/, which knows nothing about HTTP
and can therefore be driven directly by property tests.
"""

from __future__ import annotations

import asyncio
import logging
from contextlib import asynccontextmanager
from typing import Annotated, Any
from uuid import UUID

from fastapi import Depends, FastAPI, Header, Query, Request
from fastapi.responses import JSONResponse

from ledger import db
from ledger.config import get_settings
from ledger.errors import LedgerError, ValidationFailed
from ledger.schemas import (
    AccountResponse,
    BalanceResponse,
    CaptureHoldRequest,
    CreateAccountRequest,
    CreateHoldRequest,
    CreateTransactionRequest,
    EntriesPage,
    HoldResponse,
    IntegrityReport,
    ReconciliationReport,
    TransactionResponse,
    VoidHoldRequest,
)
from ledger.services import accounts as accounts_service
from ledger.services import holds as holds_service
from ledger.services import integrity as integrity_service
from ledger.services import reconciliation as reconciliation_service
from ledger.services import transactions as transactions_service
from ledger.services.idempotency import Outcome

log = logging.getLogger("ledger.api")


async def _hold_expiry_worker() -> None:
    """Relabel lapsed holds as `expired`, forever.

    Deliberately unremarkable, because it is deliberately not load-bearing:
    `available` already ignores holds past their deadline, so if this worker dies
    nobody's money is stuck. It exists to keep the partial indexes small and the
    `status` column honest. That is also why a failed sweep is logged and
    retried rather than escalated -- there is nothing urgent to escalate.
    """
    settings = get_settings()
    while True:
        try:
            # to_thread because the database layer is synchronous by design; the
            # sweep must not block the event loop serving requests.
            swept = await asyncio.to_thread(holds_service.sweep_expired_holds)
            if swept:
                log.info("expired %d lapsed hold(s)", swept)
        except asyncio.CancelledError:
            raise
        except Exception:  # noqa: BLE001 -- a worker that dies is worse
            log.exception("hold expiry sweep failed; will retry")
        await asyncio.sleep(settings.hold_expiry_poll_seconds)


@asynccontextmanager
async def lifespan(app: FastAPI):
    db.init_pool()
    settings = get_settings()

    workers: list[asyncio.Task[None]] = []
    if settings.run_hold_expiry_worker:
        workers.append(asyncio.create_task(_hold_expiry_worker()))

    try:
        yield
    finally:
        for worker in workers:
            worker.cancel()
        await asyncio.gather(*workers, return_exceptions=True)
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


# ------------------------------------------------------------------- holds ----


@app.post("/holds", response_model=HoldResponse, status_code=201)
def create_hold(request: CreateHoldRequest, key: IdempotencyKey) -> Any:
    return _idempotent(holds_service.create_hold(request, key))


@app.post("/holds/{hold_id}/capture", response_model=HoldResponse)
def capture_hold(
    hold_id: UUID, request: CaptureHoldRequest, key: IdempotencyKey
) -> Any:
    return _idempotent(holds_service.capture_hold(hold_id, request, key))


@app.post("/holds/{hold_id}/void", response_model=HoldResponse)
def void_hold(
    hold_id: UUID, key: IdempotencyKey, request: VoidHoldRequest | None = None
) -> Any:
    # The spec's endpoint list does not mark this one as requiring a key, but the
    # stated invariant is that every write endpoint does. Requiring it here means
    # a retried void replays the original response instead of failing with 409
    # "already voided", which is the behaviour a client retrying a timeout wants.
    return _idempotent(
        holds_service.void_hold(hold_id, request or VoidHoldRequest(), key)
    )


@app.get("/holds/{hold_id}", response_model=HoldResponse)
def get_hold(hold_id: UUID) -> Any:
    return holds_service.get_hold(hold_id)


@app.get("/accounts/{account_id}/holds", response_model=list[HoldResponse])
def list_holds(
    account_id: UUID,
    status: Annotated[str | None, Query(pattern="^(pending|captured|voided|expired)$")] = None,
) -> Any:
    return holds_service.list_holds(account_id, status=status)


# ------------------------------------------------------------------ health ---


@app.get("/health")
def health() -> dict[str, str]:
    with db.transaction(read_only=True) as cur:
        cur.execute("SELECT 1")
    return {"status": "ok"}


# ------------------------------------------------------------------- ops -----


@app.get("/reconciliation", response_model=ReconciliationReport)
def get_reconciliation() -> Any:
    """Run every invariant check and report. 200 whether or not it passes: the
    request succeeded, and `ok` carries the answer. A 500 here would mean the
    check itself broke, which is a different thing from the ledger being wrong."""
    return reconciliation_service.reconcile()


@app.get("/integrity", response_model=IntegrityReport)
def get_integrity() -> Any:
    return integrity_service.verify_chain()
