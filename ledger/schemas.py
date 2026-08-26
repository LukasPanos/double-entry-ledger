"""Request and response models.

Amounts are `StrictInt`: `100` is accepted, `100.0` and `"100"` are rejected at
the edge with a 422 rather than being silently coerced. A ledger that accepts
`100.5` cents and rounds it has already lost.
"""

from __future__ import annotations

from datetime import datetime
from typing import Annotated, Any, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, StrictInt, field_validator

from ledger.money import INT64_MAX, INT64_MIN

CurrencyCode = Annotated[str, Field(pattern=r"^[A-Z]{3}$", min_length=3, max_length=3)]
AmountMinor = Annotated[StrictInt, Field(ge=INT64_MIN, le=INT64_MAX)]
PositiveAmountMinor = Annotated[StrictInt, Field(gt=0, le=INT64_MAX)]

AccountTypeName = Literal[
    "user", "platform_revenue", "liquidity", "external_settlement"
]


class Strict(BaseModel):
    """Reject unknown fields. A typo'd field name in a payments request should
    be an error, not a silently ignored instruction."""

    model_config = ConfigDict(extra="forbid")


# ------------------------------------------------------------------ accounts --


class CreateAccountRequest(Strict):
    name: str = Field(min_length=1, max_length=200)
    currency: CurrencyCode
    type: AccountTypeName = "user"


class AccountResponse(Strict):
    id: UUID
    name: str
    currency: str
    type: str
    created_at: datetime


# -------------------------------------------------------------- transactions --


class EntryInput(Strict):
    account_id: UUID
    amount_minor: AmountMinor
    currency: CurrencyCode

    @field_validator("amount_minor")
    @classmethod
    def _reject_zero(cls, v: int) -> int:
        if v == 0:
            raise ValueError("amount_minor must not be zero")
        return v


class CreateTransactionRequest(Strict):
    description: str = Field(min_length=1, max_length=500)
    entries: list[EntryInput] = Field(min_length=2, max_length=1000)


class EntryResponse(Strict):
    id: int
    account_id: UUID
    amount_minor: int
    currency: str


class TransactionResponse(Strict):
    id: UUID
    seq: int
    description: str
    created_at: datetime
    entries: list[EntryResponse]
    tx_hash: str
    prev_hash: str
    replayed: bool = False


# ------------------------------------------------------------------- holds ---


class CreateHoldRequest(Strict):
    account_id: UUID
    amount_minor: PositiveAmountMinor
    currency: CurrencyCode
    expires_in_seconds: int = Field(default=3600, ge=1, le=30 * 24 * 3600)
    description: str = Field(default="hold", min_length=1, max_length=500)


class HoldResponse(Strict):
    id: UUID
    account_id: UUID
    amount_minor: int
    currency: str
    status: str
    expires_at: datetime
    captured_transaction_id: UUID | None
    captured_amount_minor: int | None
    created_at: datetime
    replayed: bool = False


class CaptureHoldRequest(Strict):
    # Omit to capture the full held amount.
    amount_minor: PositiveAmountMinor | None = None
    description: str | None = Field(default=None, max_length=500)


class VoidHoldRequest(Strict):
    reason: str | None = Field(default=None, max_length=500)


# ---------------------------------------------------------------- balances ---


class BalanceResponse(Strict):
    account_id: UUID
    currency: str
    # SUM(entries) -- the authoritative number.
    actual_minor: int
    # SUM(active holds).
    held_minor: int
    # actual - held. What a debit is authorised against.
    available_minor: int
    as_of: datetime


class EntriesPage(Strict):
    account_id: UUID
    entries: list[EntryResponse]
    next_cursor: int | None


# ---------------------------------------------------------------------- fx ---


class FxConvertRequest(Strict):
    from_account_id: UUID
    to_account_id: UUID
    # Amount debited from the source account, in its own minor units.
    sell_amount_minor: PositiveAmountMinor
    # Amount credited to the destination account, in its minor units. The
    # caller states both legs explicitly; see ledger/services/fx.py for why the
    # service does not compute one from a rate.
    buy_amount_minor: PositiveAmountMinor
    # What the platform keeps, denominated in the sell currency.
    spread_minor: StrictInt = Field(default=0, ge=0, le=INT64_MAX)
    description: str = Field(default="fx conversion", min_length=1, max_length=500)


# --------------------------------------------------- ops / introspection ----


class ReconciliationCheck(Strict):
    name: str
    passed: bool
    detail: str
    failures: list[dict[str, Any]] = Field(default_factory=list)


class ReconciliationReport(Strict):
    ok: bool
    checked_at: datetime
    duration_ms: float
    checks: list[ReconciliationCheck]


class IntegrityReport(Strict):
    ok: bool
    transactions_checked: int
    first_break: dict[str, Any] | None
    head_hash: str | None
    checked_at: datetime
    duration_ms: float
