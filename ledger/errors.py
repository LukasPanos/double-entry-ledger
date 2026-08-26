"""Domain errors.

Each carries a stable machine-readable `code` and the HTTP status it maps to.
Clients branch on `code`; the message is for humans and may change.
"""

from __future__ import annotations

from typing import Any


class LedgerError(Exception):
    code = "ledger_error"
    status = 400

    def __init__(self, message: str, **details: Any) -> None:
        super().__init__(message)
        self.message = message
        self.details = details

    def to_dict(self) -> dict[str, Any]:
        body: dict[str, Any] = {"code": self.code, "message": self.message}
        if self.details:
            body["details"] = self.details
        return {"error": body}


# ------------------------------------------------------------------ 400/422 --


class ValidationFailed(LedgerError):
    code = "validation_failed"
    status = 400


class UnbalancedTransaction(LedgerError):
    """Entries do not sum to zero in at least one currency."""

    code = "unbalanced_transaction"
    status = 422


class CurrencyMismatch(LedgerError):
    """An entry's currency does not match its account's currency."""

    code = "currency_mismatch"
    status = 422


class InsufficientFunds(LedgerError):
    """The debit would push available balance below zero."""

    code = "insufficient_funds"
    status = 422


class CaptureExceedsHold(LedgerError):
    code = "capture_exceeds_hold"
    status = 422


class UnsupportedCurrency(LedgerError):
    code = "unsupported_currency"
    status = 422


# ---------------------------------------------------------------------- 404 --


class NotFound(LedgerError):
    code = "not_found"
    status = 404


class AccountNotFound(NotFound):
    code = "account_not_found"


class HoldNotFound(NotFound):
    code = "hold_not_found"


class TransactionNotFound(NotFound):
    code = "transaction_not_found"


# ---------------------------------------------------------------------- 409 --


class IdempotencyKeyReused(LedgerError):
    """Same Idempotency-Key, different request body."""

    code = "idempotency_key_reused"
    status = 409


class IdempotencyKeyInFlight(LedgerError):
    """The original request holding this key has not committed yet."""

    code = "idempotency_key_in_flight"
    status = 409


class HoldNotPending(LedgerError):
    """Capture or void attempted on a hold that has already left `pending`."""

    code = "hold_not_pending"
    status = 409


# ---------------------------------------------------------------------- 503 --


class RetriesExhausted(LedgerError):
    code = "retries_exhausted"
    status = 503
