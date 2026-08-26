"""Money.

Amounts are Python `int` holding minor units (cents, pence, yen). There is no
float and no Decimal anywhere in the money path, so there is no rounding mode
to agree on and no representation error to bound.

The only place a ratio appears at all is FX (Phase 5), and even there the rate
is applied with integer arithmetic and an explicit rounding direction.
"""

from __future__ import annotations

import re

from ledger.errors import UnsupportedCurrency, ValidationFailed

# BIGINT. Amounts are checked against this in the application layer so that an
# oversized value is a 400 rather than a driver-level integer overflow.
INT64_MIN = -(2**63)
INT64_MAX = 2**63 - 1

_CURRENCY_RE = re.compile(r"^[A-Z]{3}$")

# Minor-unit exponents. Only needed for display and for FX rate scaling; the
# ledger itself never needs to know how many decimal places a currency has.
MINOR_UNIT_EXPONENT: dict[str, int] = {
    "USD": 2,
    "EUR": 2,
    "GBP": 2,
    "CAD": 2,
    "AUD": 2,
    "CHF": 2,
    "JPY": 0,  # no minor unit
    "KRW": 0,
    "BHD": 3,  # three decimal places
    "KWD": 3,
}

SUPPORTED_CURRENCIES = frozenset(MINOR_UNIT_EXPONENT)


def validate_currency(currency: str) -> str:
    if not isinstance(currency, str) or not _CURRENCY_RE.match(currency):
        raise ValidationFailed(
            f"currency must be a 3-letter uppercase ISO 4217 code, got {currency!r}",
            currency=currency,
        )
    if currency not in SUPPORTED_CURRENCIES:
        raise UnsupportedCurrency(
            f"currency {currency} is not configured; add its minor-unit "
            f"exponent to ledger.money.MINOR_UNIT_EXPONENT first",
            currency=currency,
        )
    return currency


def validate_amount(amount: object, *, field: str = "amount_minor") -> int:
    # bool is a subclass of int and would silently become 0 or 1.
    if isinstance(amount, bool) or not isinstance(amount, int):
        raise ValidationFailed(
            f"{field} must be an integer number of minor units, got "
            f"{type(amount).__name__}",
            field=field,
        )
    if not (INT64_MIN <= amount <= INT64_MAX):
        raise ValidationFailed(
            f"{field} is outside the range of a 64-bit integer", field=field
        )
    return amount


def format_amount(amount_minor: int, currency: str) -> str:
    """Human-readable rendering. Never used for arithmetic."""
    exponent = MINOR_UNIT_EXPONENT.get(currency, 2)
    if exponent == 0:
        return f"{amount_minor} {currency}"
    sign = "-" if amount_minor < 0 else ""
    magnitude = abs(amount_minor)
    divisor = 10**exponent
    return f"{sign}{magnitude // divisor}.{magnitude % divisor:0{exponent}d} {currency}"
