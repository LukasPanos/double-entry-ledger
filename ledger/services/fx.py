"""Currency conversion.

A conversion is not a special kind of transaction. Because the zero-sum
constraint has been *per currency* since migration 001, a cross-currency
transaction is just one whose entries balance in two currencies independently:

    sell 100.00 USD, spread 1.00 USD, rate 1.35

      user       USD  -10000     |  USD:  -10000 + 100 + 9900 = 0
      revenue    USD    + 100    |
      liquidity  USD   + 9900    |
      liquidity  CAD  -13365     |  CAD: -13365 + 13365       = 0
      user       CAD  +13365     |

Money never crosses the currency boundary. The user's USD goes to a USD pool and
the user's CAD comes out of a CAD pool; the two halves are linked only by being
in the same transaction. There is no point in the code where a USD amount is
added to a CAD amount, which is what makes "no FX sequence can create or destroy
money in any currency" true by construction rather than by careful arithmetic.

**The caller states both legs. The service never applies a rate.**

`POST /fx/convert` takes `sell_amount_minor` and `buy_amount_minor` as explicit
integers rather than taking a rate and computing one from the other. That is the
single most important decision in this module. Applying a rate means multiplying
money by a non-integer, which means picking a rounding direction, and the
rounding residue has to go somewhere -- get it wrong and the ledger leaks a cent
per conversion. Making the quoting engine hand down two integers keeps every
arithmetic operation in the money path integer addition, and moves the rounding
decision to where the rate lives.

The spread is denominated in the sell currency and credited to that currency's
`platform_revenue` account. It is a fee, not a valuation: no rate is needed to
know what the platform earned, which is exactly what a mark-to-market
"realise the pool's gain later" design would have required.
"""

from __future__ import annotations

from decimal import Decimal, localcontext
from typing import Any
from uuid import UUID, uuid4

from psycopg import Cursor

from ledger.config import get_settings
from ledger.db import STRATEGY_ISOLATION
from ledger.errors import AccountNotFound, ValidationFailed
from ledger.money import MINOR_UNIT_EXPONENT
from ledger.schemas import FxConvertRequest, FxConvertResponse
from ledger.services.idempotency import Outcome, execute_once
from ledger.services.posting import (
    Posting,
    append_transaction,
    assert_currencies_match,
    assert_no_overdraft,
    validate_postings,
)
from ledger.services.transactions import acquire_accounts


def convert(request: FxConvertRequest, idempotency_key: UUID, *, strategy: str | None = None) -> Outcome:
    strategy = strategy or get_settings().concurrency_strategy

    if request.from_account_id == request.to_account_id:
        raise ValidationFailed(
            "from_account_id and to_account_id must differ",
            account_id=str(request.from_account_id),
        )
    if request.spread_minor >= request.sell_amount_minor:
        raise ValidationFailed(
            f"spread_minor ({request.spread_minor}) must be less than "
            f"sell_amount_minor ({request.sell_amount_minor}); a conversion that "
            f"is entirely spread converts nothing",
            spread_minor=request.spread_minor,
            sell_amount_minor=request.sell_amount_minor,
        )

    transaction_id = uuid4()

    def work(cur: Cursor) -> dict[str, Any]:
        user_accounts = acquire_accounts(
            cur, [request.from_account_id, request.to_account_id], strategy
        )
        sell_currency = user_accounts[request.from_account_id].currency
        buy_currency = user_accounts[request.to_account_id].currency

        if sell_currency == buy_currency:
            raise ValidationFailed(
                f"both accounts are denominated in {sell_currency}; use "
                f"POST /transactions for a same-currency transfer",
                currency=sell_currency,
            )

        liquidity_sell = _system_account(cur, "liquidity", sell_currency)
        liquidity_buy = _system_account(cur, "liquidity", buy_currency)
        revenue = (
            _system_account(cur, "platform_revenue", sell_currency)
            if request.spread_minor > 0
            else None
        )

        converted = request.sell_amount_minor - request.spread_minor

        postings = [
            # Sell side, all in sell_currency.
            Posting(request.from_account_id, -request.sell_amount_minor, sell_currency),
            Posting(liquidity_sell, converted, sell_currency),
            # Buy side, all in buy_currency.
            Posting(liquidity_buy, -request.buy_amount_minor, buy_currency),
            Posting(request.to_account_id, request.buy_amount_minor, buy_currency),
        ]
        if revenue is not None:
            postings.insert(2, Posting(revenue, request.spread_minor, sell_currency))

        validate_postings(postings)

        # Lock (or, under the optimistic strategy, read) every account the
        # transaction touches, including the pools. Acquired in one call so the
        # ascending-id ordering in `lock_accounts` covers all of them -- locking
        # the user accounts and then the pools in two separate calls would create
        # two orderings and reintroduce deadlock risk.
        all_accounts = acquire_accounts(
            cur, [p.account_id for p in postings], strategy
        )
        assert_currencies_match(postings, all_accounts)
        assert_no_overdraft(cur, postings, all_accounts)

        tx = append_transaction(
            cur,
            description=request.description,
            idempotency_key=idempotency_key,
            postings=postings,
            transaction_id=transaction_id,
        )

        return FxConvertResponse.model_validate(
            {
                "transaction_id": tx["id"],
                "seq": tx["seq"],
                "created_at": tx["created_at"],
                "tx_hash": tx["tx_hash"],
                "from_account_id": request.from_account_id,
                "to_account_id": request.to_account_id,
                "sell_currency": sell_currency,
                "buy_currency": buy_currency,
                "sell_amount_minor": request.sell_amount_minor,
                "spread_minor": request.spread_minor,
                "converted_amount_minor": converted,
                "buy_amount_minor": request.buy_amount_minor,
                "liquidity_sell_account_id": liquidity_sell,
                "liquidity_buy_account_id": liquidity_buy,
                "revenue_account_id": revenue,
                "effective_rate": effective_rate(
                    sell_minor=request.sell_amount_minor,
                    sell_currency=sell_currency,
                    buy_minor=request.buy_amount_minor,
                    buy_currency=buy_currency,
                ),
                "entries": tx["entries"],
            }
        ).model_dump(mode="json")

    return execute_once(
        key=idempotency_key,
        fingerprint=request.fingerprint(),
        status_code=201,
        work=work,
        isolation=STRATEGY_ISOLATION[strategy],
    )


def _system_account(cur: Cursor, account_type: str, currency: str) -> UUID:
    """Resolve the one account of this type for this currency.

    Guaranteed unique by the partial index in migration 004, so this cannot
    silently pick one of several.
    """
    cur.execute(
        """
        SELECT id FROM accounts
         WHERE type = %s::account_type AND currency = %s
        """,
        (account_type, currency),
    )
    row = cur.fetchone()
    if row is None:
        raise AccountNotFound(
            f"no {account_type} account exists for {currency}; create one before "
            f"converting into or out of {currency}",
            account_type=account_type,
            currency=currency,
        )
    return row["id"]


def effective_rate(
    *, sell_minor: int, sell_currency: str, buy_minor: int, buy_currency: str
) -> str:
    """Buy units per one sell unit, as a decimal string.

    Display only. Computed with `Decimal` rather than `float`, and returned as a
    string so it cannot accidentally be fed back into an amount calculation. The
    minor-unit exponents matter here and only here: 1 USD is 100 minor units but
    1 JPY is 1, so a raw minor-unit ratio would misreport the rate by a factor of
    100 for a USD/JPY pair.
    """
    sell_exp = MINOR_UNIT_EXPONENT.get(sell_currency, 2)
    buy_exp = MINOR_UNIT_EXPONENT.get(buy_currency, 2)
    with localcontext() as ctx:
        ctx.prec = 28
        sell_major = Decimal(sell_minor) / (Decimal(10) ** sell_exp)
        buy_major = Decimal(buy_minor) / (Decimal(10) ** buy_exp)
        rate = buy_major / sell_major
        return format(rate.quantize(Decimal("0.00000001")).normalize(), "f")
