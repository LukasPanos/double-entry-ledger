-- 004_fx.sql -- Phase 5: multi-currency conversion.
--
-- No new tables. Per-currency zero-sum has been enforced since 001, so a
-- cross-currency transaction is not a special case in the ledger -- it is just a
-- transaction whose entries happen to balance in two currencies independently.
-- That is the whole reason the zero-sum trigger groups by currency: it makes FX
-- fall out of the existing model rather than needing one of its own.
--
-- The only schema change is that `liquidity` joins the account types of which
-- there is exactly one per currency.

-- One liquidity pool per currency, resolved by (type, currency), the same way
-- the settlement and revenue accounts already are. The spec called for liquidity
-- accounts per currency *pair*; one pool per currency was chosen instead because
-- `accounts` has no pair column, it is what treasury systems actually run, and it
-- keeps the account count linear in currencies rather than quadratic. See
-- docs/decisions.md 5.2.
--
-- The predicate of a partial index cannot be altered in place, so the index is
-- rebuilt rather than extended.
DROP INDEX accounts_one_system_account_per_currency;

CREATE UNIQUE INDEX accounts_one_system_account_per_currency
    ON accounts (type, currency)
    WHERE type IN ('platform_revenue', 'external_settlement', 'liquidity');
