-- 001_core.sql -- Phase 1: accounts, transactions, entries, derived balances.
--
-- Design notes that matter for reading this file:
--
--  * Money is BIGINT minor units everywhere. There is no NUMERIC and no float
--    in the money path, so there is no rounding behaviour to reason about.
--  * `entries` is append-only. Enforced by trigger, not by convention.
--  * A transaction's entries must sum to zero per currency. Enforced by a
--    DEFERRED constraint trigger, so the check happens at COMMIT -- which is
--    the only point at which "the transaction's entries" is a complete set.
--  * Balances are SUM(entries). `account_balances` is a cache and is never
--    read as authoritative state; /reconciliation proves it matches.


-- ---------------------------------------------------------------- accounts --

CREATE TYPE account_type AS ENUM (
    'user',
    'platform_revenue',
    'liquidity',
    'external_settlement'
);

CREATE TABLE accounts (
    id         uuid         PRIMARY KEY,
    name       text         NOT NULL CHECK (length(name) BETWEEN 1 AND 200),
    currency   char(3)      NOT NULL CHECK (currency ~ '^[A-Z]{3}$'),
    type       account_type NOT NULL,
    created_at timestamptz  NOT NULL DEFAULT now(),

    -- Redundant given the PK, but it gives `entries` a composite foreign key
    -- target. That makes "an entry's currency always equals its account's
    -- currency" a structural impossibility rather than an application check.
    UNIQUE (id, currency)
);

-- Money enters and leaves the system only through external_settlement, and
-- fees accrue only to platform_revenue. Exactly one of each per currency, so
-- "the system account for USD" is always unambiguous.
CREATE UNIQUE INDEX accounts_one_system_account_per_currency
    ON accounts (type, currency)
    WHERE type IN ('platform_revenue', 'external_settlement');


-- ------------------------------------------------------------ transactions --

CREATE TABLE transactions (
    id              uuid        PRIMARY KEY,

    -- The hash chain needs a total order. uuid is unordered and created_at can
    -- tie (or go backwards under clock adjustment), so ordering is carried by
    -- an explicit monotonic sequence.
    seq             bigserial   NOT NULL UNIQUE,

    idempotency_key uuid        NOT NULL UNIQUE,
    description     text        NOT NULL CHECK (length(description) BETWEEN 1 AND 500),
    created_at      timestamptz NOT NULL,

    -- Tamper evidence (Phase 7). Genesis uses 32 zero bytes rather than NULL
    -- so that UNIQUE(prev_hash) actually constrains it -- Postgres allows many
    -- NULLs in a unique index, which would permit multiple genesis rows.
    prev_hash       bytea       NOT NULL UNIQUE CHECK (octet_length(prev_hash) = 32),
    tx_hash         bytea       NOT NULL UNIQUE CHECK (octet_length(tx_hash) = 32)
);

-- UNIQUE(prev_hash) above is the load-bearing part of the chain: two rows
-- cannot claim the same predecessor, so the chain cannot fork. Rewriting
-- history therefore requires rewriting every subsequent row, and the
-- append-only trigger below forbids rewriting any row at all.

COMMENT ON COLUMN transactions.tx_hash IS
    'SHA-256 over the canonical serialization in ledger/hashing.py (v1).';


-- ----------------------------------------------------------------- entries --

CREATE TABLE entries (
    id             bigserial PRIMARY KEY,
    transaction_id uuid      NOT NULL REFERENCES transactions (id),
    account_id     uuid      NOT NULL,
    amount_minor   bigint    NOT NULL CHECK (amount_minor <> 0),
    currency       char(3)   NOT NULL,

    -- See accounts.UNIQUE(id, currency): a USD entry cannot land on a CAD
    -- account, and the database is the thing preventing it.
    FOREIGN KEY (account_id, currency) REFERENCES accounts (id, currency)
);

-- Required for balance sums. INCLUDE makes SUM(amount_minor) an index-only
-- scan, so deriving a balance never touches the heap.
CREATE INDEX entries_account_id_idx
    ON entries (account_id, id) INCLUDE (amount_minor, currency);

CREATE INDEX entries_transaction_id_idx ON entries (transaction_id);


-- -------------------------------------------------------- balance cache ----

-- Optimization only. Maintained by application code inside the same explicit
-- transaction as the entry insert (see ledger/services/transactions.py), never
-- consulted as the source of truth, and reconciled against SUM(entries) by
-- GET /reconciliation.
--
-- It also serves as the lock target for the pessimistic concurrency strategy
-- in Phase 4: one row per account gives us a stable, orderable set of rows to
-- SELECT ... FOR UPDATE.
CREATE TABLE account_balances (
    account_id    uuid        PRIMARY KEY,
    currency      char(3)     NOT NULL,
    balance_minor bigint      NOT NULL DEFAULT 0,
    entry_count   bigint      NOT NULL DEFAULT 0,
    updated_at    timestamptz NOT NULL DEFAULT now(),

    FOREIGN KEY (account_id, currency) REFERENCES accounts (id, currency)
);


-- =========================================================================
-- Defense in depth
-- =========================================================================

-- ------------------------------------------------ append-only enforcement --

CREATE FUNCTION reject_mutation() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION
        'append_only_violation: % on "%" is forbidden',
        TG_OP, TG_TABLE_NAME
        USING ERRCODE = '23514',
              HINT = 'Ledger history is immutable. Correct mistakes by posting a new reversing transaction.';
END;
$$;

-- FOR EACH STATEMENT rather than FOR EACH ROW: a statement-level trigger fires
-- even when the UPDATE/DELETE matches zero rows, so an attempt is rejected
-- rather than silently succeeding as a no-op.
CREATE TRIGGER entries_no_update
    BEFORE UPDATE ON entries
    FOR EACH STATEMENT EXECUTE FUNCTION reject_mutation();

CREATE TRIGGER entries_no_delete
    BEFORE DELETE ON entries
    FOR EACH STATEMENT EXECUTE FUNCTION reject_mutation();

CREATE TRIGGER entries_no_truncate
    BEFORE TRUNCATE ON entries
    FOR EACH STATEMENT EXECUTE FUNCTION reject_mutation();

-- `transactions` gets the same protection. The spec only demands it for
-- entries, but transactions carry the hash chain, and an immutable chain built
-- on mutable rows is not evidence of anything.
CREATE TRIGGER transactions_no_update
    BEFORE UPDATE ON transactions
    FOR EACH STATEMENT EXECUTE FUNCTION reject_mutation();

CREATE TRIGGER transactions_no_delete
    BEFORE DELETE ON transactions
    FOR EACH STATEMENT EXECUTE FUNCTION reject_mutation();

CREATE TRIGGER transactions_no_truncate
    BEFORE TRUNCATE ON transactions
    FOR EACH STATEMENT EXECUTE FUNCTION reject_mutation();


-- -------------------------------------------------- zero-sum per currency --

-- Deferred, because at the moment the first entry row is inserted the
-- transaction is legitimately unbalanced. The set of entries belonging to a
-- transaction is only complete at COMMIT, so that is when we check.
CREATE FUNCTION assert_transaction_balanced() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE
    offending record;
BEGIN
    SELECT currency, sum(amount_minor) AS total
      INTO offending
      FROM entries
     WHERE transaction_id = NEW.transaction_id
     GROUP BY currency
    HAVING sum(amount_minor) <> 0
     LIMIT 1;

    IF FOUND THEN
        RAISE EXCEPTION
            'unbalanced_transaction: transaction % sums to % in %, expected 0',
            NEW.transaction_id, offending.total, offending.currency
            USING ERRCODE = '23514';
    END IF;

    RETURN NULL;
END;
$$;

CREATE CONSTRAINT TRIGGER entries_balanced_per_currency
    AFTER INSERT ON entries
    DEFERRABLE INITIALLY DEFERRED
    FOR EACH ROW EXECUTE FUNCTION assert_transaction_balanced();


-- A transaction with zero entries would never fire the trigger above, and one
-- with a single entry is not double-entry bookkeeping. Checked from the
-- transactions side so that neither case can slip through.
CREATE FUNCTION assert_transaction_has_entries() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE
    n bigint;
BEGIN
    SELECT count(*) INTO n FROM entries WHERE transaction_id = NEW.id;

    IF n < 2 THEN
        RAISE EXCEPTION
            'unbalanced_transaction: transaction % has % entr%, a double-entry transaction needs at least 2',
            NEW.id, n, CASE WHEN n = 1 THEN 'y' ELSE 'ies' END
            USING ERRCODE = '23514';
    END IF;

    RETURN NULL;
END;
$$;

CREATE CONSTRAINT TRIGGER transactions_have_entries
    AFTER INSERT ON transactions
    DEFERRABLE INITIALLY DEFERRED
    FOR EACH ROW EXECUTE FUNCTION assert_transaction_has_entries();
