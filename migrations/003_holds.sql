-- 003_holds.sql -- Phase 3: authorization holds.
--
-- A hold is a promise, not a movement. It writes no entries. It reduces what an
-- account may *spend* without changing what the account *has*, which is why
-- balances stay derived from entries alone and the hold table only ever affects
-- the `available` figure.
--
-- The state machine is:
--
--     pending --> captured   (writes entries, may be partial)
--             --> voided     (writes nothing)
--             --> expired    (writes nothing)
--
-- Terminal states are terminal. Enforced by trigger below, not by convention.

CREATE TYPE hold_status AS ENUM ('pending', 'captured', 'voided', 'expired');

CREATE TABLE holds (
    id                      uuid        PRIMARY KEY,
    account_id              uuid        NOT NULL,

    -- The authorized ceiling. A capture may be for less, never for more.
    amount_minor            bigint      NOT NULL CHECK (amount_minor > 0),
    currency                char(3)     NOT NULL CHECK (currency ~ '^[A-Z]{3}$'),

    status                  hold_status NOT NULL DEFAULT 'pending',
    expires_at              timestamptz NOT NULL,
    captured_transaction_id uuid,
    created_at              timestamptz NOT NULL DEFAULT now(),

    -- Same composite foreign key trick as `entries`: a CAD hold cannot sit on a
    -- USD account, structurally.
    FOREIGN KEY (account_id, currency) REFERENCES accounts (id, currency),

    -- Biconditional, deliberately. `captured` without a transaction would be a
    -- capture that moved no money; a transaction link on a voided or expired
    -- hold would be a movement nobody authorized. Both are forbidden by the
    -- same line.
    CONSTRAINT holds_capture_link
        CHECK ((status = 'captured') = (captured_transaction_id IS NOT NULL))
);

-- DEFERRABLE because capture writes the hold's terminal state and the
-- transaction that realises it in one statement each, and the hold update has to
-- come first: the overdraft check inside the transaction write reads `holds`, and
-- it must no longer see this hold as pending or it would count the same money
-- twice. Deferring the foreign key lets the hold point at a transaction that
-- does not exist yet but will before COMMIT.
ALTER TABLE holds
    ADD CONSTRAINT holds_captured_transaction_id_fkey
    FOREIGN KEY (captured_transaction_id) REFERENCES transactions (id)
    DEFERRABLE INITIALLY DEFERRED;

-- Serves the available-balance query: SUM(amount_minor) for one account's live
-- holds. Partial, so terminal holds -- which will be the overwhelming majority
-- over time -- are not in the index at all.
CREATE INDEX holds_active_by_account_idx
    ON holds (account_id, expires_at) INCLUDE (amount_minor)
    WHERE status = 'pending';

-- Serves the expiry sweeper, which scans by deadline across all accounts.
CREATE INDEX holds_expiry_idx
    ON holds (expires_at)
    WHERE status = 'pending';


-- =========================================================================
-- State machine enforcement
-- =========================================================================

CREATE FUNCTION assert_hold_transition() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF OLD.status <> 'pending' THEN
        RAISE EXCEPTION
            'invalid_hold_transition: hold % is already % and cannot become %',
            OLD.id, OLD.status, NEW.status
            USING ERRCODE = '23514',
                  HINT = 'Terminal hold states are final. Issue a new hold instead.';
    END IF;

    -- Everything except the two state columns is immutable. Without this, the
    -- authorized amount could be raised after the fact, which is the hold
    -- equivalent of editing a signed cheque.
    --
    -- Checked before the "must actually transition" rule below so that an
    -- attempt to edit the amount reports *that*, rather than the less specific
    -- complaint that the status did not change.
    IF NEW.id         <> OLD.id
    OR NEW.account_id <> OLD.account_id
    OR NEW.amount_minor <> OLD.amount_minor
    OR NEW.currency   <> OLD.currency
    OR NEW.expires_at <> OLD.expires_at
    OR NEW.created_at <> OLD.created_at THEN
        RAISE EXCEPTION
            'invalid_hold_mutation: only status and captured_transaction_id may change on hold %',
            OLD.id
            USING ERRCODE = '23514';
    END IF;

    IF NEW.status = 'pending' THEN
        RAISE EXCEPTION
            'invalid_hold_transition: hold % cannot be updated while staying pending',
            OLD.id
            USING ERRCODE = '23514';
    END IF;

    -- An authorization that has lapsed cannot be turned into a movement of
    -- money, even by a code path that forgot to check. now() is the transaction
    -- start time, so this agrees with whatever the caller read a moment ago.
    IF NEW.status = 'captured' AND OLD.expires_at <= now() THEN
        RAISE EXCEPTION
            'invalid_hold_transition: hold % expired at % and cannot be captured',
            OLD.id, OLD.expires_at
            USING ERRCODE = '23514';
    END IF;

    RETURN NEW;
END;
$$;

CREATE TRIGGER holds_transition
    BEFORE UPDATE ON holds
    FOR EACH ROW EXECUTE FUNCTION assert_hold_transition();

-- Holds are part of the audit trail: an authorization that was placed and then
-- voided is a thing that happened. Rows are never removed.
CREATE TRIGGER holds_no_delete
    BEFORE DELETE ON holds
    FOR EACH STATEMENT EXECUTE FUNCTION reject_mutation();

CREATE TRIGGER holds_no_truncate
    BEFORE TRUNCATE ON holds
    FOR EACH STATEMENT EXECUTE FUNCTION reject_mutation();
