-- 002_idempotency.sql -- Phase 2: exactly-once semantics for write endpoints.
--
-- The table is deliberately boring. What makes idempotency work is not the
-- schema, it is that the row reservation and the business write happen in one
-- transaction (see ledger/services/idempotency.py). The unique primary key is
-- the whole concurrency-control mechanism.

CREATE TABLE idempotency_keys (
    key           uuid        PRIMARY KEY,

    -- SHA-256 of the canonical fingerprint of the request. Lets us tell a
    -- genuine retry (same key, same body -> replay) apart from a client bug
    -- (same key, different body -> 409).
    request_hash  bytea       NOT NULL CHECK (octet_length(request_hash) = 32),

    -- The response the original request returned, replayed verbatim on retry.
    --
    -- NULL has exactly one meaning in a *committed* row: there is no stored
    -- response for this key. That state is unreachable through the service --
    -- the reservation and the response are written in the same transaction, so
    -- a committed row always has both -- and is reserved for rows created by
    -- the backfill below, or by a future retention job that reclaims response
    -- payloads without dropping the authorization record.
    response_body jsonb,
    status_code   int         CHECK (status_code BETWEEN 100 AND 599),

    created_at    timestamptz NOT NULL DEFAULT now()
);

-- A retention job would prune by age; index accordingly.
CREATE INDEX idempotency_keys_created_at_idx ON idempotency_keys (created_at);


-- Every transaction must trace back to a recorded client request. Phase 1 wrote
-- transactions.idempotency_key with nothing to point at; now it points at the
-- authorization record.
--
-- Backfill first: this repo starts empty, but a migration that assumes that is
-- a migration that fails in the one environment that matters. Orphans get a
-- placeholder request_hash and no stored response, so a replay of a backfilled
-- key is refused rather than silently re-executed.
INSERT INTO idempotency_keys (key, request_hash, response_body, status_code)
SELECT t.idempotency_key, sha256('backfilled-by-002'::bytea), NULL, NULL
  FROM transactions t
 WHERE NOT EXISTS (
        SELECT 1 FROM idempotency_keys k WHERE k.key = t.idempotency_key
       );

ALTER TABLE transactions
    ADD CONSTRAINT transactions_idempotency_key_fkey
    FOREIGN KEY (idempotency_key) REFERENCES idempotency_keys (key);
