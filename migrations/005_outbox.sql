-- 005_outbox.sql -- Phase 6: transactional outbox.
--
-- The problem this solves: "write to the ledger, then notify the webhook" is two
-- writes to two systems with no transaction spanning them. Whichever order you
-- pick, a crash in the middle is a lie. Notify first and the crash means you
-- announced a payment that never happened; write first and the crash means a
-- payment nobody was told about.
--
-- The outbox removes the second system from the critical path. The event is
-- inserted into *this* database, in the same transaction as the entries, so it
-- commits if and only if the ledger write commits. Delivery becomes a separate,
-- retryable problem against durable state.

CREATE TYPE outbox_status AS ENUM ('pending', 'delivered', 'dead');

CREATE TABLE outbox (
    -- bigserial, because relative order of events is meaningful and uuid has
    -- none. See ledger/services/outbox.py for what is and is not guaranteed
    -- about delivery order.
    id              bigserial     PRIMARY KEY,

    event_type      text          NOT NULL
                                  CHECK (length(event_type) BETWEEN 1 AND 100),
    payload         jsonb         NOT NULL,
    created_at      timestamptz   NOT NULL DEFAULT now(),

    delivered_at    timestamptz,
    attempts        int           NOT NULL DEFAULT 0 CHECK (attempts >= 0),

    -- Doubles as the retry schedule and as a delivery lease: the relay pushes
    -- this forward when it claims a row, so a relay that dies mid-delivery
    -- releases the event automatically once the lease lapses.
    next_attempt_at timestamptz   NOT NULL DEFAULT now(),

    status          outbox_status NOT NULL DEFAULT 'pending',

    -- Same biconditional style as holds_capture_link: a delivered event without
    -- a timestamp, or a timestamp on an undelivered event, are both nonsense.
    CONSTRAINT outbox_delivered_at_matches_status
        CHECK ((status = 'delivered') = (delivered_at IS NOT NULL))
);

-- The relay's only query: pending events whose retry time has arrived, oldest
-- first. Partial, so delivered events -- which will be almost all of them -- are
-- not in the index at all.
CREATE INDEX outbox_due_idx
    ON outbox (next_attempt_at, id)
    WHERE status = 'pending';

-- Supports the reconciliation check that every committed transaction produced an
-- event, which is what proves the dual write really was closed.
CREATE INDEX outbox_transaction_idx
    ON outbox ((payload ->> 'transaction_id'))
    WHERE event_type = 'transaction.posted';

-- Operational view: how many events are stuck, and how far behind.
CREATE INDEX outbox_status_idx ON outbox (status, id);


-- Transactions that predate this table have no event, and the reconciliation
-- check added in this phase would flag every one of them forever. Backfilling
-- them as `pending` would be worse: deploying this migration would fire a
-- webhook for every transaction in history.
--
-- So they are recorded as already delivered, and marked `backfilled` in the
-- payload so nobody later mistakes them for events that were genuinely sent.
-- This repo starts empty and the insert is a no-op here; it exists because a
-- migration that assumes an empty table is a migration that fails in the one
-- environment that matters.
INSERT INTO outbox (event_type, payload, status, delivered_at, attempts)
SELECT 'transaction.posted',
       jsonb_build_object(
           'transaction_id', t.id::text,
           'seq', t.seq,
           'backfilled', true
       ),
       'delivered',
       now(),
       0
  FROM transactions t
 ORDER BY t.seq;
