# Decisions

A running log, appended as each phase is built. Each entry is a decision that
had a real alternative, with the reason the alternative lost.

---

## Phase 1 — Core ledger

### 1.1 Raw SQL over psycopg 3, no ORM

Chosen: raw SQL through `psycopg`, with `autocommit=True` on every pooled
connection and literal `BEGIN` / `COMMIT` / `ROLLBACK` issued by
`ledger/db.py:transaction()`.

Rejected: SQLAlchemy ORM sessions, and also SQLAlchemy Core's implicit
`begin()`-on-first-use behaviour.

Reason: in autocommit mode psycopg does not open transactions on our behalf, so
the only transaction boundaries in the process are the ones written in
`db.py`. The recurring bug in ledger systems is a write that commits in a
different transaction than the check which authorised it. With this setup that
bug is visible in a diff — a second `with transaction()` inside a handler is an
obvious smell — rather than hidden in a session's autoflush behaviour.

### 1.2 `entries.amount_minor` is a signed BIGINT, not a debit/credit pair

Chosen: one signed integer column. Negative is a debit, positive is a credit.

Rejected: separate `debit_minor` / `credit_minor` columns with a CHECK that
exactly one is non-null (the traditional accounting layout).

Reason: the invariant we care about most is "sums to zero", and with a signed
column that is literally `SUM(amount_minor) = 0`. With a debit/credit pair it
becomes `SUM(debit) = SUM(credit)`, which needs `COALESCE` on both sides and
gives you a second way to be wrong (both columns null, both non-null). The
accounting-facing debit/credit view is a presentation concern and can be derived
with `CASE WHEN amount_minor < 0`.

### 1.3 Zero-sum is enforced by a DEFERRED constraint trigger

Chosen: `CREATE CONSTRAINT TRIGGER ... DEFERRABLE INITIALLY DEFERRED` on
`entries`, which runs at `COMMIT`.

Rejected: (a) an immediate row-level trigger, (b) application-layer checking
only, (c) a CHECK constraint.

Reason: (a) cannot work — after the first entry insert the transaction is
legitimately unbalanced, so an immediate trigger would reject every valid write.
The set of entries belonging to a transaction is only complete at commit, so
commit is the only correct time to check. (c) is impossible: a CHECK constraint
cannot see other rows. (b) is what the application does *as well*, so the common
client error gets a clear 422 without touching the database — but a rule that
only exists in application code is one forgotten call site away from being
absent. `tests/test_phase1_db_guards.py` writes raw SQL specifically to prove the
database still refuses.

The trigger is also why zero-sum is checked **per currency** with a `GROUP BY`.
Summing across currencies would let `+100 USD` and `-100 CAD` look balanced,
which is a money printer.

### 1.4 Entry/account currency agreement is a foreign key, not a check

Chosen: `UNIQUE (id, currency)` on `accounts`, and
`FOREIGN KEY (account_id, currency) REFERENCES accounts (id, currency)` on
`entries`.

Rejected: a trigger comparing `entries.currency` to the parent account's
currency.

Reason: it costs one redundant unique index and turns "an entry's currency
always matches its account" from a rule that runs into a shape that cannot be
expressed wrongly. There is no code path, trigger order, or `session_replication_role`
setting under which a USD entry can reference a CAD account, because no such
parent row exists. The application still checks first, only so the client gets
`currency_mismatch` naming the account instead of a foreign-key violation.

### 1.5 Append-only is a statement-level trigger, and it covers `transactions` too

Chosen: `BEFORE UPDATE / DELETE / TRUNCATE ... FOR EACH STATEMENT` on both
`entries` and `transactions`, raising `append_only_violation`.

Rejected: row-level triggers; revoking UPDATE/DELETE privileges instead.

Reason: statement-level fires even when the statement matches zero rows, so
`DELETE FROM entries WHERE id = 999` is an error rather than a silent no-op —
the attempt is what we want to refuse. Privilege revocation was rejected because
the application connects as the table owner in every environment I actually
have, and an owner can bypass grants; a trigger stops the owner too.

Extending it to `transactions` goes beyond the spec, which only asked for
`entries`. The reason is 1.7: an immutable hash chain stored in mutable rows is
not evidence of anything.

**What this does not buy:** the table owner can `ALTER TABLE ... DISABLE TRIGGER`
and then rewrite whatever they like. `tests/conftest.py:reset_database()` does
exactly that, on purpose, so the limit is visible in the codebase rather than
implied. Prevention stops the application; the hash chain is what detects the
administrator.

### 1.6 Balances are derived; the cache is never read on the write path

Chosen: `GET /balance` and the overdraft check both compute `SUM(entries)`.
`account_balances` is maintained in the same transaction as the entry insert,
and is only ever read by `GET /reconciliation`.

Rejected: reading the cache for balance display (fast, and wrong in a way nobody
notices until it matters).

Reason: if the cache is consulted anywhere a decision is made, then a bug in
cache maintenance becomes a bug that authorises payments. Keeping it
write-only-plus-reconciliation means the worst outcome of such a bug is a
reconciliation failure, which is an alert, not a loss.
`test_balance_is_derived_from_entries_not_the_cache` corrupts the cache directly
and asserts the reported balance does not move.

The cache is kept for two reasons that are not "reading balances quickly": it
gives `/reconciliation` something real to check, and it gives the pessimistic
strategy in Phase 4 exactly one lockable row per account.

### 1.7 The hash chain gets an explicit `seq`, and it is a serialization point

Chosen: added `seq BIGSERIAL UNIQUE` to `transactions`. `prev_hash` is also
`UNIQUE`, with genesis set to 32 zero bytes rather than `NULL`.

Rejected: ordering by `created_at` (ties, and clock adjustments can move it
backwards); pointer-walking `prev_hash` alone (head lookup becomes an anti-join).

Reason for the zero-byte genesis: Postgres permits unlimited `NULL`s in a unique
index, so `prev_hash UNIQUE` with a `NULL` genesis would allow many genesis rows
and therefore many parallel chains. A concrete sentinel makes `UNIQUE(prev_hash)`
actually mean "no transaction has two successors", i.e. history is a line and not
a tree. `test_hash_chain_cannot_fork` asserts this.

The consequence is deliberate and is the most important performance fact about
this service: **appending to the chain is globally serialized.** Two writers read
the same head, both compute the same `prev_hash`, and the unique index rejects
one of them with `23505`. `ledger/db.py` classifies that specific constraint name
as retryable, so the loser replays against the new head.

I chose retry-on-conflict over wrapping the append in `pg_advisory_xact_lock`.
The advisory lock converts the retry into a queue, which is nicer, but it does
not raise the ceiling — one transaction per chain position either way — and it
would mask the per-account contention that Phase 4 is built to measure. Phase 4
reports the ceiling as a number instead of hiding it.

### 1.8 Canonical serialization is a bespoke text format, not JSON

Chosen: newline-delimited `LEDGER-TX-V1` text, documented in
`ledger/hashing.py`, with entries sorted by `(account_id, currency, amount)`.

Rejected: `json.dumps(..., sort_keys=True)`.

Reason: JSON has no single canonical encoding. Key order, whitespace, unicode
escaping and integer rendering are all implementation-defined, so two correct
JSON serializers can hash the same value differently — which means a verifier
written in another language could report a false break. The text format has
exactly one valid encoding per input and is readable in a terminal.

Entry surrogate `id`s are deliberately excluded from the hash: they come from a
database sequence and carry no economic meaning. What is hashed is the multiset
of `(account, currency, amount)` triples plus the transaction id, timestamp and
predecessor hash.

`created_at` is generated in Python rather than by `DEFAULT now()`, because the
writer has to know the exact microsecond value it is hashing. Sessions are pinned
to UTC in `db.py` for the same reason.

### 1.9 Pagination is keyset, not OFFSET

Chosen: `WHERE id > :cursor ORDER BY id LIMIT n` on `entries`.

Rejected: `LIMIT/OFFSET`.

Reason: `entries` is append-only and grows under a reader. With OFFSET, rows
inserted during a walk shift the window, so a client paging through history
re-reads or skips rows. A cursor on a monotonic primary key is stable under
concurrent inserts and stays O(log n) at page ten thousand.

### 1.10 Money enters only through `external_settlement`

Chosen: one `external_settlement` account per currency, enforced by a partial
unique index, and it is the only account type (along with `platform_revenue` and
`liquidity`) permitted to hold a negative balance.

Reason: this is what makes "the global ledger sums to zero" true rather than
aspirational. Funding a user is a transfer, not a creation: settlement goes
negative by exactly what users hold. A nonzero global sum is then an
unambiguous signal that something is broken, which is the single cheapest
correctness check in the system — one query, no parameters.

### 1.11 Deferred to later phases, on purpose

`POST /transactions` in Phase 1 dedups only via the `UNIQUE` constraint on
`transactions.idempotency_key`, so a retry gets `409` instead of a replay. There
is no overdraft check yet, and `held_minor` is hard-coded to `0`. Those are
Phase 2 and Phase 3; the tests assert the Phase 1 behaviour so the change is
visible when it lands.

---

## Phase 2 — Idempotency

### 2.1 Insert-first, not check-then-insert

Chosen:

```sql
INSERT INTO idempotency_keys (key, request_hash) VALUES (?, ?)
ON CONFLICT (key) DO NOTHING
```

as the *first* statement of the same transaction that does the business write.
`rowcount = 1` means we own the key and should do the work; `rowcount = 0` means
someone else owns it and we should replay their response.

Rejected: `SELECT ... WHERE key = ?`, then branch, then `INSERT`.

Reason: the select-then-insert version has a window between the two statements.
Under concurrent retries — which is the *only* situation this feature exists for
— both requests see "no row", both decide to process, and both write. Making the
insert itself the check removes the window, because the unique index is the
arbiter and exactly one insert can win. There is no application-level
coordination, no lock manager, and no state machine.

### 2.2 The loser blocks rather than polls, and that is Postgres doing it

`INSERT ... ON CONFLICT DO NOTHING` against a row inserted by an *uncommitted*
transaction does not return immediately. It blocks on that transaction's xid.
So under READ COMMITTED the second request waits, and then observes a settled
outcome:

* owner committed → the conflict is real, read the stored response, replay it.
* owner rolled back → no conflicting row remains, our insert wins, we process.

This is why the concurrency test can assert *exactly* one processed and one
replayed rather than "one processed and one got some error". It is also why there
is no "in flight, come back later" retry loop in the code.

`test_concurrent_claim_blocks_until_the_owner_commits` asserts the mechanism
directly rather than inferring it from the outcome: it holds the owning
transaction open, asserts the contender has *not* returned after a second, reads
`pg_stat_activity` to confirm the contender is genuinely waiting on a lock, then
commits and asserts the contender wakes up and inserts nothing.

### 2.3 A failed request does not consume its key

Because the reservation and the business write are one transaction, a failure
rolls back both. There is no committed `idempotency_keys` row for a request that
did not succeed, so a client that fixes its payload can reuse the key.

Rejected: Stripe's behaviour, where an error response is also recorded against
the key and replayed on retry.

Reason: recording the error requires committing the key row in a transaction
*separate* from the one that failed. That is a dual write, and it reintroduces
exactly the atomicity problem this phase exists to remove — now with the twist
that a crash between the two writes leaves a key that permanently refuses a
payment that never happened. Keeping it to one transaction means the failure mode
is "retry re-executes", which for a deterministic error just fails again
identically, and for a transient error is what you actually want.

The cost, stated plainly: an idempotency key does not protect against a client
retrying a request that failed *validation* and succeeding the second time
because account state changed in between. That is a real difference from Stripe
and it is a deliberate trade.

A consequence worth noting: `response_body IS NULL` on a *committed* row is
therefore unreachable through the service. That state is reserved for rows
written by the 002 backfill, or by a future retention job. Both are treated the
same way — refuse to replay rather than guess — because re-executing is the one
outcome that could double-write.

### 2.4 The fingerprint is hand-written per request type

Chosen: each request model implements `fingerprint()` returning a canonical
dict, hashed with sorted-key JSON.

Rejected: hashing the raw HTTP body; hashing `model_dump_json()`.

Reason for not using the raw body: whitespace and key order would make two
byte-different encodings of the same request look like a conflict. Reason for not
using `model_dump_json()`: its output depends on model field *declaration* order,
so reordering fields in a future refactor would silently invalidate every stored
fingerprint.

Two things the hand-written version buys that neither alternative does:

1. **The operation is part of the identity.** `{"op": "post_transaction", ...}`
   means one key cannot be used for both `POST /transactions` and
   `POST /holds/{id}/capture`. A generic body hash would let a key cross
   endpoints.
2. **Normalisation is a per-field decision.** Entry order in a transaction is
   semantically meaningless — the hash chain sorts entries too — so the
   fingerprint sorts them, and a client that retries with its legs reordered
   gets a replay instead of a spurious 409. Sorting preserves the multiset, so no
   two genuinely different requests can collide as a result. Blanket sorting of
   every list in every model would be wrong for some future field where order
   matters, which is why this is opt-in rather than automatic.

`test_reordered_entries_are_the_same_request` and
`test_swapped_direction_is_a_different_request` pin both halves of that.

### 2.5 A replay returns the original status code

Chosen: the stored `status_code` (201 for a created transaction), with
`replayed: true` added to the body. The route returns a raw `JSONResponse` so the
stored body is echoed rather than re-serialised through the response model.

Rejected: 200 on replay.

Reason: the client's question is "did my request happen", and the truthful answer
is the answer the first attempt gave. `replayed` is what lets the caller tell the
two apart, and it is additive so it cannot break a client that ignores it.
Echoing the stored bytes also means replay stays faithful if the response model
later gains a field — the old response does not silently acquire a new key with a
default value.

### 2.6 `transactions.idempotency_key` is a real foreign key

Chosen: `FOREIGN KEY (idempotency_key) REFERENCES idempotency_keys (key)`, with
migration 002 backfilling orphans before adding the constraint.

Reason: it makes "no transaction exists that no client asked for" structural
rather than conventional. Since the reservation is inserted first in the same
transaction, the constraint is free at write time.

The cost, which is real: idempotency keys are conventionally expired (Stripe
drops them after 24h), and the foreign key means the row cannot be deleted while
a transaction references it. The resolution is to prune the *payload* rather than
the row — a retention job would set `response_body` and `status_code` to NULL
while keeping `key` and `request_hash` as the permanent authorization record. A
replay after pruning then hits the "no stored response" path and is refused,
which is the correct answer for a key that has aged out. That job is not built
(it is outside the specified scope) but the schema and the replay path already
accommodate it, which is why the NULL case is handled rather than asserted away.

---

## Phase 3 — Holds

### 3.1 Availability excludes lapsed holds, so the sweeper is not load-bearing

Chosen:

```sql
SELECT SUM(amount_minor) FROM holds
 WHERE account_id = ? AND status = 'pending' AND expires_at > now()
```

Rejected: `WHERE status = 'pending'` alone, relying on the expiry worker to move
lapsed holds to `expired`.

Reason: this is the most important decision in the phase. If the availability
query trusted `status` alone, the background worker would be the thing that
releases customer money — and an outage in a background worker would silently
freeze funds, with no error anywhere and no failing invariant. Putting
`expires_at` in the predicate means a hold stops reserving funds at the instant it
lapses, whether or not any job has run.

What the sweeper is *for*, then: keeping the partial indexes small and making the
`status` column mean what it says. It writes no entries and changes no numbers.
`test_a_lapsed_hold_stops_reserving_funds_before_any_sweep` asserts the row still
reads `pending` while the money is already available, and
`test_the_sweeper_relabels_lapsed_holds` asserts the sweep changes labels only.

The general principle: a background job may fix up representation, never
correctness.

### 3.2 The hold is retired *before* the capture entries are written

Chosen order inside the capture transaction:

1. `SELECT ... FOR UPDATE` the hold, assert `pending` and not lapsed
2. `UPDATE holds SET status = 'captured', captured_transaction_id = <new uuid>`
3. lock accounts, check overdraft, append the transaction

Rejected: writing the entries first and closing the hold afterwards.

Reason: the overdraft check in step 3 reads live holds. If this hold were still
`pending` at that moment, the check would count the reserved amount *and* the
debit that consumes it — the same money subtracted twice — so a full capture
against a fully-reserved account would be rejected. Retiring first is what makes
the arithmetic right.

It also turns the overdraft check into a genuine safety net rather than a
constraint. With the hold retired, available rises by the full authorized amount
`H`, and the capture is for `C ≤ H`. Written out: if `available = actual − held ≥ 0`
before, then after retiring `available' = available + H ≥ H ≥ C`, so the check
provably cannot fail for a valid capture. If it ever does fire, an invariant is
broken elsewhere and the exception is the alarm.

`test_capture_provably_cannot_overdraft` pins the boundary case: every cent held,
available exactly zero, full capture succeeds.

This ordering is why `holds.captured_transaction_id` has a **DEFERRABLE** foreign
key. Step 2 points the hold at a transaction that does not exist until step 3.
Deferring the constraint to COMMIT lets the terminal state and its link be written
in one statement, so there is no instant — even mid-transaction — where a hold is
`captured` with a null link.

### 3.3 `captured` ⟺ `captured_transaction_id IS NOT NULL`, as one CHECK

```sql
CONSTRAINT holds_capture_link
    CHECK ((status = 'captured') = (captured_transaction_id IS NOT NULL))
```

A biconditional rather than two one-way checks. `captured` with no link is a
capture that moved no money; a link on a `voided` or `expired` hold is a movement
nobody authorized. One line forbids both, and there is no third case to forget.

### 3.4 The captured amount is derived, not stored

Chosen: `captured_amount_minor` comes from `-SUM(entries.amount_minor)` over the
capture transaction's entries on the held account. `released_amount_minor` is
`amount_minor - captured_amount_minor`.

Rejected: adding a `captured_amount_minor` column.

Reason: same argument as balances (1.6). A stored copy is a second source of truth
that can drift from the entries.

This is *why* a capture may not credit the account the hold is against. If the
held account appeared as both a debit and a credit in the same transaction, the
sum over that account would net the two and the captured amount would be
unrecoverable. Forbidding it keeps the derivation unambiguous, and the error says
so.

### 3.5 Partial capture spends the whole authorization

Capturing 3,000 against a 5,000 hold leaves no 2,000 hold behind. The hold becomes
`captured` and the remaining 2,000 is released.

Rejected: reducing the hold's amount and leaving it `pending` for a second capture.

Reason: the hold's amount is immutable (3.6), and "one authorization, at most one
capture" is far easier to reason about than a partially-consumed hold with a
mutable remaining balance. Card networks work this way too. A merchant who needs
to capture twice needs two authorizations.

### 3.6 Terminal states and immutable fields are enforced by trigger

`assert_hold_transition()` rejects, in this order: any update to a hold that has
already left `pending`; any change to `id`, `account_id`, `amount_minor`,
`currency`, `expires_at` or `created_at`; any update that leaves the status
`pending`; and capturing a hold whose deadline has passed.

The immutability check is ordered *before* the "must actually transition" check so
that an attempt to edit the authorized amount reports `invalid_hold_mutation`
rather than the less specific complaint that the status did not change. The first
version had these the other way round and a test caught it.

Raising the authorized amount after the fact is the hold equivalent of editing a
signed cheque, so it is refused at the database level, not just in the service.
`holds` also gets the DELETE/TRUNCATE guard: an authorization that was placed and
then voided is a thing that happened.

The expiry rule uses `now()`, which in Postgres is transaction start time, so it
agrees with whatever the caller read a moment earlier in the same transaction
rather than racing a statement clock.

### 3.7 Lock ordering: holds before accounts

Capture locks its hold row, then account rows in ascending id order. Plain
transfers lock only accounts. Voids lock only a hold. With a single global
ordering no cycle can form, so there is no deadlock. Written down because it is
exactly the kind of rule the next feature violates if it is not.

### 3.8 The sweeper uses `FOR UPDATE SKIP LOCKED`

So it never blocks a capture that is mid-flight on the same row — it leaves that
hold for the next pass. Batched, so one pass cannot lock an unbounded number of
rows.

`test_the_sweeper_does_not_block_a_concurrent_capture` asserts it. An earlier
version of that test failed for an unrelated reason worth recording: the helper
that back-dates a hold used `ALTER TABLE ... DISABLE TRIGGER`, which takes an
ACCESS EXCLUSIVE lock on the whole table and blocked the sweeper's *read*. The
helper now uses `SET LOCAL session_replication_role = 'replica'`, which suppresses
triggers for the transaction without any table lock.

### 3.9 `POST /holds/{id}/void` requires an Idempotency-Key

The endpoint list in the spec does not mark this one as requiring a key, but the
stated invariant is that every write endpoint does. I required it.

Reason: without a key, a client whose void request times out and retries gets
`409 hold_not_pending` and cannot tell whether its own first attempt succeeded or
somebody else voided the hold. With a key it replays the original response, which
is the answer it actually wants.

### 3.10 Capture destination is supplied by the capture request

The `holds` table as specified has no destination account, so the destination has
to come from somewhere. The capture request carries a list of credit legs that
must sum to the captured amount, which handles the marketplace case (split
between merchant and platform revenue) in one transaction.

The known weakness, stated plainly: **the destination is not authorized at hold
time.** Whoever can capture a hold chooses where the money lands. Putting
`destination_account_id` on `holds` would fix that and is the stronger design for
a real system; it was not chosen because it changes the specified schema and
forecloses the fee-split case without a second transaction.
