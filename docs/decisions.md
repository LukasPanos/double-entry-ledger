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

---

## Phase 4 — Concurrency, two ways

### 4.1 The strategies differ in exactly one function

`acquire_accounts()` in `ledger/services/transactions.py` is the whole
difference:

| | pessimistic | optimistic |
|---|---|---|
| isolation | READ COMMITTED | SERIALIZABLE |
| account read | `SELECT … FOR UPDATE` ordered by id | plain `SELECT` |
| conflict handling | writers queue | DB aborts one, we replay it |

Everything downstream — the overdraft check, the append, the balance cache, the
hash chain — is byte-for-byte identical. That was a deliberate constraint on the
design: if the two paths diverged in several places, the benchmark would be
comparing two implementations rather than two concurrency-control disciplines,
and no conclusion could be drawn.

Every correctness test in `test_phase4_concurrency.py` is parameterised over both
strategies, because comparing the performance of a correct implementation against
a subtly broken one is worthless.

### 4.2 `ORDER BY` in the lock query is load-bearing, and it is asserted

`lock_accounts` sorts account ids and relies on Postgres putting the `LockRows`
node at the *top* of the plan, so rows are locked in the order the plan emits
them. Without a single global order, transaction A holding account 1 and wanting
2 while B holds 2 and wants 1 is a deadlock.

`test_the_lock_query_locks_rows_in_sorted_order` asserts the plan shape via
EXPLAIN rather than trusting the documentation. Worth recording: the first
version of that test asserted a `Sort` node and **broke** once the test database
had enough rows for the planner to prefer an ordered index scan on
`account_balances_pkey` instead. Both shapes are correct — ordering is
established below `LockRows` either way — so the test now asserts the invariant
(LockRows is the root, ordering comes from a Sort *or* an ordered pkey scan)
rather than one incidental plan.

### 4.3 Only chain-conflict unique violations are retryable

`conflict_kind()` returns the *reason* a transaction can be retried, not a
boolean. `40001` and `40P01` are always retryable. `23505` is retryable **only**
when `diag.constraint_name` is one of the hash-chain constraints.

This is the most dangerous place in the codebase to be sloppy. A blanket "retry
all unique violations" would retry an idempotency-key collision, which would
defeat Phase 2 entirely — the retry would find the key already claimed and could
double-process. `test_idempotency_collisions_are_never_retried` provokes a real
collision and asserts `conflict_kind()` returns `None` for it.

Returning the reason rather than a boolean is what makes 4.5 possible.

### 4.4 What the benchmark actually measures

The driver calls the service layer directly from a thread pool: no HTTP, no
uvicorn, no event loop. The question is about database contention, and a web
stack in the path would add scheduling noise unrelated to row locks. These are
therefore **not** end-to-end API latencies; they are transaction latencies, which
is the part the strategy choice controls. Stated on the chart itself so the
number cannot be misquoted.

The ledger is truncated and rebuilt before every configuration. Without that,
runs later in the sweep would carry every earlier run's entries, and the
per-payer `SUM(entries)` in the overdraft check would get steadily more
expensive — so whichever strategy was measured last would look worse for a
reason that has nothing to do with the strategy. Warmup transactions run outside
the timed window for the same reason: first-call overhead would otherwise land
entirely in the concurrency-1 column.

`assert_reconciled()` runs after every single configuration. A benchmark that
leaves the books wrong has measured nothing worth knowing.

### 4.5 The result, and the finding I did not expect

Measured on PostgreSQL 16, 600 transactions per point (`docs/hot-account-benchmark.json`):

| workload | strategy | 1 writer | 32 writers | p95 @ 32 | conflicts @ 32 |
|---|---|---|---|---|---|
| shared hot account | pessimistic | 1070 tps | **1202 tps** | 32 ms | **0** |
| shared hot account | optimistic | 1139 tps | 526 tps | 399 ms | 489 serialization failures |
| no shared account | pessimistic | 1147 tps | 529 tps | 348 ms | 771 chain conflicts |
| no shared account | optimistic | 1013 tps | 493 tps | 420 ms | 571 serialization failures |

Pessimistic locking wins the hot-account case decisively: 2.3× the throughput and
12× better p95. That much was expected — a queue does no wasted work, whereas
every optimistic abort throws away a transaction that had already done its reads.
The bimodal latency is the visible signature of that: optimistic p50 stays at
~1 ms while p95 blows out to 399 ms, because the winners are fast and the losers
pay for a full replay.

Throughput on a single writer is a fair fight (1070 vs 1139, optimistic slightly
ahead) because the pessimistic path does one extra thing: it joins
`account_balances` to take the lock, where the optimistic path reads `accounts`
alone. That cost is inherent to the strategy — you cannot lock a row you do not
select — so it stays in the measurement rather than being normalised away.

The unexpected result is in the last two rows. **Pessimistic throughput on the
hot account (1202 tps) is 2.3× its throughput with no shared account at all
(529 tps).** Sharing a row made it faster.

The reason is 1.7. Appending to the hash chain is a global serialization point:
every writer reads the same chain head and `UNIQUE(prev_hash)` rejects all but
one. In the hot-account workload the row lock on the shared fee account
*incidentally orders the chain appends too* — writers queue on the account, so
they reach the chain one at a time and never collide. The retry counter proves it:
**zero conflicts of any kind, at every concurrency level.** Remove the shared
account and nothing imposes that order, so the same strategy records 771 chain
conflicts and loses 56% of its throughput.

This is why `conflict_kind()` returns a reason. Throughput alone cannot tell you
*what* the writers were fighting over; the per-kind breakdown can, and it turns a
surprising number into an explained one.

The honest reading: this service has two serialization points, the hot account
row and the chain head, and the benchmark measures both. The chain is the harder
ceiling of the two, because unlike the account it cannot be sharded away — it is
one linked list by construction. The mitigation is 4.6.

### 4.6 What I would change to raise the ceiling

Not built, because Phase 4 asked for two strategies and this would be a third,
but this is where the next work goes:

1. **Advisory lock around the chain append.** `pg_advisory_xact_lock` converts
   chain conflicts into queueing, which the numbers above show is strictly
   cheaper than aborting. It does not raise the ceiling — still one transaction
   per chain position — but it removes the wasted work. I left it out of Phase 1
   precisely because it would have masked the per-account contention this
   benchmark exists to isolate.
2. **Periodic sealing instead of per-transaction chaining.** Hash batches of
   transactions on a timer rather than chaining each one. Tamper evidence becomes
   coarser — you learn which batch was altered, not which transaction — in
   exchange for removing the global serialization point entirely. This is the
   real fix, and it is a genuine trade of evidence granularity for throughput.
3. **Conflict-free balance cache.** The cached balance row is what the optimistic
   strategy fights over. Replacing the running total with append-only deltas
   aggregated lazily would remove that conflict for credit-only accounts (a fee
   account is never debited, so it needs no overdraft check and therefore no
   serialized read).

### 4.7 Reconciliation runs in one snapshot

The whole report is assembled inside a single `REPEATABLE READ READ ONLY`
transaction.

Rejected: a check-per-transaction loop.

Reason: with a snapshot per check, a transaction committing between the
global-sum check and the per-account check would make the two disagree, and the
report would fail on a perfectly healthy ledger. A reconciliation tool that
produces false alarms under load is worse than no tool, because people learn to
ignore it. Read-only at REPEATABLE READ also means the report can never abort and
never blocks a writer.

`test_reconciliation_does_not_cry_wolf_during_concurrent_writes` runs
reconciliation in a loop for three seconds while a writer hammers the ledger, and
asserts every report passes.

### 4.8 Every check is a query that must return zero rows

Rather than computing a number and comparing it to another number. "This query
returns nothing" has exactly one passing state and needs nobody to decide what
"close enough" means. When a check fails it returns the offending rows, so the
report names the account or transaction rather than just asserting that something
somewhere is wrong.

**Every check is tested against a deliberately corrupted database.** A
reconciliation suite that has only ever been run against a healthy ledger might
be ten queries that can never fail. `tests/factories.corrupt()` forges damage
with `session_replication_role = 'replica'`, which is the same door a database
administrator has.

One check turned out to be unprovokable, and that is recorded rather than hidden:
`non_captured_holds_have_no_transaction` cannot be made to fail, because
`session_replication_role` suppresses *triggers* but a CHECK constraint is not a
trigger, so `holds_capture_link` refuses the write anyway. The check is kept — it
costs one indexed scan, and if a future migration relaxes the CHECK, the report is
where that regression should surface. The test asserts the database's refusal
instead.

### 4.9 A bug the corruption tests found

`SUM(bigint)` is `numeric` in Postgres, which psycopg returns as `Decimal`. The
first version of the report's row serialiser fell through to `str()` for anything
that was not an `int`, `float` or `bool` — so every monetary total in a failure
report came out as a JSON *string*: `"total_minor": "1000000"`.

Nothing in the happy path noticed, because a passing check has no failure rows.
It only surfaced once the tests started asserting on the *contents* of a failure.
The fix converts `Decimal` explicitly.

The SQL deliberately still does not cast the sums back to `bigint`: a sum over
many rows can exceed int64 even when every individual row fits, and an overflow
error inside the reconciliation path would take out the tool you use to diagnose
problems. The widening stays; the conversion happens in Python, where the values
are known to be whole numbers of minor units.

### 4.10 `/integrity` arrived early

It belongs to Phase 7, but one of the reconciliation checks the spec requires is
"hash chain intact", so the chain walk had to exist now. Phase 7 adds the chaos
runner and the Hypothesis properties, not the endpoint.

Two things about the walk that needed thought:

* **`seq` gaps are not breaks.** `seq` is a `bigserial` and a rolled-back
  transaction still consumes its value, so checking for contiguity would report a
  failure every time a request was rejected. The walk only compares adjacent
  committed rows.
* **`seq` order cannot disagree with chain order.** For B to store A's hash as
  its `prev_hash`, B must have read A's committed row, so A called `nextval`
  first and `A.seq < B.seq`. Sorting by `seq` therefore reconstructs the chain
  exactly.

`test_editing_a_hashed_field_breaks_the_chain` is the one that matters most: it
shifts a `created_at` by a day, which is hashed but affects no balance. Every
other reconciliation check still passes and only `hash_chain_intact` fails —
which is precisely the attack tamper evidence exists to catch, an edit that
leaves the books adding up.

---

## Phase 5 — Multi-currency and FX

### 5.1 FX needed no new schema, because zero-sum was already per currency

A cross-currency transaction is not a special case. Because the deferred
constraint trigger has grouped by currency since migration 001, a conversion is
simply a transaction whose entries balance in two currencies independently:

```
sell 100.00 USD, spread 1.00 USD, rate 1.35

  user       USD  -10000     USD:  -10000 + 100 + 9900 = 0
  revenue    USD    + 100
  liquidity  USD   + 9900
  liquidity  CAD  -13365     CAD: -13365 + 13365       = 0
  user       CAD  +13365
```

Money never crosses the currency boundary. The user's USD goes into a USD pool
and their CAD comes out of a CAD pool; the two halves are connected only by
sharing a transaction id. **There is no point in the codebase where a USD amount
is added to a CAD amount**, which is what makes "no FX sequence can create or
destroy money in any currency" true by construction rather than by careful
arithmetic.

Migration 004 contains one statement: adding `liquidity` to the set of account
types with exactly one instance per currency.

### 5.2 The caller states both legs; the service never applies a rate

`POST /fx/convert` takes `sell_amount_minor` and `buy_amount_minor` as explicit
integers.

Rejected: taking a rate and computing the other leg.

Reason: this is the most important decision in the module. Applying a rate means
multiplying money by a non-integer, which means choosing a rounding direction,
and the rounding residue has to be credited somewhere. Get that wrong and the
ledger leaks a fraction of a cent per conversion — the classic FX bug, and one
that reconciliation would only catch after it had happened thousands of times.
Requiring two integers keeps every arithmetic operation in the money path to
integer addition, and moves the rounding decision up to the quoting engine where
the rate actually lives.

The one place a ratio appears is `effective_rate()`, which is display-only,
computed with `Decimal`, and **returned as a string** so it cannot be fed back
into an amount calculation by accident. It is also the only function that needs
`MINOR_UNIT_EXPONENT`: 1 USD is 100 minor units but 1 JPY is 1, so a raw
minor-unit ratio would misreport a USD/JPY rate by 100×.

### 5.3 Five entries, not four

The specified "four-entry structure" and "the spread accruing to platform
revenue" cannot both hold. Four entries balance as `USD: −X, +X` and
`CAD: −Y, +Y`; a spread credited to `platform_revenue` needs its own entry in a
real currency, giving three legs on one side and five entries in total.

Chosen: five entries, with the spread as an explicit fee denominated in the
**sell** currency. The four-entry structure is then exactly the `spread = 0`
case, which `test_zero_spread_conversion_writes_exactly_four_entries` pins.

Rejected: keeping four entries and letting the spread accumulate as the pools
being long one currency and short the other, realised into revenue later.

Reason for rejecting it: computing that gain requires valuing one currency in
another, which drags in a mark-to-market rate and a rate oracle, and puts a
non-integer multiplication back in the money path — undoing 5.2. With an explicit
fee, no rate is needed to know what the platform earned. It is also the number
you could show a customer.

The spread going to the *sell* currency's revenue account rather than the buy
currency's is deliberate and has its own test, because crediting the wrong
currency's revenue account still balances per currency and would slip past a
zero-sum-only check.

### 5.4 One liquidity pool per currency, not per pair

The spec asked for liquidity accounts per currency pair. `accounts` has no pair
column, so honouring that literally meant either adding one or encoding the pair
in `name` and looking accounts up by string.

Chosen: one pool per currency, resolved by `(type, currency)` exactly as the
settlement and revenue accounts already are, guaranteed unique by the partial
index so the lookup cannot silently pick one of several.

Reason: it is what treasury systems actually run, it keeps the account count
linear in currencies rather than quadratic (5 currencies → 5 accounts, not 40),
and it needs no schema change. Per-pair pools only buy something if you want to
account for each pair's P&L separately, which is a reporting concern that can be
derived from the entries.

A pool going negative is allowed — `liquidity` is one of the types permitted
below zero — and means the platform is short that currency, which is a real
funded position.

### 5.5 Pool inventory limits are deliberately not enforced

Nothing stops a conversion from taking a pool arbitrarily negative. That is a
treasury *policy* decision (how short is the platform willing to be in CAD),
not a ledger invariant, and encoding it here would mean the ledger refusing a
transaction for a reason it has no way to evaluate correctly. The ledger's job is
to record what happened and guarantee it adds up.

What the ledger does guarantee is that the position is always visible: the pool's
balance is `SUM(entries)` like everything else, so a risk system can read it
without needing anything this service does not already expose.

### 5.6 The property test uses a model, not just the zero-sum invariant

`test_no_operation_sequence_creates_or_destroys_money` maintains a Python dict of
what every account's balance *should* be, applies each generated operation to both
the ledger and the model, and compares all of them at the end.

Rejected: asserting only that the global sum per currency is zero.

Reason: zero-sum is a weak oracle for FX specifically. Crediting the spread to the
wrong currency's revenue account, or swapping the two liquidity pools, still
balances per currency and would pass. A per-account model catches those. The test
also asserts that operations rejected by the service moved *nothing* — the model
is only advanced when the call succeeded — which is how a partial write would show
up.

One property in there is worth naming separately: **FX never touches
`external_settlement`.** Settlement is the door money enters through, so its
balance should be unchanged by any amount of conversion activity. Asserting that
directly catches a whole class of "where did this money come from" bug that a
zero-sum check cannot see.

### 5.7 A same-currency conversion is an error, not a no-op

`POST /fx/convert` with two accounts in the same currency returns 422 pointing at
`POST /transactions`. It would be easy to let it through as a degenerate
conversion, but it would route an ordinary transfer through the liquidity pools
and inflate their turnover with movements that are not FX — making pool volume
useless as a metric, for no benefit.

---

## Phase 6 — Outbox and webhook delivery

### 6.1 The emit lives inside `append_transaction`, not in the callers

`outbox.emit()` takes a **cursor**, not a connection, and is called from
`append_transaction` on the same cursor as the entry inserts.

Rejected: emitting from each service (`transactions`, `holds`, `fx`) after the
write returns.

Reason: the same argument as the balance cache in 1.6. Every entry this service
writes goes through `append_transaction`, so putting the emit there makes "a
committed transaction always has an event" structurally true rather than a rule
each new call site has to remember. A `emit()` that opened its own transaction
would silently reintroduce the exact dual write the pattern exists to remove,
which is why it takes a cursor — the signature makes the mistake hard to make.

`every_transaction_has_an_outbox_event` in `/reconciliation` is what turns that
from an argument into a verified fact, and it is specifically the check that
would catch someone refactoring the emit into its own transaction.
`test_a_failure_after_the_emit_still_leaves_no_event` covers the other direction:
it blows up *after* the outbox row is inserted and asserts the row does not
survive the rollback.

The expiry sweeper emits inside its own transaction too. A background job is not
exempt from the dual-write problem.

### 6.2 The claim is a lease, and the HTTP call is outside the transaction

`claim_due()` increments `attempts`, pushes `next_attempt_at` forward, and
**commits** — then delivery happens, then a second transaction records the
outcome.

Rejected: delivering inside the claiming transaction.

Reason: an HTTP call inside a database transaction holds a row lock for a network
round trip, and a hung endpoint pins it for the whole timeout. Worse, it makes
the database's availability depend on a third party's.

The consequence is deliberate and is the crux of the whole phase: **if the relay
dies after claiming and before recording, the event is delivered twice.** That is
at-least-once, and it is the strongest guarantee available without a transaction
spanning both systems. Using `next_attempt_at` as the lease means recovery needs
no separate reaper — a crashed relay's events simply become due again.

`test_a_claim_is_a_lease_that_expires` simulates the crash directly: it claims,
records nothing, asserts a second relay pass sees nothing while the lease holds,
then expires the lease and asserts the event comes back with `attempts = 2`.

### 6.3 Exactly-once is completed at the receiver, and the test proves duplicates happen

The relay cannot deliver exactly once. Nothing can, across two systems without a
shared transaction. So the contract is at-least-once delivery plus dedup on the
event id, and `scripts/receiver.py` implements the consumer half.

The important detail is **where the receiver's injected failure happens**: it
records the event and *then* returns 500. That is the failure mode that actually
tests the contract — the event was processed and the acknowledgement was lost, so
the redelivery is a genuine duplicate. A receiver that failed *before* processing
would only test that retries occur, which is the easy half.

This matters because it is the difference between a test that proves something and
one that passes vacuously. The headline test asserts:

* the unique event set matches the outbox exactly (nothing lost, nothing extra),
* `failures_injected > 0` — the fault injection did something,
* `duplicates > 0` — a duplicate really was delivered,
* `request_count > unique_events` — retries really happened.

Measured on a 41-event run at a 30% failure rate: **63 requests, 41 unique
events, 22 duplicates, one event needing 4 attempts, 0 dead-lettered.**

### 6.4 What is guaranteed about ordering, and what is not

Events are claimed `ORDER BY id` and delivered sequentially, so on the happy path
a consumer sees them in commit order. **Retries break that** — a failed event is
redelivered behind events created after it.

Rejected: head-of-line blocking, where a failing event stalls everything behind
it until it dead-letters.

Reason: that buys strict ordering at the price of letting one poison event stop
notifications for every account in the system. For a payments notification stream
that is the wrong trade. Consumers must be idempotent anyway (6.3), and
idempotent consumers are usually order-tolerant.

This is what `bigserial` buys and all it buys: a total order to deliver *in*, not
a guarantee the consumer observes it.

### 6.5 The gotcha this relay avoids

A tempting relay tracks a high-water mark: `WHERE id > last_seen_id`. **That
silently loses events.** Sequence values are handed out before commit, so the
transaction holding id 5 can commit before the transaction holding id 4; a reader
that reaches 5 first will never look at 4 again.

This relay keys off `status = 'pending'` instead, so an event is only ever
dismissed once its outcome is recorded. Written down because it is the single most
common way a hand-rolled outbox is wrong, and the bug is invisible under light
load.

### 6.6 Webhooks are HMAC signed over the exact bytes sent

`X-Signature: sha256=<hex>` over the request body, with `webhook_secret`.

Signed over the *bytes on the wire* rather than over the payload object, so the
receiver verifies without re-serialising — otherwise a difference in key order or
whitespace between two JSON encoders would produce a valid-but-rejected signature.
The receiver uses `hmac.compare_digest`, not `==`, because a plain comparison
leaks how much of the signature matched through timing.

The secret is optional so the relay works against a bare receiver, but an endpoint
with no signature has no way to distinguish a genuine event from a forged one, and
webhook payloads here contain balances.

### 6.7 A failing endpoint cannot affect the ledger

Dead-lettering after `outbox_max_attempts` means a permanently broken consumer
stops consuming relay capacity, and `GET /outbox/stats` reports the backlog and
the age of the oldest undelivered event.

The property worth stating plainly, and tested directly in
`test_the_ledger_is_unaffected_by_delivery_failure`: with the endpoint entirely
unreachable and events dead-lettering, the balances are correct and all eleven
reconciliation checks pass. The outbox is what makes notification an availability
concern rather than a correctness one.

`scripts/relay.py` exists as a separate process even though the API also runs the
relay as a background task, because the two scale differently: the API is
latency-bound on request handling, the relay is throughput-bound on somebody
else's endpoint being slow. Running them apart means a webhook consumer having a
bad day cannot consume the API's thread pool. Multiple relays are safe —
`FOR UPDATE SKIP LOCKED` partitions the work, which
`test_two_relays_do_not_deliver_the_same_event` asserts with four concurrent
claimers.

### 6.8 Historical transactions are backfilled as delivered

Migration 005 inserts a `delivered` outbox row for every pre-existing transaction,
marked `"backfilled": true` in the payload.

Rejected: backfilling them as `pending`.

Reason: deploying the migration would then fire a webhook for every transaction in
history. Marking them delivered is not strictly true, which is exactly why the
payload says `backfilled` — so nobody later mistakes them for events that were
genuinely sent. Without the backfill, the new reconciliation check would flag
every historical transaction forever.
