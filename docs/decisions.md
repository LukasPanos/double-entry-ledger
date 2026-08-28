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
| shared hot account | pessimistic | 1140 tps | **1204 tps** | 30 ms | **0** |
| shared hot account | optimistic | 1195 tps | 530 tps | 351 ms | 473 serialization failures |
| no shared account | pessimistic | 1254 tps | 401 tps | 492 ms | 1039 chain conflicts |
| no shared account | optimistic | 1207 tps | 545 tps | 390 ms | 453 serialization failures |

Pessimistic locking wins the hot-account case decisively: 2.3× the throughput and
12× better p95. That much was expected — a queue does no wasted work, whereas
every optimistic abort throws away a transaction that had already done its reads.
The bimodal latency is the visible signature of that: optimistic p50 stays at
~1 ms while p95 blows out to 351 ms, because the winners are fast and the losers
pay for a full replay.

The unexpected result is in the last two rows. **Pessimistic throughput on the
hot account (1204 tps) is three times its throughput with no shared account at
all (401 tps).** Sharing a row made it faster.

The reason is 1.7. Appending to the hash chain is a global serialization point:
every writer reads the same chain head and `UNIQUE(prev_hash)` rejects all but
one. In the hot-account workload the row lock on the shared fee account
*incidentally orders the chain appends too* — writers queue on the account, so
they reach the chain one at a time and never collide. The retry counter proves it:
**zero conflicts of any kind, at every concurrency level.** Remove the shared
account and nothing imposes that order, so the same strategy records 1039 chain
conflicts and loses 67% of its throughput.

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
