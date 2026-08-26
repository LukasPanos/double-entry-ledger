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
