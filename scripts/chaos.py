#!/usr/bin/env python
"""Kill the server at random moments and check the books still add up.

    python -m scripts.chaos                     # ~90s, default settings
    python -m scripts.chaos --duration 300      # longer
    python -m scripts.chaos --seed 42           # reproducible op mix

What this does:

  1. Starts the API as a real subprocess (uvicorn), not a test client.
  2. Drives it with randomized traffic from several threads: transfers, holds,
     captures, voids, FX conversions, and deliberate replays of
     already-used idempotency keys.
  3. At random intervals, kills the server -- mostly SIGKILL, so there is no
     chance to clean up, flush, or roll anything back gracefully.
  4. While the server is down and the database is quiescent, runs the full
     reconciliation suite and the hash-chain walk, and **aborts the run** if
     either fails.
  5. Restarts, sometimes switching the concurrency strategy, and continues.

Then the part that makes it more than a smoke test: every request sent is
recorded with its client-observed outcome, and at the end **every idempotency key
is replayed with a byte-identical body**. A request the client saw succeed must
have exactly one matching transaction whose entries are exactly what was asked
for; a request whose fate the client never learned must have either all of its
entries or none of them. That is the property a crash at an arbitrary point is
most likely to break, and it is not something reconciliation alone can see --
reconciliation proves the ledger is self-consistent, not that it agrees with what
clients were told.

Why SIGKILL specifically: SIGTERM lets uvicorn finish in-flight requests, which
tests the graceful path. SIGKILL severs the process mid-statement, and Postgres
then rolls back whatever those connections were doing. Uncommitted work
disappearing is exactly the behaviour every invariant in this service is built on,
so SIGKILL is the interesting case and gets the majority of kills.
"""

from __future__ import annotations

import argparse
import json
import os
import random
import signal
import subprocess
import sys
import threading
import time
from dataclasses import dataclass, field
from typing import Any
from uuid import uuid4

import httpx

from ledger import db
from ledger.config import get_settings
from ledger.services import outbox as outbox_service
from ledger.services.integrity import verify_chain
from ledger.services.reconciliation import reconcile
from scripts.receiver import ReceiverServer

CURRENCIES = ("USD", "CAD")


@dataclass
class SentRequest:
    key: str
    kind: str
    path: str
    body: dict[str, Any]
    outcome: str = "unknown"  # confirmed | rejected | unknown
    status: int | None = None


@dataclass
class Counters:
    sent: int = 0
    confirmed: int = 0
    rejected: int = 0
    unknown: int = 0
    replays_sent: int = 0
    kills: int = 0
    checks: int = 0
    by_kind: dict[str, int] = field(default_factory=dict)

    def bump(self, kind: str) -> None:
        self.by_kind[kind] = self.by_kind.get(kind, 0) + 1


class InvariantViolation(AssertionError):
    pass


class Chaos:
    def __init__(self, args: argparse.Namespace) -> None:
        self.args = args
        self.rng = random.Random(args.seed)
        self.port = args.port
        self.base = f"http://127.0.0.1:{self.port}"

        self.stop = threading.Event()
        self.server: subprocess.Popen[bytes] | None = None
        self.server_lock = threading.Lock()

        self.log_lock = threading.Lock()
        self.sent: list[SentRequest] = []
        self.counters = Counters()

        # Shared ledger topology, built before any chaos starts.
        self.accounts: dict[str, list[str]] = {c: [] for c in CURRENCIES}
        self.holds: list[str] = []
        self.holds_lock = threading.Lock()

        self.receiver: ReceiverServer | None = None
        self.strategy = "pessimistic"
        self.failures: list[str] = []

    # ----------------------------------------------------------------- setup --

    def prepare_database(self) -> None:
        db.init_pool()
        db.migrate()
        with db.transaction() as cur:
            cur.execute("SET LOCAL session_replication_role = 'replica'")
            cur.execute(
                """
                SELECT tablename FROM pg_tables
                 WHERE schemaname = 'public' AND tablename <> 'schema_migrations'
                 ORDER BY tablename
                """
            )
            tables = ", ".join(f'"{r["tablename"]}"' for r in cur.fetchall())
            cur.execute(f"TRUNCATE {tables} RESTART IDENTITY CASCADE")

        settings = get_settings()
        settings.outbox_max_attempts = 30
        settings.outbox_backoff_base_seconds = 0.05
        settings.outbox_backoff_cap_seconds = 1.0
        # The driver drains the outbox itself at the end, to finish whatever the
        # killed relays left behind. It therefore needs the same signing secret
        # the server was given -- without it the receiver returns 401 and (since
        # a 401 is a permanent failure) every leftover event dead-letters.
        if self.args.webhook:
            settings.webhook_secret = "chaos-secret"

    def build_topology(self) -> None:
        """Create system and user accounts directly, before the server starts.

        Deliberately not through the API: account creation is not what is being
        tested, and a kill landing during setup would just make the run
        uninteresting.
        """
        from ledger.schemas import CreateAccountRequest
        from ledger.services.accounts import create_account

        system: dict[str, dict[str, str]] = {}
        for currency in CURRENCIES:
            system[currency] = {}
            for type_ in ("external_settlement", "platform_revenue", "liquidity"):
                system[currency][type_] = str(
                    create_account(
                        CreateAccountRequest(
                            name=f"{type_} {currency}", currency=currency, type=type_
                        )
                    )["id"]
                )
            for i in range(self.args.accounts):
                self.accounts[currency].append(
                    str(
                        create_account(
                            CreateAccountRequest(
                                name=f"user {i} {currency}",
                                currency=currency,
                                type="user",
                            )
                        )["id"]
                    )
                )
        self.system = system

        # Fund every user account and every liquidity pool from settlement.
        from ledger.schemas import CreateTransactionRequest
        from ledger.services.transactions import post_transaction

        for currency in CURRENCIES:
            targets = self.accounts[currency] + [system[currency]["liquidity"]]
            for target in targets:
                amount = 100_000_000 if target == system[currency]["liquidity"] else 5_000_000
                post_transaction(
                    CreateTransactionRequest(
                        description="chaos funding",
                        entries=[
                            {
                                "account_id": system[currency]["external_settlement"],
                                "amount_minor": -amount,
                                "currency": currency,
                            },
                            {
                                "account_id": target,
                                "amount_minor": amount,
                                "currency": currency,
                            },
                        ],
                    ),
                    uuid4(),
                    strategy="pessimistic",
                )

    # ---------------------------------------------------------------- server --

    def start_server(self) -> None:
        env = os.environ.copy()
        env["LEDGER_DATABASE_URL"] = get_settings().database_url
        # Alternate the concurrency strategy across restarts, so the run
        # exercises both Phase 4 code paths against the same data.
        self.strategy = self.rng.choice(["pessimistic", "optimistic"])
        env["LEDGER_CONCURRENCY_STRATEGY"] = self.strategy
        env["LEDGER_RUN_HOLD_EXPIRY_WORKER"] = "true"
        env["LEDGER_HOLD_EXPIRY_POLL_SECONDS"] = "1"
        env["LEDGER_MAX_RETRIES"] = "25"
        env["LEDGER_OUTBOX_MAX_ATTEMPTS"] = "30"
        env["LEDGER_OUTBOX_BACKOFF_BASE_SECONDS"] = "0.05"
        env["LEDGER_OUTBOX_BACKOFF_CAP_SECONDS"] = "1.0"
        env["LEDGER_OUTBOX_POLL_SECONDS"] = "0.2"
        if self.receiver is not None:
            env["LEDGER_WEBHOOK_URL"] = self.receiver.url
            env["LEDGER_RUN_OUTBOX_RELAY"] = "true"
            env["LEDGER_WEBHOOK_SECRET"] = "chaos-secret"

        with self.server_lock:
            self.server = subprocess.Popen(
                [
                    sys.executable,
                    "-m",
                    "uvicorn",
                    "ledger.api:app",
                    "--host",
                    "127.0.0.1",
                    "--port",
                    str(self.port),
                    "--log-level",
                    "warning",
                ],
                env=env,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                # Own process group, so a kill cannot reach this driver.
                start_new_session=True,
            )
        self.wait_healthy()

    def wait_healthy(self, timeout: float = 30.0) -> None:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            try:
                response = httpx.get(f"{self.base}/health", timeout=1.0)
                if response.status_code == 200:
                    return
            except httpx.HTTPError:
                pass
            time.sleep(0.05)
        raise RuntimeError(f"server did not become healthy on port {self.port}")

    def kill_server(self, hard: bool) -> None:
        with self.server_lock:
            if self.server is None or self.server.poll() is not None:
                return
            sig = signal.SIGKILL if hard else signal.SIGTERM
            os.killpg(os.getpgid(self.server.pid), sig)
            try:
                self.server.wait(timeout=10)
            except subprocess.TimeoutExpired:
                os.killpg(os.getpgid(self.server.pid), signal.SIGKILL)
                self.server.wait(timeout=10)
            self.server = None

    # ----------------------------------------------------------------- traffic --

    def _record(self, request: SentRequest) -> None:
        with self.log_lock:
            self.sent.append(request)
            self.counters.sent += 1
            self.counters.bump(request.kind)
            if request.outcome == "confirmed":
                self.counters.confirmed += 1
            elif request.outcome == "rejected":
                self.counters.rejected += 1
            else:
                self.counters.unknown += 1

    def _send(
        self, client: httpx.Client, request: SentRequest, *, replay: bool = False
    ) -> dict[str, Any] | None:
        try:
            response = client.post(
                f"{self.base}{request.path}",
                json=request.body,
                headers={"Idempotency-Key": request.key},
                timeout=5.0,
            )
        except httpx.HTTPError:
            # The server died mid-request, or we could not reach it. The client
            # genuinely does not know whether the write happened -- which is the
            # situation idempotency keys exist for.
            request.outcome = "unknown"
            if not replay:
                self._record(request)
            return None

        request.status = response.status_code
        if response.status_code < 300:
            request.outcome = "confirmed"
        elif response.status_code < 500:
            # A business rejection is a definite answer: nothing was written.
            request.outcome = "rejected"
        else:
            request.outcome = "unknown"

        if not replay:
            self._record(request)
        try:
            return response.json()
        except ValueError:
            return None

    def _random_transfer(self) -> SentRequest:
        currency = self.rng.choice(CURRENCIES)
        pool = self.accounts[currency]
        payer, payee = self.rng.sample(pool, 2)
        amount = self.rng.randint(1, 5_000)
        legs = [
            {"account_id": payer, "amount_minor": -amount, "currency": currency},
            {"account_id": payee, "amount_minor": amount, "currency": currency},
        ]
        if amount > 100 and self.rng.random() < 0.4:
            # Three-leg payment with a fee, so the revenue account is a hot row.
            fee = max(1, amount // 50)
            legs[1]["amount_minor"] = amount - fee
            legs.append(
                {
                    "account_id": self.system[currency]["platform_revenue"],
                    "amount_minor": fee,
                    "currency": currency,
                }
            )
        return SentRequest(
            key=str(uuid4()),
            kind="transfer",
            path="/transactions",
            body={"description": "chaos transfer", "entries": legs},
        )

    def _random_hold(self) -> SentRequest:
        currency = self.rng.choice(CURRENCIES)
        account = self.rng.choice(self.accounts[currency])
        return SentRequest(
            key=str(uuid4()),
            kind="hold",
            path="/holds",
            body={
                "account_id": account,
                "amount_minor": self.rng.randint(1, 5_000),
                "currency": currency,
                # Short expiries so the sweeper has real work to do mid-chaos.
                "expires_in_seconds": self.rng.choice([1, 2, 5, 3600]),
            },
        )

    def _random_capture(self) -> SentRequest | None:
        with self.holds_lock:
            if not self.holds:
                return None
            hold_id = self.rng.choice(self.holds)
        try:
            hold = httpx.get(f"{self.base}/holds/{hold_id}", timeout=3.0).json()
        except (httpx.HTTPError, ValueError):
            return None
        if hold.get("status") != "pending":
            return None

        currency = hold["currency"]
        amount = hold["amount_minor"]
        if self.rng.random() < 0.5 and amount > 1:
            amount = self.rng.randint(1, amount)  # partial capture
        destination = self.rng.choice(
            [a for a in self.accounts[currency] if a != hold["account_id"]]
        )
        return SentRequest(
            key=str(uuid4()),
            kind="capture",
            path=f"/holds/{hold_id}/capture",
            body={
                "amount_minor": amount,
                "credits": [{"account_id": destination, "amount_minor": amount}],
            },
        )

    def _random_void(self) -> SentRequest | None:
        with self.holds_lock:
            if not self.holds:
                return None
            hold_id = self.rng.choice(self.holds)
        return SentRequest(
            key=str(uuid4()),
            kind="void",
            path=f"/holds/{hold_id}/void",
            body={"reason": "chaos"},
        )

    def _random_fx(self) -> SentRequest:
        sell, buy = self.rng.sample(CURRENCIES, 2)
        sell_amount = self.rng.randint(100, 20_000)
        spread = self.rng.randint(0, max(1, sell_amount // 100))
        buy_amount = max(1, int(sell_amount * self.rng.uniform(0.6, 1.6)))
        return SentRequest(
            key=str(uuid4()),
            kind="fx",
            path="/fx/convert",
            body={
                "from_account_id": self.rng.choice(self.accounts[sell]),
                "to_account_id": self.rng.choice(self.accounts[buy]),
                "sell_amount_minor": sell_amount,
                "buy_amount_minor": buy_amount,
                "spread_minor": spread,
            },
        )

    def worker(self, index: int) -> None:
        rng = random.Random(self.args.seed + index * 7919)
        with httpx.Client() as client:
            while not self.stop.is_set():
                roll = rng.random()

                # Replay an already-sent key verbatim. This is the operation
                # that matters most: after a kill, a replay must return the
                # original result or process exactly once, never twice.
                if roll < 0.15:
                    with self.log_lock:
                        candidates = [
                            r for r in self.sent if r.kind in ("transfer", "fx")
                        ]
                        original = rng.choice(candidates) if candidates else None
                    if original is not None:
                        with self.log_lock:
                            self.counters.replays_sent += 1
                        self._send(client, original, replay=True)
                        continue

                if roll < 0.55:
                    request: SentRequest | None = self._random_transfer()
                elif roll < 0.70:
                    request = self._random_hold()
                elif roll < 0.82:
                    request = self._random_capture()
                elif roll < 0.88:
                    request = self._random_void()
                else:
                    request = self._random_fx()

                if request is None:
                    time.sleep(0.01)
                    continue

                body = self._send(client, request)
                if request.kind == "hold" and body and body.get("id"):
                    with self.holds_lock:
                        self.holds.append(body["id"])
                if request.outcome == "unknown":
                    # Server is probably down; do not spin.
                    time.sleep(0.05)

    # -------------------------------------------------------------- invariants --

    def check_invariants(self, label: str) -> None:
        """Run every check. Raises on the first violation.

        Called while the server is down, so the database has no in-flight
        transactions and the snapshot is unambiguous.
        """
        with self.log_lock:
            self.counters.checks += 1

        report = reconcile()
        if not report["ok"]:
            broken = [c for c in report["checks"] if not c["passed"]]
            raise InvariantViolation(
                f"[{label}] reconciliation failed:\n"
                + json.dumps(broken, indent=2, default=str)
            )

        chain = verify_chain()
        if not chain["ok"]:
            raise InvariantViolation(
                f"[{label}] hash chain broken:\n"
                + json.dumps(chain["first_break"], indent=2, default=str)
            )

    def killer(self) -> None:
        while not self.stop.is_set():
            delay = self.rng.uniform(self.args.kill_min, self.args.kill_max)
            if self.stop.wait(delay):
                return

            hard = self.rng.random() < 0.8  # mostly SIGKILL
            with self.log_lock:
                self.counters.kills += 1
                kill_number = self.counters.kills
            print(
                f"  kill #{kill_number} ({'SIGKILL' if hard else 'SIGTERM'}) "
                f"after {self.strategy} run",
                flush=True,
            )
            self.kill_server(hard=hard)

            # Database is quiescent now: nothing can be mid-transaction.
            try:
                self.check_invariants(f"after kill #{kill_number}")
            except InvariantViolation as exc:
                self.failures.append(str(exc))
                self.stop.set()
                return

            if self.stop.is_set():
                return
            try:
                self.start_server()
            except RuntimeError as exc:
                self.failures.append(f"restart failed: {exc}")
                self.stop.set()
                return

    # ----------------------------------------------------------------- verify --

    def verify_replays(self) -> None:
        """Re-send every recorded key and check the ledger agrees with what
        clients were told.

        This is the assertion reconciliation cannot make. Reconciliation proves
        the ledger is internally consistent; it has no idea what any client was
        promised. Here, for each key:

          * a request the client saw succeed must have exactly one transaction,
            whose entries are exactly the ones requested;
          * a request whose outcome the client never learned must have either all
            of its entries or none -- never a subset;
          * replaying must not create a second transaction for a key that
            already had one.
        """
        with self.log_lock:
            recorded = list(self.sent)

        ledger_keys = self._transactions_by_key()
        before = self._transaction_count()

        confirmed_missing: list[str] = []
        partial: list[str] = []

        for request in recorded:
            if request.kind not in ("transfer", "fx"):
                continue
            found = ledger_keys.get(request.key)

            if request.outcome == "confirmed" and found is None:
                confirmed_missing.append(request.key)
                continue
            if request.outcome == "rejected" and found is not None:
                partial.append(
                    f"{request.key}: client got {request.status} but a "
                    f"transaction exists"
                )
                continue
            if found is None:
                continue

            expected = self._expected_legs(request)
            if expected is not None and found["legs"] != expected:
                partial.append(
                    f"{request.key}: stored legs {found['legs']} != requested "
                    f"{expected}"
                )

        if confirmed_missing:
            raise InvariantViolation(
                f"{len(confirmed_missing)} confirmed request(s) have no "
                f"transaction: {confirmed_missing[:5]}"
            )
        if partial:
            raise InvariantViolation(
                "ledger disagrees with what clients were told:\n"
                + "\n".join(partial[:5])
            )

        # Now replay everything. Keys that committed must replay; keys that did
        # not may now process, but each at most once.
        replayed = 0
        newly_processed = 0
        with httpx.Client() as client:
            for request in recorded:
                if request.kind not in ("transfer", "fx"):
                    continue
                body = self._send(
                    client,
                    SentRequest(
                        key=request.key,
                        kind=request.kind,
                        path=request.path,
                        body=request.body,
                    ),
                    replay=True,
                )
                if body is None:
                    continue
                if body.get("replayed") is True:
                    replayed += 1
                elif request.key not in ledger_keys:
                    newly_processed += 1

        after_keys = self._transactions_by_key()
        duplicates = [
            key for key, value in after_keys.items() if value["count"] > 1
        ]
        if duplicates:
            raise InvariantViolation(
                f"idempotency violated: {len(duplicates)} key(s) map to more "
                f"than one transaction: {duplicates[:5]}"
            )

        for key in ledger_keys:
            if key not in after_keys:
                raise InvariantViolation(f"transaction for key {key} disappeared")

        after = self._transaction_count()
        if after - before != newly_processed:
            raise InvariantViolation(
                f"replaying created {after - before} transactions but only "
                f"{newly_processed} were reported as newly processed"
            )

        print(
            f"  replay verification: {replayed} replayed, "
            f"{newly_processed} newly processed, 0 duplicated"
        )

    def _expected_legs(self, request: SentRequest) -> list[tuple[str, int, str]] | None:
        if request.kind != "transfer":
            return None  # FX legs include system accounts the client does not name
        return sorted(
            (e["account_id"], e["amount_minor"], e["currency"])
            for e in request.body["entries"]
        )

    def _transactions_by_key(self) -> dict[str, dict[str, Any]]:
        with db.transaction(read_only=True) as cur:
            cur.execute(
                """
                SELECT t.idempotency_key::text AS key,
                       count(DISTINCT t.id)     AS count,
                       -- min(uuid) is not a Postgres aggregate; compare as text.
                       min(t.id::text)          AS transaction_id
                  FROM transactions t
                 GROUP BY t.idempotency_key
                """
            )
            rows = {r["key"]: dict(r) for r in cur.fetchall()}

            cur.execute(
                """
                SELECT t.idempotency_key::text AS key,
                       e.account_id::text      AS account_id,
                       e.amount_minor,
                       e.currency
                  FROM transactions t
                  JOIN entries e ON e.transaction_id = t.id
                 ORDER BY t.idempotency_key, e.id
                """
            )
            legs: dict[str, list[tuple[str, int, str]]] = {}
            for row in cur.fetchall():
                legs.setdefault(row["key"], []).append(
                    (row["account_id"], row["amount_minor"], row["currency"].strip())
                )

        for key, value in rows.items():
            value["legs"] = sorted(legs.get(key, []))
        return rows

    def _transaction_count(self) -> int:
        with db.transaction(read_only=True) as cur:
            cur.execute("SELECT count(*) AS n FROM transactions")
            return cur.fetchone()["n"]  # type: ignore[index]

    def verify_outbox(self) -> None:
        if self.receiver is None:
            return
        # Finish whatever the killed relays left behind.
        outbox_service.drain(self.receiver.url, max_passes=2000)
        stats = outbox_service.stats()
        snapshot = self.receiver.snapshot()

        with db.transaction(read_only=True) as cur:
            cur.execute("SELECT count(*) AS n FROM outbox")
            total = cur.fetchone()["n"]  # type: ignore[index]

        if stats["pending"]:
            raise InvariantViolation(f"{stats['pending']} event(s) never delivered")
        if stats["dead"]:
            raise InvariantViolation(f"{stats['dead']} event(s) dead-lettered")
        if snapshot["unique_events"] != total:
            raise InvariantViolation(
                f"receiver saw {snapshot['unique_events']} unique events but the "
                f"outbox holds {total}"
            )
        print(
            f"  outbox: {total} events, {snapshot['request_count']} requests, "
            f"{snapshot['duplicates']} duplicates, "
            f"{snapshot['rejected_signatures']} bad signatures, 0 lost"
        )

    # -------------------------------------------------------------------- run --

    def run(self) -> int:
        print(f"chaos: seed={self.args.seed} duration={self.args.duration}s")
        self.prepare_database()
        self.build_topology()
        self.check_invariants("before any chaos")

        receiver_ctx = (
            ReceiverServer(fail_rate=self.args.webhook_fail_rate,
                           secret="chaos-secret", seed=self.args.seed)
            if self.args.webhook
            else None
        )
        if receiver_ctx is not None:
            receiver_ctx.__enter__()
            self.receiver = receiver_ctx

        try:
            self.start_server()
            workers = [
                threading.Thread(target=self.worker, args=(i,), daemon=True)
                for i in range(self.args.workers)
            ]
            killer = threading.Thread(target=self.killer, daemon=True)
            for thread in workers:
                thread.start()
            killer.start()

            self.stop.wait(self.args.duration)
            self.stop.set()
            for thread in workers:
                thread.join(timeout=30)
            killer.join(timeout=60)

            # Final state: make sure the server is up for the replay pass.
            with self.server_lock:
                running = self.server is not None and self.server.poll() is None
            if not running:
                self.start_server()

            if not self.failures:
                self.check_invariants("final")
                try:
                    self.verify_replays()
                    self.check_invariants("after replay")
                    self.verify_outbox()
                except InvariantViolation as exc:
                    self.failures.append(str(exc))
        finally:
            self.kill_server(hard=True)
            if receiver_ctx is not None:
                receiver_ctx.__exit__(None, None, None)

        return self.report()

    def report(self) -> int:
        c = self.counters
        print()
        print(f"operations sent      {c.sent}")
        print(f"  confirmed          {c.confirmed}")
        print(f"  rejected (4xx)     {c.rejected}")
        print(f"  outcome unknown    {c.unknown}")
        print(f"  replays sent       {c.replays_sent}")
        print(f"by kind              {c.by_kind}")
        print(f"server kills         {c.kills}")
        print(f"invariant checks     {c.checks}")
        print(f"transactions         {self._transaction_count()}")

        if self.failures:
            print("\nFAILED")
            for failure in self.failures:
                print(failure)
            return 1

        print(
            f"\nOK: all invariants held across {c.kills} kills "
            f"and {c.checks} full checks"
        )
        return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--duration", type=float, default=75.0)
    parser.add_argument("--workers", type=int, default=8)
    parser.add_argument("--accounts", type=int, default=6)
    parser.add_argument("--port", type=int, default=8077)
    parser.add_argument("--seed", type=int, default=1)
    parser.add_argument("--kill-min", type=float, default=4.0)
    parser.add_argument("--kill-max", type=float, default=9.0)
    parser.add_argument(
        "--webhook",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="also run a flaky webhook receiver and verify exactly-once delivery",
    )
    parser.add_argument("--webhook-fail-rate", type=float, default=0.15)
    args = parser.parse_args(argv)

    try:
        return Chaos(args).run()
    finally:
        db.close_pool()


if __name__ == "__main__":
    sys.exit(main())
