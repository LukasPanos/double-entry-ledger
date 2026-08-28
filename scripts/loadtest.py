#!/usr/bin/env python
"""The hot-account benchmark.

    python -m scripts.loadtest            # full run, ~2 minutes
    python -m scripts.loadtest --quick    # smoke run

Question being answered: when every transaction has to touch one shared account
-- a platform fee account, which in a real payments system every single payment
credits -- what does that cost, and does pessimistic locking or optimistic retry
pay for it more cheaply?

The two strategies differ in exactly one function (`acquire_accounts` in
ledger/services/transactions.py). Everything downstream is identical, so this is
a comparison of concurrency control and not of two implementations.

**What is being measured, precisely.** The load driver calls the service layer
directly from a thread pool -- no HTTP, no uvicorn, no event loop. That is
deliberate: the question is about database contention, and putting a web stack in
the path would add scheduling noise that has nothing to do with row locks. These
numbers are therefore *not* end-to-end API latency; they are the latency of the
transaction itself, which is the part the strategy choice controls.

**Two workloads, because this ledger has two serialization points.**

  hot       three legs, of which the third is one shared fee account. Contends
            on that account's balance row *and* on the hash chain head.
  disjoint  identical shape and identical work, but the third leg is the
            worker's own account. Nothing is shared except the chain head, so
            this workload isolates the chain.

Together they let every retry be attributed. The interesting consequence, visible
in the numbers: under the pessimistic strategy the hot workload records *zero*
conflicts, because the row lock on the shared account incidentally orders the
chain appends too. Sharing an account makes it faster, not slower.
"""

from __future__ import annotations

import argparse
import json
import os
import queue
import statistics
import sys
import threading
import time
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any
from uuid import UUID, uuid4

from ledger import db
from ledger.config import get_settings
from ledger.errors import LedgerError
from ledger.schemas import CreateAccountRequest, CreateTransactionRequest
from ledger.services import transactions as transactions_service
from ledger.services.accounts import create_account
from ledger.services.reconciliation import assert_reconciled

DOCS = Path(__file__).resolve().parent.parent / "docs"

STRATEGIES = ("pessimistic", "optimistic")
WORKLOADS = ("hot", "disjoint")

PAYMENT_MINOR = 100
FEE_MINOR = 3


@dataclass
class RunResult:
    strategy: str
    workload: str
    concurrency: int
    transactions: int
    duration_s: float
    throughput_tps: float
    p50_ms: float
    p95_ms: float
    p99_ms: float
    max_ms: float
    retries: dict[str, int] = field(default_factory=dict)
    retries_total: int = 0
    failures: dict[str, int] = field(default_factory=dict)


@dataclass
class Fixture:
    """Per-worker accounts, plus the one account they may share."""

    payers: list[UUID]
    merchants: list[UUID]
    hot: UUID
    cold: list[UUID]


# --------------------------------------------------------------------- setup --


def _reset_ledger() -> None:
    """Empty every table so each configuration starts from the same state.

    Without this, runs later in the sweep would carry the entries of every
    earlier run, and the per-payer `SUM(entries)` in the overdraft check would
    get steadily more expensive -- so the last strategy measured would look worse
    for a reason that has nothing to do with the strategy.

    `session_replication_role` rather than `ALTER TABLE ... DISABLE TRIGGER`,
    which would take an ACCESS EXCLUSIVE lock on each table. This is a benchmark
    harness deliberately switching off the append-only guards; nothing in the
    service can do this.
    """
    with db.transaction() as cur:
        cur.execute("SET LOCAL session_replication_role = 'replica'")
        cur.execute(
            """
            SELECT tablename FROM pg_tables
             WHERE schemaname = 'public' AND tablename <> 'schema_migrations'
             ORDER BY tablename
            """
        )
        tables = [row["tablename"] for row in cur.fetchall()]
        joined = ", ".join(f'"{t}"' for t in tables)
        cur.execute(f"TRUNCATE {joined} RESTART IDENTITY CASCADE")


def _build_fixture(workers: int, transactions: int) -> Fixture:
    settlement = create_account(
        CreateAccountRequest(
            name="External Settlement USD",
            currency="USD",
            type="external_settlement",
        )
    )["id"]
    hot = create_account(
        CreateAccountRequest(
            name="Platform Revenue USD", currency="USD", type="platform_revenue"
        )
    )["id"]

    payers, merchants, cold = [], [], []
    for i in range(workers):
        payers.append(
            create_account(
                CreateAccountRequest(name=f"payer {i}", currency="USD", type="user")
            )["id"]
        )
        merchants.append(
            create_account(
                CreateAccountRequest(name=f"merchant {i}", currency="USD", type="user")
            )["id"]
        )
        cold.append(
            create_account(
                CreateAccountRequest(name=f"cold {i}", currency="USD", type="liquidity")
            )["id"]
        )

    # Fund every payer far beyond what the run will spend, so no transaction is
    # ever rejected for insufficient funds -- a refusal is fast and would flatter
    # the throughput number.
    funding = (transactions + 100) * PAYMENT_MINOR
    for payer in payers:
        transactions_service.post_transaction(
            CreateTransactionRequest(
                description="benchmark funding",
                entries=[
                    {
                        "account_id": settlement,
                        "amount_minor": -funding,
                        "currency": "USD",
                    },
                    {"account_id": payer, "amount_minor": funding, "currency": "USD"},
                ],
            ),
            uuid4(),
            strategy="pessimistic",
        )

    return Fixture(payers=payers, merchants=merchants, hot=hot, cold=cold)


# ------------------------------------------------------------------ the work --


def _request(fixture: Fixture, workload: str, worker: int) -> CreateTransactionRequest:
    payer = fixture.payers[worker]
    merchant = fixture.merchants[worker]

    if workload == "hot":
        # Three legs. The payer and merchant are this worker's own, so the ONLY
        # row every transaction in the run has in common is the fee account.
        third = fixture.hot
    else:
        # Same shape, same number of legs, same amount of work -- but the third
        # leg is this worker's own account, so nothing is shared at all.
        third = fixture.cold[worker]

    return CreateTransactionRequest(
        description=f"benchmark {workload}",
        entries=[
            {"account_id": payer, "amount_minor": -PAYMENT_MINOR, "currency": "USD"},
            {
                "account_id": merchant,
                "amount_minor": PAYMENT_MINOR - FEE_MINOR,
                "currency": "USD",
            },
            {"account_id": third, "amount_minor": FEE_MINOR, "currency": "USD"},
        ],
    )


def _run_one(
    strategy: str, workload: str, concurrency: int, transactions: int, warmup: int
) -> RunResult:
    _reset_ledger()
    fixture = _build_fixture(concurrency, transactions + warmup)

    tasks: queue.SimpleQueue[int] = queue.SimpleQueue()
    for _ in range(transactions):
        tasks.put(1)

    latencies: list[list[float]] = [[] for _ in range(concurrency)]
    failures: dict[str, int] = {}
    failures_lock = threading.Lock()
    start_barrier = threading.Barrier(concurrency + 1)

    def worker(index: int) -> None:
        request = _request(fixture, workload, index)
        # Warm up outside the timed window: first-call overhead (prepared
        # statements, pool growth, plan caching) would otherwise land entirely
        # in the concurrency-1 column and make it look slow.
        for _ in range(warmup):
            try:
                transactions_service.post_transaction(
                    request, uuid4(), strategy=strategy
                )
            except LedgerError:
                pass

        start_barrier.wait()
        while True:
            try:
                tasks.get_nowait()
            except queue.Empty:
                return
            began = time.perf_counter()
            try:
                transactions_service.post_transaction(
                    request, uuid4(), strategy=strategy
                )
            except LedgerError as exc:
                with failures_lock:
                    failures[exc.code] = failures.get(exc.code, 0) + 1
            except Exception as exc:  # noqa: BLE001
                with failures_lock:
                    name = type(exc).__name__
                    failures[name] = failures.get(name, 0) + 1
            latencies[index].append((time.perf_counter() - began) * 1000)

    threads = [
        threading.Thread(target=worker, args=(i,), daemon=True)
        for i in range(concurrency)
    ]
    for t in threads:
        t.start()

    start_barrier.wait()
    # Retry counters are reset after warmup so they describe the timed window.
    db.RETRIES.reset()
    began = time.perf_counter()
    for t in threads:
        t.join()
    duration = time.perf_counter() - began

    samples = sorted(x for worker_samples in latencies for x in worker_samples)
    retries = db.RETRIES.snapshot()

    return RunResult(
        strategy=strategy,
        workload=workload,
        concurrency=concurrency,
        transactions=len(samples),
        duration_s=round(duration, 4),
        throughput_tps=round(len(samples) / duration, 1) if duration else 0.0,
        p50_ms=round(_pct(samples, 50), 2),
        p95_ms=round(_pct(samples, 95), 2),
        p99_ms=round(_pct(samples, 99), 2),
        max_ms=round(samples[-1], 2) if samples else 0.0,
        retries=retries,
        retries_total=sum(retries.values()),
        failures=failures,
    )


def _pct(sorted_samples: list[float], pct: float) -> float:
    if not sorted_samples:
        return 0.0
    # Nearest-rank on a sorted sample. No interpolation: with a few hundred
    # samples, interpolating invents precision the measurement does not have.
    rank = max(0, min(len(sorted_samples) - 1, int(round(pct / 100 * len(sorted_samples))) - 1))
    return sorted_samples[rank]


# ------------------------------------------------------------------- charting --

# Slots 1 and 2 of the reference categorical palette. Validated for this chart:
# adjacent CVD Delta E 24.7 (protan) / 32.7 (tritan), normal-vision 33.6, both
# above 3:1 against the surface. Marker shape is a second, redundant encoding, so
# the series stay distinguishable in greyscale and in print.
SERIES_COLOR = {"pessimistic": "#2a78d6", "optimistic": "#eb6834"}
SERIES_MARKER = {"pessimistic": "o", "optimistic": "s"}
# Vertical stagger, in points, applied to the end-of-line labels so they stay
# legible where the two series converge.
SERIES_LABEL_DY = 8.0
SERIES_LABEL = {
    "pessimistic": "pessimistic (FOR UPDATE)",
    "optimistic": "optimistic (SERIALIZABLE + retry)",
}

SURFACE = "#fcfcfb"
INK = "#0b0b0b"
INK_SECONDARY = "#52514e"
GRID = "#e6e5e1"
AXIS = "#c9c8c3"


def _chart(results: list[RunResult], out_path: Path) -> None:
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    from matplotlib.lines import Line2D

    levels = sorted({r.concurrency for r in results})

    fig, axes = plt.subplots(2, 2, figsize=(11.5, 8.0), dpi=200)
    fig.patch.set_facecolor(SURFACE)

    panels = [
        ("hot", "throughput_tps", "Throughput (transactions/second)"),
        ("hot", "p95_ms", "p95 latency (ms)"),
        ("disjoint", "throughput_tps", "Throughput (transactions/second)"),
        ("disjoint", "p95_ms", "p95 latency (ms)"),
    ]
    row_titles = {
        "hot": "Shared hot account — every transaction credits the fee account",
        "disjoint": "No shared account — only the hash chain head is contended",
    }

    for ax, (workload, metric, ylabel) in zip(axes.flat, panels):
        ax.set_facecolor(SURFACE)
        ax.grid(axis="y", color=GRID, linewidth=0.6, zorder=0)
        ax.set_axisbelow(True)
        for side in ("top", "right"):
            ax.spines[side].set_visible(False)
        for side in ("left", "bottom"):
            ax.spines[side].set_color(AXIS)
            ax.spines[side].set_linewidth(0.8)
        ax.tick_params(colors=INK_SECONDARY, labelsize=9, length=3, width=0.8)

        ends: list[tuple[str, float, float]] = []
        for strategy in STRATEGIES:
            series = [
                r
                for r in results
                if r.strategy == strategy and r.workload == workload
            ]
            series.sort(key=lambda r: r.concurrency)
            xs = [r.concurrency for r in series]
            ys = [getattr(r, metric) for r in series]
            ax.plot(
                xs,
                ys,
                color=SERIES_COLOR[strategy],
                marker=SERIES_MARKER[strategy],
                markersize=5.5,
                markeredgecolor=SURFACE,
                markeredgewidth=1.0,
                linewidth=2.0,
                zorder=3,
                clip_on=False,
            )
            if xs:
                ends.append((strategy, xs[-1], ys[-1]))

        # Direct labels at the end of each line -- identity is never carried by
        # colour alone. Text takes an ink token, not the series hue.
        #
        # The stagger is derived from where the lines actually finish, not fixed
        # per series: the higher line's label goes up and the lower one's goes
        # down. A fixed offset pushed them *together* in the panel where the two
        # series converge, which is precisely the panel that needs the help.
        ends.sort(key=lambda e: e[2], reverse=True)
        for rank, (strategy, x_end, y_end) in enumerate(ends):
            dy = SERIES_LABEL_DY if len(ends) > 1 else 0.0
            offset = dy if rank == 0 else -dy
            ax.annotate(
                strategy,
                xy=(x_end, y_end),
                xytext=(8, offset),
                textcoords="offset points",
                color=INK_SECONDARY,
                fontsize=8.5,
                va="center",
                annotation_clip=False,
            )

        ax.set_xscale("log", base=2)
        ax.set_xticks(levels)
        ax.set_xticklabels([str(x) for x in levels])
        ax.set_xlim(min(levels) * 0.92, max(levels) * 1.5)
        ax.set_ylim(bottom=0)
        ax.set_ylabel(ylabel, color=INK, fontsize=9.5, labelpad=8)
        ax.set_xlabel("concurrent writers", color=INK_SECONDARY, fontsize=9)

    for row, workload in enumerate(WORKLOADS):
        axes[row][0].set_title(
            row_titles[workload],
            color=INK,
            fontsize=10.5,
            loc="left",
            pad=12,
            fontweight="medium",
        )

    fig.suptitle(
        "Hot-account contention: pessimistic row locks vs optimistic retry",
        color=INK,
        fontsize=13,
        x=0.055,
        y=0.985,
        ha="left",
        va="top",
        fontweight="semibold",
    )

    def at_max(workload: str, strategy: str) -> RunResult:
        return next(
            r
            for r in results
            if r.workload == workload
            and r.strategy == strategy
            and r.concurrency == max(levels)
        )

    hot_p, hot_o = at_max("hot", "pessimistic"), at_max("hot", "optimistic")
    cold_p = at_max("disjoint", "pessimistic")
    fig.text(
        0.055,
        0.947,
        f"PostgreSQL 16 · {hot_p.transactions} transactions per point · service layer "
        f"called directly, no HTTP\n"
        f"At {max(levels)} writers on the hot account: "
        f"{hot_p.throughput_tps:.0f} tps pessimistic (p95 {hot_p.p95_ms:.0f} ms) vs "
        f"{hot_o.throughput_tps:.0f} tps optimistic (p95 {hot_o.p95_ms:.0f} ms, "
        f"{hot_o.retries_total} transactions retried).\n"
        f"Pessimistic records zero conflicts on the hot account, because the row lock "
        f"also orders the chain appends — so sharing an account is "
        f"{hot_p.throughput_tps / cold_p.throughput_tps:.1f}× faster than not sharing one.",
        color=INK_SECONDARY,
        fontsize=8.5,
        ha="left",
        va="top",
        linespacing=1.5,
    )

    handles = [
        Line2D(
            [],
            [],
            color=SERIES_COLOR[s],
            marker=SERIES_MARKER[s],
            markersize=5.5,
            markeredgecolor=SURFACE,
            linewidth=2.0,
            label=SERIES_LABEL[s],
        )
        for s in STRATEGIES
    ]
    legend = fig.legend(
        handles=handles,
        loc="lower left",
        bbox_to_anchor=(0.055, 0.0),
        ncols=2,
        frameon=False,
        fontsize=9,
    )
    for text in legend.get_texts():
        text.set_color(INK_SECONDARY)

    fig.tight_layout(rect=(0.02, 0.05, 0.97, 0.885))
    out_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out_path, facecolor=SURFACE)
    print(f"\nchart written to {out_path}")


# ----------------------------------------------------------------------- main --


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--quick", action="store_true", help="fast smoke run")
    parser.add_argument(
        "--replot",
        action="store_true",
        help="redraw the chart from the saved JSON without re-measuring",
    )
    parser.add_argument("--transactions", type=int, default=300)
    parser.add_argument("--warmup", type=int, default=5)
    parser.add_argument(
        "--levels", type=int, nargs="+", default=[1, 2, 4, 8, 16, 32]
    )
    args = parser.parse_args(argv)

    if args.replot:
        saved = json.loads((DOCS / "hot-account-benchmark.json").read_text())
        _chart(
            [RunResult(**row) for row in saved["results"]],
            DOCS / "hot-account-benchmark.png",
        )
        return 0

    if args.quick:
        args.transactions = 60
        args.levels = [1, 8]
        args.warmup = 2

    settings = get_settings()
    # The optimistic strategy needs enough retry budget that we measure its
    # latency rather than its give-up rate. Exhaustion is still counted and
    # reported -- see the `failures` column.
    settings.max_retries = 40

    results: list[RunResult] = []
    header = (
        f"{'strategy':<12} {'workload':<9} {'conc':>5} {'tps':>9} "
        f"{'p50':>8} {'p95':>8} {'p99':>8} {'retries':>8}  failures"
    )

    for workload in WORKLOADS:
        print(f"\n=== workload: {workload} ===")
        print(header)
        for strategy in STRATEGIES:
            for concurrency in args.levels:
                db.close_pool()
                db.init_pool(max_size=concurrency + 4, min_size=1)
                result = _run_one(
                    strategy,
                    workload,
                    concurrency,
                    args.transactions,
                    args.warmup,
                )
                # Non-negotiable: the ledger must be consistent after every
                # single run. A benchmark that leaves the books wrong has
                # measured nothing worth knowing.
                assert_reconciled()
                results.append(result)
                print(
                    f"{strategy:<12} {workload:<9} {concurrency:>5} "
                    f"{result.throughput_tps:>9.1f} {result.p50_ms:>8.2f} "
                    f"{result.p95_ms:>8.2f} {result.p99_ms:>8.2f} "
                    f"{result.retries_total:>8}  {result.failures or '-'}"
                )

    # The retry breakdown is the actual proof. Throughput alone cannot tell you
    # *what* the writers were fighting over; this can.
    print("\nretry breakdown by conflict kind:")
    for workload in WORKLOADS:
        for strategy in STRATEGIES:
            rows = [
                r
                for r in results
                if r.workload == workload and r.strategy == strategy and r.retries
            ]
            if not rows:
                print(f"  {workload:<9} {strategy:<12} no conflicts at any level")
                continue
            for result in sorted(rows, key=lambda r: r.concurrency):
                print(
                    f"  {workload:<9} {strategy:<12} c={result.concurrency:<3} "
                    f"{result.retries}"
                )

    payload = {
        "postgres": _server_version(),
        "transactions_per_point": args.transactions,
        "levels": args.levels,
        "results": [asdict(r) for r in results],
    }
    (DOCS / "hot-account-benchmark.json").write_text(
        json.dumps(payload, indent=2) + "\n"
    )
    _chart(results, DOCS / "hot-account-benchmark.png")

    db.close_pool()
    return 0


def _server_version() -> str:
    with db.transaction(read_only=True) as cur:
        cur.execute("SHOW server_version")
        return cur.fetchone()["server_version"]  # type: ignore[index]


if __name__ == "__main__":
    os.environ.setdefault("LEDGER_RUN_HOLD_EXPIRY_WORKER", "false")
    sys.exit(main())
