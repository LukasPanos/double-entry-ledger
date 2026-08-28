"""Phase 7: a short chaos run, inside the test suite.

`scripts/chaos.py` is the real thing and takes minutes. This runs the same code
path for a few seconds so that `make test` catches a regression in it, rather
than the chaos harness quietly rotting until someone runs it by hand.

The assertions are the harness's own: it aborts on the first invariant violation
and reports failures, so the test only has to check that it came back clean and
that it actually did some work.
"""

from __future__ import annotations

import argparse
import socket

import pytest

from scripts import chaos as chaos_module


def _free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


@pytest.mark.slow
def test_a_short_chaos_run_holds_every_invariant() -> None:
    args = argparse.Namespace(
        duration=14.0,
        workers=4,
        accounts=4,
        port=_free_port(),
        seed=20260827,
        kill_min=3.0,
        kill_max=5.0,
        webhook=True,
        webhook_fail_rate=0.2,
    )
    run = chaos_module.Chaos(args)
    exit_code = run.run()

    assert run.failures == [], run.failures
    assert exit_code == 0

    # It has to have actually done something, or a broken harness would pass by
    # doing nothing at all.
    assert run.counters.kills >= 2, run.counters
    assert run.counters.checks >= 3, run.counters
    assert run.counters.confirmed > 200, run.counters
    assert run.counters.replays_sent > 0, run.counters
    # Some requests must have been cut off mid-flight, or the kills were landing
    # in idle gaps and the interesting case never happened.
    assert run.counters.unknown > 0, run.counters
    # Every operation type was exercised.
    assert set(run.counters.by_kind) >= {"transfer", "hold", "fx"}, run.counters
