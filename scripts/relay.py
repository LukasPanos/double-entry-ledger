#!/usr/bin/env python
"""The outbox relay, as a standalone process.

    LEDGER_WEBHOOK_URL=http://127.0.0.1:8001/webhook python -m scripts.relay
    python -m scripts.relay --once      # single pass, then exit
    python -m scripts.relay --drain     # until nothing is pending

The API also runs this as a background task (see `_outbox_relay_worker` in
ledger/api.py), which is enough for a single-service deployment. It exists as a
separate entry point because the two concerns scale differently: the API is
latency-bound on request handling, the relay is throughput-bound on somebody
else's endpoint being slow. Running it apart means a webhook consumer having a bad
day cannot consume the API's thread pool.

Multiple relays can run at once. `claim_due` uses FOR UPDATE SKIP LOCKED, so they
partition the work rather than duplicating it.
"""

from __future__ import annotations

import argparse
import logging
import signal
import sys
import time
from types import FrameType

from ledger import db
from ledger.config import get_settings
from ledger.services import outbox

log = logging.getLogger("ledger.relay")

_stopping = False


def _handle_signal(signum: int, _frame: FrameType | None) -> None:
    """Finish the pass in flight, then exit.

    Not a hard exit: a relay killed mid-delivery leaves events leased, and while
    the lease guarantees they come back, a clean stop avoids the needless
    redelivery.
    """
    global _stopping
    log.info("received signal %s, finishing current pass", signum)
    _stopping = True


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--once", action="store_true", help="one pass, then exit")
    parser.add_argument(
        "--drain", action="store_true", help="run until nothing is pending"
    )
    parser.add_argument("--url", default=None, help="override LEDGER_WEBHOOK_URL")
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)-7s %(message)s"
    )
    settings = get_settings()
    url = args.url or settings.webhook_url
    if not url:
        print(
            "no webhook URL configured; set LEDGER_WEBHOOK_URL or pass --url",
            file=sys.stderr,
        )
        return 2

    signal.signal(signal.SIGTERM, _handle_signal)
    signal.signal(signal.SIGINT, _handle_signal)

    db.init_pool()
    try:
        if args.once:
            _report(outbox.relay_once(url))
        elif args.drain:
            _report(outbox.drain(url))
        else:
            log.info("relaying to %s", url)
            while not _stopping:
                stats = outbox.relay_once(url)
                if stats.claimed:
                    _report(stats)
                    if stats.delivered == stats.claimed:
                        continue  # more may be waiting
                time.sleep(settings.outbox_poll_seconds)
        print(outbox.stats())
    finally:
        db.close_pool()
    return 0


def _report(stats: outbox.RelayStats) -> None:
    log.info(
        "claimed=%d delivered=%d retried=%d dead=%d",
        stats.claimed,
        stats.delivered,
        stats.retried,
        stats.dead,
    )
    for error in stats.errors[:5]:
        log.info("  %s", error)


if __name__ == "__main__":
    sys.exit(main())
