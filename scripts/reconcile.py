#!/usr/bin/env python
"""Print the reconciliation report and exit nonzero if anything is off.

    python -m scripts.reconcile

Exit code is the point: this is meant to be runnable from cron or CI, where the
useful signal is "did it pass", not the formatted output.
"""

from __future__ import annotations

import sys

from ledger import db
from ledger.services.integrity import verify_chain
from ledger.services.reconciliation import reconcile


def main() -> int:
    db.init_pool()
    try:
        report = reconcile()
        chain = verify_chain()
    finally:
        db.close_pool()

    width = max(len(c["name"]) for c in report["checks"])
    for check in report["checks"]:
        mark = "PASS" if check["passed"] else "FAIL"
        print(f"[{mark}] {check['name']:<{width}}  {check['detail']}")
        for failure in check["failures"]:
            print(f"         {failure}")

    print()
    print(f"chain head        {chain['head_hash']}")
    print(f"transactions      {chain['transactions_checked']}")
    print(f"reconciled in     {report['duration_ms']} ms")
    print(f"result            {'OK' if report['ok'] else 'FAILED'}")
    return 0 if report["ok"] else 1


if __name__ == "__main__":
    sys.exit(main())
