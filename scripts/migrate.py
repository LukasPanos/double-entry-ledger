#!/usr/bin/env python
"""Apply pending SQL migrations.

    python -m scripts.migrate

Each file in migrations/ runs inside its own explicit transaction and is
recorded in schema_migrations. Postgres has transactional DDL, so a migration
that fails halfway leaves no partial schema behind.
"""

from __future__ import annotations

import logging
import sys

from ledger import db


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    db.init_pool()
    try:
        applied = db.migrate()
    finally:
        db.close_pool()

    if applied:
        print(f"applied {len(applied)} migration(s): {', '.join(applied)}")
    else:
        print("schema already up to date")
    return 0


if __name__ == "__main__":
    sys.exit(main())
