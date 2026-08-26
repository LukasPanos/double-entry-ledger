"""Test fixtures.

Tests run against a real PostgreSQL 16, never a mock or SQLite. Most of the
invariants in this service are enforced by Postgres itself -- deferred constraint
triggers, composite foreign keys, row locks, SERIALIZABLE conflict detection --
so a test against a stand-in would be testing nothing that matters.
"""

from __future__ import annotations

import os
from collections.abc import Iterator
from uuid import UUID, uuid4

import pytest

TEST_DATABASE_URL = os.environ.get(
    "LEDGER_TEST_DATABASE_URL", "postgresql://ledger@127.0.0.1:55432/ledger_test"
)
os.environ["LEDGER_DATABASE_URL"] = TEST_DATABASE_URL

from ledger import db  # noqa: E402  -- must follow the env var above
from ledger.config import reset_settings  # noqa: E402

reset_settings()


# Never truncated: it is the record of which migrations have run.
_PRESERVED_TABLES = {"schema_migrations"}


@pytest.fixture(scope="session", autouse=True)
def _schema() -> Iterator[None]:
    db.init_pool()
    db.migrate()
    yield
    db.close_pool()


def reset_database() -> None:
    """Empty every table between tests.

    This has to disable the append-only triggers to run, which is the honest
    demonstration of what those triggers do and do not buy you: they stop
    application code and stray SQL from rewriting history, and they do nothing
    against someone who owns the table. That is why tamper *evidence* (the hash
    chain) exists alongside tamper *prevention*.
    """
    with db.transaction() as cur:
        cur.execute(
            "SELECT tablename FROM pg_tables WHERE schemaname = 'public' ORDER BY tablename"
        )
        tables = [
            row["tablename"]
            for row in cur.fetchall()
            if row["tablename"] not in _PRESERVED_TABLES
        ]
        if not tables:
            return

        for table in tables:
            cur.execute(f'ALTER TABLE "{table}" DISABLE TRIGGER USER')
        joined = ", ".join(f'"{t}"' for t in tables)
        cur.execute(f"TRUNCATE {joined} RESTART IDENTITY CASCADE")
        for table in tables:
            cur.execute(f'ALTER TABLE "{table}" ENABLE TRIGGER USER')


@pytest.fixture(autouse=True)
def clean_db(_schema: None) -> Iterator[None]:
    reset_database()
    yield


# --------------------------------------------------------------- utilities ---


@pytest.fixture
def key() -> UUID:
    """A fresh idempotency key."""
    return uuid4()


def new_key() -> UUID:
    return uuid4()


@pytest.fixture
def client() -> Iterator["object"]:
    from fastapi.testclient import TestClient

    from ledger.api import app

    with TestClient(app) as test_client:
        yield test_client
