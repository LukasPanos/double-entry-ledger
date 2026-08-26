"""Phase 1: the database-layer invariants.

Every test here bypasses the service layer and writes raw SQL, because the point
is to show that the guards hold when application code is wrong. If the only
thing stopping an unbalanced transaction is a Python `if`, then one missing call
site is a hole in the ledger.
"""

from __future__ import annotations

from uuid import uuid4

import psycopg
import pytest

from ledger import db
from tests import factories as f


# ------------------------------------------------------------- append-only ---


def test_update_on_entries_is_rejected() -> None:
    account = f.make_account()
    f.fund(account, 500)

    with pytest.raises(psycopg.errors.CheckViolation) as exc:
        with db.transaction() as cur:
            cur.execute("UPDATE entries SET amount_minor = 999999")

    assert "append_only_violation" in str(exc.value)
    assert f.derived_balance(account) == 500


def test_delete_on_entries_is_rejected() -> None:
    account = f.make_account()
    f.fund(account, 500)

    with pytest.raises(psycopg.errors.CheckViolation) as exc:
        with db.transaction() as cur:
            cur.execute("DELETE FROM entries WHERE account_id = %s", (account,))

    assert "append_only_violation" in str(exc.value)
    assert f.derived_balance(account) == 500


def test_truncate_on_entries_is_rejected() -> None:
    with pytest.raises(psycopg.errors.CheckViolation) as exc:
        with db.transaction() as cur:
            cur.execute("TRUNCATE entries CASCADE")
    assert "append_only_violation" in str(exc.value)


def test_delete_matching_zero_rows_is_still_rejected() -> None:
    """The trigger is statement-level, so an attempt that would have matched
    nothing is still refused rather than quietly succeeding."""
    with pytest.raises(psycopg.errors.CheckViolation):
        with db.transaction() as cur:
            cur.execute("DELETE FROM entries WHERE account_id = %s", (uuid4(),))


def test_transactions_are_append_only_too() -> None:
    account = f.make_account()
    f.fund(account, 500)

    with pytest.raises(psycopg.errors.CheckViolation):
        with db.transaction() as cur:
            cur.execute("UPDATE transactions SET description = 'rewritten'")

    with pytest.raises(psycopg.errors.CheckViolation):
        with db.transaction() as cur:
            cur.execute("DELETE FROM transactions")


# ----------------------------------------------------- zero-sum per currency --


def test_unbalanced_entries_rejected_at_commit_not_at_insert() -> None:
    """The zero-sum constraint is DEFERRED, and the test proves it.

    Both inserts must succeed -- a transaction is legitimately unbalanced after
    its first entry -- and the failure must arrive at COMMIT, when the set of
    entries is finally complete.
    """
    account = f.make_account()
    other = f.make_account()

    with pytest.raises(psycopg.errors.CheckViolation) as exc:
        with db.transaction() as cur:
            tx_id = f.raw_insert_transaction(cur)
            cur.execute(
                "INSERT INTO entries (transaction_id, account_id, amount_minor, currency)"
                " VALUES (%s, %s, %s, %s)",
                (tx_id, account, 100, "USD"),
            )
            cur.execute(
                "INSERT INTO entries (transaction_id, account_id, amount_minor, currency)"
                " VALUES (%s, %s, %s, %s)",
                (tx_id, other, -50, "USD"),
            )
            # Still inside the transaction: both rows are visible to us, and the
            # constraint has not fired.
            cur.execute(
                "SELECT count(*) AS n FROM entries WHERE transaction_id = %s", (tx_id,)
            )
            assert cur.fetchone()["n"] == 2

    assert "unbalanced_transaction" in str(exc.value)
    assert "sums to 50" in str(exc.value)
    assert f.count_rows("entries") == 0
    assert f.count_rows("transactions") == 0


def test_balanced_in_one_currency_unbalanced_in_another_is_rejected() -> None:
    """Zero-sum is per currency, not over the sum of all amounts. Without the
    GROUP BY, +100 USD and -100 CAD would look balanced and would be a way to
    mint money."""
    usd = f.make_account(currency="USD")
    cad = f.make_account(currency="CAD")

    with pytest.raises(psycopg.errors.CheckViolation) as exc:
        with db.transaction() as cur:
            tx_id = f.raw_insert_transaction(cur)
            cur.execute(
                "INSERT INTO entries (transaction_id, account_id, amount_minor, currency)"
                " VALUES (%s, %s, %s, %s)",
                (tx_id, usd, 100, "USD"),
            )
            cur.execute(
                "INSERT INTO entries (transaction_id, account_id, amount_minor, currency)"
                " VALUES (%s, %s, %s, %s)",
                (tx_id, cad, -100, "CAD"),
            )

    assert "unbalanced_transaction" in str(exc.value)


def test_transaction_with_no_entries_is_rejected() -> None:
    with pytest.raises(psycopg.errors.CheckViolation) as exc:
        with db.transaction() as cur:
            f.raw_insert_transaction(cur)
    assert "has 0 entries" in str(exc.value)


def test_transaction_with_one_entry_is_rejected() -> None:
    """A single entry can only balance if its amount is zero, and zero amounts
    are refused by their own CHECK. So the two guards close the gap between
    them: `count < 2` catches the empty transaction, and the zero-sum trigger
    catches every single-entry transaction that could actually be written."""
    account = f.make_account()

    with pytest.raises(psycopg.errors.CheckViolation) as exc:
        with db.transaction() as cur:
            tx_id = f.raw_insert_transaction(cur)
            cur.execute(
                "INSERT INTO entries (transaction_id, account_id, amount_minor, currency)"
                " VALUES (%s, %s, %s, %s)",
                (tx_id, account, 100, "USD"),
            )
    assert "unbalanced_transaction" in str(exc.value)


def test_zero_amount_entry_is_rejected() -> None:
    account = f.make_account()
    with pytest.raises(psycopg.errors.CheckViolation) as exc:
        with db.transaction() as cur:
            tx_id = f.raw_insert_transaction(cur)
            cur.execute(
                "INSERT INTO entries (transaction_id, account_id, amount_minor, currency)"
                " VALUES (%s, %s, %s, %s)",
                (tx_id, account, 0, "USD"),
            )
    assert "amount_minor" in str(exc.value)


# ------------------------------------------------------- structural currency --


def test_entry_currency_must_match_account_currency() -> None:
    """Enforced by the composite foreign key (account_id, currency), not by
    application code. A USD entry on a CAD account has no valid parent row."""
    cad_account = f.make_account(currency="CAD")

    with pytest.raises(psycopg.errors.ForeignKeyViolation) as exc:
        with db.transaction() as cur:
            tx_id = f.raw_insert_transaction(cur)
            cur.execute(
                "INSERT INTO entries (transaction_id, account_id, amount_minor, currency)"
                " VALUES (%s, %s, %s, %s)",
                (tx_id, cad_account, 100, "USD"),
            )
    assert "entries_account_id_currency_fkey" in str(exc.value)


def test_entry_requires_an_existing_transaction() -> None:
    account = f.make_account()
    with pytest.raises(psycopg.errors.ForeignKeyViolation):
        with db.transaction() as cur:
            cur.execute(
                "INSERT INTO entries (transaction_id, account_id, amount_minor, currency)"
                " VALUES (%s, %s, %s, %s)",
                (uuid4(), account, 100, "USD"),
            )


# ------------------------------------------------------------ system accounts --


def test_only_one_settlement_account_per_currency() -> None:
    f.settlement_account("USD")
    with pytest.raises(Exception) as exc:
        f.settlement_account("USD")
    assert "already exists" in str(exc.value)

    # A different currency is fine.
    f.settlement_account("CAD")


# ----------------------------------------------------------------- chain -----


def test_hash_chain_cannot_fork() -> None:
    """UNIQUE(prev_hash) means two transactions cannot claim the same
    predecessor, so history is a line and not a tree."""
    shared_prev = b"\x11" * 32
    a = f.make_account()
    b = f.make_account()

    def insert_with_prev(prev: bytes, tx_hash: bytes) -> None:
        with db.transaction() as cur:
            tx_id = f.raw_insert_transaction(cur, prev_hash=prev, tx_hash=tx_hash)
            cur.execute(
                "INSERT INTO entries (transaction_id, account_id, amount_minor, currency)"
                " VALUES (%s, %s, %s, %s)",
                (tx_id, a, 10, "USD"),
            )
            cur.execute(
                "INSERT INTO entries (transaction_id, account_id, amount_minor, currency)"
                " VALUES (%s, %s, %s, %s)",
                (tx_id, b, -10, "USD"),
            )

    insert_with_prev(shared_prev, b"\x01" * 32)
    with pytest.raises(psycopg.errors.UniqueViolation) as exc:
        insert_with_prev(shared_prev, b"\x02" * 32)
    assert "transactions_prev_hash_key" in str(exc.value)


def test_hash_columns_must_be_32_bytes() -> None:
    with pytest.raises(psycopg.errors.CheckViolation):
        with db.transaction() as cur:
            f.raw_insert_transaction(cur, tx_hash=b"short")
