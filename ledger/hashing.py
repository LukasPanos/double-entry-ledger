"""Tamper-evidence hash chain (Phase 7, but written in Phase 1 because every
transaction has to be hashed from the very first one).

Each transaction stores `prev_hash` (its predecessor's `tx_hash`) and its own
`tx_hash`. Changing any hashed field of any historical transaction changes its
`tx_hash`, which breaks the link its successor stores, which breaks every link
after that. GET /integrity recomputes the whole chain and reports the first
transaction whose stored hash disagrees with its recomputed hash.

This proves *evidence of tampering*, not prevention. Anyone with write access to
the database and the ability to disable triggers can rewrite the entire chain
consistently. What it defeats is a targeted edit: silently changing one amount
in one historical row without anyone noticing.

Canonical serialization, version v1 -- newline-delimited UTF-8 text:

    LEDGER-TX-V1
    id:<uuid, lowercase canonical form>
    created_at:<YYYY-MM-DDTHH:MM:SS.ffffffZ>
    prev:<64 lowercase hex chars>
    entries:<count>
    <account_id>:<currency>:<amount_minor>     (repeated `count` times, sorted)

with a trailing newline after every line including the last.

Why a bespoke text format rather than JSON: JSON has no single canonical form.
Key order, whitespace, unicode escaping and integer rendering are all
implementation-defined, so two correct JSON serializers can produce different
bytes for the same value and thus different hashes. The format above has exactly
one valid encoding for any given input, and you can eyeball it in a terminal.

Entry rows' surrogate `id`s are deliberately not hashed: they are assigned by a
database sequence and carry no economic meaning. What is hashed is the multiset
of (account, currency, amount) triples, which is what the transaction actually
asserts.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from datetime import datetime, timezone
from uuid import UUID

HASH_VERSION = "LEDGER-TX-V1"

# Genesis uses 32 zero bytes rather than NULL so that UNIQUE(prev_hash) in the
# schema actually forbids a second genesis row.
GENESIS_PREV_HASH = b"\x00" * 32

HASH_SIZE = 32


@dataclass(frozen=True, slots=True)
class HashableEntry:
    account_id: UUID
    currency: str
    amount_minor: int

    def _sort_key(self) -> tuple[str, str, int]:
        return (str(self.account_id), self.currency, self.amount_minor)


def _format_timestamp(created_at: datetime) -> str:
    if created_at.tzinfo is None:
        raise ValueError("created_at must be timezone-aware")
    utc = created_at.astimezone(timezone.utc)
    # Microseconds, matching Postgres timestamptz precision exactly. Anything
    # coarser or finer would make the stored value unhashable back to the same
    # bytes after a round trip.
    return utc.strftime("%Y-%m-%dT%H:%M:%S.%f") + "Z"


def canonical_bytes(
    *,
    transaction_id: UUID,
    created_at: datetime,
    entries: list[HashableEntry],
    prev_hash: bytes,
) -> bytes:
    if len(prev_hash) != HASH_SIZE:
        raise ValueError(f"prev_hash must be {HASH_SIZE} bytes, got {len(prev_hash)}")

    ordered = sorted(entries, key=HashableEntry._sort_key)

    lines = [
        HASH_VERSION,
        f"id:{transaction_id}",
        f"created_at:{_format_timestamp(created_at)}",
        f"prev:{prev_hash.hex()}",
        f"entries:{len(ordered)}",
    ]
    lines += [
        f"{e.account_id}:{e.currency}:{e.amount_minor}" for e in ordered
    ]
    return ("\n".join(lines) + "\n").encode("utf-8")


def transaction_hash(
    *,
    transaction_id: UUID,
    created_at: datetime,
    entries: list[HashableEntry],
    prev_hash: bytes,
) -> bytes:
    return hashlib.sha256(
        canonical_bytes(
            transaction_id=transaction_id,
            created_at=created_at,
            entries=entries,
            prev_hash=prev_hash,
        )
    ).digest()
