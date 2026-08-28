"""Transactional outbox and webhook relay.

## Why this exists

Posting a transaction and notifying a webhook are writes to two different
systems, and no database transaction spans both. Whichever order you choose, a
crash between them produces a lie: notify-then-write can announce a payment that
never happened, write-then-notify can process a payment nobody hears about.

`emit()` takes a cursor that is already inside the caller's transaction, so the
event row commits if and only if the ledger write commits. There is no window.
Delivery then becomes a separate problem against durable state, which is a
problem retries can actually solve.

## The claim is a lease, not a checkout

`claim_due()` increments `attempts` and pushes `next_attempt_at` forward, then
**commits** before any HTTP happens. Doing the HTTP call inside the claiming
transaction would hold a row lock for the duration of a network round trip, and
a hung endpoint would pin a lock for the whole timeout.

The consequence is deliberate: if the relay dies after claiming and before
recording the outcome, the event becomes due again when the lease lapses and is
delivered a second time. That is at-least-once, and it is the strongest
guarantee available without a transaction spanning both systems. Exactly-once is
completed at the *receiver*, by discarding event ids it has already seen. There
is no way to move that responsibility upstream, which is why the event id is in
the payload and in a header.

## What is guaranteed about ordering

Events are claimed and delivered in `id` order, so on the happy path a consumer
sees them in the order they were committed. **Retries break that**: an event that
fails is redelivered later, behind events that were created after it. Consumers
must therefore be idempotent and order-tolerant.

The alternative -- head-of-line blocking, where a failing event stalls everything
behind it until it dead-letters -- would give strict ordering at the cost of
letting one poison event stop notifications for every account in the system. For
a payments notification stream that is the wrong trade.

## The gotcha this design avoids

A tempting relay reads `WHERE id > last_seen_id`. That silently loses events.
Sequence values are handed out before commit, so a transaction holding id 5 can
commit before the transaction holding id 4; a high-water-mark reader that reaches
5 first will never look at 4 again. This relay keys off `status = 'pending'`
instead, so an event is only ever dismissed once its outcome is recorded.
"""

from __future__ import annotations

import hashlib
import hmac
import logging
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

import httpx
from psycopg import Cursor
from psycopg.types.json import Jsonb

from ledger.config import get_settings
from ledger.db import transaction

log = logging.getLogger("ledger.outbox")

EVENT_TRANSACTION_POSTED = "transaction.posted"
EVENT_HOLD_CREATED = "hold.created"
EVENT_HOLD_CAPTURED = "hold.captured"
EVENT_HOLD_VOIDED = "hold.voided"
EVENT_HOLD_EXPIRED = "hold.expired"


# ------------------------------------------------------------------- writing --


def emit(cur: Cursor, event_type: str, payload: dict[str, Any]) -> int:
    """Queue an event inside the caller's transaction. Returns the event id.

    Takes a cursor rather than opening its own transaction: that is the entire
    point of the pattern, and a version of this function that managed its own
    transaction would silently reintroduce the dual write it exists to remove.
    """
    cur.execute(
        """
        INSERT INTO outbox (event_type, payload)
        VALUES (%s, %s)
        RETURNING id
        """,
        (event_type, Jsonb(payload)),
    )
    row = cur.fetchone()
    assert row is not None
    return row["id"]


# ------------------------------------------------------------------ delivery --


@dataclass
class RelayStats:
    claimed: int = 0
    delivered: int = 0
    retried: int = 0
    dead: int = 0
    errors: list[str] = field(default_factory=list)

    def __bool__(self) -> bool:
        return self.claimed > 0


def backoff_seconds(attempts: int) -> float:
    """Exponential, capped. `attempts` is the count *including* the one that just
    failed, so the first retry waits `base`, not zero."""
    settings = get_settings()
    delay = settings.outbox_backoff_base_seconds * (2 ** max(0, attempts - 1))
    return min(settings.outbox_backoff_cap_seconds, delay)


def envelope(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": row["id"],
        "type": row["event_type"],
        "attempt": row["attempts"],
        "data": row["payload"],
    }


def sign(body: bytes, secret: str) -> str:
    return "sha256=" + hmac.new(
        secret.encode("utf-8"), body, hashlib.sha256
    ).hexdigest()


def claim_due(limit: int, lease_seconds: float) -> list[dict[str, Any]]:
    """Take a batch of due events and extend their lease. Commits immediately.

    SKIP LOCKED so two relay instances never fight over the same row, and
    `ORDER BY id` so delivery follows commit order.
    """
    with transaction() as cur:
        cur.execute(
            """
            UPDATE outbox
               SET attempts = attempts + 1,
                   next_attempt_at = now() + make_interval(secs => %s)
             WHERE id IN (
                     SELECT id FROM outbox
                      WHERE status = 'pending'
                        AND next_attempt_at <= now()
                      ORDER BY id
                      LIMIT %s
                        FOR UPDATE SKIP LOCKED
                   )
            RETURNING id, event_type, payload, attempts
            """,
            (lease_seconds, limit),
        )
        rows = cur.fetchall()
    return sorted(rows, key=lambda row: row["id"])


def mark_delivered(event_id: int) -> None:
    with transaction() as cur:
        cur.execute(
            """
            UPDATE outbox
               SET status = 'delivered', delivered_at = now(),
                   next_attempt_at = now()
             WHERE id = %s AND status = 'pending'
            """,
            (event_id,),
        )


def mark_dead(event_id: int, attempts: int, reason: str) -> None:
    with transaction() as cur:
        cur.execute(
            "UPDATE outbox SET status = 'dead' WHERE id = %s AND status = 'pending'",
            (event_id,),
        )
    log.error(
        "outbox event %s dead-lettered after %s attempt(s): %s",
        event_id,
        attempts,
        reason,
    )


def mark_failed(event_id: int, attempts: int, reason: str) -> str:
    """Schedule a retry, or dead-letter the event. Returns the new status."""
    settings = get_settings()
    if attempts >= settings.outbox_max_attempts:
        mark_dead(event_id, attempts, reason)
        return "dead"

    delay = backoff_seconds(attempts)
    with transaction() as cur:
        cur.execute(
            """
            UPDATE outbox
               SET next_attempt_at = now() + make_interval(secs => %s)
             WHERE id = %s AND status = 'pending'
            """,
            (delay, event_id),
        )
    log.info(
        "outbox event %s failed (attempt %s), retrying in %.1fs: %s",
        event_id,
        attempts,
        delay,
        reason,
    )
    return "pending"


class PermanentDeliveryFailure(Exception):
    """The endpoint rejected the event in a way retrying cannot fix."""


# 4xx codes that *are* worth retrying. Everything else in the 4xx range means the
# request itself is unacceptable -- a bad signature, a wrong path, a payload the
# consumer refuses -- and sending it again unchanged will get the same answer.
RETRYABLE_CLIENT_STATUS = frozenset({408, 425, 429})


def deliver(client: httpx.Client, url: str, row: dict[str, Any]) -> None:
    """POST one event.

    Raises `PermanentDeliveryFailure` for a client error that retrying cannot
    fix, and any other exception for something worth retrying.

    The distinction matters operationally. Before it existed, a misconfigured
    webhook secret meant every event burned the full retry budget on 401s and
    logged an error each time -- thousands of log lines describing one
    configuration mistake, and a backlog that took the whole backoff schedule to
    clear. Now a permanent rejection dead-letters on the first attempt, which is
    both cheaper and a much clearer signal.
    """
    settings = get_settings()
    body = _serialise(envelope(row))
    headers = {
        "Content-Type": "application/json",
        # The receiver dedups on this. It is the outbox primary key, so it is
        # unique and stable across redeliveries of the same event.
        "X-Event-Id": str(row["id"]),
        "X-Event-Type": row["event_type"],
        "X-Attempt": str(row["attempts"]),
    }
    if settings.webhook_secret:
        # Signed over the exact bytes on the wire, so the receiver can verify
        # without re-serialising and risking a different encoding.
        headers["X-Signature"] = sign(body, settings.webhook_secret)

    response = client.post(url, content=body, headers=headers)
    if response.is_success:
        return
    if (
        response.status_code >= 500
        or response.status_code in RETRYABLE_CLIENT_STATUS
    ):
        response.raise_for_status()
    raise PermanentDeliveryFailure(
        f"HTTP {response.status_code} {response.reason_phrase}"
    )


def _serialise(payload: dict[str, Any]) -> bytes:
    import json

    return json.dumps(
        payload, sort_keys=True, separators=(",", ":"), default=str
    ).encode("utf-8")


def relay_once(url: str | None = None) -> RelayStats:
    """One pass: claim what is due, deliver it, record each outcome."""
    settings = get_settings()
    url = url or settings.webhook_url
    stats = RelayStats()
    if not url:
        return stats

    lease = settings.outbox_http_timeout_seconds * 2 + 5
    rows = claim_due(settings.outbox_batch_size, lease)
    stats.claimed = len(rows)
    if not rows:
        return stats

    with httpx.Client(timeout=settings.outbox_http_timeout_seconds) as client:
        for row in rows:
            try:
                deliver(client, url, row)
            except PermanentDeliveryFailure as exc:
                # No retry budget spent: sending the same bytes again would get
                # the same rejection.
                stats.errors.append(str(exc))
                mark_dead(row["id"], row["attempts"], str(exc))
                stats.dead += 1
            except Exception as exc:  # noqa: BLE001 -- every failure is a retry
                # First line only: httpx's status errors carry a multi-line
                # message with a documentation link, which makes relay logs
                # unreadable at any real volume.
                first_line = str(exc).splitlines()[0] if str(exc) else ""
                reason = f"{type(exc).__name__}: {first_line}"
                stats.errors.append(reason)
                if mark_failed(row["id"], row["attempts"], reason) == "dead":
                    stats.dead += 1
                else:
                    stats.retried += 1
            else:
                mark_delivered(row["id"])
                stats.delivered += 1

    return stats


def drain(
    url: str | None = None,
    *,
    max_passes: int = 200,
    sleep_seconds: float = 0.02,
) -> RelayStats:
    """Relay until nothing is left pending, or `max_passes` is reached.

    Used by tests and by `scripts/relay.py --once`. Retry backoff is honoured, so
    this respects `next_attempt_at` rather than hammering a failing endpoint --
    which means tests need a small `outbox_backoff_base_seconds`.
    """
    total = RelayStats()
    for _ in range(max_passes):
        if pending_count() == 0:
            break
        stats = relay_once(url)
        total.claimed += stats.claimed
        total.delivered += stats.delivered
        total.retried += stats.retried
        total.dead += stats.dead
        total.errors.extend(stats.errors)
        if stats.claimed == 0:
            # Everything left is waiting on its backoff; give it a moment.
            time.sleep(sleep_seconds)
    return total


# ---------------------------------------------------------------- inspection --


def pending_count() -> int:
    with transaction(read_only=True) as cur:
        cur.execute("SELECT count(*) AS n FROM outbox WHERE status = 'pending'")
        return cur.fetchone()["n"]  # type: ignore[index]


def stats() -> dict[str, Any]:
    with transaction(read_only=True) as cur:
        cur.execute(
            """
            SELECT status::text AS status,
                   count(*)     AS events,
                   max(attempts) AS max_attempts
              FROM outbox
             GROUP BY status
            """
        )
        by_status = {
            row["status"]: {
                "events": row["events"],
                "max_attempts": row["max_attempts"],
            }
            for row in cur.fetchall()
        }
        cur.execute(
            """
            SELECT min(created_at) AS oldest_pending
              FROM outbox WHERE status = 'pending'
            """
        )
        oldest = cur.fetchone()["oldest_pending"]  # type: ignore[index]

    lag_seconds = None
    if oldest is not None:
        lag_seconds = round(
            (datetime.now(timezone.utc) - oldest).total_seconds(), 3
        )

    return {
        "by_status": by_status,
        "pending": by_status.get("pending", {}).get("events", 0),
        "delivered": by_status.get("delivered", {}).get("events", 0),
        "dead": by_status.get("dead", {}).get("events", 0),
        "oldest_pending_age_seconds": lag_seconds,
    }
