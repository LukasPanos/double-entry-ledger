#!/usr/bin/env python
"""A deliberately unreliable webhook receiver.

    python -m scripts.receiver --port 8001 --fail-rate 0.3 --secret hunter2

Endpoints:

    POST /webhook   accept an event, then fail with probability --fail-rate
    GET  /events    what it has seen: unique ids, request count, duplicates
    POST /reset     forget everything

The important detail is *where* the failure happens. This receiver **records the
event first and then returns 500**. That is the failure mode that actually
exercises the delivery contract: the event was processed, the acknowledgement was
lost, and the relay will send it again. A receiver that failed before processing
would only test that retries happen, which is the easy half.

So every simulated failure produces a duplicate delivery, and the dedup set is
what makes the end result exactly-once. `GET /events` reports `duplicates`
precisely so a test can assert that duplicates really occurred rather than
passing vacuously.

Uses only the standard library, so it can be run without the project's
dependencies installed -- it stands in for someone else's server.
"""

from __future__ import annotations

import argparse
import hashlib
import hmac
import json
import random
import threading
from collections import Counter
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any


class Recorder:
    """Thread-safe record of what arrived. The dedup set is the whole point."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self.seen: dict[str, dict[str, Any]] = {}
        self.request_count = 0
        self.duplicate_count = 0
        self.rejected_signatures = 0
        self.failures_injected = 0
        self.by_type: Counter[str] = Counter()
        self.attempts_per_event: dict[str, int] = {}

    def record(self, event_id: str, event: dict[str, Any]) -> bool:
        """Returns True if this is the first time we have seen `event_id`."""
        with self._lock:
            self.request_count += 1
            self.attempts_per_event[event_id] = (
                self.attempts_per_event.get(event_id, 0) + 1
            )
            if event_id in self.seen:
                self.duplicate_count += 1
                return False
            self.seen[event_id] = event
            self.by_type[str(event.get("type"))] += 1
            return True

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            return {
                "unique_events": len(self.seen),
                "request_count": self.request_count,
                "duplicates": self.duplicate_count,
                "rejected_signatures": self.rejected_signatures,
                "failures_injected": self.failures_injected,
                "by_type": dict(self.by_type),
                "event_ids": sorted(self.seen, key=int),
                "max_attempts_for_one_event": max(
                    self.attempts_per_event.values(), default=0
                ),
            }

    def reset(self) -> None:
        with self._lock:
            self.seen.clear()
            self.attempts_per_event.clear()
            self.by_type.clear()
            self.request_count = 0
            self.duplicate_count = 0
            self.rejected_signatures = 0
            self.failures_injected = 0


def make_handler(
    recorder: Recorder, fail_rate: float, secret: str | None, seed: int | None
) -> type[BaseHTTPRequestHandler]:
    rng = random.Random(seed)
    rng_lock = threading.Lock()

    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def log_message(self, *args: Any) -> None:  # noqa: A003
            pass  # quiet; the test asserts on state, not on stderr

        def _send(self, status: int, body: dict[str, Any]) -> None:
            encoded = json.dumps(body).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(encoded)))
            self.end_headers()
            self.wfile.write(encoded)

        def do_GET(self) -> None:  # noqa: N802
            if self.path.startswith("/events"):
                self._send(200, recorder.snapshot())
            else:
                self._send(404, {"error": "not found"})

        def do_POST(self) -> None:  # noqa: N802
            length = int(self.headers.get("Content-Length", "0"))
            raw = self.rfile.read(length)

            if self.path.startswith("/reset"):
                recorder.reset()
                self._send(200, {"ok": True})
                return

            if not self.path.startswith("/webhook"):
                self._send(404, {"error": "not found"})
                return

            if secret is not None:
                expected = (
                    "sha256="
                    + hmac.new(secret.encode("utf-8"), raw, hashlib.sha256).hexdigest()
                )
                supplied = self.headers.get("X-Signature", "")
                # compare_digest, not ==: a plain comparison leaks how much of
                # the signature matched via timing.
                if not hmac.compare_digest(expected, supplied):
                    with recorder._lock:
                        recorder.rejected_signatures += 1
                    self._send(401, {"error": "bad signature"})
                    return

            event_id = self.headers.get("X-Event-Id")
            if not event_id:
                self._send(400, {"error": "missing X-Event-Id"})
                return

            try:
                event = json.loads(raw)
            except json.JSONDecodeError:
                self._send(400, {"error": "invalid json"})
                return

            first_time = recorder.record(event_id, event)

            # Record first, then fail. See the module docstring.
            with rng_lock:
                should_fail = rng.random() < fail_rate
            if should_fail:
                with recorder._lock:
                    recorder.failures_injected += 1
                self._send(500, {"error": "injected failure", "id": event_id})
                return

            self._send(200, {"ok": True, "id": event_id, "duplicate": not first_time})

    return Handler


class ReceiverServer:
    """Runs the receiver in a background thread. Port 0 picks a free port."""

    def __init__(
        self,
        *,
        port: int = 0,
        fail_rate: float = 0.3,
        secret: str | None = None,
        seed: int | None = None,
    ) -> None:
        self.recorder = Recorder()
        handler = make_handler(self.recorder, fail_rate, secret, seed)
        self._server = ThreadingHTTPServer(("127.0.0.1", port), handler)
        self._server.daemon_threads = True
        self.port = self._server.server_address[1]
        self.url = f"http://127.0.0.1:{self.port}/webhook"
        self._thread = threading.Thread(target=self._server.serve_forever, daemon=True)

    def __enter__(self) -> "ReceiverServer":
        self._thread.start()
        return self

    def __exit__(self, *exc: Any) -> None:
        self._server.shutdown()
        self._server.server_close()
        self._thread.join(timeout=5)

    def snapshot(self) -> dict[str, Any]:
        return self.recorder.snapshot()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--port", type=int, default=8001)
    parser.add_argument("--fail-rate", type=float, default=0.3)
    parser.add_argument("--secret", default=None)
    parser.add_argument("--seed", type=int, default=None)
    args = parser.parse_args()

    recorder = Recorder()
    handler = make_handler(recorder, args.fail_rate, args.secret, args.seed)
    server = ThreadingHTTPServer(("127.0.0.1", args.port), handler)
    print(
        f"receiver listening on http://127.0.0.1:{args.port}/webhook "
        f"(fail rate {args.fail_rate:.0%}"
        f"{', signature required' if args.secret else ''})"
    )
    print(f"state at   http://127.0.0.1:{args.port}/events")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print(f"\n{json.dumps(recorder.snapshot(), indent=2)}")
    finally:
        server.server_close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
