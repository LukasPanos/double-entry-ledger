"""Runtime configuration.

Everything is environment-driven so that the same image can be run as the API,
as the outbox relay, or as a load-test target with a different concurrency
strategy.
"""

from __future__ import annotations

from typing import Literal

from pydantic_settings import BaseSettings, SettingsConfigDict

ConcurrencyStrategy = Literal["pessimistic", "optimistic"]


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="LEDGER_",
        env_file=".env",
        extra="ignore",
    )

    database_url: str = "postgresql://ledger@127.0.0.1:55432/ledger"

    pool_min_size: int = 2
    pool_max_size: int = 32

    # Phase 4. Selects how POST /transactions serialises concurrent writers to
    # the same account. See ledger/services/transactions.py.
    concurrency_strategy: ConcurrencyStrategy = "pessimistic"

    # Only used by the optimistic strategy, which runs at SERIALIZABLE and must
    # retry on serialization failure (SQLSTATE 40001) and deadlock (40P01).
    max_retries: int = 10
    retry_base_delay_seconds: float = 0.002
    retry_max_delay_seconds: float = 0.25

    # Phase 3.
    hold_expiry_poll_seconds: float = 5.0
    run_hold_expiry_worker: bool = True

    # Phase 6.
    webhook_url: str | None = None
    # HMAC-SHA256 over the exact request body, sent as X-Signature. Optional so
    # the relay works against a bare receiver, but a real endpoint has no way to
    # tell a genuine event from a forged one without it.
    webhook_secret: str | None = None
    outbox_poll_seconds: float = 1.0
    outbox_batch_size: int = 32
    outbox_max_attempts: int = 6
    outbox_backoff_base_seconds: float = 1.0
    outbox_backoff_cap_seconds: float = 300.0
    outbox_http_timeout_seconds: float = 5.0
    run_outbox_relay: bool = True


_settings: Settings | None = None


def get_settings() -> Settings:
    global _settings
    if _settings is None:
        _settings = Settings()
    return _settings


def reset_settings() -> None:
    """Test hook: forget the cached Settings so env changes take effect."""
    global _settings
    _settings = None
