"""Configuration, read from ``RATCHET_*`` environment variables. docs/operations.md has the full table."""

import os
import secrets
import socket
from datetime import timedelta

from pydantic import Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


def _default_worker_id() -> str:
    return f"{socket.gethostname()}-{os.getpid()}-{secrets.token_hex(3)}"


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="RATCHET_", extra="ignore")

    database_url: SecretStr
    app: str = Field(description="module:attribute of the Registry this process serves")

    worker_id: str = Field(default_factory=_default_worker_id)
    concurrency: int = Field(default=32, ge=1, le=1024)
    pool_size: int = Field(default=10, ge=3, description="Postgres connections per process; see docs/operations.md")
    lease_seconds: float = Field(default=30, ge=1)
    idle_wait_max_seconds: float = Field(default=5, gt=0)
    max_history: int = Field(default=10_000, ge=10)
    shutdown_grace_seconds: float = Field(default=20, ge=0)

    @property
    def lease(self) -> timedelta:
        return timedelta(seconds=self.lease_seconds)

    @property
    def idle_wait_max(self) -> timedelta:
        return timedelta(seconds=self.idle_wait_max_seconds)

    @property
    def shutdown_grace(self) -> timedelta:
        return timedelta(seconds=self.shutdown_grace_seconds)

    api_token: SecretStr | None = None
    max_payload_bytes: int = Field(default=256 * 1024, ge=1024)

    otlp_endpoint: str | None = None
    log_level: str = "INFO"
