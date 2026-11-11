"""Environment configuration shared by service processes."""

import math
import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    database_url: str
    lease_seconds: float
    heartbeat_seconds: float
    max_attempts: int
    poll_seconds: float
    simulation_seconds: float = 0.1
    max_outstanding_jobs: int = 1000
    db_pool_enabled: bool = True
    db_pool_size: int = 4
    db_pool_timeout: float = 1.0

    @classmethod
    def from_env(cls) -> "Settings":
        database_url = os.environ.get("DATABASE_URL")
        if not database_url:
            raise ValueError("DATABASE_URL is required")
        settings = cls(
            database_url=database_url,
            lease_seconds=float(os.getenv("LEASE_SECONDS", "15")),
            heartbeat_seconds=float(os.getenv("HEARTBEAT_SECONDS", "3")),
            max_attempts=int(os.getenv("MAX_ATTEMPTS", "5")),
            poll_seconds=float(os.getenv("POLL_SECONDS", "1")),
            simulation_seconds=float(os.getenv("SIMULATION_SECONDS", "0.1")),
            max_outstanding_jobs=int(os.getenv("MAX_OUTSTANDING_JOBS", "1000")),
            db_pool_enabled=os.getenv("DB_POOL_ENABLED", "1") == "1",
            db_pool_size=int(os.getenv("DB_POOL_SIZE", "4")),
            db_pool_timeout=float(os.getenv("DB_POOL_TIMEOUT", "1")),
        )
        for name in ("lease_seconds", "heartbeat_seconds", "poll_seconds"):
            value = getattr(settings, name)
            if not math.isfinite(value) or value <= 0:
                raise ValueError(f"{name} must be finite and positive")
        if settings.heartbeat_seconds >= settings.lease_seconds:
            raise ValueError("heartbeat_seconds must be less than lease_seconds")
        if settings.max_attempts < 1:
            raise ValueError("max_attempts must be positive")
        if not 1 <= settings.max_outstanding_jobs <= 1000000:
            raise ValueError("max_outstanding_jobs must be between 1 and 1000000")
        if os.getenv("DB_POOL_ENABLED", "1") not in ("0", "1"):
            raise ValueError("DB_POOL_ENABLED must be 0 or 1")
        if not 1 <= settings.db_pool_size <= 16:
            raise ValueError("db_pool_size must be between 1 and 16")
        if not math.isfinite(settings.db_pool_timeout) or not 0 < settings.db_pool_timeout <= 5:
            raise ValueError("db_pool_timeout must be finite, positive and at most 5 seconds")
        if not math.isfinite(settings.simulation_seconds) or not 0 <= settings.simulation_seconds <= 300:
            raise ValueError("simulation_seconds must be finite and between 0 and 300")
        return settings
