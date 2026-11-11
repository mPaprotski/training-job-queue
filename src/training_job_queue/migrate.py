"""Apply ordered SQL files atomically, with checksums and a migration lock."""

import hashlib
import logging
from pathlib import Path

from training_job_queue.db import connect
from training_job_queue.settings import Settings

logger = logging.getLogger(__name__)
MIGRATION_LOCK = 714309221


def migrate(directory: Path) -> None:
    if not directory.is_dir():
        raise ValueError(f"Migration directory does not exist: {directory}")
    with connect(Settings.from_env(), autocommit=True) as connection:
        # Session lock serializes complete runs, including first-time bootstrap.
        connection.execute("SELECT pg_advisory_lock(%s)", (MIGRATION_LOCK,))
        try:
            connection.execute("""
                CREATE TABLE IF NOT EXISTS schema_migrations (
                    name TEXT PRIMARY KEY,
                    checksum TEXT NOT NULL,
                    applied_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
                )
            """)
            for path in sorted(directory.glob("*.sql")):
                source = path.read_bytes()
                checksum = hashlib.sha256(source).hexdigest()
                with connection.transaction():
                    existing = connection.execute(
                        "SELECT checksum FROM schema_migrations WHERE name = %s", (path.name,)
                    ).fetchone()
                    if existing:
                        if existing["checksum"] != checksum:
                            raise RuntimeError(f"Applied migration was modified: {path.name}")
                        continue
                    connection.execute(source.decode("utf-8"))
                    connection.execute(
                        "INSERT INTO schema_migrations (name, checksum) VALUES (%s, %s)",
                        (path.name, checksum),
                    )
                logger.info("Applied %s", path.name)
        finally:
            connection.execute("SELECT pg_advisory_unlock(%s)", (MIGRATION_LOCK,))
    logger.info("Migrations complete")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    migrate(Path("migrations"))
