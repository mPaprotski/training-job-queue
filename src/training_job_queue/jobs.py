"""Transactional job operations. Every writer locks jobs before attempt rows."""

import base64
import binascii
from datetime import datetime
import json
import secrets
from typing import Any
from uuid import UUID

from psycopg.types.json import Jsonb

from training_job_queue.db import transaction as connect
from training_job_queue.settings import Settings


class JobNotFound(Exception):
    pass


class JobConflict(Exception):
    pass


class QueueFull(Exception):
    pass


ADMISSION_RETRIES = 16


def _existing_job(connection, key, payload):
    existing = connection.execute(
        "SELECT *, payload = %s AS same_payload FROM jobs WHERE idempotency_key = %s",
        (Jsonb(payload), key),
    ).fetchone()
    if existing is not None and not existing.pop("same_payload"):
        raise JobConflict("Idempotency key already belongs to a different payload")
    return existing


def create_job(settings: Settings, key: str, payload: dict) -> tuple[dict, bool]:
    with connect(settings) as connection:
        existing = _existing_job(connection, key, payload)
        if existing is not None:
            return existing, False
        capacity = settings.max_outstanding_jobs
        # Existing jobs survive a lowered limit. Drain out-of-range reservations
        # first; otherwise free in-range slots could admit above that new limit.
        oversized = connection.execute(
            """SELECT 1 FROM jobs WHERE state IN ('queued', 'running')
               AND admission_slot > %s LIMIT 1""", (capacity,),
        ).fetchone()
        if oversized is None:
            start = secrets.randbelow(capacity)
            for _ in range(ADMISSION_RETRIES):
                candidate = connection.execute(
                    """SELECT candidate.slot FROM (
                        SELECT n AS ordinal, 1 + ((n + %s) %% %s) AS slot
                        FROM generate_series(0, %s - 1) AS candidates(n)
                    ) AS candidate WHERE NOT EXISTS (
                        SELECT 1 FROM jobs WHERE state IN ('queued', 'running')
                        AND admission_slot = candidate.slot
                    ) ORDER BY candidate.ordinal LIMIT 1""", (start, capacity, capacity),
                ).fetchone()
                if candidate is None:
                    break
                # The partial unique index reserves this slot until the job
                # becomes terminal. Different free slots commit independently.
                job = connection.execute(
                    """INSERT INTO jobs (idempotency_key, payload, max_attempts, admission_slot)
                       VALUES (%s, %s, %s, %s) ON CONFLICT DO NOTHING RETURNING *""",
                    (key, Jsonb(payload), settings.max_attempts, candidate['slot']),
                ).fetchone()
                if job:
                    return job, True
                # READ COMMITTED sees a committed key/slot winner on retry.
                existing = _existing_job(connection, key, payload)
                if existing is not None:
                    return existing, False
        # A concurrent same-key winner must replay even if it filled the queue.
        existing = _existing_job(connection, key, payload)
        if existing is not None:
            return existing, False
        raise QueueFull


def get_job(settings: Settings, job_id: UUID) -> dict:
    with connect(settings) as connection:
        job = connection.execute("SELECT * FROM jobs WHERE id = %s", (job_id,)).fetchone()
        if job is None:
            raise JobNotFound
        return job


def encode_cursor(job: dict) -> str:
    raw = json.dumps([job["created_at"].isoformat(), str(job["id"])], separators=(",", ":"))
    return base64.urlsafe_b64encode(raw.encode()).decode().rstrip("=")


def decode_cursor(cursor: str) -> tuple[datetime, UUID]:
    try:
        raw = base64.b64decode(cursor + "=" * (-len(cursor) % 4), altchars=b"-_", validate=True)
        values = json.loads(raw)
        if not isinstance(values, list) or len(values) != 2 or not all(isinstance(x, str) for x in values):
            raise ValueError
        created_at, job_id = datetime.fromisoformat(values[0]), UUID(values[1])
        if created_at.tzinfo is None or created_at.utcoffset() is None:
            raise ValueError
        return created_at, job_id
    except (ValueError, TypeError, binascii.Error, UnicodeError) as error:
        raise ValueError("Invalid pagination cursor") from error


def list_jobs(settings: Settings, limit: int, after: tuple[datetime, UUID] | None) -> dict[str, Any]:
    with connect(settings) as connection:
        if after is None:
            rows = connection.execute(
                "SELECT * FROM jobs ORDER BY created_at, id LIMIT %s", (limit + 1,),
            ).fetchall()
        else:
            rows = connection.execute(
                "SELECT * FROM jobs WHERE (created_at, id) > (%s, %s) ORDER BY created_at, id LIMIT %s",
                (*after, limit + 1),
            ).fetchall()
    items = rows[:limit]
    return {"items": items, "next_cursor": encode_cursor(items[-1]) if len(rows) > limit else None}


def cancel_job(settings: Settings, job_id: UUID) -> dict:
    with connect(settings) as connection:
        job = connection.execute("SELECT * FROM jobs WHERE id = %s FOR UPDATE", (job_id,)).fetchone()
        if job is None:
            raise JobNotFound
        if job["state"] == "cancelled":
            return job
        if job["state"] in ("succeeded", "failed"):
            raise JobConflict("Job is already completed and cannot be cancelled")
        if job["state"] == "running":
            attempt = connection.execute(
                """UPDATE job_attempts SET outcome = 'cancelled', finished_at = clock_timestamp()
                   WHERE job_id = %s AND attempt_no = %s AND outcome = 'running' RETURNING attempt_no""",
                (job_id, job["attempt_no"]),
            ).fetchone()
            if attempt is None:
                raise RuntimeError("Running job has no matching active attempt")
        return connection.execute(
            """UPDATE jobs SET state = 'cancelled', worker_id = NULL, lease_expires_at = NULL,
               finished_at = clock_timestamp(), error = NULL WHERE id = %s RETURNING *""",
            (job_id,),
        ).fetchone()
