"""Lease/fencing protocol. Lock order is always job, then attempt, then effect."""

from datetime import timedelta
from uuid import UUID

import psycopg
from psycopg.types.json import Jsonb

from training_job_queue.db import transaction as connect
from training_job_queue.settings import Settings


def _now(connection: psycopg.Connection):
    # Called only AFTER acquiring the job row lock, never transaction-start time.
    return connection.execute("SELECT clock_timestamp() AS now").fetchone()["now"]


def _close_attempt(connection, job: dict, outcome: str, now, error: str | None = None) -> None:
    row = connection.execute(
        """UPDATE job_attempts SET outcome = %s, finished_at = %s, error = %s
           WHERE job_id = %s AND attempt_no = %s AND worker_id = %s AND outcome = 'running'
           RETURNING attempt_no""",
        (outcome, now, error, job["id"], job["attempt_no"], job["worker_id"]),
    ).fetchone()
    if row is None:
        raise RuntimeError("Running job has no matching active attempt")


def claim_job(settings: Settings, worker_id: str) -> dict | None:
    with connect(settings) as connection:
        job = connection.execute(
            """SELECT * FROM jobs WHERE state = 'queued' AND attempt_no < max_attempts
               ORDER BY created_at, id LIMIT 1 FOR UPDATE SKIP LOCKED""",
        ).fetchone()
        if job is None:
            return None
        now = _now(connection)
        job = connection.execute(
            """UPDATE jobs SET state = 'running', attempt_no = attempt_no + 1,
               worker_id = %s, lease_expires_at = %s, started_at = COALESCE(started_at, %s),
               error = NULL WHERE id = %s RETURNING *""",
            (worker_id, now + timedelta(seconds=settings.lease_seconds), now, job["id"]),
        ).fetchone()
        connection.execute(
            "INSERT INTO job_attempts (job_id, attempt_no, worker_id, started_at) VALUES (%s, %s, %s, %s)",
            (job["id"], job["attempt_no"], worker_id, now),
        )
        return job


def _owned_job(connection, job_id: UUID, worker_id: str, attempt_no: int):
    job = connection.execute("SELECT * FROM jobs WHERE id = %s FOR UPDATE", (job_id,)).fetchone()
    if job is None or job["state"] != "running" or job["worker_id"] != worker_id or job["attempt_no"] != attempt_no:
        return None, None
    now = _now(connection)
    if job["lease_expires_at"] <= now:
        return None, None
    return job, now


def renew_lease(settings: Settings, job_id: UUID, worker_id: str, attempt_no: int) -> bool:
    with connect(settings) as connection:
        job, now = _owned_job(connection, job_id, worker_id, attempt_no)
        if job is None:
            return False
        connection.execute(
            "UPDATE jobs SET lease_expires_at = %s WHERE id = %s",
            (now + timedelta(seconds=settings.lease_seconds), job_id),
        )
        return True


def complete_job(settings: Settings, job_id: UUID, worker_id: str, attempt_no: int) -> bool:
    with connect(settings) as connection:
        job, now = _owned_job(connection, job_id, worker_id, attempt_no)
        if job is None:
            return False
        effect = {"job_id": str(job_id), "attempt_no": attempt_no, "worker_id": worker_id}
        _close_attempt(connection, job, "succeeded", now)
        # A pre-existing effect on a running job is an invariant violation.
        connection.execute(
            "INSERT INTO job_effects (job_id, attempt_no, effect, created_at) VALUES (%s, %s, %s, %s)",
            (job_id, attempt_no, Jsonb(effect), now),
        )
        connection.execute(
            """UPDATE jobs SET state = 'succeeded', worker_id = NULL, lease_expires_at = NULL,
               finished_at = %s, result = %s, error = NULL WHERE id = %s""",
            (now, Jsonb(effect), job_id),
        )
        return True


def fail_job(
    settings: Settings, job_id: UUID, worker_id: str, attempt_no: int,
    error: str, *, permanent: bool = False,
) -> bool:
    with connect(settings) as connection:
        job, now = _owned_job(connection, job_id, worker_id, attempt_no)
        if job is None:
            return False
        terminal = permanent or job["attempt_no"] >= job["max_attempts"]
        _close_attempt(connection, job, "failed", now, error)
        connection.execute(
            """UPDATE jobs SET state = %s, worker_id = NULL, lease_expires_at = NULL,
               finished_at = %s, error = %s WHERE id = %s""",
            ("failed" if terminal else "queued", now if terminal else None, error, job_id),
        )
        return True


def recover_expired(settings: Settings, batch_size: int = 100) -> int:
    with connect(settings) as connection:
        jobs = connection.execute(
            """SELECT * FROM jobs WHERE state = 'running' AND lease_expires_at <= clock_timestamp()
               ORDER BY lease_expires_at, id LIMIT %s FOR UPDATE SKIP LOCKED""", (batch_size,),
        ).fetchall()
        recovered = 0
        for job in jobs:
            now = _now(connection)
            if job["lease_expires_at"] > now:
                continue
            exhausted = job["attempt_no"] >= job["max_attempts"]
            error = "Worker lease expired" + ("; attempt budget exhausted" if exhausted else "")
            _close_attempt(connection, job, "expired", now, error)
            connection.execute(
                """UPDATE jobs SET state = %s, worker_id = NULL, lease_expires_at = NULL,
                   finished_at = %s, error = %s WHERE id = %s""",
                ("failed" if exhausted else "queued", now if exhausted else None, error, job["id"]),
            )
            recovered += 1
        return recovered
