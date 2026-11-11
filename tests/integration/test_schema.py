"""Database constraints against an isolated, automatically removed PostgreSQL DB."""

from concurrent.futures import ThreadPoolExecutor
from threading import Barrier
import unittest
from uuid import uuid4

import psycopg

from training_job_queue.db import connect
from support import DatabaseTestCase


class SchemaTests(DatabaseTestCase):
    def setUp(self):
        self.connection = connect(self.settings)
        self.addCleanup(self.connection.close)

    def job(self, key=None):
        return self.connection.execute(
            "INSERT INTO jobs (idempotency_key, payload, admission_slot) VALUES (%s, '{}'::jsonb, (SELECT COALESCE(max(admission_slot),0)+1 FROM jobs)) RETURNING id",
            (key or uuid4().hex,),
        ).fetchone()["id"]

    def running_job(self):
        job_id = self.job()
        self.connection.execute(
            """UPDATE jobs SET state = 'running', attempt_no = 1, worker_id = 'worker-1',
               started_at = clock_timestamp(), lease_expires_at = clock_timestamp() + interval '15 seconds'
               WHERE id = %s""", (job_id,),
        )
        self.connection.execute(
            "INSERT INTO job_attempts (job_id, attempt_no, worker_id) VALUES (%s, 1, 'worker-1')",
            (job_id,),
        )
        return job_id

    def rejects(self, error, statement, parameters=()):
        with self.assertRaises(error):
            with self.connection.transaction():
                self.connection.execute(statement, parameters)

    def test_fresh_migration_and_rerun(self):
        rows = self.connection.execute("SELECT name FROM schema_migrations ORDER BY name").fetchall()
        self.assertEqual(rows, [{"name": "0001_job_schema.sql"}, {"name": "0002_outstanding_index.sql"}, {"name": "0003_admission_slots.sql"}])

    def test_valid_successful_lifecycle(self):
        job_id = self.running_job()
        self.connection.execute(
            "INSERT INTO job_effects (job_id, attempt_no, effect) VALUES (%s, 1, '{\"written\": true}')",
            (job_id,),
        )
        self.connection.execute(
            "UPDATE job_attempts SET outcome = 'succeeded', finished_at = clock_timestamp() WHERE job_id = %s",
            (job_id,),
        )
        self.connection.execute(
            """UPDATE jobs SET state = 'succeeded', worker_id = NULL, lease_expires_at = NULL,
               finished_at = clock_timestamp(), result = '{}'::jsonb WHERE id = %s""", (job_id,),
        )
        self.connection.commit()
        self.assertEqual(self.connection.execute(
            "SELECT count(*) AS n FROM job_effects WHERE job_id = %s", (job_id,),
        ).fetchone()["n"], 1)

    def test_concurrent_duplicate_keys(self):
        key, barrier = uuid4().hex, Barrier(2)

        def insert():
            with connect(self.settings, autocommit=True) as connection:
                barrier.wait(timeout=10)
                try:
                    connection.execute(
                        "INSERT INTO jobs (idempotency_key, payload, admission_slot) VALUES (%s, '{}', (SELECT COALESCE(max(admission_slot),0)+1 FROM jobs))", (key,),
                    )
                    return "inserted"
                except psycopg.errors.UniqueViolation:
                    return "duplicate"

        with ThreadPoolExecutor(max_workers=2) as executor:
            self.assertCountEqual(list(executor.map(lambda _: insert(), range(2))), ["inserted", "duplicate"])

    def test_duplicate_effect(self):
        job_id = self.running_job()
        statement = "INSERT INTO job_effects (job_id, attempt_no, effect) VALUES (%s, 1, '{}')"
        self.connection.execute(statement, (job_id,))
        self.rejects(psycopg.errors.UniqueViolation, statement, (job_id,))

    def test_effect_requires_existing_attempt(self):
        self.rejects(psycopg.errors.ForeignKeyViolation,
                     "INSERT INTO job_effects (job_id, attempt_no, effect) VALUES (%s, 1, '{}')",
                     (self.job(),))

    def test_only_one_running_attempt(self):
        self.rejects(psycopg.errors.UniqueViolation,
                     "INSERT INTO job_attempts (job_id, attempt_no, worker_id) VALUES (%s, 2, 'worker-2')",
                     (self.running_job(),))

    def test_retry_after_expired_attempt(self):
        job_id = self.running_job()
        self.connection.execute(
            "UPDATE job_attempts SET outcome = 'expired', finished_at = clock_timestamp() WHERE job_id = %s",
            (job_id,),
        )
        self.connection.execute(
            "INSERT INTO job_attempts (job_id, attempt_no, worker_id) VALUES (%s, 2, 'worker-2')", (job_id,),
        )

    def test_invalid_job_shapes(self):
        job_id = self.job()
        for update in (
            "state = 'unknown'", "state = 'running'", "state = 'cancelled'",
            "state = 'failed', finished_at = clock_timestamp()",
            "result = '{}'::jsonb", "worker_id = 'unexpected'", "attempt_no = 6",
            "max_attempts = 0", "payload = '[]'::jsonb", "idempotency_key = '   '",
        ):
            with self.subTest(update=update):
                # All SQL fragments above are static test inputs, never user data.
                self.rejects(psycopg.errors.CheckViolation,
                             f"UPDATE jobs SET {update} WHERE id = %s", (job_id,))

    def test_cancel_queued_and_fail_running(self):
        self.connection.execute(
            "UPDATE jobs SET state = 'cancelled', finished_at = clock_timestamp() WHERE id = %s",
            (self.job(),),
        )
        job_id = self.running_job()
        self.connection.execute(
            """UPDATE jobs SET state = 'failed', worker_id = NULL, lease_expires_at = NULL,
               finished_at = clock_timestamp(), error = 'permanent failure' WHERE id = %s""", (job_id,),
        )
        self.connection.execute(
            """UPDATE job_attempts SET outcome = 'failed', finished_at = clock_timestamp(),
               error = 'permanent failure' WHERE job_id = %s""", (job_id,),
        )

    def test_attempt_shapes_and_identity(self):
        job_id = self.running_job()
        self.rejects(psycopg.errors.UniqueViolation,
                     "INSERT INTO job_attempts (job_id, attempt_no, worker_id) VALUES (%s, 1, 'worker-1')",
                     (job_id,))
        for update in ("outcome = 'unknown'", "outcome = 'succeeded'", "finished_at = clock_timestamp()"):
            with self.subTest(update=update):
                self.rejects(psycopg.errors.CheckViolation,
                             f"UPDATE job_attempts SET {update} WHERE job_id = %s", (job_id,))


if __name__ == "__main__":
    unittest.main()
