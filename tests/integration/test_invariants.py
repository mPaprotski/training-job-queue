"""Negative fixtures prove that the chaos checker can actually reject bad data."""

from uuid import uuid4

from harness.invariants import validate_database
from training_job_queue.db import connect
from training_job_queue.jobs import create_job
from training_job_queue.processing import claim_job, complete_job
from support import DatabaseTestCase


class InvariantCheckerTests(DatabaseTestCase):
    def setUp(self):
        with connect(self.settings) as connection:
            connection.execute("TRUNCATE job_effects, job_attempts, jobs")
        self.key, self.payload = uuid4().hex, {"value": 1}
        self.expected = {self.key: self.payload}

    def success(self):
        job, _ = create_job(self.settings, self.key, self.payload)
        claim_job(self.settings, "worker")
        complete_job(self.settings, job["id"], "worker", 1)
        return job

    def violations(self, **kwargs):
        summary = validate_database(self.settings, self.expected, **kwargs)
        self.assertFalse(summary["passed"])
        return {row["invariant"] for row in summary["violations"]}

    def test_healthy_fixture_passes(self):
        self.success()
        self.assertTrue(validate_database(self.settings, self.expected)["passed"])

    def test_missing_job_is_detected(self):
        self.assertIn("manifest_matches_jobs", self.violations())

    def test_stuck_running_is_detected(self):
        create_job(self.settings, self.key, self.payload)
        claim_job(self.settings, "worker")
        names = self.violations()
        self.assertIn("all_jobs_terminal", names)
        self.assertIn("no_open_attempts_after_drain", names)

    def test_duplicate_effect_is_detected_independently_of_constraint(self):
        self.success()
        with connect(self.settings) as connection:
            connection.execute("ALTER TABLE job_effects DROP CONSTRAINT job_effects_pkey")
            connection.execute("INSERT INTO job_effects SELECT * FROM job_effects")
        try:
            self.assertIn("effect_count_matches_state", self.violations())
        finally:
            with connect(self.settings) as connection:
                connection.execute("TRUNCATE job_effects")
                connection.execute("ALTER TABLE job_effects ADD PRIMARY KEY (job_id)")

    def test_bad_success_result_is_detected(self):
        self.success()
        with connect(self.settings) as connection:
            connection.execute("UPDATE jobs SET result = '{}'::jsonb")
        self.assertIn("success_result_and_producing_attempt_agree", self.violations())

    def test_duplicate_logical_job_is_detected_without_unique_constraint(self):
        with connect(self.settings) as connection:
            connection.execute("ALTER TABLE jobs DROP CONSTRAINT jobs_idempotency_key_unique")
            for _ in range(2):
                connection.execute(
                    """INSERT INTO jobs (idempotency_key, payload, state, finished_at)
                       VALUES (%s, '{\"value\":1}', 'cancelled', clock_timestamp())""", (self.key,),
                )
        try:
            self.assertIn("one_job_per_key", self.violations())
        finally:
            with connect(self.settings) as connection:
                connection.execute("TRUNCATE job_effects, job_attempts, jobs")
                connection.execute("ALTER TABLE jobs ADD CONSTRAINT jobs_idempotency_key_unique UNIQUE (idempotency_key)")

    def test_boolean_payload_is_not_equal_to_number(self):
        self.success()
        with connect(self.settings) as connection:
            connection.execute("UPDATE jobs SET payload = '{\"value\":true}'::jsonb")
        self.assertIn("manifest_matches_jobs", self.violations())

    def test_terminal_change_and_lost_acknowledged_cancel_are_detected(self):
        job = self.success()
        names = self.violations(terminal_seen={str(job["id"]): "failed"}, cancelled_ids={str(job["id"])})
        self.assertIn("observed_terminal_state_immutable", names)
        self.assertIn("acknowledged_cancel_persists", names)
