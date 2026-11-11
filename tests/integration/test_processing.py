"""Worker protocol and execution checks on an isolated PostgreSQL database."""

from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
import os
import subprocess
import sys
import tempfile
from threading import Barrier, Event
import time
from uuid import uuid4

import psycopg

from training_job_queue.db import connect
from training_job_queue.jobs import cancel_job, create_job, get_job, JobConflict
from training_job_queue.processing import claim_job, complete_job, fail_job, recover_expired, renew_lease
from training_job_queue.worker import execute_job
from support import DatabaseTestCase


class ProcessingTests(DatabaseTestCase):
    def setUp(self):
        with connect(self.settings) as connection:
            connection.execute("TRUNCATE job_effects, job_attempts, jobs")

    def create(self, payload=None, settings=None):
        return create_job(settings or self.settings, uuid4().hex, payload or {})[0]

    def expire(self, job_id):
        with connect(self.settings) as connection:
            connection.execute(
                "UPDATE jobs SET lease_expires_at = clock_timestamp() - interval '1 second' WHERE id = %s",
                (job_id,),
            )

    def effects(self, job_id):
        with connect(self.settings) as connection:
            return connection.execute("SELECT * FROM job_effects WHERE job_id = %s", (job_id,)).fetchall()

    def attempts(self, job_id):
        with connect(self.settings) as connection:
            return connection.execute(
                "SELECT * FROM job_attempts WHERE job_id = %s ORDER BY attempt_no", (job_id,),
            ).fetchall()

    def test_eight_concurrent_claims_have_one_winner(self):
        job, barrier = self.create(), Barrier(8)

        def claim(index):
            barrier.wait(timeout=10)
            return claim_job(self.settings, f"worker-{index}")

        with ThreadPoolExecutor(max_workers=8) as executor:
            claimed = [row for row in executor.map(claim, range(8)) if row is not None]
        self.assertEqual([row["id"] for row in claimed], [job["id"]])
        self.assertEqual(len(self.attempts(job["id"])), 1)

    def test_claim_skips_locked_job(self):
        first, second = self.create(), self.create()
        with connect(self.settings) as blocker:
            blocker.execute("SELECT id FROM jobs WHERE id = %s FOR UPDATE", (first["id"],))
            claimed = claim_job(self.settings, "worker")
            self.assertEqual(claimed["id"], second["id"])

    def test_completion_is_atomic_and_repeat_safe(self):
        self.create()
        job = claim_job(self.settings, "worker")
        self.assertTrue(complete_job(self.settings, job["id"], "worker", 1))
        self.assertFalse(complete_job(self.settings, job["id"], "worker", 1))
        self.assertFalse(fail_job(self.settings, job["id"], "worker", 1, "late failure"))
        final = get_job(self.settings, job["id"])
        effects = self.effects(job["id"])
        self.assertEqual(final["state"], "succeeded")
        self.assertEqual(len(effects), 1)
        self.assertEqual(final["result"], effects[0]["effect"])
        self.assertEqual(self.attempts(job["id"])[0]["outcome"], "succeeded")

    def test_effect_write_failure_rolls_back_attempt_and_job(self):
        self.create()
        job = claim_job(self.settings, "worker")
        with connect(self.settings) as connection:
            connection.execute("""
                CREATE FUNCTION reject_effect() RETURNS trigger LANGUAGE plpgsql AS $$
                BEGIN RAISE EXCEPTION 'injected effect failure'; END; $$;
                CREATE TRIGGER reject_effect BEFORE INSERT ON job_effects
                FOR EACH ROW EXECUTE FUNCTION reject_effect();
            """)
        try:
            with self.assertRaises(psycopg.errors.RaiseException):
                complete_job(self.settings, job["id"], "worker", 1)
            self.assertEqual(get_job(self.settings, job["id"])["state"], "running")
            self.assertEqual(self.attempts(job["id"])[0]["outcome"], "running")
            self.assertEqual(self.effects(job["id"]), [])
        finally:
            with connect(self.settings) as connection:
                connection.execute("DROP TRIGGER reject_effect ON job_effects; DROP FUNCTION reject_effect();")
        self.assertTrue(complete_job(self.settings, job["id"], "worker", 1))

    def test_heartbeat_requires_current_owner_and_token(self):
        self.create()
        job = claim_job(self.settings, "worker")
        self.assertFalse(renew_lease(self.settings, job["id"], "other", 1))
        self.assertFalse(renew_lease(self.settings, job["id"], "worker", 2))
        self.assertTrue(renew_lease(self.settings, job["id"], "worker", 1))
        self.assertGreater(get_job(self.settings, job["id"])["lease_expires_at"], job["lease_expires_at"])

    def test_final_state_write_failure_rolls_back_inserted_effect(self):
        self.create()
        job = claim_job(self.settings, "worker")
        with connect(self.settings) as connection:
            connection.execute("""
                CREATE FUNCTION reject_success() RETURNS trigger LANGUAGE plpgsql AS $$
                BEGIN RAISE EXCEPTION 'injected final update failure'; END; $$;
                CREATE TRIGGER reject_success BEFORE UPDATE ON jobs
                FOR EACH ROW WHEN (NEW.state = 'succeeded') EXECUTE FUNCTION reject_success();
            """)
        try:
            with self.assertRaises(psycopg.errors.RaiseException):
                complete_job(self.settings, job["id"], "worker", 1)
            self.assertEqual(self.effects(job["id"]), [])
            self.assertEqual(get_job(self.settings, job["id"])["state"], "running")
            self.assertEqual(self.attempts(job["id"])[0]["outcome"], "running")
        finally:
            with connect(self.settings) as connection:
                connection.execute("DROP TRIGGER reject_success ON jobs; DROP FUNCTION reject_success();")

    def test_expired_attempt_cannot_renew_complete_or_fail(self):
        self.create()
        job = claim_job(self.settings, "worker")
        self.expire(job["id"])
        self.assertFalse(renew_lease(self.settings, job["id"], "worker", 1))
        self.assertFalse(complete_job(self.settings, job["id"], "worker", 1))
        self.assertFalse(fail_job(self.settings, job["id"], "worker", 1, "stale"))
        self.assertEqual(self.effects(job["id"]), [])

    def test_recovery_fences_old_token_even_with_same_worker_id(self):
        self.create()
        old = claim_job(self.settings, "worker")
        self.expire(old["id"])
        self.assertEqual(recover_expired(self.settings), 1)
        new = claim_job(self.settings, "worker")
        self.assertEqual(new["attempt_no"], 2)
        self.assertEqual(new["started_at"], old["started_at"])
        self.assertFalse(renew_lease(self.settings, old["id"], "worker", 1))
        self.assertFalse(complete_job(self.settings, old["id"], "worker", 1))
        self.assertFalse(fail_job(self.settings, old["id"], "worker", 1, "stale"))
        self.assertTrue(complete_job(self.settings, new["id"], "worker", 2))
        self.assertEqual([row["outcome"] for row in self.attempts(old["id"])], ["expired", "succeeded"])
        self.assertEqual(self.effects(old["id"])[0]["attempt_no"], 2)

    def test_concurrent_recovery_only_recovers_once(self):
        self.create()
        job = claim_job(self.settings, "worker")
        self.expire(job["id"])
        barrier = Barrier(4)

        def recover(_):
            barrier.wait(timeout=10)
            return recover_expired(self.settings)

        with ThreadPoolExecutor(max_workers=4) as executor:
            self.assertEqual(sum(executor.map(recover, range(4))), 1)
        self.assertEqual(get_job(self.settings, job["id"])["state"], "queued")

    def test_live_lease_is_not_recovered(self):
        self.create()
        job = claim_job(self.settings, "worker")
        self.assertEqual(recover_expired(self.settings), 0)
        self.assertEqual(get_job(self.settings, job["id"])["state"], "running")

    def test_expiry_exhausts_attempt_budget(self):
        self.create(settings=replace(self.settings, max_attempts=1))
        job = claim_job(self.settings, "worker")
        self.expire(job["id"])
        self.assertEqual(recover_expired(self.settings), 1)
        final = get_job(self.settings, job["id"])
        self.assertEqual(final["state"], "failed")
        self.assertIn("budget exhausted", final["error"])
        self.assertIsNone(claim_job(self.settings, "worker"))
        self.assertEqual(self.effects(job["id"]), [])

    def test_retryable_failures_then_success(self):
        job = self.create({"simulation": {"duration_seconds": 0, "fail_until_attempt": 2}})
        for attempt in range(1, 4):
            claimed = claim_job(self.settings, "worker")
            self.assertEqual(claimed["attempt_no"], attempt)
            execute_job(self.settings, claimed, "worker", Event())
        self.assertEqual(get_job(self.settings, job["id"])["state"], "succeeded")
        self.assertEqual([row["outcome"] for row in self.attempts(job["id"])], ["failed", "failed", "succeeded"])
        self.assertEqual(len(self.effects(job["id"])), 1)

    def test_permanent_failure_and_retry_budget(self):
        for simulation, expected_attempts in (
            ({"permanent_failure": True}, 1), ({"fail_until_attempt": 100}, 2),
        ):
            with self.subTest(simulation=simulation):
                job = self.create({"simulation": {"duration_seconds": 0, **simulation}},
                                  settings=replace(self.settings, max_attempts=2))
                for _ in range(expected_attempts):
                    execute_job(self.settings, claim_job(self.settings, "worker"), "worker", Event())
                self.assertEqual(get_job(self.settings, job["id"])["state"], "failed")
                self.assertEqual(len(self.attempts(job["id"])), expected_attempts)
                self.assertEqual(self.effects(job["id"]), [])

    def test_long_execution_renews_lease(self):
        settings = replace(self.settings, lease_seconds=0.5, heartbeat_seconds=0.05)
        self.create({"simulation": {"duration_seconds": 1.2}})
        job = claim_job(settings, "worker")
        execute_job(settings, job, "worker", Event())
        self.assertEqual(get_job(settings, job["id"])["state"], "succeeded")
        self.assertEqual(len(self.attempts(job["id"])), 1)

    def test_cancelled_work_cannot_publish_or_renew(self):
        self.create()
        job = claim_job(self.settings, "worker")
        cancel_job(self.settings, job["id"])
        self.assertFalse(renew_lease(self.settings, job["id"], "worker", 1))
        self.assertFalse(complete_job(self.settings, job["id"], "worker", 1))
        self.assertFalse(fail_job(self.settings, job["id"], "worker", 1, "stale"))
        self.assertEqual(self.effects(job["id"]), [])

    def test_completion_before_cancellation_is_immutable(self):
        self.create()
        job = claim_job(self.settings, "worker")
        complete_job(self.settings, job["id"], "worker", 1)
        with self.assertRaises(JobConflict):
            cancel_job(self.settings, job["id"])
        self.assertEqual(len(self.effects(job["id"])), 1)

    def test_shutdown_leaves_job_for_lease_recovery(self):
        self.create()
        job = claim_job(self.settings, "worker")
        stopped = Event()
        stopped.set()
        execute_job(self.settings, job, "worker", stopped)
        self.assertEqual(get_job(self.settings, job["id"])["state"], "running")
        self.assertEqual(self.effects(job["id"]), [])
        self.expire(job["id"])
        self.assertEqual(recover_expired(self.settings), 1)

    def test_cancellation_interrupts_long_simulation_at_heartbeat(self):
        settings = replace(self.settings, heartbeat_seconds=0.05)
        self.create({"simulation": {"duration_seconds": 10}})
        job = claim_job(settings, "worker")
        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(execute_job, settings, job, "worker", Event())
            cancel_job(settings, job["id"])
            future.result(timeout=3)
        self.assertEqual(get_job(settings, job["id"])["state"], "cancelled")
        self.assertEqual(self.effects(job["id"]), [])

    def start_process(self, *, lease_seconds=15):
        log = tempfile.TemporaryFile(mode="w+")
        self.addCleanup(log.close)
        environment = dict(os.environ, DATABASE_URL=self.settings.database_url,
                           LEASE_SECONDS=str(lease_seconds), HEARTBEAT_SECONDS="0.1", POLL_SECONDS="0.02")
        process = subprocess.Popen([sys.executable, "-m", "training_job_queue.worker"],
                                   env=environment, stdout=log, stderr=subprocess.STDOUT)

        def stop():
            if process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=5)
        self.addCleanup(stop)
        return process

    def wait_for(self, predicate, timeout=15):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if predicate():
                return
            time.sleep(0.02)
        self.fail("Worker condition was not reached within the deadline")

    def test_four_real_worker_processes_drain_queue(self):
        for _ in range(4):
            self.start_process()
        for _ in range(4):
            self.create({"simulation": {"duration_seconds": 2}})

        def all_four_running():
            with connect(self.settings) as connection:
                row = connection.execute(
                    "SELECT count(DISTINCT worker_id) AS n FROM jobs WHERE state = 'running'",
                ).fetchone()
                return row["n"] == 4

        self.wait_for(all_four_running)
        for _ in range(20):
            self.create({"simulation": {"duration_seconds": 0.02}})

        def drained():
            with connect(self.settings) as connection:
                return connection.execute("SELECT count(*) AS n FROM jobs WHERE state <> 'succeeded'").fetchone()["n"] == 0

        self.wait_for(drained)
        with connect(self.settings) as connection:
            self.assertEqual(connection.execute("SELECT count(*) AS n FROM job_effects").fetchone()["n"], 24)
            self.assertEqual(connection.execute("SELECT count(DISTINCT worker_id) AS n FROM job_attempts").fetchone()["n"], 4)

    def test_sigkill_then_replacement_recovers_job(self):
        job = self.create({"simulation": {"duration_seconds": 1.5}})
        victim = self.start_process(lease_seconds=1.2)
        self.wait_for(lambda: get_job(self.settings, job["id"])["state"] == "running")
        victim.kill()
        self.assertEqual(victim.wait(timeout=5), -9)
        self.start_process(lease_seconds=1.2)
        self.wait_for(lambda: get_job(self.settings, job["id"])["state"] == "succeeded")
        self.assertEqual([row["outcome"] for row in self.attempts(job["id"])], ["expired", "succeeded"])
        self.assertEqual(len(self.effects(job["id"])), 1)
        self.assertEqual(self.effects(job["id"])[0]["attempt_no"], 2)
