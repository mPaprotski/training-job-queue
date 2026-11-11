"""Controlled schedules and real process signals for silent-failure cases."""

from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from dataclasses import replace
import os
import select
import signal
import subprocess
import sys
import tempfile
from threading import Event
import time
from unittest.mock import patch
from uuid import uuid4

from psycopg.conninfo import make_conninfo

from training_job_queue import jobs, processing
from training_job_queue.db import connect
from support import DatabaseTestCase


class AdversarialTests(DatabaseTestCase):
    def setUp(self):
        with connect(self.settings) as connection:
            connection.execute("TRUNCATE job_effects, job_attempts, jobs")

    def claimed(self, *, settings=None, payload=None):
        settings = settings or self.settings
        jobs.create_job(settings, uuid4().hex, payload or {})
        return processing.claim_job(settings, "owner")

    def wait_for(self, predicate, timeout=4):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if predicate():
                return
            time.sleep(0.01)
        self.fail("Controlled schedule did not reach its checkpoint")

    def waiting(self, application_name):
        with connect(self.settings) as connection:
            return connection.execute(
                "SELECT count(*) AS n FROM pg_stat_activity WHERE application_name = %s AND wait_event_type = 'Lock'",
                (application_name,),
            ).fetchone()["n"] == 1

    def effect_count(self, job_id):
        with connect(self.settings) as connection:
            return connection.execute("SELECT count(*) AS n FROM job_effects WHERE job_id = %s", (job_id,)).fetchone()["n"]

    @contextmanager
    def gate_after(self, query_marker, reached, release):
        class Proxy:
            def __init__(self, connection):
                self.connection = connection

            def execute(self, query, parameters=None):
                cursor = self.connection.execute(query, parameters)
                if query_marker in query:
                    reached.set()
                    if not release.wait(timeout=2):
                        raise RuntimeError("Gate was not released")
                return cursor

        @contextmanager
        def gated_connect(settings):
            with connect(settings) as connection:
                yield Proxy(connection)

        yield gated_connect

    def test_cancel_commits_while_completion_is_blocked(self):
        job = self.claimed()
        reached, release = Event(), Event()
        loser = replace(self.settings, database_url=make_conninfo(self.settings.database_url, application_name="late_complete"))
        with self.gate_after("UPDATE jobs SET state = 'cancelled'", reached, release) as gated:
            with patch.object(jobs, "connect", gated), ThreadPoolExecutor(max_workers=2) as executor:
                cancellation = executor.submit(jobs.cancel_job, self.settings, job["id"])
                self.assertTrue(reached.wait(timeout=2))
                completion = executor.submit(processing.complete_job, loser, job["id"], "owner", 1)
                try:
                    self.wait_for(lambda: self.waiting("late_complete"), timeout=1)
                finally:
                    release.set()
                self.assertEqual(cancellation.result(timeout=3)["state"], "cancelled")
                self.assertFalse(completion.result(timeout=3))
        self.assertEqual(self.effect_count(job["id"]), 0)

    def test_completion_commits_while_cancel_is_blocked(self):
        job = self.claimed()
        reached, release = Event(), Event()
        loser = replace(self.settings, database_url=make_conninfo(self.settings.database_url, application_name="late_cancel"))
        with self.gate_after("INSERT INTO job_effects", reached, release) as gated:
            with patch.object(processing, "connect", gated), ThreadPoolExecutor(max_workers=2) as executor:
                completion = executor.submit(processing.complete_job, self.settings, job["id"], "owner", 1)
                self.assertTrue(reached.wait(timeout=2))
                cancellation = executor.submit(jobs.cancel_job, loser, job["id"])
                try:
                    self.wait_for(lambda: self.waiting("late_cancel"), timeout=1)
                finally:
                    release.set()
                self.assertTrue(completion.result(timeout=3))
                with self.assertRaises(jobs.JobConflict):
                    cancellation.result(timeout=3)
        self.assertEqual(self.effect_count(job["id"]), 1)

    def test_expiry_is_checked_after_waiting_for_row_lock(self):
        settings = replace(self.settings, lease_seconds=0.5)
        job = self.claimed(settings=settings)
        waiter = replace(settings, database_url=make_conninfo(settings.database_url, application_name="expiry_waiter"))
        with ThreadPoolExecutor(max_workers=1) as executor:
            with connect(settings) as blocker:
                blocker.execute("SELECT id FROM jobs WHERE id = %s FOR UPDATE", (job["id"],))
                future = executor.submit(processing.complete_job, waiter, job["id"], "owner", 1)
                self.wait_for(lambda: self.waiting("expiry_waiter"))

                def expired():
                    return blocker.execute("SELECT clock_timestamp() > %s AS expired", (job["lease_expires_at"],)).fetchone()["expired"]

                self.wait_for(expired)
            self.assertFalse(future.result(timeout=3))
        self.assertEqual(self.effect_count(job["id"]), 0)

    def test_clock_skew_does_not_change_database_ownership(self):
        for skew in (-86400 * 365, 86400 * 365):
            with self.subTest(skew=skew), patch("time.time", return_value=time.time() + skew):
                job = self.claimed()
                self.assertTrue(processing.renew_lease(self.settings, job["id"], "owner", 1))
                with connect(self.settings) as connection:
                    row = connection.execute(
                        "SELECT extract(epoch FROM lease_expires_at - clock_timestamp()) AS seconds FROM jobs WHERE id = %s",
                        (job["id"],),
                    ).fetchone()
                    self.assertGreater(row["seconds"], self.settings.lease_seconds - 2)
                    self.assertLessEqual(row["seconds"], self.settings.lease_seconds)
                    self.assertEqual(connection.execute("SHOW transaction_isolation").fetchone()["transaction_isolation"], "read committed")
                self.assertTrue(processing.complete_job(self.settings, job["id"], "owner", 1))

    def crash_at(self, phase):
        job = self.claimed()
        child = subprocess.Popen(
            [sys.executable, "tests/fixtures/completion_gate.py", phase, str(job["id"]), "owner"],
            env=dict(os.environ, DATABASE_URL=self.settings.database_url),
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
        )
        try:
            readable, _, _ = select.select([child.stdout], [], [], 3)
            self.assertTrue(readable, "Completion process did not reach crash gate")
            self.assertEqual(child.stdout.readline().strip(), "GATE")
            child.kill()
            self.assertEqual(child.wait(timeout=3), -signal.SIGKILL)
        finally:
            if child.poll() is None:
                child.kill()
                child.wait(timeout=3)
            child.stdin.close()
            child.stdout.close()
            child.stderr.close()
        final = jobs.get_job(self.settings, job["id"])
        if phase == "after_commit":
            self.assertEqual(final["state"], "succeeded")
            self.assertFalse(processing.complete_job(self.settings, job["id"], "owner", 1))
        else:
            self.assertEqual(final["state"], "running")
            self.assertEqual(self.effect_count(job["id"]), 0)
            with connect(self.settings) as connection:
                connection.execute("UPDATE jobs SET lease_expires_at = clock_timestamp() - interval '1 second' WHERE id = %s", (job["id"],))
            self.assertEqual(processing.recover_expired(self.settings), 1)
            claimed = processing.claim_job(self.settings, "replacement")
            self.assertTrue(processing.complete_job(self.settings, claimed["id"], "replacement", 2))
        self.assertEqual(self.effect_count(job["id"]), 1)

    def test_sigkill_before_effect(self):
        self.crash_at("before_effect")

    def test_sigkill_after_effect_before_commit(self):
        self.crash_at("after_effect")

    def test_sigkill_after_commit_before_acknowledgement(self):
        self.crash_at("after_commit")

    def test_sigstop_resume_zombie_cannot_overwrite_replacement(self):
        job, _ = jobs.create_job(self.settings, uuid4().hex, {"simulation": {"duration_seconds": 3}})
        with tempfile.TemporaryFile(mode="w+") as log:
            child = subprocess.Popen(
                [sys.executable, "-m", "training_job_queue.worker"], stdout=log, stderr=subprocess.STDOUT,
                env=dict(os.environ, DATABASE_URL=self.settings.database_url, LEASE_SECONDS="0.6",
                         HEARTBEAT_SECONDS="0.1", POLL_SECONDS="0.02"),
            )
            try:
                self.wait_for(lambda: jobs.get_job(self.settings, job["id"])["state"] == "running")
                os.kill(child.pid, signal.SIGSTOP)

                def expired():
                    with connect(self.settings) as connection:
                        return connection.execute(
                            "SELECT lease_expires_at <= clock_timestamp() AS expired FROM jobs WHERE id = %s", (job["id"],),
                        ).fetchone()["expired"]

                self.wait_for(expired)
                self.assertEqual(processing.recover_expired(self.settings), 1)
                new = processing.claim_job(self.settings, "replacement")
                self.assertTrue(processing.complete_job(self.settings, new["id"], "replacement", 2))
                os.kill(child.pid, signal.SIGCONT)

                def observed_rejection():
                    log.seek(0)
                    return "ownership lost" in log.read()

                self.wait_for(observed_rejection)
                self.assertEqual(jobs.get_job(self.settings, job["id"])["result"]["worker_id"], "replacement")
                self.assertEqual(self.effect_count(job["id"]), 1)
            finally:
                if child.poll() is None:
                    os.kill(child.pid, signal.SIGCONT)
                    child.terminate()
                    try:
                        child.wait(timeout=3)
                    except subprocess.TimeoutExpired:
                        child.kill()
                        child.wait(timeout=3)
