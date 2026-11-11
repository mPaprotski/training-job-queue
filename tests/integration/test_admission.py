"""Capacity accounting stays atomic under concurrent creates and terminal races."""

from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from dataclasses import replace
from threading import Barrier, Event
from unittest.mock import patch
from uuid import uuid4

from training_job_queue import jobs, processing
from training_job_queue.db import connect
from support import DatabaseTestCase


class AdmissionTests(DatabaseTestCase):
    def setUp(self):
        with connect(self.settings) as connection:
            connection.execute('TRUNCATE job_effects, job_attempts, jobs')
        self.limited = replace(self.runtime_settings, max_outstanding_jobs=1)

    def create(self, key=None):
        return jobs.create_job(self.limited, key or uuid4().hex, {})[0]

    def assert_full(self):
        with self.assertRaises(jobs.QueueFull):
            self.create()

    def test_sixteen_concurrent_new_keys_compete_for_one_slot(self):
        barrier = Barrier(16)

        def attempt(_):
            barrier.wait(timeout=5)
            try:
                self.create()
                return True
            except jobs.QueueFull:
                return False

        with ThreadPoolExecutor(max_workers=16) as executor:
            results = list(executor.map(attempt, range(16)))
        self.assertEqual(sum(results), 1)
        with connect(self.settings) as connection:
            self.assertEqual(connection.execute('SELECT count(*) AS n FROM jobs').fetchone()['n'], 1)

    def test_colliding_slot_choices_retry_without_exceeding_four_slots(self):
        limited = replace(self.limited, max_outstanding_jobs=4)
        barrier = Barrier(16)

        def attempt(_):
            barrier.wait(timeout=5)
            try:
                return jobs.create_job(limited, uuid4().hex, {})[0]
            except jobs.QueueFull:
                return None

        with patch.object(jobs.secrets, 'randbelow', return_value=0), ThreadPoolExecutor(max_workers=16) as executor:
            accepted = [job for job in executor.map(attempt, range(16)) if job]
        self.assertEqual(len(accepted), 4)
        self.assertEqual({job['admission_slot'] for job in accepted}, {1, 2, 3, 4})

    def test_different_slot_can_commit_while_first_insert_is_uncommitted(self):
        inserted, release = Event(), Event()
        limited = replace(self.limited, max_outstanding_jobs=2)

        class Proxy:
            def __init__(self, connection):
                self.connection = connection

            def execute(self, statement, parameters=None):
                cursor = self.connection.execute(statement, parameters)
                if 'INSERT INTO jobs' in statement and parameters[0] == 'held':
                    inserted.set()
                    if not release.wait(5):
                        raise TimeoutError('Admission test gate was not released')
                return cursor

        @contextmanager
        def gated(settings):
            with connect(settings) as connection:
                yield Proxy(connection)

        with patch.object(jobs, 'connect', gated), patch.object(jobs.secrets, 'randbelow', side_effect=[0, 1]):
            with ThreadPoolExecutor(max_workers=2) as executor:
                first = executor.submit(jobs.create_job, limited, 'held', {})
                try:
                    self.assertTrue(inserted.wait(3))
                    second = executor.submit(jobs.create_job, limited, 'independent', {}).result(timeout=3)
                    self.assertTrue(second[1])
                    self.assertEqual(second[0]['admission_slot'], 2)
                finally:
                    release.set()
                self.assertEqual(first.result(timeout=3)[0]['admission_slot'], 1)

    def test_lower_limit_drains_out_of_range_slots_before_new_admission(self):
        job = jobs.create_job(replace(self.limited, max_outstanding_jobs=4), 'legacy', {})[0]
        with connect(self.settings) as connection:
            connection.execute('UPDATE jobs SET admission_slot=4 WHERE id=%s', (job['id'],))
        self.assert_full()
        self.assertEqual(jobs.create_job(self.limited, 'legacy', {})[0]['id'], job['id'])
        jobs.cancel_job(self.limited, job['id'])
        self.assertEqual(self.create()['admission_slot'], 1)

    def test_rolled_back_terminal_transition_does_not_release_slot(self):
        job = self.create()
        with self.assertRaises(ValueError):
            with connect(self.settings) as connection:
                connection.execute("UPDATE jobs SET state='cancelled', finished_at=clock_timestamp() WHERE id=%s", (job['id'],))
                raise ValueError('Rollback gate')
        self.assert_full()
        jobs.cancel_job(self.limited, job['id'])
        self.create()

    def test_concurrent_same_key_replays_even_at_capacity(self):
        barrier = Barrier(8)

        def attempt(_):
            barrier.wait(timeout=5)
            return jobs.create_job(self.limited, 'same-key', {})

        with ThreadPoolExecutor(max_workers=8) as executor:
            results = list(executor.map(attempt, range(8)))
        self.assertEqual(sum(created for _, created in results), 1)
        self.assertEqual(len({job['id'] for job, _ in results}), 1)

    def test_full_queue_preserves_replay_and_payload_conflict(self):
        job = self.create('existing')
        self.assert_full()
        self.assertEqual(jobs.create_job(self.limited, 'existing', {})[0]['id'], job['id'])
        with self.assertRaises(jobs.JobConflict):
            jobs.create_job(self.limited, 'existing', {'changed': True})

    def test_running_job_counts_and_success_releases_capacity(self):
        job = self.create()
        processing.claim_job(self.limited, 'owner')
        self.assert_full()
        processing.complete_job(self.limited, job['id'], 'owner', 1)
        self.create()

    def test_queued_and_running_cancellation_release_capacity(self):
        job = self.create()
        jobs.cancel_job(self.limited, job['id'])
        job = self.create()
        processing.claim_job(self.limited, 'owner')
        jobs.cancel_job(self.limited, job['id'])
        self.create()

    def test_retry_keeps_slot_and_permanent_failure_releases_it(self):
        job = self.create()
        processing.claim_job(self.limited, 'owner')
        processing.fail_job(self.limited, job['id'], 'owner', 1, 'retry')
        self.assert_full()
        processing.claim_job(self.limited, 'owner')
        processing.fail_job(self.limited, job['id'], 'owner', 2, 'permanent', permanent=True)
        self.create()

    def test_expiry_requeue_keeps_capacity_occupied(self):
        job = self.create()
        processing.claim_job(self.limited, 'owner')
        with connect(self.settings) as connection:
            connection.execute("UPDATE jobs SET lease_expires_at = clock_timestamp() - interval '1 second'")
        processing.recover_expired(self.limited)
        self.assertEqual(jobs.get_job(self.limited, job['id'])['state'], 'queued')
        self.assert_full()

    def test_expiry_exhaustion_releases_capacity(self):
        limited = replace(self.limited, max_attempts=1)
        job, _ = jobs.create_job(limited, 'expiry', {})
        processing.claim_job(limited, 'owner')
        with connect(self.settings) as connection:
            connection.execute("UPDATE jobs SET lease_expires_at = clock_timestamp() - interval '1 second'")
        self.assert_full()
        processing.recover_expired(limited)
        self.assertEqual(jobs.get_job(limited, job['id'])['state'], 'failed')
        self.create()
