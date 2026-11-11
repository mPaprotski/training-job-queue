"""Concurrent commits cannot mix final checks with newer database states."""

from concurrent.futures import ThreadPoolExecutor
from types import SimpleNamespace
from unittest.mock import Mock, patch
from uuid import uuid4

from harness.invariants import database_snapshot, validate_database, validate_database_on_connection
from harness.load import LoadRun
from training_job_queue.db import connect
from training_job_queue.jobs import create_job
from training_job_queue.processing import claim_job, complete_job
from support import DatabaseTestCase


class SnapshotTests(DatabaseTestCase):
    def setUp(self):
        with connect(self.settings) as connection:
            connection.execute('TRUNCATE job_effects, job_attempts, jobs')
        self.key = uuid4().hex
        self.payload = {'phase': 'measure'}
        self.expected = {self.key: self.payload}
        self.job, _ = create_job(self.settings, self.key, self.payload)
        claim_job(self.settings, 'snapshot-worker')

    def finish_in_other_transaction(self):
        with ThreadPoolExecutor(max_workers=1) as executor:
            executor.submit(complete_job, self.settings, self.job['id'], 'snapshot-worker', 1).result(timeout=5)

    def test_snapshot_remains_stable_across_concurrent_completion(self):
        with database_snapshot(self.settings) as connection:
            self.assertEqual(connection.execute('SELECT state FROM jobs').fetchone()['state'], 'running')
            self.finish_in_other_transaction()
            summary = validate_database_on_connection(connection, self.expected)
        self.assertEqual(summary['states'], {'running': 1})
        self.assertEqual(summary['attempts'], {'running': 1})
        self.assertEqual(summary['effects'], 0)
        self.assertFalse(summary['passed'])  # A stable snapshot does not excuse unfinished work.
        fresh = validate_database(self.settings, self.expected)
        self.assertTrue(fresh['passed'])
        self.assertEqual(fresh['states'], {'succeeded': 1})
        self.assertEqual(fresh['effects'], 1)

    def test_load_metrics_and_checker_share_snapshot(self):
        run = LoadRun.__new__(LoadRun)
        run.settings = self.settings
        run.expected = self.expected
        run.accepted_keys = {self.key}
        run.terminal_seen, run.cancelled_ids = {}, set()
        run.args = SimpleNamespace(warmup=0, duration=1)
        run.records = [{'phase': 'measure', 'dropped': True, 'valid': False}]
        run.samples = []
        run.after_requests = {'running': 1}
        run.drain_seconds, run.drain_timed_out = 0, True
        run.write_json = Mock()

        def finish_then_check(connection, *args):
            self.finish_in_other_transaction()
            return validate_database_on_connection(connection, *args)

        with patch('harness.load.validate_database_on_connection', side_effect=finish_then_check):
            summary = run.validate()
        self.assertEqual(summary['metrics']['measurement_job_states'], {'running': 1})
        self.assertEqual(summary['metrics']['job_latency_samples'], 0)
        self.assertEqual(summary['states'], {'running': 1})
        self.assertEqual(summary['effects'], 0)
        self.assertFalse(summary['invariants_passed'])
        self.assertTrue(summary['metrics']['drain_timed_out'])
        self.assertTrue(validate_database(self.settings, self.expected)['passed'])
