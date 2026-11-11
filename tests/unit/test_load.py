"""Regression checks for load-response validation; no database or server needed."""

import io
import json
from threading import Lock
import time
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch
from urllib.error import HTTPError

from harness.load import LoadRun


class LoadCancellationTests(unittest.TestCase):
    job_id = "11111111-1111-1111-1111-111111111111"
    other_id = "22222222-2222-2222-2222-222222222222"

    def run_cancel(self, status, body):
        # Exercise the real request/validation/recording path without Run.setup()
        # or its temporary databases, output directories and subprocesses.
        run = LoadRun.__new__(LoadRun)
        run.args = SimpleNamespace(seed=1)
        run.traffic_start = time.monotonic()
        run.base_url = "http://localhost:8000"
        run.state_lock = Lock()
        run.request_lock = Lock()
        run.known = {"existing-key": self.job_id}
        run.cancelled_ids = set()
        run.records = []
        run.requests_output = io.StringIO()
        run.observe = Mock()
        slot = Mock()
        response = io.BytesIO(body)
        response.status = status
        error = HTTPError(run.base_url, status, "test response", {}, response)
        options = {"side_effect": error} if status >= 400 else {"return_value": response}
        with patch("harness.load.urlopen", **options) as request:
            run.request_once({"index": 0, "kind": "cancel", "phase": "measure"},
                             run.traffic_start, slot)
        self.assertEqual(request.call_args.args[0].full_url,
                         f"{run.base_url}/jobs/{self.job_id}/cancel")
        slot.release.assert_called_once_with()
        self.assertEqual(len(run.records), 1)
        self.assertEqual(json.loads(run.requests_output.getvalue()), run.records[0])
        return run

    def test_matching_cancelled_job_is_accepted(self):
        run = self.run_cancel(200, json.dumps({"id": self.job_id, "state": "cancelled"}).encode())
        self.assertTrue(run.records[0]["valid"])
        self.assertEqual(run.cancelled_ids, {self.job_id})
        self.assertEqual(run.records[0]["requested_job_id"], self.job_id)
        self.assertEqual(run.records[0]["response_job_id"], self.job_id)
        run.observe.assert_called_once()

    def test_other_job_is_rejected_and_both_ids_are_logged(self):
        run = self.run_cancel(200, json.dumps({"id": self.other_id, "state": "cancelled"}).encode())
        self.assertFalse(run.records[0]["valid"])
        self.assertEqual(run.records[0]["requested_job_id"], self.job_id)
        self.assertEqual(run.records[0]["response_job_id"], self.other_id)
        self.assertEqual(run.cancelled_ids, set())
        run.observe.assert_not_called()

    def test_missing_id_is_rejected(self):
        run = self.run_cancel(200, b'{"state":"cancelled"}')
        self.assertFalse(run.records[0]["valid"])
        self.assertEqual(run.cancelled_ids, set())
        run.observe.assert_not_called()

    def test_non_cancelled_state_is_rejected(self):
        run = self.run_cancel(200, json.dumps({"id": self.job_id, "state": "running"}).encode())
        self.assertFalse(run.records[0]["valid"])
        self.assertEqual(run.cancelled_ids, set())
        run.observe.assert_not_called()

    def test_terminal_conflict_is_accepted_without_recording_cancellation(self):
        run = self.run_cancel(409, b'{"detail":"Job is already completed and cannot be cancelled"}')
        self.assertTrue(run.records[0]["valid"])
        self.assertEqual(run.cancelled_ids, set())
        run.observe.assert_not_called()

    def test_server_error_is_rejected(self):
        run = self.run_cancel(500, b'{"detail":"Internal server error"}')
        self.assertFalse(run.records[0]["valid"])
        self.assertEqual(run.cancelled_ids, set())
        run.observe.assert_not_called()

    def test_malformed_json_is_rejected(self):
        run = self.run_cancel(200, b'not JSON')
        self.assertFalse(run.records[0]["valid"])
        self.assertIn("error", run.records[0])
        self.assertEqual(run.cancelled_ids, set())
        run.observe.assert_not_called()


class LoadAdmissionTests(unittest.TestCase):
    def test_capacity_rejection_remains_an_error_and_is_not_admitted(self):
        run = LoadRun.__new__(LoadRun)
        run.args = SimpleNamespace(seed=1)
        run.traffic_start, run.base_url = time.monotonic(), 'http://localhost:8000'
        run.state_lock, run.request_lock = Lock(), Lock()
        run.known, run.accepted_keys = {}, set()
        run.expected = {'new-key': {}}
        run.records, run.requests_output = [], io.StringIO()
        run.observe = Mock()
        response = io.BytesIO(b'{"detail":"Queue capacity reached","code":"queue_full"}')
        error = HTTPError(run.base_url, 503, 'full', {}, response)
        slot = Mock()
        with patch('harness.load.urlopen', side_effect=error):
            run.request_once({'index': 0, 'kind': 'create', 'phase': 'measure', 'key': 'new-key'},
                             run.traffic_start, slot)
        self.assertFalse(run.records[0]['valid'])
        self.assertEqual(run.records[0]['error_code'], 'queue_full')
        self.assertEqual(run.accepted_keys, set())
        self.assertEqual(run.known, {})
        run.observe.assert_not_called()
        slot.release.assert_called_once_with()


if __name__ == "__main__":
    unittest.main()
