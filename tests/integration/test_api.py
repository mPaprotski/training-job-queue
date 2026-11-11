"""Real HTTP + PostgreSQL contract tests; no mock database or extra HTTP client."""

from concurrent.futures import ThreadPoolExecutor
from contextlib import ExitStack
from dataclasses import replace
from datetime import datetime, timezone
import base64
import json
from http.client import HTTPConnection
import os
import socket
from threading import Barrier, Thread
import time
import unittest
from unittest.mock import patch
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen
from uuid import uuid4

import uvicorn

from training_job_queue.api import app
from training_job_queue.body_limit import MAX_REQUEST_BODY_BYTES
from training_job_queue.db import connect, transaction
from support import DatabaseTestCase


class APITests(DatabaseTestCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.listener = socket.socket()
        cls.listener.bind(("127.0.0.1", 0))
        cls.listener.listen(128)
        cls.base_url = f"http://127.0.0.1:{cls.listener.getsockname()[1]}"
        cls.server = uvicorn.Server(uvicorn.Config(app, log_level="warning", access_log=False))
        cls.thread = Thread(target=cls.server.run, kwargs={"sockets": [cls.listener]}, daemon=True)
        cls.addClassCleanup(cls.stop_server)
        with patch.dict(os.environ, {"DATABASE_URL": cls.runtime_settings.database_url, "MAX_ATTEMPTS": "7"}):
            cls.thread.start()
            deadline = time.monotonic() + 10
            while time.monotonic() < deadline:
                try:
                    with urlopen(cls.base_url + "/health/ready", timeout=0.5) as response:
                        if response.status == 200:
                            break
                except (URLError, TimeoutError):
                    time.sleep(0.05)
            else:
                raise RuntimeError("Test HTTP server did not become ready")

    @classmethod
    def stop_server(cls):
        cls.server.should_exit = True
        cls.thread.join(timeout=10)
        cls.listener.close()
        if cls.thread.is_alive():
            raise RuntimeError("Test HTTP server did not stop")

    def setUp(self):
        with connect(self.settings) as connection:
            connection.execute("TRUNCATE job_effects, job_attempts, jobs")

    def request(self, method, path, body=None, key=None, raw=None):
        headers = {}
        if key is not None:
            headers["Idempotency-Key"] = key
        data = raw if raw is not None else (json.dumps(body).encode() if body is not None else None)
        if data is not None:
            headers["Content-Type"] = "application/json"
        request = Request(self.base_url + path, data=data, headers=headers, method=method)
        try:
            response = urlopen(request, timeout=10)
        except HTTPError as error:
            response = error
        with response:
            return response.status, json.load(response), response.headers

    def create(self, payload=None, key=None):
        return self.request("POST", "/jobs", {"payload": payload if payload is not None else {}}, key or uuid4().hex)

    def test_full_queue_returns_503_but_replay_and_conflict_are_preserved(self):
        with patch.object(app.state, 'settings', replace(app.state.settings, max_outstanding_jobs=1)):
            status, first, _ = self.create({}, 'capacity-first')
            self.assertEqual(status, 201)
            self.assertEqual(self.create({}, 'capacity-first')[0], 200)
            self.assertEqual(self.create({'different': True}, 'capacity-first')[0], 409)
            status, body, headers = self.create({}, 'capacity-second')
            self.assertEqual(status, 503)
            self.assertEqual(body['code'], 'queue_full')
            self.assertEqual(headers['Retry-After'], '1')
            self.request('POST', f"/jobs/{first['id']}/cancel")
            self.assertEqual(self.create({}, 'capacity-second')[0], 201)

    def test_exhausted_pool_returns_503_then_recovers(self):
        settings = replace(app.state.settings, db_pool_timeout=0.05)
        with patch.object(app.state, 'settings', settings):
            with ExitStack() as held:
                for _ in range(settings.db_pool_size):
                    held.enter_context(transaction(settings))
                status, body, _ = self.request('GET', '/jobs')
                self.assertEqual(status, 503)
                self.assertEqual(body['detail'], 'Database temporarily unavailable')
            self.assertEqual(self.request('GET', '/jobs')[0], 200)

    def test_request_body_limit_and_no_database_write_on_rejection(self):
        prefix, suffix = b'{"payload":{"value":"', b'"}}'
        body = prefix + b'x' * (MAX_REQUEST_BODY_BYTES - len(prefix) - len(suffix)) + suffix
        status, job, _ = self.request('POST', '/jobs', key='at-limit', raw=body)
        self.assertEqual(status, 201)
        self.assertEqual(len(job['payload']['value']), MAX_REQUEST_BODY_BYTES - len(prefix) - len(suffix))
        status, _, _ = self.request('POST', '/jobs', key='over-limit', raw=body + b' ')
        self.assertEqual(status, 413)
        with connect(self.settings) as connection:
            self.assertEqual(connection.execute('SELECT count(*) AS n FROM jobs').fetchone()['n'], 1)

    def test_chunked_body_without_content_length_is_limited(self):
        address = self.base_url.removeprefix('http://')
        connection = HTTPConnection(address, timeout=10)
        try:
            connection.request('POST', '/jobs', body=iter([b'x' * 32768, b'x' * 32768, b'x']),
                               headers={'Content-Type': 'application/json', 'Idempotency-Key': 'chunked'},
                               encode_chunked=True)
            response = connection.getresponse()
            self.assertEqual(response.status, 413)
            self.assertIn('detail', json.loads(response.read()))
        finally:
            connection.close()
        with connect(self.settings) as connection:
            self.assertEqual(connection.execute('SELECT count(*) AS n FROM jobs').fetchone()['n'], 0)

    def test_create_get_and_retry_after_discarding_response(self):
        key = uuid4().hex
        status, job, headers = self.create({"epochs": 3}, key)
        self.assertEqual(status, 201)
        self.assertEqual(job["state"], "queued")
        self.assertEqual(job["attempt_no"], 0)
        self.assertEqual(job["max_attempts"], 7)
        self.assertEqual(headers["Location"], "/jobs/" + job["id"])
        status, repeated, _ = self.create({"epochs": 3}, key)
        self.assertEqual(status, 200)
        self.assertEqual(repeated, job)
        self.assertEqual(self.request("GET", headers["Location"])[:2], (200, job))

    def test_payload_equality_uses_jsonb_semantics(self):
        key = uuid4().hex
        self.assertEqual(self.create({"a": {"x": 1, "y": 2}, "b": [1, 2]}, key)[0], 201)
        self.assertEqual(self.create({"b": [1, 2], "a": {"y": 2, "x": 1.0}}, key)[0], 200)
        self.assertEqual(self.create({"a": {"x": True, "y": 2}, "b": [1, 2]}, key)[0], 409)
        self.assertEqual(self.create({"a": {"x": 1, "y": 2}, "b": [2, 1]}, key)[0], 409)

    def test_concurrent_retries_create_one_job(self):
        key, barrier = uuid4().hex, Barrier(8)

        def create(_):
            barrier.wait(timeout=10)
            return self.create({"value": 42}, key)

        with ThreadPoolExecutor(max_workers=8) as executor:
            responses = list(executor.map(create, range(8)))
        self.assertEqual(sum(status == 201 for status, _, _ in responses), 1)
        self.assertEqual(sum(status == 200 for status, _, _ in responses), 7)
        self.assertEqual(len({job["id"] for _, job, _ in responses}), 1)
        with connect(self.settings) as connection:
            self.assertEqual(connection.execute("SELECT count(*) AS n FROM jobs").fetchone()["n"], 1)

    def test_concurrent_conflicting_payloads(self):
        key, barrier = uuid4().hex, Barrier(2)

        def create(value):
            barrier.wait(timeout=10)
            return self.create({"value": value}, key)[0]

        with ThreadPoolExecutor(max_workers=2) as executor:
            self.assertCountEqual(list(executor.map(create, (1, 2))), [201, 409])

    def test_validation_and_unknown_ids(self):
        cases = [
            ("POST", "/jobs", {"payload": {}}, None),
            ("POST", "/jobs", {"payload": {}}, " " * 3),
            ("POST", "/jobs", {"payload": {}}, "k" * 256),
            ("POST", "/jobs", {"payload": []}, "key"),
            ("POST", "/jobs", {"payload": {}, "state": "succeeded"}, "key"),
            ("POST", "/jobs", {"payload": {"value": float("nan")}}, "key"),
            ("POST", "/jobs", {"payload": {"value": "\x00"}}, "key"),
            ("POST", "/jobs", {"payload": {"value": "\ud800"}}, "key"),
            ("POST", "/jobs", {"payload": {"simulation": {"duration_seconds": -1}}}, "key"),
            ("POST", "/jobs", {"payload": {"simulation": {"duration_seconds": 301}}}, "key"),
            ("POST", "/jobs", {"payload": {"simulation": {"fail_until_attempt": True}}}, "key"),
            ("POST", "/jobs", {"payload": {"simulation": {"permanent_failure": "yes"}}}, "key"),
            ("GET", "/jobs/not-a-uuid", None, None),
            ("GET", "/jobs?limit=0", None, None),
            ("GET", "/jobs?limit=101", None, None),
            ("GET", "/jobs?cursor=invalid!", None, None),
        ]
        for method, path, body, key in cases:
            with self.subTest(path=path, body=repr(body), key=key):
                self.assertEqual(self.request(method, path, body, key)[0], 422)
        self.assertEqual(self.request("POST", "/jobs", key="key", raw=b"{")[0], 422)
        self.assertEqual(self.request("GET", "/jobs/" + str(uuid4()))[0], 404)
        self.assertEqual(self.request("POST", "/jobs/" + str(uuid4()) + "/cancel")[0], 404)
        self.assertEqual(self.create({"value": "\\u0000"})[0], 201)

    def test_pagination_ties_and_state_changes(self):
        ids = [self.create()[1]["id"] for _ in range(7)]
        with connect(self.settings) as connection:
            connection.execute("UPDATE jobs SET created_at = %s", (datetime(2026, 1, 1, tzinfo=timezone.utc),))
        seen, cursor = [], None
        while True:
            query = {"limit": 2}
            if cursor:
                query["cursor"] = cursor
            status, page, _ = self.request("GET", "/jobs?" + urlencode(query))
            self.assertEqual(status, 200)
            seen.extend(job["id"] for job in page["items"])
            # Status changes must not change pagination ordering.
            self.request("POST", "/jobs/" + ids[-1] + "/cancel")
            cursor = page["next_cursor"]
            if cursor is None:
                break
        self.assertEqual(seen, sorted(ids))
        empty_cursor = base64.urlsafe_b64encode(json.dumps(["9999-01-01T00:00:00+00:00", str(uuid4())]).encode()).decode()
        self.assertEqual(self.request("GET", "/jobs?" + urlencode({"cursor": empty_cursor}))[1],
                         {"items": [], "next_cursor": None})

    def test_rejects_malformed_cursor_values(self):
        for values in (["2026-01-01", str(uuid4())], ["bad", "bad"], {}, [1, 2], ["2026-01-01T00:00:00Z"]):
            cursor = base64.urlsafe_b64encode(json.dumps(values).encode()).decode()
            with self.subTest(values=values):
                self.assertEqual(self.request("GET", "/jobs?" + urlencode({"cursor": cursor}))[0], 422)

    def test_cancel_queued_is_repeatable_and_retry_keeps_cancelled(self):
        key = uuid4().hex
        job = self.create(key=key)[1]
        path = "/jobs/" + job["id"] + "/cancel"
        status, cancelled, _ = self.request("POST", path)
        self.assertEqual(status, 200)
        self.assertEqual(cancelled["state"], "cancelled")
        self.assertIsNotNone(cancelled["finished_at"])
        self.assertEqual(self.request("POST", path)[1], cancelled)
        self.assertEqual(self.create(key=key)[:2], (200, cancelled))

    def make_running(self):
        job_id = self.create()[1]["id"]
        with connect(self.settings) as connection:
            connection.execute(
                """UPDATE jobs SET state = 'running', attempt_no = 1, worker_id = 'test-worker',
                   started_at = clock_timestamp(), lease_expires_at = clock_timestamp() + interval '15 seconds'
                   WHERE id = %s""", (job_id,),
            )
            connection.execute(
                "INSERT INTO job_attempts (job_id, attempt_no, worker_id) VALUES (%s, 1, 'test-worker')", (job_id,),
            )
        return job_id

    def test_concurrent_running_cancellation_closes_attempt(self):
        job_id, barrier = self.make_running(), Barrier(4)

        def cancel(_):
            barrier.wait(timeout=10)
            return self.request("POST", "/jobs/" + job_id + "/cancel")

        with ThreadPoolExecutor(max_workers=4) as executor:
            responses = list(executor.map(cancel, range(4)))
        self.assertTrue(all(status == 200 and job["state"] == "cancelled" for status, job, _ in responses))
        self.assertEqual(len({job["finished_at"] for _, job, _ in responses}), 1)
        with connect(self.settings) as connection:
            row = connection.execute("SELECT outcome, finished_at FROM job_attempts WHERE job_id = %s", (job_id,)).fetchone()
            self.assertEqual(row["outcome"], "cancelled")
            self.assertIsNotNone(row["finished_at"])
            self.assertEqual(connection.execute("SELECT count(*) AS n FROM job_effects").fetchone()["n"], 0)

    def test_terminal_jobs_cannot_be_cancelled(self):
        for state in ("succeeded", "failed"):
            job_id = self.make_running()
            with connect(self.settings) as connection:
                if state == "succeeded":
                    connection.execute("INSERT INTO job_effects (job_id, attempt_no, effect) VALUES (%s, 1, '{}')", (job_id,))
                connection.execute(
                    """UPDATE jobs SET state = %s, worker_id = NULL, lease_expires_at = NULL,
                       finished_at = clock_timestamp(), result = %s::jsonb, error = %s WHERE id = %s""",
                    (state, "{}" if state == "succeeded" else None, "failure" if state == "failed" else None, job_id),
                )
                connection.execute(
                    "UPDATE job_attempts SET outcome = %s, finished_at = clock_timestamp(), error = %s WHERE job_id = %s",
                    (state, "failure" if state == "failed" else None, job_id),
                )
            before = self.request("GET", "/jobs/" + job_id)[1]
            self.assertEqual(self.request("POST", "/jobs/" + job_id + "/cancel")[0], 409)
            self.assertEqual(self.request("GET", "/jobs/" + job_id)[1], before)

    def test_openapi_contract(self):
        status, document, _ = self.request("GET", "/openapi.json")
        self.assertEqual(status, 200)
        self.assertIn("/jobs/{job_id}/cancel", document["paths"])
        self.assertIn("201", document["paths"]["/jobs"]["post"]["responses"])

    def test_retry_after_client_discards_connection_without_reading_response(self):
        key = uuid4().hex
        port = int(self.base_url.rsplit(":", 1)[1])
        body = b'{"payload":{"value":42}}'
        with socket.create_connection(("127.0.0.1", port), timeout=3) as stream:
            headers = (f"POST /jobs HTTP/1.1\r\nHost: localhost\r\nIdempotency-Key: {key}\r\n"
                       f"Content-Type: application/json\r\nContent-Length: {len(body)}\r\nConnection: close\r\n\r\n")
            stream.sendall(headers.encode() + body)
            stream.shutdown(socket.SHUT_RD)
        deadline = time.monotonic() + 3
        while time.monotonic() < deadline:
            with connect(self.settings) as connection:
                row = connection.execute("SELECT id FROM jobs WHERE idempotency_key = %s", (key,)).fetchone()
            if row:
                break
            time.sleep(0.01)
        self.assertIsNotNone(row)
        status, replay, _ = self.create({"value": 42}, key)
        self.assertEqual(status, 200)
        self.assertEqual(replay["id"], str(row["id"]))


if __name__ == "__main__":
    unittest.main()
