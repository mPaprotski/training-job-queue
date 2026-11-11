"""Seeded workload + SIGKILL supervisor. Run through the Compose chaos profile."""

import argparse
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from datetime import datetime, timezone
import hashlib
from importlib.metadata import version
import json
import math
import os
from pathlib import Path
import platform
import random
import signal
import socket
import subprocess
import sys
from threading import Event, Lock, Thread
import time
import traceback
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen
from uuid import uuid4

from psycopg import sql
from psycopg.conninfo import make_conninfo

from harness.invariants import validate_database
from training_job_queue.db import connect
from training_job_queue.migrate import migrate
from training_job_queue.permissions import grant_runtime_permissions, runtime_settings
from training_job_queue.settings import Settings


class Run:
    def __init__(self, args, *, prefix="chaos", workload=None):
        self.args = args
        self.started = time.monotonic()
        stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        self.run_id = f"{prefix}-{stamp}-seed{args.seed}-{uuid4().hex[:8]}"
        self.directory = args.output_dir / self.run_id
        self.directory.mkdir(parents=True, exist_ok=False)
        self.events = (self.directory / "events.jsonl").open("x")
        self.event_lock, self.state_lock = Lock(), Lock()
        self.stop, self.abort = Event(), Event()
        self.errors, self.terminal_seen, self.cancelled_ids = [], {}, set()
        self.known, self.counters, self.workers = {}, {}, []
        self.api_process = None
        self.database_created = False
        self.admin_settings = Settings.from_env()
        self.database_name = "chaos_" + uuid4().hex
        self.settings = replace(self.admin_settings,
                                database_url=make_conninfo(self.admin_settings.database_url, dbname=self.database_name))
        self.runtime_settings = runtime_settings(self.settings)
        self.environment = dict(os.environ, DATABASE_URL=self.runtime_settings.database_url,
                                MAX_OUTSTANDING_JOBS=str(args.max_outstanding_jobs),
                                LEASE_SECONDS=str(args.lease_seconds), HEARTBEAT_SECONDS=str(args.heartbeat_seconds),
                                POLL_SECONDS=str(getattr(args, "poll_seconds", 0.02)),
                                SIMULATION_SECONDS="0.01", MAX_ATTEMPTS="5")
        rng = random.Random(args.seed)
        self.expected = {}
        self.entries = []
        for index in range(args.jobs):
            duration = 4.0 if rng.random() < 0.05 else 0.01
            retry = 1 if rng.random() < 0.02 else 0
            payload = {"index": index, "seed": args.seed,
                       "simulation": {"duration_seconds": duration, "fail_until_attempt": retry}}
            key = f"{self.run_id}:{index}"
            self.expected[key] = payload
            self.entries.append((key, payload))
        if workload is not None:
            self.entries = workload(self.run_id)
            self.expected = dict(self.entries)
        self.write_json("manifest.json", self.expected)

    def write_json(self, name, value):
        path = self.directory / name
        temporary = path.with_suffix(path.suffix + ".tmp")
        temporary.write_text(json.dumps(value, indent=2, default=str, allow_nan=False) + "\n")
        temporary.replace(path)

    def event(self, kind, **fields):
        record = {"elapsed": round(time.monotonic() - self.started, 6), "event": kind, **fields}
        with self.event_lock:
            self.events.write(json.dumps(record, default=str, allow_nan=False) + "\n")
            self.events.flush()

    def increment(self, name):
        with self.state_lock:
            self.counters[name] = self.counters.get(name, 0) + 1

    def error(self, where, error):
        with self.state_lock:
            self.errors.append({"where": where, "error": str(error)})
        self.event("error", where=where, error=str(error), traceback=traceback.format_exc())

    def observe(self, job):
        if job["state"] in ("succeeded", "failed", "cancelled"):
            with self.state_lock:
                previous = self.terminal_seen.setdefault(job["id"], job["state"])
                if previous != job["state"]:
                    raise AssertionError("Observed terminal state changed")

    def http(self, method, path, *, key=None, payload=None):
        headers = {}
        data = None
        if payload is not None:
            data = json.dumps({"payload": payload}, allow_nan=False).encode()
            headers["Content-Type"] = "application/json"
        if key:
            headers["Idempotency-Key"] = key
        request = Request(self.base_url + path, data=data, method=method, headers=headers)
        for retry in range(5):
            if self.abort.is_set() or time.monotonic() - self.started > self.args.timeout:
                raise TimeoutError("Harness deadline reached")
            try:
                try:
                    response = urlopen(request, timeout=5)
                except HTTPError as error:
                    response = error
                with response:
                    status, body = response.status, json.load(response)
                self.event("http", method=method, path=path, key=key, status=status,
                           job_id=body.get("id"), state=body.get("state"), retry=retry)
                if status != 503:
                    return status, body
            except (URLError, TimeoutError, ConnectionError) as error:
                self.event("http_transport_error", method=method, path=path, key=key, error=str(error), retry=retry)
            self.abort.wait(0.05)
        raise RuntimeError(f"HTTP operation exhausted retries: {method} {path}")

    def submit(self, entry):
        key, payload = entry
        status, job = self.http("POST", "/jobs", key=key, payload=payload)
        if status not in (200, 201) or job.get("idempotency_key") != key:
            raise AssertionError(f"Create rejected or wrong key: {status} {job}")
        with self.state_lock:
            self.known[key] = job["id"]
        self.observe(job)
        self.increment("unique_posts")

    def traffic(self, kind, seed):
        rng = random.Random(seed)
        try:
            while not self.stop.is_set():
                with self.state_lock:
                    entry = rng.choice(list(self.known.items())) if self.known else None
                if entry is None:
                    self.stop.wait(0.01)
                    continue
                key, job_id = entry
                if kind == "repeat":
                    status, job = self.http("POST", "/jobs", key=key, payload=self.expected[key])
                    if status != 200 or job.get("id") != job_id:
                        raise AssertionError("Request retry created a different job")
                    self.increment("repeated_posts")
                else:
                    # Pure random cancellation can exercise only queued jobs.
                    # Alternate it with observed running jobs, still selected
                    # randomly; final attempt outcomes prove whether it landed.
                    if rng.random() < 0.5:
                        with connect(self.settings) as connection:
                            running = connection.execute(
                                "SELECT id, idempotency_key FROM jobs WHERE state = 'running'",
                            ).fetchall()
                        with self.state_lock:
                            candidates = [row for row in running if row["idempotency_key"] in self.known]
                        if candidates:
                            candidate = rng.choice(candidates)
                            key, job_id = candidate["idempotency_key"], str(candidate["id"])
                            self.increment("cancel_running_observed")
                    status, job = self.http("POST", f"/jobs/{job_id}/cancel")
                    if status == 200 and job.get("state") == "cancelled":
                        with self.state_lock:
                            self.cancelled_ids.add(job_id)
                        self.increment("cancel_accepted")
                    elif status == 409:
                        status, job = self.http("GET", f"/jobs/{job_id}")
                        if status != 200 or job.get("state") not in ("succeeded", "failed"):
                            raise AssertionError("Cancel conflict did not reference a completed job")
                        self.increment("cancel_too_late")
                    else:
                        raise AssertionError(f"Unexpected cancel response: {status} {job}")
                    self.increment("cancel_requests")
                self.observe(job)
                self.stop.wait(0.02)
        except Exception as error:
            if not self.abort.is_set():
                self.error(kind, error)

    def spawn(self, slot):
        path = self.directory / f"worker-{slot:02d}.log"
        with path.open("ab") as log:
            offset = log.tell()
            process = subprocess.Popen([sys.executable, "-m", "training_job_queue.worker"],
                                       env=self.environment, stdout=log, stderr=subprocess.STDOUT)
        info = {"process": process, "slot": slot, "path": path, "offset": offset}
        self.event("worker_started", slot=slot, pid=process.pid)
        return info

    def check_workers(self):
        for slot, info in enumerate(self.workers):
            code = info["process"].poll()
            if code is not None:
                self.error("worker_exit", f"slot={slot} unexpected exit={code}")
                self.workers[slot] = self.spawn(slot)

    def kill_random(self, rng):
        slot = rng.randrange(len(self.workers))
        info = self.workers[slot]
        with info["path"].open("rb") as log:
            log.seek(info["offset"])
            lines = log.read().decode(errors="replace").splitlines()
        names = [line.split("worker=", 1)[1].split()[0] for line in lines if "worker=" in line and line.endswith(" started")]
        worker_id = names[-1] if names else None
        with connect(self.settings) as connection:
            affected = connection.execute(
                "SELECT id, attempt_no FROM jobs WHERE state = 'running' AND worker_id = %s", (worker_id,),
            ).fetchall()
        info["process"].kill()
        code = info["process"].wait(timeout=5)
        if code != -signal.SIGKILL:
            raise AssertionError(f"Expected SIGKILL exit, got {code}")
        self.increment("sigkills")
        if affected:
            self.increment("sigkills_with_running_job")
        self.event("sigkill", slot=slot, pid=info["process"].pid, worker_id=worker_id, jobs=affected, returncode=code)
        self.workers[slot] = self.spawn(slot)

    def progress(self, phase):
        with connect(self.settings) as connection:
            states = {row["state"]: row["n"] for row in connection.execute("SELECT state, count(*) AS n FROM jobs GROUP BY state").fetchall()}
        self.event("progress", phase=phase, states=states, counters=dict(self.counters))
        print(json.dumps({"phase": phase, "elapsed": round(time.monotonic() - self.started, 1),
                          "states": states, "sigkills": self.counters.get("sigkills", 0)}), flush=True)
        return states

    def setup(self):
        with connect(self.admin_settings, autocommit=True) as admin:
            admin.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(self.database_name)))
        self.database_created = True
        original = os.environ["DATABASE_URL"]
        try:
            os.environ["DATABASE_URL"] = self.settings.database_url
            migrate(Path("migrations"))
            grant_runtime_permissions(self.settings)
        finally:
            os.environ["DATABASE_URL"] = original
        files = [Path("requirements.txt"), Path("pyproject.toml")]
        for root, pattern in (("src", "*.py"), ("harness", "*.py"), ("migrations", "*.sql")):
            files.extend(sorted(Path(root).rglob(pattern)))
        hashes = {str(path): hashlib.sha256(path.read_bytes()).hexdigest() for path in files}
        metadata = {"run_id": self.run_id, "seed": self.args.seed, "database": self.database_name,
                    "parameters": vars(self.args), "python": sys.version, "platform": platform.platform(),
                    "cpus_visible": os.cpu_count(), "source_revision": os.getenv("SOURCE_REVISION", "uncommitted/not supplied"),
                    "source_files_sha256": hashes,
                    "packages": {name: version(name) for name in ("fastapi", "starlette", "uvicorn", "psycopg")}}
        with connect(self.runtime_settings) as connection:
            metadata["runtime_role"] = connection.execute(
                "SELECT current_user AS name, rolsuper, rolcreatedb, rolcreaterole FROM pg_roles WHERE rolname = current_user"
            ).fetchone()
        with connect(self.settings) as connection:
            metadata["postgres"] = connection.execute("SELECT version() AS version").fetchone()["version"]
        self.write_json("metadata.json", metadata)
        listener = socket.socket()
        listener.bind(("127.0.0.1", 0))
        listener.listen(128)
        self.base_url = f"http://127.0.0.1:{listener.getsockname()[1]}"
        try:
            with (self.directory / "api.log").open("ab") as log:
                self.api_process = subprocess.Popen(
                    [sys.executable, "-m", "uvicorn", "training_job_queue.api:app", "--fd", str(listener.fileno()),
                     "--log-level", "warning", "--no-access-log"],
                    pass_fds=(listener.fileno(),), env=self.environment, stdout=log, stderr=subprocess.STDOUT)
        finally:
            listener.close()
        deadline = time.monotonic() + 15
        while time.monotonic() < deadline:
            if self.api_process.poll() is not None:
                raise RuntimeError("Harness API exited during startup; inspect api.log")
            try:
                with urlopen(self.base_url + "/health/ready", timeout=0.5) as response:
                    if response.status == 200:
                        break
            except (URLError, TimeoutError):
                time.sleep(0.05)
        else:
            raise TimeoutError("Harness API failed to start")
        for slot in range(getattr(self.args, "workers", 8)):
            self.workers.append(self.spawn(slot))

    def workload(self):
        executor = ThreadPoolExecutor(max_workers=self.args.submitters)
        threads = [Thread(target=self.traffic, args=(kind, self.args.seed + offset), daemon=True)
                   for offset, kind in enumerate(("repeat", "repeat", "cancel"), start=100)]
        futures = []
        try:
            futures = [executor.submit(self.submit, entry) for entry in self.entries]
            for thread in threads:
                thread.start()
            rng = random.Random(self.args.seed + 1000)
            phase_start = time.monotonic()
            next_kill, next_progress = phase_start + self.args.kill_interval, phase_start
            while True:
                now = time.monotonic()
                if now - self.started > self.args.timeout:
                    raise TimeoutError("Deadline during submissions/fault phase")
                self.check_workers()
                if self.api_process.poll() is not None:
                    raise RuntimeError("API exited; inspect api.log")
                if all(future.done() for future in futures) and now - phase_start >= self.args.fault_seconds:
                    break
                if now >= next_kill:
                    self.kill_random(rng)
                    next_kill = time.monotonic() + self.args.kill_interval
                if now >= next_progress:
                    self.progress("faults")
                    next_progress = now + 5
                self.stop.wait(0.05)
            self.stop.set()
            for future in futures:
                try:
                    future.result()
                except Exception as error:
                    self.error("submit", error)
            for thread in threads:
                thread.join(timeout=30)
                if thread.is_alive():
                    raise TimeoutError("Traffic thread failed to stop")
            self.event("fault_phase_finished")
            next_progress = 0
            while True:
                if time.monotonic() - self.started > self.args.timeout:
                    raise TimeoutError("Deadline while draining queue")
                self.check_workers()
                if self.api_process.poll() is not None:
                    raise RuntimeError("API exited during drain; inspect api.log")
                if time.monotonic() >= next_progress:
                    states = self.progress("drain")
                    next_progress = time.monotonic() + 5
                else:
                    with connect(self.settings) as connection:
                        states = {row["state"]: row["n"] for row in connection.execute(
                            "SELECT state, count(*) AS n FROM jobs GROUP BY state").fetchall()}
                if not states.get("queued", 0) and not states.get("running", 0):
                    break
                time.sleep(0.2)
        finally:
            self.stop.set()
            self.abort.set()
            for future in futures:
                future.cancel()
            executor.shutdown(wait=True, cancel_futures=True)
            for thread in threads:
                if thread.ident is not None:
                    thread.join(timeout=6)

    def snapshots(self):
        with connect(self.settings) as connection:
            for table in ("jobs", "job_attempts", "job_effects"):
                with (self.directory / f"{table}.jsonl").open("x") as output:
                    # Table names are a fixed internal allowlist, never input.
                    cursor = connection.execute(f"SELECT * FROM {table}")
                    for row in cursor:
                        output.write(json.dumps(row, default=str) + "\n")

    @staticmethod
    def stop_process(process):
        if process is not None and process.poll() is None:
            process.terminate()
            try:
                process.wait(timeout=6)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=3)

    def validate(self):
        return validate_database(self.settings, self.expected, self.terminal_seen, self.cancelled_ids)

    def check_coverage(self, summary):
        for requirement in ("sigkills", "sigkills_with_running_job", "repeated_posts", "cancel_requests", "cancel_accepted"):
            if not self.counters.get(requirement):
                self.error("coverage", f"No {requirement} observed")
        for outcome in ("expired", "cancelled"):
            if not summary["attempts"].get(outcome):
                self.error("coverage", f"No {outcome} attempt observed")

    def run(self):
        print(f"Run artifacts: {self.directory}", flush=True)
        summary = {"run_id": self.run_id, "passed": False}
        try:
            self.setup()
            self.workload()
            summary.update(self.validate())
            self.check_coverage(summary)
        except (Exception, KeyboardInterrupt) as error:
            self.error("run", error)
        finally:
            self.stop.set()
            self.abort.set()
            for process in [info["process"] for info in self.workers] + [self.api_process]:
                try:
                    self.stop_process(process)
                except Exception as error:
                    self.error("process_cleanup", error)
            retained = self.database_created
            if self.database_created:
                snapshot_ok = False
                try:
                    self.snapshots()
                    snapshot_ok = True
                except Exception as error:
                    self.error("snapshots", error)
                # If export fails, retain the database for forensics.
                if not self.args.keep_database and snapshot_ok:
                    try:
                        with connect(self.admin_settings, autocommit=True) as admin:
                            admin.execute(sql.SQL("DROP DATABASE {}").format(sql.Identifier(self.database_name)))
                        retained = False
                    except Exception as error:
                        self.error("database_cleanup", error)
            summary.update({"counters": self.counters, "errors": self.errors,
                            "database": self.database_name, "database_retained": retained,
                            "elapsed_seconds": round(time.monotonic() - self.started, 3)})
            summary["passed"] = summary["passed"] and not self.errors
            self.write_json("summary.json", summary)
            self.event("finished", passed=summary["passed"])
            self.events.close()
        print(json.dumps(summary, default=str), flush=True)
        return 0 if summary["passed"] else 1


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--jobs", type=int, default=10000)
    parser.add_argument("--max-outstanding-jobs", type=int, default=10000,
                        help="Admission capacity for correctness workload; deployment default is 1000")
    parser.add_argument("--seed", type=int, default=1)
    parser.add_argument("--submitters", type=int, default=16)
    parser.add_argument("--kill-interval", type=float, default=2)
    parser.add_argument("--fault-seconds", type=float, default=30)
    parser.add_argument("--lease-seconds", type=float, default=3)
    parser.add_argument("--heartbeat-seconds", type=float, default=0.5)
    parser.add_argument("--poll-seconds", type=float, default=0.02)
    parser.add_argument("--timeout", type=float, default=900)
    parser.add_argument("--output-dir", type=Path, default=Path("results"))
    parser.add_argument("--keep-database", action="store_true")
    args = parser.parse_args()
    timings = (args.kill_interval, args.fault_seconds, args.timeout, args.heartbeat_seconds, args.lease_seconds, args.poll_seconds)
    if (not all(math.isfinite(value) for value in timings)
            or args.jobs < 1 or not 1 <= args.max_outstanding_jobs <= 1000000
            or not 1 <= args.submitters <= 32 or args.kill_interval <= 0
            or args.fault_seconds <= args.kill_interval or args.timeout <= args.fault_seconds
            or args.poll_seconds <= 0
            or not 0 < args.heartbeat_seconds < args.lease_seconds):
        parser.error("Invalid workload size, timing or concurrency")
    def terminate(signum, frame):
        raise KeyboardInterrupt("Harness terminated")

    signal.signal(signal.SIGTERM, terminate)
    return Run(args).run()


if __name__ == "__main__":
    sys.exit(main())
