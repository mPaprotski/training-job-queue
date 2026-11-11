"""Open-loop, fixed-arrival-rate benchmark; no retries inside measurement."""

import argparse
from concurrent.futures import ThreadPoolExecutor
import json
import math
import os
from pathlib import Path
import random
import signal
from threading import BoundedSemaphore, Event, Lock, Thread
import time
from urllib.error import HTTPError
from urllib.request import Request, urlopen

from harness.chaos import Run
from harness.invariants import database_snapshot, validate_database_on_connection
from training_job_queue.db import connect


def percentile(values, percent):
    if not values:
        return None
    ordered = sorted(values)
    return ordered[max(0, math.ceil(len(ordered) * percent / 100) - 1)]


def latency_summary(values):
    return {"p50": percentile(values, 50), "p95": percentile(values, 95), "p99": percentile(values, 99)}


class LoadRun(Run):
    def __init__(self, args):
        self.plan = []
        self.seed_keys = []

        def workload(run_id):
            entries = []
            for index in range(50):
                key = f"{run_id}:bootstrap:{index}"
                self.seed_keys.append(key)
                entries.append((key, {"phase": "bootstrap", "index": index}))
            mixed = ("create", "get", "repeat", "get", "create", "get", "repeat", "cancel", "get", "repeat")
            for index in range(round((args.warmup + args.duration) * args.rps)):
                phase = "warmup" if index < round(args.warmup * args.rps) else "measure"
                kind = "create" if args.profile == "create" else mixed[index % len(mixed)]
                item = {"index": index, "phase": phase, "kind": kind}
                if kind == "create":
                    item["key"] = f"{run_id}:{phase}:{index}"
                    item["payload"] = {"phase": phase, "index": index}
                    entries.append((item["key"], item["payload"]))
                self.plan.append(item)
            return entries

        super().__init__(args, prefix="load", workload=workload)
        args.jobs = len(self.expected)
        self.environment.update(POLL_SECONDS=str(args.poll_seconds), SIMULATION_SECONDS="0.1",
                                DB_POOL_ENABLED="1" if args.pool == "on" else "0",
                                DB_POOL_SIZE=str(args.pool_size), DB_POOL_TIMEOUT="1")
        self.request_lock = Lock()
        self.records, self.samples = [], []
        self.accepted_keys = set()
        self.requests_output = (self.directory / "requests.jsonl").open("x")
        self.samples_output = (self.directory / "samples.jsonl").open("x")
        self.monitor_stop = Event()
        self.drain_timed_out = False

    def setup(self):
        super().setup()
        metadata = json.loads((self.directory / "metadata.json").read_text())
        metadata["deployment_settings"] = {"workers": self.args.workers, "lease_seconds": self.args.lease_seconds,
                                           "heartbeat_seconds": self.args.heartbeat_seconds,
                                           "poll_seconds": self.args.poll_seconds, "simulation_seconds": 0.1,
                                           "max_outstanding_jobs": self.args.max_outstanding_jobs,
                                           "pool_enabled": self.args.pool == "on",
                                           "api_pool_size": self.args.pool_size, "worker_pool_size": 1,
                                           "pool_wait_seconds": 1}
        metadata["cgroup"] = {}
        for name in ("cpu.max", "memory.max", "memory.current"):
            path = Path("/sys/fs/cgroup") / name
            metadata["cgroup"][name] = path.read_text().strip() if path.exists() else None
        metadata["meminfo"] = Path("/proc/meminfo").read_text().splitlines()[:3]
        self.write_json("metadata.json", metadata)

    def queue_sample(self):
        with connect(self.settings) as connection:
            row = connection.execute("""
                SELECT count(*) FILTER (WHERE state = 'queued') AS queued,
                       count(*) FILTER (WHERE state = 'running') AS running,
                       count(*) FILTER (WHERE state = 'succeeded') AS succeeded,
                       count(*) FILTER (WHERE state = 'cancelled') AS cancelled,
                       COALESCE(extract(epoch FROM clock_timestamp() - min(created_at)
                           FILTER (WHERE state = 'queued')), 0) AS oldest_queued_seconds FROM jobs
            """).fetchone()
            row["db_connections"] = connection.execute(
                "SELECT count(*) AS n FROM pg_stat_activity WHERE datname = current_database()"
            ).fetchone()["n"]
        row = {key: float(value) if key == "oldest_queued_seconds" else value for key, value in row.items()}
        row["elapsed"] = time.monotonic() - self.traffic_start
        return row

    def resources(self):
        ticks = rss = 0
        for process in [self.api_process] + [info["process"] for info in self.workers]:
            try:
                fields = Path(f"/proc/{process.pid}/stat").read_text().rsplit(")", 1)[1].split()
                ticks += int(fields[11]) + int(fields[12])
                rss += int(fields[21]) * os.sysconf("SC_PAGE_SIZE")
            except (FileNotFoundError, ProcessLookupError):
                pass
        return {"application_cpu_seconds": ticks / os.sysconf("SC_CLK_TCK"), "application_rss_bytes": rss}

    def monitor(self):
        try:
            while not self.monitor_stop.is_set():
                sample = {**self.queue_sample(), **self.resources()}
                self.samples.append(sample)
                self.samples_output.write(json.dumps(sample) + "\n")
                self.samples_output.flush()
                if len(self.samples) % 5 == 0:
                    print(json.dumps({"phase": "load", **sample}), flush=True)
                self.monitor_stop.wait(1)
        except Exception as error:
            self.error("load_monitor", error)

    def record(self, record):
        with self.request_lock:
            self.records.append(record)
            self.requests_output.write(json.dumps(record, allow_nan=False) + "\n")
            self.requests_output.flush()

    def request_once(self, item, scheduled, slot):
        record = {**item, "scheduled": scheduled - self.traffic_start, "dropped": False}
        started = time.monotonic()
        record["started"] = started - self.traffic_start
        try:
            rng = random.Random(self.args.seed + item["index"])
            kind, key, job_id = item["kind"], item.get("key"), None
            if kind != "create":
                with self.state_lock:
                    key, job_id = rng.choice(list(self.known.items()))
            record["requested_job_id"] = job_id
            method, path, body = "GET", f"/jobs/{job_id}", None
            headers = {}
            if kind in ("create", "repeat"):
                method, path = "POST", "/jobs"
                body = json.dumps({"payload": self.expected[key]}).encode()
                headers = {"Content-Type": "application/json", "Idempotency-Key": key}
            elif kind == "cancel":
                method, path = "POST", f"/jobs/{job_id}/cancel"
            request = Request(self.base_url + path, data=body, headers=headers, method=method)
            try:
                response = urlopen(request, timeout=5)
            except HTTPError as error:
                response = error
            with response:
                status, job = response.status, json.load(response)
            valid = ((kind == "create" and status == 201 and job.get("idempotency_key") == key)
                     or (kind in ("get", "repeat") and status == 200 and job.get("id") == job_id)
                     or (kind == "cancel" and (status == 409 or (
                         status == 200 and job.get("id") == job_id and job.get("state") == "cancelled"))))
            record.update(status=status, valid=valid, key=key, job_id=job.get("id", job_id),
                          response_job_id=job.get("id"), error_code=job.get("code"))
            if valid and kind == "create":
                with self.state_lock:
                    self.known[key] = job["id"]
                    self.accepted_keys.add(key)
            if valid and kind == "cancel" and status == 200:
                with self.state_lock:
                    self.cancelled_ids.add(job_id)
            if valid and status != 409:
                self.observe(job)
        except Exception as error:
            record.update(valid=False, error=str(error))
        finally:
            ended = time.monotonic()
            record.update(ended=ended - self.traffic_start, dispatch_lag_seconds=started - scheduled,
                          response_seconds=ended - started, scheduled_latency_seconds=ended - scheduled)
            try:
                self.record(record)
            finally:
                slot.release()

    def workload(self):
        # Bootstrap outside warmup/measurement; all later requests have real IDs.
        for key in self.seed_keys:
            status, job = self.http("POST", "/jobs", key=key, payload=self.expected[key])
            if status != 201:
                raise RuntimeError("Bootstrap creation failed")
            self.known[key] = job["id"]
            self.accepted_keys.add(key)
        self.traffic_start = time.monotonic()
        bootstrap_deadline = time.monotonic() + 15
        while True:
            bootstrap = self.queue_sample()
            if not bootstrap["queued"] and not bootstrap["running"]:
                break
            if time.monotonic() > bootstrap_deadline:
                raise TimeoutError("Bootstrap queue did not drain")
            time.sleep(0.1)
        self.traffic_start = time.monotonic() + 0.2
        monitor = Thread(target=self.monitor, daemon=True)
        monitor.start()
        executor = ThreadPoolExecutor(max_workers=self.args.concurrency)
        slot = BoundedSemaphore(self.args.concurrency)
        futures = []
        try:
            for item in self.plan:
                scheduled = self.traffic_start + item["index"] / self.args.rps
                delay = scheduled - time.monotonic()
                if delay > 0:
                    self.stop.wait(delay)
                if time.monotonic() - self.started > self.args.timeout:
                    raise TimeoutError("Load scheduling deadline")
                if not slot.acquire(blocking=False):
                    self.record({**item, "scheduled": scheduled - self.traffic_start, "dropped": True, "valid": False})
                else:
                    futures.append(executor.submit(self.request_once, item, scheduled, slot))
                if item["index"] % self.args.rps == 0:
                    self.check_workers()
                    if self.api_process.poll() is not None:
                        raise RuntimeError("Load API exited")
            self.traffic_end = self.traffic_start + self.args.warmup + self.args.duration
            for future in futures:
                future.result(timeout=10)
            self.after_requests = self.queue_sample()
            drain_started = time.monotonic()
            while True:
                sample = self.queue_sample()
                if not sample["queued"] and not sample["running"]:
                    break
                if (time.monotonic() - drain_started > self.args.drain_timeout
                        or time.monotonic() - self.started > self.args.timeout):
                    self.drain_timed_out = True
                    self.event("load_drain_deadline", sample=sample)
                    break  # Still compute metrics from explicitly partial data.
                self.check_workers()
                time.sleep(0.2)
            self.drain_seconds = time.monotonic() - drain_started
        finally:
            self.abort.set()
            executor.shutdown(wait=True, cancel_futures=True)
            self.monitor_stop.set()
            monitor.join(timeout=10)
            self.requests_output.close()
            self.samples_output.close()
            if monitor.is_alive():
                raise TimeoutError("Load monitor did not stop")

    def validate(self):
        with database_snapshot(self.settings) as connection:
            keys = [row["idempotency_key"] for row in connection.execute("SELECT idempotency_key FROM jobs").fetchall()]
            completion = [float(row["seconds"]) for row in connection.execute("""
                SELECT extract(epoch FROM finished_at - created_at) AS seconds FROM jobs
                WHERE state = 'succeeded' AND payload->>'phase' = 'measure'
            """).fetchall()]
            measurement_states = {row["state"]: row["n"] for row in connection.execute("""
                SELECT state, count(*) AS n FROM jobs WHERE payload->>'phase' = 'measure' GROUP BY state
            """).fetchall()}
            # Include acknowledged creations even if missing; uncertain commits
            # enter the manifest only if present in this same database snapshot.
            expected = {key: self.expected[key] for key in set(keys) | self.accepted_keys if key in self.expected}
            summary = validate_database_on_connection(
                connection, expected, self.terminal_seen, self.cancelled_ids)
        self.write_json("admission.json", {"planned_keys": len(self.expected), "acknowledged_keys": len(self.accepted_keys),
                                           "durable_known_keys": len(expected),
                                           "not_durable_keys": sorted(set(self.expected) - set(keys))})
        measured = [row for row in self.records if row["phase"] == "measure"]
        issued = [row for row in measured if not row["dropped"]]
        samples = [row for row in self.samples if self.args.warmup <= row["elapsed"] <= self.args.warmup + self.args.duration]
        growth = samples[-1]["queued"] - samples[0]["queued"] if samples else None
        errors = sum(not row["valid"] for row in measured)
        started_in_window = sum(self.args.warmup <= row["started"] < self.args.warmup + self.args.duration for row in issued)
        metrics = {"offered_requests": len(measured), "issued_requests": len(issued),
                   "issued_within_window_rps": started_in_window / self.args.duration,
                   "errors_including_drops": errors, "error_fraction": errors / len(measured),
                   "dropped_requests": len(measured) - len(issued),
                   "admission_rejections": sum(row.get("status") == 503 and row.get("error_code") == "queue_full" for row in issued),
                   "acknowledged_measurement_creations": sum(row.get("kind") == "create" and row.get("valid", False) for row in issued),
                   "scheduled_latency_seconds": latency_summary([row["scheduled_latency_seconds"] for row in issued]),
                   "response_seconds": latency_summary([row["response_seconds"] for row in issued]),
                   "dispatch_lag_seconds": latency_summary([row["dispatch_lag_seconds"] for row in issued]),
                   "job_completion_seconds": latency_summary(completion),
                   "job_latency_samples": len(completion), "measurement_job_states": measurement_states,
                   "queue_growth": growth, "peak_queue": max(row["queued"] for row in samples) if samples else None,
                   "after_requests": self.after_requests, "drain_seconds": self.drain_seconds,
                   "drain_timed_out": self.drain_timed_out,
                   "peak_application_rss_bytes": max(row["application_rss_bytes"] for row in samples) if samples else None,
                   "peak_db_connections": max(row["db_connections"] for row in samples) if samples else None,
                   "statuses": {str(status): sum(row.get("status") == status for row in issued)
                                for status in {row.get("status") for row in issued}}}
        if len(samples) > 1:
            metrics["average_application_cpu_cores"] = (samples[-1]["application_cpu_seconds"] - samples[0]["application_cpu_seconds"]) / (samples[-1]["elapsed"] - samples[0]["elapsed"])
        summary.update(metrics=metrics, invariants_passed=summary["passed"])
        return summary

    def check_coverage(self, summary):
        metrics = summary["metrics"]
        p95 = metrics["scheduled_latency_seconds"]["p95"]
        p99 = metrics["scheduled_latency_seconds"]["p99"]
        completion = metrics["job_completion_seconds"]["p95"]
        criteria = {
            "exact_offered_count": metrics["offered_requests"] == self.args.rps * self.args.duration,
            "target_rps": metrics["issued_within_window_rps"] >= self.args.rps * 0.99,
            "no_generator_drops": metrics["dropped_requests"] == 0,
            "error_fraction_le_0_001": metrics["error_fraction"] <= 0.001,
            "scheduled_p95_le_0_5s": p95 is not None and p95 <= 0.5,
            "scheduled_p99_le_1s": p99 is not None and p99 <= 1,
            "dispatch_p95_le_0_05s": metrics["dispatch_lag_seconds"]["p95"] is not None and metrics["dispatch_lag_seconds"]["p95"] <= 0.05,
            "queue_growth_le_20": metrics["queue_growth"] is not None and metrics["queue_growth"] <= 20,
            "oldest_queued_le_2s": metrics["after_requests"]["oldest_queued_seconds"] <= 2,
            "job_completion_p95_le_5s": completion is not None and completion <= 5,
            "drain_le_60s": self.drain_seconds <= 60,
            "drain_completed": not self.drain_timed_out,
        }
        summary.update(criteria=criteria, benchmark_passed=all(criteria.values()))
        summary["passed"] = summary["passed"] and summary["benchmark_passed"]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--profile", choices=("mixed", "create"), default="mixed")
    parser.add_argument("--rps", type=int, default=100)
    parser.add_argument("--warmup", type=int, default=10)
    parser.add_argument("--duration", type=int, default=60)
    parser.add_argument("--seed", type=int, default=1)
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--poll-seconds", type=float, default=1)
    parser.add_argument("--pool", choices=("on", "off"), default="on")
    parser.add_argument("--pool-size", type=int, default=4)
    parser.add_argument("--max-outstanding-jobs", type=int, default=1000)
    parser.add_argument("--concurrency", type=int, default=64)
    parser.add_argument("--timeout", type=float, default=900)
    parser.add_argument("--drain-timeout", type=float, default=300)
    parser.add_argument("--output-dir", type=Path, default=Path("results"))
    parser.add_argument("--keep-database", action="store_true")
    args = parser.parse_args()
    if not (1 <= args.rps <= 1000 and args.warmup >= 0 and args.duration >= 1
            and 1 <= args.workers <= 16 and 1 <= args.concurrency <= 128
            and 1 <= args.pool_size <= 16 and 1 <= args.max_outstanding_jobs <= 1000000
            and math.isfinite(args.poll_seconds) and 0 < args.poll_seconds <= 5
            and math.isfinite(args.timeout) and math.isfinite(args.drain_timeout)
            and args.drain_timeout > 0 and args.timeout > args.warmup + args.duration):
        parser.error("Invalid rate, duration, concurrency or timeout")
    args.jobs, args.lease_seconds, args.heartbeat_seconds = 0, 15, 3
    def terminate(signum, frame):
        raise KeyboardInterrupt("Load benchmark terminated")

    signal.signal(signal.SIGTERM, terminate)
    return LoadRun(args).run()


if __name__ == "__main__":
    raise SystemExit(main())
