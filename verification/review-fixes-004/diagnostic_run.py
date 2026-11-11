"""One load repeat with PostgreSQL wait/blocker observations, preserving all output."""

import json
from pathlib import Path
import subprocess
import sys
from threading import Event, Thread
import time

from training_job_queue.db import connect
from training_job_queue.permissions import runtime_settings
from training_job_queue.settings import Settings

stopped = Event()


def sample():
    with Path('/review/postgres-waits.jsonl').open('x') as output:
        try:
            with connect(runtime_settings(Settings.from_env()), autocommit=True) as connection:
                while not stopped.is_set():
                    rows = connection.execute('''SELECT clock_timestamp() AS observed_at, datname, pid,
                        state, wait_event_type, wait_event, left(query, 180) AS query,
                        pg_blocking_pids(pid) AS blockers
                        FROM pg_stat_activity WHERE datname LIKE 'chaos_%%' AND usename='queue_runtime'
                    ''').fetchall()
                    output.write(json.dumps({'observed_unix': time.time(), 'sessions': rows}, default=str) + '\n')
                    output.flush()
                    stopped.wait(0.1)
        except Exception as error:
            output.write(json.dumps({'sampling_error': repr(error)}) + '\n')
            raise


monitor = Thread(target=sample)
monitor.start()
try:
    result = subprocess.run([sys.executable, '-m', 'harness.load', '--profile', 'create', '--pool', 'on',
                             '--workers', '16', '--pool-size', '4', '--max-outstanding-jobs', '1000',
                             '--rps', '100', '--warmup', '10', '--duration', '60', '--concurrency', '128',
                             '--drain-timeout', '120', '--timeout', '600', '--seed', '41',
                             '--output-dir', '/review/harness'])
finally:
    stopped.set()
    monitor.join(timeout=10)
sys.exit(result.returncode)
