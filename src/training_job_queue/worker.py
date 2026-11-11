"""Single-job worker with bounded simulation and cooperative lease heartbeats."""

import logging
import signal
import socket
from threading import Event
import time
from uuid import uuid4

import psycopg
from psycopg_pool import PoolTimeout, TooManyRequests
from pydantic import ValidationError

from training_job_queue.models import Simulation
from training_job_queue.db import connection_pool
from training_job_queue.processing import claim_job, complete_job, fail_job, recover_expired, renew_lease
from training_job_queue.settings import Settings

logger = logging.getLogger(__name__)


def execute_job(settings: Settings, job: dict, worker_id: str, stopped: Event) -> None:
    job_id, attempt_no = job["id"], job["attempt_no"]
    try:
        simulation = Simulation.model_validate(job["payload"].get("simulation", {}))
    except ValidationError:
        fail_job(settings, job_id, worker_id, attempt_no, "Invalid simulation configuration", permanent=True)
        return
    duration = simulation.duration_seconds
    if duration is None:
        duration = settings.simulation_seconds
    deadline = time.monotonic() + duration
    heartbeat_at = time.monotonic() + settings.heartbeat_seconds
    while not stopped.is_set():
        now = time.monotonic()
        if now >= heartbeat_at:
            if not renew_lease(settings, job_id, worker_id, attempt_no):
                logger.info("job=%s attempt=%s ownership lost; abandoning work", job_id, attempt_no)
                return
            recovered = recover_expired(settings)
            if recovered:
                logger.info("worker=%s recovered=%s during execution", worker_id, recovered)
            heartbeat_at = time.monotonic() + settings.heartbeat_seconds
        if time.monotonic() >= deadline:
            break
        stopped.wait(max(0, min(deadline, heartbeat_at) - time.monotonic()))
    if stopped.is_set():
        # Expiry/recovery handles shutdown or SIGKILL; do not mutate ownership.
        return
    if simulation.permanent_failure or attempt_no <= simulation.fail_until_attempt:
        accepted = fail_job(
            settings, job_id, worker_id, attempt_no, "Simulated execution failure",
            permanent=simulation.permanent_failure,
        )
        logger.info("job=%s attempt=%s failure recorded=%s", job_id, attempt_no, accepted)
    else:
        accepted = complete_job(settings, job_id, worker_id, attempt_no)
        logger.info("job=%s attempt=%s completion recorded=%s", job_id, attempt_no, accepted)


def run_worker(settings: Settings, worker_id: str, stopped: Event) -> None:
    logger.info("worker=%s started", worker_id)
    while not stopped.is_set():
        try:
            recovered = recover_expired(settings)
            if recovered:
                logger.info("worker=%s recovered=%s", worker_id, recovered)
            job = claim_job(settings, worker_id)
            if job is None:
                stopped.wait(settings.poll_seconds)
                continue
            logger.info("worker=%s job=%s attempt=%s claimed", worker_id, job["id"], job["attempt_no"])
            execute_job(settings, job, worker_id, stopped)
        except (psycopg.OperationalError, psycopg.InterfaceError, PoolTimeout, TooManyRequests,
                psycopg.errors.LockNotAvailable, psycopg.errors.QueryCanceled) as error:
            # Commit might have succeeded. Abandon local execution; subsequent
            # claims read durable state and recovery only acts on expired leases.
            logger.warning("worker=%s database operation uncertain (%s); abandoning local work",
                           worker_id, type(error).__name__)
            stopped.wait(settings.poll_seconds)
        # Integrity/invariant/programming errors escape with a traceback instead
        # of masquerading as successful work.
    logger.info("worker=%s stopped", worker_id)


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    settings = Settings.from_env()
    stopped = Event()

    def stop(signum: int, frame: object) -> None:
        stopped.set()

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    with connection_pool(settings, size=1):
        run_worker(settings, f"{socket.gethostname()}:{uuid4().hex[:8]}", stopped)


if __name__ == "__main__":
    main()
