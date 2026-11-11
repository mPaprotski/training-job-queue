"""Independent final-state checker, using its own expected workload manifest."""

from contextlib import contextmanager

from psycopg.types.json import Jsonb

from training_job_queue.db import connect


@contextmanager
def database_snapshot(settings):
    """One stable snapshot for all related final checks and load metrics."""
    with connect(settings) as connection:
        connection.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
        yield connection


def validate_database(settings, expected, terminal_seen=None, cancelled_ids=None):
    with database_snapshot(settings) as connection:
        return validate_database_on_connection(connection, expected, terminal_seen, cancelled_ids)


def validate_database_on_connection(connection, expected, terminal_seen=None, cancelled_ids=None):
    """Use a caller-owned snapshot; the caller controls transaction lifetime."""
    violations = []
    connection.execute("CREATE TEMP TABLE expected_jobs (key TEXT PRIMARY KEY, payload JSONB NOT NULL) ON COMMIT DROP")
    with connection.cursor() as cursor:
        cursor.executemany("INSERT INTO expected_jobs VALUES (%s, %s)",
                           ((key, Jsonb(payload)) for key, payload in expected.items()))

    def check(name, query):
        count = connection.execute(f"SELECT count(*) AS n FROM ({query}) AS violations").fetchone()["n"]
        if count:
            samples = connection.execute(f"SELECT * FROM ({query}) AS violations LIMIT 5").fetchall()
            violations.append({"invariant": name, "count": count, "samples": samples})

    check("manifest_matches_jobs", """
        SELECT COALESCE(e.key, j.idempotency_key) AS key FROM expected_jobs e
        FULL JOIN jobs j ON j.idempotency_key = e.key
        WHERE e.key IS NULL OR j.id IS NULL OR j.payload IS DISTINCT FROM e.payload
    """)
    check("all_jobs_terminal", "SELECT id, state FROM jobs WHERE state NOT IN ('succeeded', 'failed', 'cancelled')")
    check("one_job_per_key", """
        SELECT idempotency_key, count(*) AS n FROM jobs
        GROUP BY idempotency_key HAVING count(*) <> 1
    """)
    check("effect_count_matches_state", """
        SELECT j.id, j.state, count(e.job_id) AS effects FROM jobs j
        LEFT JOIN job_effects e ON e.job_id = j.id GROUP BY j.id, j.state
        HAVING count(e.job_id) > 1 OR (j.state = 'succeeded' AND count(e.job_id) <> 1)
            OR (j.state <> 'succeeded' AND count(e.job_id) <> 0)
    """)
    check("success_result_and_producing_attempt_agree", """
        SELECT j.id FROM jobs j LEFT JOIN job_effects e ON e.job_id = j.id
        LEFT JOIN job_attempts a ON a.job_id = e.job_id AND a.attempt_no = e.attempt_no
        WHERE j.state = 'succeeded' AND (
            e.effect IS DISTINCT FROM j.result OR e.attempt_no IS DISTINCT FROM j.attempt_no
            OR a.outcome IS DISTINCT FROM 'succeeded'
            OR e.effect IS DISTINCT FROM jsonb_build_object(
                'job_id', j.id::text, 'attempt_no', j.attempt_no, 'worker_id', a.worker_id))
    """)
    check("attempt_history_is_contiguous", """
        SELECT j.id, j.attempt_no FROM jobs j LEFT JOIN job_attempts a ON a.job_id = j.id
        GROUP BY j.id, j.attempt_no HAVING count(a.attempt_no) <> j.attempt_no
            OR COALESCE(max(a.attempt_no), 0) <> j.attempt_no
    """)
    check("no_open_attempts_after_drain", "SELECT job_id, attempt_no FROM job_attempts WHERE outcome = 'running'")
    check("failed_current_attempt_agrees", """
        SELECT j.id FROM jobs j JOIN job_attempts a ON a.job_id = j.id AND a.attempt_no = j.attempt_no
        WHERE (j.state = 'failed' AND a.outcome NOT IN ('failed', 'expired'))
           OR (j.state = 'cancelled' AND a.outcome NOT IN ('cancelled', 'failed', 'expired'))
    """)
    rows = connection.execute("SELECT id, state FROM jobs").fetchall()
    final = {str(row["id"]): row["state"] for row in rows}
    for job_id, state in (terminal_seen or {}).items():
        if final.get(job_id) != state:
            violations.append({"invariant": "observed_terminal_state_immutable", "job_id": job_id, "expected": state})
    for job_id in cancelled_ids or ():
        if final.get(job_id) != "cancelled":
            violations.append({"invariant": "acknowledged_cancel_persists", "job_id": job_id})
    states = {row["state"]: row["n"] for row in connection.execute(
        "SELECT state, count(*) AS n FROM jobs GROUP BY state").fetchall()}
    attempts = {row["outcome"]: row["n"] for row in connection.execute(
        "SELECT outcome, count(*) AS n FROM job_attempts GROUP BY outcome").fetchall()}
    effects = connection.execute("SELECT count(*) AS n FROM job_effects").fetchone()["n"]
    return {"passed": not violations, "expected_jobs": len(expected), "actual_jobs": len(rows),
            "states": states, "attempts": attempts, "effects": effects, "violations": violations}
