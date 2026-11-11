CREATE TABLE jobs (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    idempotency_key TEXT NOT NULL,
    payload JSONB NOT NULL,
    state TEXT NOT NULL DEFAULT 'queued',
    attempt_no INTEGER NOT NULL DEFAULT 0,
    max_attempts INTEGER NOT NULL DEFAULT 5,
    worker_id TEXT,
    lease_expires_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    started_at TIMESTAMPTZ,
    finished_at TIMESTAMPTZ,
    result JSONB,
    error TEXT,

    CONSTRAINT jobs_idempotency_key_unique UNIQUE (idempotency_key),
    CONSTRAINT jobs_key_valid CHECK (
        length(idempotency_key) BETWEEN 1 AND 255 AND btrim(idempotency_key) <> ''
    ),
    CONSTRAINT jobs_payload_object CHECK (jsonb_typeof(payload) = 'object'),
    CONSTRAINT jobs_state_valid CHECK (
        state IN ('queued', 'running', 'succeeded', 'failed', 'cancelled')
    ),
    CONSTRAINT jobs_attempt_budget CHECK (
        max_attempts > 0 AND attempt_no BETWEEN 0 AND max_attempts
    ),
    CONSTRAINT jobs_worker_valid CHECK (
        worker_id IS NULL OR length(worker_id) BETWEEN 1 AND 255
    ),
    CONSTRAINT jobs_lease_shape CHECK (
        (state = 'running' AND worker_id IS NOT NULL AND lease_expires_at IS NOT NULL)
        OR (state <> 'running' AND worker_id IS NULL AND lease_expires_at IS NULL)
    ),
    CONSTRAINT jobs_attempt_started CHECK (
        (attempt_no = 0 AND started_at IS NULL)
        OR (attempt_no > 0 AND started_at IS NOT NULL)
    ),
    CONSTRAINT jobs_execution_requires_attempt CHECK (
        state NOT IN ('running', 'succeeded') OR attempt_no > 0
    ),
    CONSTRAINT jobs_finished_shape CHECK (
        (state IN ('succeeded', 'failed', 'cancelled') AND finished_at IS NOT NULL)
        OR (state IN ('queued', 'running') AND finished_at IS NULL)
    ),
    CONSTRAINT jobs_result_shape CHECK (
        (state = 'succeeded' AND result IS NOT NULL AND error IS NULL)
        OR (state <> 'succeeded' AND result IS NULL)
    ),
    CONSTRAINT jobs_failure_reason CHECK (
        state <> 'failed' OR (error IS NOT NULL AND length(error) > 0)
    )
);

CREATE INDEX jobs_pagination_idx ON jobs (created_at, id);
CREATE INDEX jobs_queued_claim_idx ON jobs (created_at, id) WHERE state = 'queued';
CREATE INDEX jobs_expired_lease_idx ON jobs (lease_expires_at, id) WHERE state = 'running';

CREATE TABLE job_attempts (
    job_id UUID NOT NULL REFERENCES jobs (id),
    attempt_no INTEGER NOT NULL CHECK (attempt_no > 0),
    worker_id TEXT NOT NULL CHECK (length(worker_id) BETWEEN 1 AND 255),
    outcome TEXT NOT NULL DEFAULT 'running',
    started_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    finished_at TIMESTAMPTZ,
    error TEXT,

    PRIMARY KEY (job_id, attempt_no),
    CONSTRAINT job_attempts_outcome_valid CHECK (
        outcome IN ('running', 'succeeded', 'failed', 'expired', 'cancelled')
    ),
    CONSTRAINT job_attempts_finished_shape CHECK (
        (outcome = 'running' AND finished_at IS NULL)
        OR (outcome <> 'running' AND finished_at IS NOT NULL)
    ),
    CONSTRAINT job_attempts_failure_reason CHECK (
        outcome <> 'failed' OR (error IS NOT NULL AND length(error) > 0)
    )
);

-- Close the old attempt before recording a new claim.
CREATE UNIQUE INDEX job_attempts_one_running_idx ON job_attempts (job_id)
    WHERE outcome = 'running';

CREATE TABLE job_effects (
    job_id UUID PRIMARY KEY,
    attempt_no INTEGER NOT NULL,
    effect JSONB NOT NULL CHECK (jsonb_typeof(effect) = 'object'),
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),

    CONSTRAINT job_effects_attempt_fk FOREIGN KEY (job_id, attempt_no)
        REFERENCES job_attempts (job_id, attempt_no)
);
