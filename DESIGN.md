# Design note

The design predates implementation; the [original draft](docs/DESIGN_INITIAL.md)
is retained. This note condenses the same ownership and effect protocol.

## Architecture and contract

FastAPI, PostgreSQL and workers: 16 in Compose by default, eight in
chaos. PostgreSQL provides concurrent row claims and one transaction for state
and effect; SQLite's serialized writes are less suitable for this experiment.
`docker compose up` starts the database, migrations and service without a broker.

Review additions: bounded process-local pools and at most 1,000 live jobs by
default. Each queued/running job reserves a slot protected by a partial unique
index. Different slots commit independently; terminal transitions release them
atomically, while retries retain them. Existing keys replay at saturation.
Enabled pools must be initialized explicitly. These additions are evaluated in
later recorded experiments; the original pre-code draft is unchanged.

Create requires an idempotency key: a unique constraint resolves concurrent POSTs.
Same key/equivalent validated JSONB payload returns the existing job; different
payload returns 409. Status read, cursor listing by `(created_at, id)` and cancel
complete the API. Terminal states are immutable through application operations.

Transitions: `queued -> running -> succeeded | failed | cancelled`, and
`queued -> cancelled`. Expired running attempts requeue or fail when their stored
budget is exhausted. Defaults: lease 15 seconds, heartbeat 3 seconds, five attempts.
Retryable failures requeue within that budget; permanent failures terminate.

## Claims and dead workers

A short READ COMMITTED transaction selects queued work with
`FOR UPDATE SKIP LOCKED`, sets running/owner/lease, increments the attempt number
(fencing token), and records the attempt. Computation runs outside transactions.
Heartbeats lock the job and renew only its matching, unexpired ownership.

Workers recover expired leases in locked batches with SKIP LOCKED, close the old
attempt and requeue/fail the job. Recovery runs between jobs and at heartbeats
while simulating long work. A frozen worker can resume computing, but its old token
cannot renew ownership, publish success or record a failure after reassignment.

Lease decisions use PostgreSQL `clock_timestamp()` sampled after acquiring the
row lock, never worker wall time or transaction-start time. An expired lease cannot
be revived. Database time jumps remain a limitation. Idle-transaction and statement
timeouts bound locks held by a frozen database session.

## Cancellation and completion

Every writer locks the job before its attempt. Cancellation changes queued/running
jobs to cancelled, clears ownership and closes an active attempt atomically.
Repeating cancellation is safe; cancelling succeeded/failed work returns 409.

Completion checks running state, owner, token and lease under the same lock,
then closes the attempt, inserts the effect and sets succeeded in one transaction.
The first committed valid terminal transition wins: cancellation prevents later
publication; committed completion makes cancellation too late. Physical computation
can stop at the next heartbeat without weakening this protection.

## Guarantee and limits

Processing attempts are retryable (at-least-once delivery subject to cancellation
and the retry budget); succeeded jobs have exactly one committed database effect.
`job_effects.job_id` is unique. A pre-commit crash rolls back effect/state/attempt;
a post-commit crash leaves immutable success. An uncertain commit is resolved from
durable state, never by blindly writing another effect. This does not guarantee
one computation, eventual success, or exactly-once external file/HTTP effects.

`jobs`, `job_attempts` and `job_effects` separate state, attempts and effects.
Invariants: one job per key, one effect per success and none otherwise, consistent
attempt/result data, and terminal completion after faults stop within the test
budget. Controlled races, crash windows and chaos provide evidence; the separate
100-RPS measurements determine the verdict and its hardware/workload limits.
