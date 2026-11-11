# Training job queue: design before code

**Scope.** Python/FastAPI API, PostgreSQL, and independent worker processes; four workers by default, eight in the chaos harness. `docker compose up` starts the database, applies migrations, and starts the service. PostgreSQL provides transactional row locking and lets the job state and simulated effect commit together. No external broker is needed.

## Contract and state

`POST /jobs` requires an idempotency key. A unique database constraint resolves concurrent creates: the same key and equivalent validated JSON payload return the same job; different payload returns `409`. Compare normalized JSON values, ignoring object-key order. A lost response is retried with the same key. `GET /jobs/{id}` returns status; `GET /jobs` uses bounded cursor pagination ordered by immutable `(created_at, id)`; `POST /jobs/{id}/cancel` requests cancellation. Unknown IDs return `404`.

Transitions: `queued -> running -> succeeded | failed | cancelled`; `queued -> cancelled`; expired `running -> queued`, or `failed` if the attempt budget is exhausted. Terminal states are immutable. Initially use a **15-second lease, 3-second heartbeat, and five attempts**; expose these as configuration. Simulation duration is bounded and configurable. A recoverable execution error requeues within the budget; an explicitly permanent error fails immediately.

`jobs` stores payload, state, owner, increasing attempt number, lease expiry, timestamps and result/error. `job_attempts` records each claim and its outcome. `job_effects` stores the simulated committed effect with a unique `job_id` and the producing attempt number. Attempts and committed effects are counted separately.

## Claim, expiry and stale workers

A short transaction selects a queued job using `FOR UPDATE SKIP LOCKED`, changes it to `running`, increments its attempt number (the **fencing token**), sets owner/lease, and records the attempt. Work runs outside this transaction. A heartbeat locks the row and renews only a still-running, matching, unexpired attempt. An expired lease cannot be revived.

Workers periodically recover expired jobs in short transactions using row locks and `SKIP LOCKED`: mark the old attempt expired, then requeue or fail it according to the attempt budget. Multiple recovery loops are safe. A paused worker may still compute after expiry, but every heartbeat and result transaction checks its token and current state; it cannot commit stale work.

Use **READ COMMITTED**, explicit row locks and conditional writes. Lease decisions use PostgreSQL time sampled **after acquiring the row lock** (`clock_timestamp()`), never worker wall clocks or a transaction-start timestamp that may be stale after waiting. Worker clock skew does not control ownership; database clock jumps remain a limitation. Loss of database connectivity never grants permission to publish a result.

## Completion and cancellation

Completion locks the job row, then checks `running`, owner, token and unexpired lease. In the same transaction it inserts the effect, sets `succeeded`, and closes the attempt. Failure at any point rolls back all three changes. On an ambiguous commit outcome, read durable state before any retry; never repeat an effect unconditionally.

Cancellation locks the same row. For queued/running jobs it sets `cancelled` and closes an active attempt. Repeating cancellation returns the cancelled job; cancelling a succeeded/failed job returns `409`. The first committed terminal transition wins. If cancellation commits first, completion is rejected; if completion commits first, cancellation is too late. A worker observes cancellation at its next heartbeat/check, but correctness does not depend on immediate physical interruption.

## Guarantee and evidence required

**Retryable, at-least-once processing attempts with exactly-once committed database effect for succeeded jobs.** Bounded retries permit final failure. A crash before completion commit leaves no effect and allows recovery; a crash after commit leaves an immutable successful job. This does not promise eventual success, one execution attempt, or exactly-once external HTTP/file effects.

Required invariants: one job per key; at most one effect per job; succeeded means exactly one effect; every other state means zero effects; terminal states never change. After faults stop and a healthy database/workers remain available, all jobs must become terminal within the harness deadline. Verify concurrent claims/creates, both cancel/complete orders, expired-worker return, crash windows and clock independence with controlled integration tests, then 10,000-job chaos runs. Determine **SHIP / DON'T SHIP at 100 RPS only after measurement**; random chaos alone cannot establish production readiness.
