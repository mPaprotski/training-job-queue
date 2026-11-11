# AI usage note

## Summary

I used AI tools (Claude) as a code-writing accelerator. Design decisions,
invariants, failure experiments, interpretation of results and the verdict
are mine. AI wrote code against specifications I provided; I reviewed,
ran and corrected it.

## What I did myself

- **Design note:** PostgreSQL and its rationale, row-lock claim mechanism,
  lease/dead-worker detection, fencing, cancellation of running work and the
  guarantee of at-least-once attempts with exactly-once committed database
  effects for successful jobs. AI was used only to proofread wording.
  See [DESIGN.md](DESIGN.md) and the [original draft](docs/DESIGN_INITIAL.md).
- **Invariants:** no lost admitted jobs, at most one effect per job, no work
  stuck in `running` after drain, consistent final states, and one logical
  job per idempotency key. I decided how each is verified.
- **Silent-failure experiments:** double claim, zombie worker, cancel/complete
  race, duplicate request retry, transaction isolation and application clock
  skew. Each was reproduced by me; the cases and limits are described in
  [SILENT_FAILURES.md](SILENT_FAILURES.md), with the latest combined
  [test output](verification/review-fixes-006/integration-tests-final.txt).
- **Interpretation of runs:** analysis of failed runs, harness blind spots,
  the SHIP/DON'T SHIP decision and the closing reflection. All chaos/load
  runs, including failures, are indexed in [RUNS.md](RUNS.md).

## What AI wrote from my specification

- Project scaffold, Docker Compose, Dockerfile and SQL migrations.
- HTTP API: create, status, cursor pagination and cancellation.
- Worker loop, heartbeat/recovery plumbing and logging.
- Chaos harness process control, SIGKILL scheduling and traffic generation.
- SQL invariant checks, negative fixtures and result collection scripts.
- Connection-pool and admission-slot changes after review.

## Core logic: AI proposed, I reviewed line by line

| Component | AI suggestion | What I did |
| --- | --- | --- |
| Claim query | Select queued work with `FOR UPDATE SKIP LOCKED`, then set owner/lease and increment the attempt token in one transaction | Accepted; checked eight simultaneous claims produce one winner and a locked job is skipped |
| Result write / fencing | Require `running`, matching worker/token and an unexpired lease under the job lock | Accepted; checked expired tokens, reassignment and a real SIGSTOP/SIGCONT zombie cannot publish stale work |
| Effect + status atomicity | Close the attempt, insert the unique effect and set success in one transaction | Accepted; checked rollback and SIGKILL before insertion, after insertion/before commit, and after real commit |
| Reaper / lease expiry | Recover expired jobs with row locks and SKIP LOCKED; retry within the stored budget or fail finally | Accepted; checked concurrent recovery, live leases, exhausted budgets and replacement-worker recovery |
| Idempotent insert | Unique key plus `ON CONFLICT`, then a fresh READ COMMITTED lookup | Kept JSONB equality so booleans and numbers differ; checked concurrent replay/conflict and a client losing the response |
| Cancel handling | Lock job before attempt; first committed terminal transition wins | Accepted; checked both race winners, repeated cancellation and rejected completion/heartbeat after cancellation |

Concrete checks are in [processing tests](tests/integration/test_processing.py),
[adversarial tests](tests/integration/test_adversarial.py) and
[API tests](tests/integration/test_api.py).

## AI code review and manual follow-up

After the implementation was working, I asked AI to review the code as a
second pair of eyes. I did not treat its findings as verdicts: for each one I
checked the code and the run data myself, decided whether it was real, and
only then used AI to help with the fix. Findings retain their original severity.

### Finding P2 — admission control created a shared serial section

**Location:** `create_job()` in [jobs.py](src/training_job_queue/jobs.py).
The original line reference pointed to the advisory lock; the implementation
has since changed.

**What the review said.** Every new job took the same transaction advisory
lock, held through commit. The bound was correct, but waiting in that critical
section could delay all other new admissions.

**My manual analysis.**

- The [diagnostic wait samples](verification/review-fixes-004/postgres-waits.jsonl)
  and [summary](verification/review-fixes-004/postgres-waits-summary.json)
  contain 785 snapshots, including 507 non-idle session observations waiting
  on an advisory lock, 246 on WALSync and 167 on WALWrite.
- These are sampled session observations, not independent requests, measured
  lock-hold durations or a transaction-latency histogram. I did not collect
  a dedicated distribution of admission-lock/transaction durations.
- The first 16-worker create run had HTTP p95 about 897 ms and 13 generator
  drops; a diagnostic repeat passed with p95 about 340 ms. That variability
  did not establish one proven root cause. The repeat also had a different
  observer, so I did not substitute it for the failed matrix row.
- I checked drops, dispatch lag, HTTP status, job completion, queue age/growth
  and database connection counts. The evidence did not rule out every host,
  storage or scheduling contribution.
- Simply removing coordination would make count-then-insert unsafe. I kept
  the bound under tests with 16 concurrent new keys competing for one slot.
  I do not claim a separate mutation run that merely deleted the old lock.

**Resolution.**

1. Replaced the shared lock with `admission_slot`: a partial unique index allows
   at most one queued/running job per slot. New jobs select inside the configured
   range and retry bounded conflicts; terminal transitions release the slot
   atomically. Retry/recovery retains it. Migration backfills existing live jobs.
2. Added tests for forced collisions, saturation/replay, rollback, lower limits
   and an uncommitted insert on one slot while another slot commits independently.
   See [admission tests](tests/integration/test_admission.py) and
   [migration test](tests/integration/test_slot_migration.py).
3. Preserved the subsequent 1-second-poll failure: queue growth was +27 against
   the unchanged +20 criterion. I changed actual idle polling to 0.1 second,
   then required three create passes and a mixed confirmation. All four passed
   at 100 RPS with no errors/drops; create HTTP p95 was 51–61 ms and job p95
   0.182–0.186 s. [Results](verification/review-fixes-006/capacity-poll.json).

**Status: fixed.** The global serial section is removed and the capacity bound
is retained. This does not prove a unique explanation for historical tail spikes.
The separately measured connection microprobe improved from 12.368 to 0.337 ms
per SELECT transaction with pooling; that is connection-overhead evidence,
not an admission-lock-duration measurement.

### Finding P3 — `transaction()` did not guarantee pool use

**Location:** [db.py](src/training_job_queue/db.py).

**What the review said.** If no pool was registered, the helper silently opened
an ordinary connection even when pooling was enabled.

**My manual analysis.** I checked that API lifespan and worker entrypoints
opened their pools, together with the pool/HTTP tests and recorded backend
counts. No active missing-pool bug was confirmed in those entrypoints. The
risk was a later refactor silently restoring per-operation connections.

**Resolution.** Enabled runtime transactions now raise `RuntimePoolNotStarted`
if the pool is absent. Raw runtime mode requires explicit `DB_POOL_ENABLED=0`;
migration/administrative connections remain explicitly raw. Protocol fixtures
opt out deliberately, while API/workers and dedicated pool tests use real pools.
The [missing-pool regression](tests/unit/test_pool_lifecycle.py) asserts that no
raw connection is opened. [Pool tests](tests/integration/test_pool.py) cover
reuse, rollback, bounded checkout, broken idle connections and shutdown;
[API tests](tests/integration/test_api.py) cover HTTP 503 and recovery after
exhaustion.

**Status: fixed.**

### What this shows about my process

AI review identified the admission bottleneck and the latent pool-contract risk.
I kept their interpretation bounded: sampled waits were a profiling lead, and
missing pool initialization was a future-refactoring risk rather than a proven
active incident. After the changes, the final image passed all **88 integration
tests** and **21 unit tests**. The first failed slot test and failed capacity
runs remain available alongside the passing results.

## Where the AI was wrong or unreliable

1. **Cancellation validation accepted the wrong job.** The generated load
   checker accepted HTTP 200/`cancelled` without matching the response ID to
   the requested ID. During review I reproduced this with a mocked response
   containing another job's ID. The regression failed against the original
   checker: [raw output](verification/review-fixes-001/baseline-negative-test.txt).
   The fix checks both ID and state, records both IDs and acknowledges only
   the requested cancellation. [Tests](tests/unit/test_load.py).
2. **SQL candidate order was assumed rather than guaranteed.** The first
   slot query lacked explicit ORDER BY. The independent-commit test timed out:
   [failed suite](verification/review-fixes-006/integration-tests-attempt-001.txt).
   I checked the query, added explicit ordering and reran the focused case and
   complete suite: [focused pass](verification/review-fixes-006/independent-slots-attempt-002.txt),
   [final suite](verification/review-fixes-006/integration-tests-final.txt).
3. **Success was reported before a test finished.** AI described that slot
   test as passing while it was still running. The final result was one timeout,
   so the status was corrected, the failure investigated and outputs retained.
   Final exit codes/summaries, rather than intermediate progress, decide success.

There is no recorded case where an AI claim that all data invariants held was
contradicted by the completed chaos result. Some load runs passed final data
invariants but failed performance criteria; they remain failed capacity results.
The latest full chaos passed 10,000 jobs/eight workers/32 SIGKILL with 9,666
successes, 334 cancellations and no final invariant violations:
[summary](verification/review-fixes-006/harness/chaos-20261111T081108Z-seed51-4174187c/summary.json).

## How I verified that checks can fail

I used deliberately inconsistent fixtures in isolated databases, not only green
happy-path runs. [Checker tests](tests/integration/test_invariants.py) verify:

- Missing manifest jobs and stuck/open running attempts are rejected.
- Temporarily removing the effect primary key and inserting a duplicate is
  detected independently of that constraint.
- Temporarily removing idempotency-key uniqueness exposes duplicate logical jobs,
  which the checker detects.
- Wrong success results/producing attempts and JSON boolean/number differences
  are rejected.
- Changed observed terminal states and lost acknowledged cancellations fail.
- A healthy fixture passes, so unconditional failure cannot satisfy the suite.

These are fixture/constraint mutations. I do not claim that SKIP LOCKED or the
fencing condition was disabled in a separate mutation-testing campaign. Those
behaviors are exercised by real concurrent-claim and zombie-worker tests.
Timeout/drain guard failures and all workload runs remain in [RUNS.md](RUNS.md);
the current [integration output](verification/review-fixes-006/integration-tests-final.txt)
includes the negative checker cases.

## What I did not delegate

Conclusions about run results, the SHIP/DON'T SHIP verdict, and the
"what surprised / concerns me" paragraph were written by me. AI helped with
implementation and review; I retain responsibility for the final submission.
The verdict is limited to the measured simulated workload and configuration,
not arbitrary production training workloads or untested database failure modes.
