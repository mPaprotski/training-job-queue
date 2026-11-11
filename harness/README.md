# Chaos and capacity harnesses

## Chaos: 10,000 jobs and worker failure

From the repository root, with PostgreSQL initialized:

```sh
docker compose up -d db
docker compose build chaos
docker compose --profile tools run --rm chaos
```

Default workload: 10,000 jobs, exactly eight worker processes, random SIGKILL
about every two seconds, concurrent duplicate POSTs and cancellations. It creates
its own database/API/workers, without clearing the normal service database or
requiring a Docker socket. Supervisor uses `queue_tools`; children use restricted
`queue_runtime`. Fresh setup and existing-volume upgrades: [README.md](../README.md).

Each invocation writes `results/chaos-<UTC>-seed<seed>-<suffix>/`. Set `--seed`
for another workload. Default output UID/GID is 1000; match your account with
`LOCAL_UID`/`LOCAL_GID`. Save outer stdout/stderr too. Never overwrite/delete a
failed run before retrying. A shorter probe is not a substitute for the full run:

```sh
docker compose --profile tools run --rm chaos python -m harness.chaos \
  --jobs 1000 --seed 21 --fault-seconds 20 --timeout 180
```

Sixteen submitters create a seeded manifest. Two streams retry known keys; another
cancels both arbitrary acknowledged jobs and observed running jobs. Simulation
uses 95% short 0.01-second jobs, 5% four-second jobs and 2% retryable first-attempt
failures. Workers killed with SIGKILL are replaced. Unexpected exits are errors.
Fault injection lasts at least 30 seconds and continues until submissions finish;
then traffic/kills stop and all eight workers drain the queue. Overall timeout is
900 seconds. Exports precede removal of the temporary database; failed exports
retain it. `--keep-database` requests retention explicitly.

Correctness configuration explicitly has 10,000 admission slots, lease 3 s,
heartbeat 0.5 s and poll 0.02 s. These differ from deployment's 1,000 slots,
16 workers, lease 15 s, heartbeat 3 s and poll 0.1 s. To test deployment lease/poll
with the required eight chaos workers, pass
`--lease-seconds 15 --heartbeat-seconds 3 --poll-seconds 0.1`.

The checker compares keys/payloads to the manifest, verifies all jobs terminal,
no open attempts, exactly one effect per success/zero otherwise, consistent
results/producing attempts, contiguous attempt history, immutable observed
terminal states and persistent acknowledged cancellations. Coverage requires
actual kills, observed running work, duplicate POSTs, accepted cancellations,
and expired/cancelled attempts. A pre-kill observation is not proof that work was
still running at signal time; expired attempts demonstrate actual recovery.
Final load/checker queries share a REPEATABLE READ snapshot; worker transactions
use READ COMMITTED. Exit 0 requires invariants and coverage; nonzero is retained.

## Capacity at 100 RPS

These are engineering acceptance assumptions, not latency requirements supplied
by the brief. Run capacity separately from chaos and stop normal background
workers during measurement:

```sh
docker compose stop api worker
docker compose --profile tools run --rm chaos python -m harness.load \
  --profile create --workers 16 --poll-seconds 0.1 --pool on --pool-size 4 \
  --max-outstanding-jobs 1000 --rps 100 --warmup 10 --duration 60 \
  --concurrency 128 --drain-timeout 120 --seed 71
docker compose --profile tools run --rm chaos python -m harness.load \
  --profile mixed --workers 16 --poll-seconds 0.1 --pool on --pool-size 4 \
  --max-outstanding-jobs 1000 --rps 100 --warmup 10 --duration 60 \
  --concurrency 128 --drain-timeout 120 --seed 74
docker compose up -d api worker
```

The recorded create confirmations use seeds 71, 72 and 73. Load CLI defaults
remain the historical control (four workers/poll 1 s); selected deployment must
be passed explicitly. `--pool off` reproduces the connection-per-operation control.

Each run bootstraps/drains 50 jobs, warms up for 10 seconds, then offers exactly
6,000 operations in 60 seconds. Create = all new keys. Mixed = 20% create, 40%
status reads, 30% idempotent repeats and 10% cancels. Expected cancel 409 is valid.
The open-loop generator records drops rather than slowing the offered rate; no
measurement retries occur. Queue-full 503 and transport failures remain errors.

Every criterion must pass: at least 99% of intended RPS starts in-window; zero
generator drops; total error fraction <=0.1%; scheduled-arrival HTTP p95 <=0.5 s,
p99 <=1 s; dispatch p95 <=0.05 s; queue growth <=20; oldest queued after traffic
<=2 s; successful measurement-job completion p95 <=5 s; complete drain <=60 s;
and final data invariants. The 120-second observation cap does not relax the
60-second acceptance threshold. Partial drains retain metrics with explicit
unfinished states and censored latency samples.

Queue samples include size/age/states/backend count. Application CPU and summed
RSS exclude PostgreSQL/client. Optional host sampler (use the actual project):

```sh
python3 harness/docker_stats.py --project training-job-queue \
  --seconds 400 --output results/resource-samples.jsonl
```

## Raw evidence

| File | Contents |
| --- | --- |
| `manifest.json`, `metadata.json` | Declared workload, parameters, runtime roles, versions, source hashes |
| `events.jsonl`, `api.log`, `worker-*.log` | Requests, failures, signals, progress, raw process output |
| `jobs.jsonl`, `job_attempts.jsonl`, `job_effects.jsonl` | Final or partial table exports |
| `requests.jsonl`, `samples.jsonl` | Load schedule/start/end/status/error and periodic observations |
| `admission.json`, `summary.json` | Durable/admitted keys, invariants, counters, criteria and verdict |

All preserved runs and counts: [RUNS.md](../RUNS.md), exact parameters/hashes:
[RUNS.json](../RUNS.json). Regenerate only the index with
`python scripts/index_runs.py`; raw runs are not changed. Current verdict:
[VERDICT.md](../VERDICT.md). Independent current export/request verification:
`python verification/review-fixes-006/verify_evidence.py` from the repository root.

## What these tests do not establish

Seeds reproduce workload choices, not OS scheduling, UUIDs or all interleavings.
SIGSTOP/SIGCONT zombie return and precise commit/crash windows are covered by
[dedicated tests](../SILENT_FAILURES.md), not SIGKILL alone. The harness does not
cover host/database crashes, failover, full disks, corruption, partitions or
PostgreSQL clock jumps. Its row effect is not exactly-once external HTTP/file
behavior. Chaos throughput is not a capacity benchmark. Short, shared-host load
runs with simulated waiting do not establish performance of real training jobs,
long soaks or another deployment environment.
