# SHIP at 100 RPS for the measured simulation configuration

The selected Compose configuration uses **16 workers, 0.1-second idle poll,
a four-connection API pool, one connection per worker and 1,000 admission slots**.
Three consecutive all-create runs and a mixed confirmation passed every existing
100-RPS criterion with zero errors, generator drops or admission rejections.
This verdict covers the specified small-payload, 0.1-second simulated workload
on the recorded local host. Real training workloads and other infrastructure
require their own capacity validation.

## Correctness and changes

The original global admission transaction lock is replaced by positive live-job
slots protected by a partial unique index. Different free slots commit
independently. Terminal state changes release reservations in their existing
transaction; retries and recovery retain the slot. The key is rechecked before
rejection, preserving replay/conflict semantics at saturation. Migration 0003
backfills existing live jobs without deleting them and rejects invalid slots.
Enabled runtime pools now fail explicitly if lifecycle setup is missing.

The rebuilt candidate passed all 88 integration tests and 21 unit tests. They
cover forced slot collisions, independent concurrent commits, rollback, lowering
the limit, legacy-schema upgrade and the original crash/zombie/cancellation/
isolation cases. A full 10,000-job/eight-worker chaos pass produced 9,666 successes,
334 cancellations and 32 SIGKILL, with no final invariant violations. Its declared
capacity is 10,000 so the entire correctness manifest is admitted; deployment
and load experiments use 1,000. Computation remains outside transactions, and
owner/token/lease checks fence stale workers. State, producing attempt and one
unique database effect commit together; external exactly-once effects are outside
the guarantee.

## Capacity evidence

All measured runs retain 100 offered RPS, 10 seconds of warmup, a 60-second/
6,000-request measurement, concurrency 128 and the original SLOs. PostgreSQL
fsync, full_page_writes and synchronous_commit remain enabled.

| Selected config | HTTP p95 | Job p95 | Errors/drops | Result |
| --- | ---: | ---: | ---: | --- |
| Create repeat 1 | 60 ms | 0.182 s | 0 | PASS |
| Create repeat 2 | 61 ms | 0.186 s | 0 | PASS |
| Create repeat 3 | 51 ms | 0.183 s | 0 | PASS |
| Mixed confirmation | 46 ms | 0.198 s | 0 | PASS |

Mixed = 20% creates, 40% reads, 30% idempotent repeats, 10% cancels. Job latency
covers successful admitted measurement jobs. All create measurements admitted
and completed the entire 6,000-job measured cohort. [Exact metrics and raw output](verification/review-fixes-006/capacity-poll.json),
[protocol and acceptance criteria](harness/README.md#capacity-at-100-rps).

One earlier slot-based 1-second-poll repetition failed the queue-growth criterion
(+27 versus +20), despite zero errors and fast HTTP/job completion. It remains
FAIL. The follow-up changed actual polling configuration, not the criterion,
and then required three successful repetitions plus mixed confirmation. Earlier
HTTP-tail failures remain in [the earlier matrix](verification/review-fixes-004/capacity-matrix.json)
and [the complete run index](RUNS.md). Observed advisory-lock/WAL waits supported
profiling but did not prove a unique cause for every historical latency spike.

## Scope and remaining limits

The host has eight logical CPUs and about 15.14 GiB Docker-visible RAM, shared
with development activity. The simulation waits rather than performing real
model training. The evidence does not cover long soak periods, PostgreSQL
failover, production storage/network failure, database-clock jumps, or external
side-effect deduplication. The service binds its HTTP port to loopback and uses
documented local credentials/role bootstrap. Validate credentials, workload,
resource budget and SLOs for the actual deployment environment.

Reflection: the initial connection-per-operation implementation consumed
substantial capacity. Pooling removed measured setup overhead, while explicit
slots removed global admission serialization without weakening the bound.
The independent-commit regression initially failed because candidate order was
not explicitly specified in SQL; the corrected ORDER BY and retained failed
output show why that assumption needed a test. Smaller idle polling then reduced
queue oscillation under the same acceptance criteria. Short passing results
support this specific configuration rather than a universal production promise.
AI work and verification are attributed in [AI_USAGE.md](AI_USAGE.md); independent
candidate verification has not been recorded.
