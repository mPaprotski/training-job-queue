# Silent failures: checks and evidence

Core protocol checks below use real PostgreSQL at READ COMMITTED; the final
checker uses a separate REPEATABLE READ snapshot. Latest combined results are
indexed in [the current test output](verification/review-fixes-006/integration-tests-final.txt). Synchronization wrappers
pause around real database operations in test processes only; application code
has no test-only endpoints or bypasses. Raw test output is preserved in
`results/adversarial-tests-001.txt` (50 tests) and
`results/invariant-checker-tests-002.txt` (8 checker tests). The earlier 7-test
checker output is also preserved.

| Failure | Protection | Evidence |
| --- | --- | --- |
| Double claim | Row lock + SKIP LOCKED; one open attempt per job | Eight simultaneous claims produce one winner; a locked job is skipped |
| Zombie worker | Owner + increasing attempt token + unexpired lease | Real worker receives SIGSTOP, replacement completes after expiry, resumed worker logs ownership rejection and cannot overwrite the effect |
| Cancel/complete race | Same job lock; terminal-state checks | Gate one transaction, observe the other waiting in `pg_stat_activity`, release; test each winning order |
| Duplicate request retry | Unique key, JSONB equality, separate SELECT after conflict | Eight concurrent POSTs; conflicting payloads; raw client socket closes without reading the response, retry returns the committed job |
| Isolation-level assumptions | Explicit READ COMMITTED, locks and database uniqueness | Assert actual level; exercise concurrent inserts, claims, recovery and terminal transitions |
| Clock skew | Ownership uses PostgreSQL time after acquiring lock | Move Python wall clock a year in either direction; lease remains based on DB time; separately hold a row lock until its lease expires and verify waiting completion rejects it |
| Crash before effect | Completion is transactional | SIGKILL at a process gate before INSERT; rollback, recovery, one final effect |
| Crash after effect before commit | Effect and final state share transaction | SIGKILL after INSERT; no effect survives; recovery completes once |
| Commit succeeds but acknowledgement is lost | Terminal state is durable and checked before writes | SIGKILL immediately after real commit; replay cannot write another effect |

`tests/fixtures/completion_gate.py` wraps the real connection to expose the three
crash boundaries to the parent process; its `after_commit` gate runs only after
the real connection context has committed. Tests do not substitute an in-memory
database or pretend that a mocked commit is durable.

The clock-skew check changes process-level Python wall time, not the host/kernel
or PostgreSQL clock. It establishes that ownership operations do not depend on
that application clock. Database clock jumps, storage durability and database
failover remain outside this evidence.

## Checking the checker

`test_invariants.py` supplies intentionally inconsistent data in isolated databases.
It temporarily removes the effect primary key and request-key unique constraint
to prove duplicate detection does not merely rely on those constraints. Other fixtures cover missing jobs,
running jobs, mismatched results, JSON boolean/number differences, changed terminal
observations and a lost acknowledged cancellation. A healthy fixture must pass.

These address known schedules. They do not exhaust every interleaving or replace
the chaos and load experiments.
