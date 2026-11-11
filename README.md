# Training Job Queue

A FastAPI/PostgreSQL service that queues simulated training jobs, executes them
with independent workers, exposes status/pagination/cancellation and records one
committed database effect per successful job.

## Deliverables

| Brief requirement | Evidence |
| --- | --- |
| Design, database choice and guarantees | [DESIGN.md](DESIGN.md), [original pre-code draft](docs/DESIGN_INITIAL.md) |
| Chaos harness, reproduction and blind spots | [harness/README.md](harness/README.md) |
| Every chaos/load run, including failures | [RUNS.md](RUNS.md), [parameters/hashes](RUNS.json) |
| Silent failures and controlled checks | [SILENT_FAILURES.md](SILENT_FAILURES.md) |
| 100-RPS verdict, measurements and reflection | [VERDICT.md](VERDICT.md) |
| AI usage, accepted/changed suggestions and verification | [AI_USAGE.md](AI_USAGE.md) |

## Start

Requires Docker Engine and Docker Compose v2:

```sh
docker compose up --build
curl --fail http://localhost:8000/health/ready
docker compose ps
```

For a fresh volume, Compose initializes PostgreSQL roles, applies three
checksummed migrations, then starts the API and **16 workers**. An exited
migration container with code 0 is expected. API access is loopback-only on port
8000; PostgreSQL is available inside the Compose network. OpenAPI: `/docs`.
`/health/live` checks the process; `/health/ready` checks database access.

```sh
curl -i http://localhost:8000/jobs \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: example-001' \
  -d '{"payload":{"model":"demo","epochs":3}}'
```

Stop with `docker compose down`; the named database volume is retained. Build
again after source changes. Example environment overrides: [.env.example](.env.example).

## API

| Operation | Behavior |
| --- | --- |
| `POST /jobs`, body `{"payload": {...}}`, `Idempotency-Key` header | 201 new; 200 same key/equivalent JSONB payload; 409 conflicting payload |
| `GET /jobs/{id}` | Current job, or 404; malformed UUID returns 422 |
| `GET /jobs?limit=50&cursor=...` | Limit 1–100; opaque next cursor; stable `(created_at,id)` order |
| `POST /jobs/{id}/cancel` | 200 queued/running/already-cancelled; 409 succeeded/failed; 404 unknown |

Pagination is a live traversal rather than a frozen snapshot. Terminal states
are immutable through the API. Reuse the same key after an uncertain POST
response; idempotency keys do not expire. Key length is 1–255, not whitespace-only.
Extra top-level body fields, invalid/nonfinite JSON and NUL characters are rejected.
Bodies exceeding 65,536 bytes return 413 before JSON parsing, including chunked input.

At most 1,000 jobs are queued/running by default. New admission without available
capacity returns 503/`queue_full` and `Retry-After: 1`, without reserving a key.
Existing-key replay/conflict semantics remain intact at saturation. All service
instances must share the capacity configuration. A lowered limit drains existing
out-of-range live slots before new admission resumes. Transient database/pool
failures return 503; retry against the same key/ID because commit may be uncertain.

Optional `payload.simulation` fields: `duration_seconds` 0–300 (omitted/null uses
configured default), `fail_until_attempt` integer 0–1000, `permanent_failure`
boolean. Unknown simulation fields/types return 422. Duration applies to each
attempt; recoverable failures retry within the stored budget. The durable effect
is a PostgreSQL row, not an external action or real model training.

## Configuration

| Setting | Compose default |
| --- | --- |
| `API_PORT` / `WORKER_REPLICAS` | 8000 / 16 |
| `LEASE_SECONDS` / `HEARTBEAT_SECONDS` | 15 / 3 |
| `POLL_SECONDS` / `SIMULATION_SECONDS` | 0.1 / 0.1 |
| `MAX_ATTEMPTS` / `MAX_OUTSTANDING_JOBS` | 5 / 1000 |
| `DB_POOL_ENABLED` / `DB_POOL_SIZE` / `DB_POOL_TIMEOUT` | 1 / 4 / 1 second |

API owns at most four connections; each serial worker owns one. Enabled runtime
pooling requires lifecycle setup and fails explicitly if absent. Administrative
connections are raw; raw runtime mode is an explicit experimental control.
Standalone workers retain a 1-second poll fallback unless the variable is set.

PostgreSQL roles: `queue_admin` initializes the cluster; `queue_migrator` owns
schema/migrations; `queue_runtime` has working-table DML without schema/cluster
administration; `queue_tools` has CREATEDB for temporary test databases. The
bundled passwords are local-development defaults. Migrations are forward-only
SQL files, applied transactionally with checksum validation; do not edit an
already-applied file. Schema and locking guarantees are explained in DESIGN.md.

## Upgrade an existing volume

Initialization scripts do not rerun on existing volumes. Back up data and stop
writers before role or slot-schema upgrades. For the original installation whose
administrator is `queue`, provision the roles first:

```sh
docker compose stop api worker
docker compose exec -T db psql -v ON_ERROR_STOP=1 -U queue -d queue \
  < docker/postgres/001-roles.sql
```

Skip that provisioning command if roles are already configured. Then:

```sh
docker compose build
docker compose run --rm --no-deps migrate
docker compose up -d --no-build api worker
```

Provisioning transfers only known application objects, without deleting jobs.
Migration 0003 backfills live-job slots and adds their constraints. A fresh
installation uses administrator `queue_admin`; existing original volumes retain
administrator `queue`. Never reset a volume as an upgrade procedure.

## Tests and recorded evidence

With the database initialized and the image built:

```sh
docker compose --profile tools run --rm --no-deps -v "$PWD/tests:/app/tests:ro" chaos \
  python -m unittest discover -s tests/integration -v
docker compose --profile tools run --rm --no-deps -v "$PWD/tests:/app/tests:ro" chaos \
  python -m unittest discover -s tests/unit -v
```

Current image: **88 integration tests and 21 unit tests passed**.
[Integration output](verification/review-fixes-006/integration-tests-final.txt),
[unit output](verification/review-fixes-006/unit-tests-final.txt),
[configured API smoke](verification/review-fixes-006/final-api-smoke.json).
Fixtures use isolated temporary databases. Earlier clean-clone checks refer to
their recorded versions; their raw evidence remains in `verification/`.

Every raw chaos/load run is linked from RUNS.md. `results/` contains original
experiments; `verification/` contains subsequent checks and experiments. Failed
runs remain FAIL. Markdown was consolidated after measurement; historical hash
manifests may mention removed explanatory documents, while raw output and data
remain unchanged. No source, SQL or test behavior was changed by that cleanup.
