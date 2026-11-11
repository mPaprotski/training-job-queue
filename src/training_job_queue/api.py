"""HTTP entry point for job lifecycle operations and health checks."""

from contextlib import asynccontextmanager
import logging
from typing import Annotated
from uuid import UUID

from fastapi import FastAPI, Header, HTTPException, Query, Request, Response
from fastapi.responses import JSONResponse
from fastapi.exceptions import RequestValidationError
import psycopg

from training_job_queue.db import ping, connection_pool
from psycopg_pool import PoolTimeout, TooManyRequests
from training_job_queue.body_limit import BodyLimitMiddleware, MAX_REQUEST_BODY_BYTES
from training_job_queue import jobs
from training_job_queue.models import CreateJob, Job, JobPage
from training_job_queue.settings import Settings

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.settings = Settings.from_env()
    with connection_pool(app.state.settings):
        yield


app = FastAPI(title="Training Job Queue", version="0.1.0", lifespan=lifespan,
              responses={413: {"description": f"Request body exceeds {MAX_REQUEST_BODY_BYTES} bytes"}})
app.add_middleware(BodyLimitMiddleware)


@app.exception_handler(RequestValidationError)
async def invalid_request(request: Request, error: RequestValidationError):
    # Do not echo malformed/nonfinite input into JSON error responses.
    details = [{"loc": item["loc"], "msg": item["msg"], "type": item["type"]} for item in error.errors()]
    return JSONResponse(status_code=422, content={"detail": details})


@app.exception_handler(jobs.JobNotFound)
async def missing_job(request: Request, error: jobs.JobNotFound):
    return JSONResponse(status_code=404, content={"detail": "Job not found"})


@app.exception_handler(jobs.JobConflict)
async def conflicting_job(request: Request, error: jobs.JobConflict):
    return JSONResponse(status_code=409, content={"detail": str(error)})


@app.exception_handler(psycopg.OperationalError)
@app.exception_handler(PoolTimeout)
@app.exception_handler(TooManyRequests)
@app.exception_handler(psycopg.InterfaceError)
@app.exception_handler(psycopg.errors.LockNotAvailable)
@app.exception_handler(psycopg.errors.QueryCanceled)
async def database_unavailable(request: Request, error: Exception):
    logger.warning("Database operation unavailable: %s", type(error).__name__)
    return JSONResponse(status_code=503, content={"detail": "Database temporarily unavailable"})


@app.exception_handler(jobs.QueueFull)
async def queue_full(request: Request, error: jobs.QueueFull):
    return JSONResponse(status_code=503, headers={"Retry-After": "1"}, content={
        "detail": "Queue capacity reached", "code": "queue_full"})


@app.post("/jobs", response_model=Job, status_code=201, responses={200: {"model": Job}, 409: {"description": "Key conflict"},
          503: {"description": "Queue full or database temporarily unavailable"}})
def create_job(
    body: CreateJob,
    request: Request,
    response: Response,
    idempotency_key: Annotated[str, Header(alias="Idempotency-Key", min_length=1, max_length=255)],
):
    if not idempotency_key.strip():
        raise HTTPException(status_code=422, detail="Idempotency-Key cannot be blank")
    try:
        job, created = jobs.create_job(request.app.state.settings, idempotency_key, body.payload)
    except (psycopg.errors.NumericValueOutOfRange, psycopg.errors.UntranslatableCharacter):
        raise HTTPException(status_code=422, detail="Payload cannot be represented in PostgreSQL JSONB") from None
    response.status_code = 201 if created else 200
    response.headers["Location"] = f"/jobs/{job['id']}"
    return job


@app.get("/jobs", response_model=JobPage)
def list_jobs(
    request: Request,
    limit: Annotated[int, Query(ge=1, le=100)] = 50,
    cursor: Annotated[str | None, Query(min_length=1, max_length=512)] = None,
):
    try:
        after = jobs.decode_cursor(cursor) if cursor is not None else None
    except ValueError:
        raise HTTPException(status_code=422, detail="Invalid pagination cursor") from None
    return jobs.list_jobs(request.app.state.settings, limit, after)


@app.get("/jobs/{job_id}", response_model=Job)
def get_job(job_id: UUID, request: Request):
    return jobs.get_job(request.app.state.settings, job_id)


@app.post("/jobs/{job_id}/cancel", response_model=Job)
def cancel_job(job_id: UUID, request: Request):
    return jobs.cancel_job(request.app.state.settings, job_id)


@app.get("/health/live")
def live() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/health/ready")
def ready(request: Request) -> dict[str, str]:
    try:
        ping(request.app.state.settings)
    except psycopg.Error:
        logger.warning("Database readiness check failed")
        raise HTTPException(status_code=503, detail="Database unavailable") from None
    return {"status": "ok", "database": "ok"}
