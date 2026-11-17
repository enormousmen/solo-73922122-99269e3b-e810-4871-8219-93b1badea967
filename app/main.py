"""FastAPI application: shared diagnostic SQL service.

Endpoints
---------
POST /jobs                 submit 1..10 statements (queued, 202)
GET  /jobs                 list recent jobs
GET  /jobs/{id}            job detail incl. per-statement results
POST /jobs/{id}/cancel     cancel a queued/running job (202) ; 409 if finished
GET  /pool                 state of the two pooled connections
GET  /health               liveness
GET  /docs                 Swagger UI
"""
from __future__ import annotations

import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException
from fastapi.responses import RedirectResponse

from . import config
from .models import (
    CancelRequest,
    CancelResponse,
    JobDetail,
    JobSummary,
    PoolStatus,
    SubmitRequest,
    SubmitResponse,
)
from .pool import Pool
from .sqlguard import SqlRejected, check_statements
from .store import TERMINAL_STATES, Job, JobStore, utcnow

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
log = logging.getLogger("diag.api")

store = JobStore(capacity=config.JOB_HISTORY)
pool = Pool(store)


@asynccontextmanager
async def lifespan(_app: FastAPI):
    pool.start()
    log.info(
        "pool started: %d fixed connections, row_limit=%d",
        len(pool.connections),
        config.ROW_LIMIT,
    )
    yield
    pool.shutdown()


app = FastAPI(
    title="Shared Diagnostic SQL Service",
    version="1.0.0",
    description=(
        "Shared diagnostic SQL endpoint for on-call DB engineers. "
        "Jobs of up to 10 trusted statements run sequentially in autocommit "
        "mode on a fixed pool of two reused PostgreSQL connections. "
        "Session state (SET, temp tables, advisory locks, ...) is cleaned "
        "after every job; explicit transaction control is rejected; "
        "unfinished jobs can be cancelled."
    ),
    lifespan=lifespan,
)


@app.get("/", include_in_schema=False)
def root():
    return RedirectResponse(url="/docs")


@app.get("/health")
def health():
    return {"status": "ok"}


@app.post("/jobs", response_model=SubmitResponse, status_code=202)
def submit_job(req: SubmitRequest):
    try:
        statements = check_statements(req.statements)
    except SqlRejected as exc:
        raise HTTPException(status_code=422, detail=str(exc))
    job = Job(
        statements,
        cleanup_delay_ms=req.options.cleanup_delay_ms,
        break_connection_after=req.options.break_connection_after,
    )
    if not pool.submit(job):
        raise HTTPException(
            status_code=429,
            detail=f"queue full ({config.MAX_QUEUED_JOBS} jobs); retry later",
        )
    store.add(job)
    return SubmitResponse(
        job_id=job.id,
        status="queued",
        queue_depth=pool.job_queue.qsize(),
        detail=f"job accepted with {len(statements)} statement(s)",
    )


@app.get("/jobs", response_model=list[JobSummary])
def list_jobs():
    return store.list_summaries()


@app.get("/jobs/{job_id}", response_model=JobDetail)
def get_job(job_id: str):
    job = store.get(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail=f"unknown job {job_id}")
    return job.snapshot()


@app.post("/jobs/{job_id}/cancel", response_model=CancelResponse, status_code=202)
def cancel_job(job_id: str, req: CancelRequest | None = None):
    job = store.get(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail=f"unknown job {job_id}")

    with job.lock:
        if job.status in TERMINAL_STATES:
            # Late cancel: must be a no-op so it can never disturb the job
            # now running on the same connection.
            raise HTTPException(
                status_code=409,
                detail=f"job already {job.status}; cancel ignored",
            )
        already = job.cancel_requested.is_set()
        if not already:
            job.cancel_requested.set()
            job.cancel_accepted_at = utcnow()
        status = job.status

    if already:
        return CancelResponse(
            job_id=job.id,
            accepted=True,
            job_status=status,
            detail="cancel was already requested",
        )

    if status == "queued":
        # Never started: nothing runs on the server, nothing to clean.
        job.finish("cancelled", "cancelled while queued")
        job.cancel_send_done.set()
        return CancelResponse(
            job_id=job.id,
            accepted=True,
            job_status="cancelled",
            detail="job was queued; cancelled before any statement ran",
        )

    # Running: stop after the current statement.  The cancel send itself is
    # handed to the sender thread (with the optional test-hook delay) so the
    # API accepts the cancel immediately.
    delay_ms = (
        req.delay_ms
        if req is not None and req.delay_ms is not None
        else config.CANCEL_SEND_DELAY_MS
    )
    pool.request_cancel_send(job.id, delay_ms)
    return CancelResponse(
        job_id=job.id,
        accepted=True,
        job_status=status,
        detail="cancel accepted; running statement is interrupted and "
        "remaining statements are skipped (committed effects stay)",
    )


@app.get("/pool", response_model=PoolStatus)
def pool_status():
    return pool.status()
