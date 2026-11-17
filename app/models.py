"""Pydantic request/response models (also drive the /docs schema)."""
from __future__ import annotations

from pydantic import BaseModel, Field

from . import config


class SubmitOptions(BaseModel):
    cleanup_delay_ms: int | None = Field(
        default=None,
        ge=0,
        le=config.MAX_HOOK_DELAY_MS,
        description="Test hook: pause this many ms inside the post-job "
        "session cleanup (simulates slow cleanup). Defaults to the "
        "server-wide CLEANUP_DELAY_MS.",
    )
    break_connection_after: bool = Field(
        default=False,
        description="Test hook: close this job's connection right after the "
        "job (simulates a connection failure). The subsequent session "
        "cleanup then fails, so the pool retires the connection and builds "
        "a replacement.",
    )


class SubmitRequest(BaseModel):
    statements: list[str] = Field(
        min_length=1,
        description=f"1..{config.MAX_STATEMENTS} SQL statements, executed in "
        "order on one shared connection, each auto-committed. Explicit "
        "transaction control (BEGIN/COMMIT/ROLLBACK/SAVEPOINT/...) is "
        "rejected.",
    )
    options: SubmitOptions = SubmitOptions()


class CancelRequest(BaseModel):
    delay_ms: int | None = Field(
        default=None,
        ge=0,
        le=config.MAX_HOOK_DELAY_MS,
        description="Test hook: wait this many ms before the cancel is "
        "actually sent to PostgreSQL (simulates a slow cancel pipeline). "
        "The cancel itself is accepted immediately. Defaults to the "
        "server-wide CANCEL_SEND_DELAY_MS.",
    )


class StatementResult(BaseModel):
    index: int
    sql: str
    status: str  # pending|running|ok|error|cancelled|skipped
    started_at: str | None
    duration_ms: float | None
    columns: list[str] | None
    rows: list[list] | None
    rowcount: int | None
    truncated: bool
    error: str | None


class JobDetail(BaseModel):
    id: str
    status: str  # queued|running|succeeded|failed|cancelled
    created_at: str
    started_at: str | None
    finished_at: str | None
    connection_id: int | None
    cleanup_delay_ms: int | None
    error: str | None
    cancel_requested: bool
    cancel_accepted_at: str | None
    statements: list[StatementResult]


class JobSummary(BaseModel):
    id: str
    status: str
    created_at: str
    started_at: str | None
    finished_at: str | None
    connection_id: int | None
    num_statements: int


class SubmitResponse(BaseModel):
    job_id: str
    status: str
    queue_depth: int
    detail: str


class CancelResponse(BaseModel):
    job_id: str
    accepted: bool
    job_status: str
    detail: str


class ConnectionStatus(BaseModel):
    id: int
    state: str  # idle|busy|cleaning|reconnecting
    current_job_id: str | None
    generation: int
    jobs_executed: int
    cleanups_failed: int


class PoolStatus(BaseModel):
    size: int
    queue_depth: int
    connections: list[ConnectionStatus]


# ---- replication-slot WAL lag sampling ------------------------------------


class SlotSampleModel(BaseModel):
    taken_at: str = Field(description="ISO-8601 UTC timestamp of the probe")
    current_wal_lsn: str = Field(
        description="pg_current_wal_lsn() at probe time, canonical X/XXXXXXXX text"
    )
    slot_name: str
    slot_type: str  # physical|logical
    active: bool
    restart_lsn: str | None = Field(
        description="Slot restart LSN as text; NULL means the slot has never "
        "reserved WAL and is preserved as null (never interpreted as zero)."
    )
    wal_status: str | None = Field(
        description="reserved|extended|unreserved|lost; NULL when restart_lsn is NULL"
    )
    safe_wal_size: int | None = Field(
        description="Bytes of WAL that can still be consumed before the slot "
        "would be invalidated; NULL means no finite limit "
        "(max_slot_wal_keep_size = -1)."
    )


class SlotLagChange(BaseModel):
    retained_bytes_sample_1: int
    retained_bytes_sample_2: int
    delta_retained_bytes: int = Field(
        description="Net change of retained bytes (sample 2 minus sample 1)."
    )
    current_wal_lsn_advanced_bytes: int
    restart_lsn_advanced_bytes: int


class SlotLagEstimate(BaseModel):
    estimation: str
    caveat: str
    net_growth_bytes_per_second: float
    safe_wal_size_bytes: int
    estimated_remaining_seconds: float
    estimated_remaining_human: str


class SlotLagResponse(BaseModel):
    slot_name: str
    status: str = Field(
        description="estimated: a short-window extrapolation is included; "
        "not_estimable: see reason/detail for why no countdown is given."
    )
    reason: str | None = Field(
        description="Machine-readable reason when no estimate is possible "
        "(slot_not_found, restart_lsn_null, wal_status_unreserved, "
        "wal_status_lost, no_net_growth, no_retention_limit, ...)."
    )
    detail: str | None = None
    interval_seconds: float
    elapsed_seconds: float | None = None
    samples: list[SlotSampleModel | None]
    lag: SlotLagChange | None = None
    estimate: SlotLagEstimate | None = None
