"""Fixed-size connection pool: two worker threads, each owning one
long-lived PostgreSQL connection in autocommit mode.

Design notes
------------
* Connections are created once at startup and reused for every job
  (no per-job reconnect).  A connection is only ever replaced when the
  post-job session cleanup fails (or the connection itself broke): the old
  one is discarded and a fresh one is built in its place.
* Cancel: the API marks ``job.cancel_requested`` and hands the actual
  cancel send to a dedicated sender thread (optionally after a test-hook
  delay).  The sender only transmits ``conn.cancel()`` while the job is
  still the connection's current job and not yet terminal -- a late cancel
  can therefore never hit the *next* job on the same connection.
* A connection is returned to the idle pool only after SQL execution has
  stopped, the cancel send (if any) has completed, and session cleanup has
  finished.
"""
from __future__ import annotations

import logging
import queue
import threading
import time
from datetime import date, datetime, time as dtime
from decimal import Decimal

import psycopg
from psycopg.errors import QueryCanceled

from . import config
from .store import Job, JobStore

log = logging.getLogger("diag.pool")

# Session cleanup executed after every job (success, failure or cancel).
# Together these reset SET/ROLE state, prepared statements, sequence
# currval state, temp tables/views/sequences and session advisory locks.
CLEANUP_SQL = [
    "RESET ALL",
    "RESET ROLE",
    "RESET SESSION AUTHORIZATION",
    "DEALLOCATE ALL",
    "DISCARD SEQUENCES",
    # drop every temp object owned by this session
    """DO $$
DECLARE r record;
BEGIN
  FOR r IN SELECT c.relname, c.relkind FROM pg_class c
           WHERE c.relnamespace = pg_my_temp_schema()
             AND c.relkind IN ('r','v','m','S')
  LOOP
    EXECUTE format('DROP %s IF EXISTS %I CASCADE',
        CASE r.relkind WHEN 'r' THEN 'TABLE'
                       WHEN 'v' THEN 'VIEW'
                       WHEN 'm' THEN 'MATERIALIZED VIEW'
                       ELSE 'SEQUENCE' END,
        r.relname);
  END LOOP;
END $$""",
    "SELECT pg_advisory_unlock_all()",
]


def _elapsed_ms(t0: float) -> float:
    return round((time.monotonic() - t0) * 1000.0, 3)


def _err_text(exc: BaseException) -> str:
    text = str(exc).strip()
    return f"{type(exc).__name__}: {text}" if text else type(exc).__name__


def _jsonable(value):
    """Convert a psycopg result value into something JSON-serializable."""
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    if isinstance(value, (bytes, bytearray, memoryview)):
        return "\\x" + bytes(value).hex()
    if isinstance(value, (datetime, date, dtime)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, (list, tuple)):
        return [_jsonable(v) for v in value]
    if isinstance(value, dict):
        return {str(k): _jsonable(v) for k, v in value.items()}
    return str(value)


class ConnectionInfo:
    """Book-keeping for one pooled connection (guarded by ``lock``)."""

    def __init__(self, cid: int):
        self.id = cid
        self.lock = threading.Lock()
        self.conn: psycopg.Connection | None = None
        self.state = "connecting"  # idle|busy|cleaning|reconnecting
        self.current_job_id: str | None = None
        self.generation = 0  # +1 every time the connection is rebuilt
        self.jobs_executed = 0
        self.cleanups_failed = 0

    def set_state(self, state: str, current_job_id: str | None = None) -> None:
        with self.lock:
            self.state = state
            self.current_job_id = current_job_id

    def snapshot(self) -> dict:
        with self.lock:
            return {
                "id": self.id,
                "state": self.state,
                "current_job_id": self.current_job_id,
                "generation": self.generation,
                "jobs_executed": self.jobs_executed,
                "cleanups_failed": self.cleanups_failed,
            }


class Pool:
    def __init__(self, store: JobStore):
        self.store = store
        self.job_queue: queue.Queue[Job | None] = queue.Queue()
        self.cancel_queue: queue.Queue[tuple[str, int] | None] = queue.Queue()
        self.stopping = threading.Event()
        self.connections = [ConnectionInfo(i) for i in range(config.POOL_SIZE)]
        self.workers = [_Worker(self, cinfo) for cinfo in self.connections]
        self.cancel_sender = _CancelSender(self)

    # ---- lifecycle ---------------------------------------------------------

    def start(self) -> None:
        self.cancel_sender.start()
        for worker in self.workers:
            worker.start()

    def shutdown(self) -> None:
        self.stopping.set()
        self.cancel_queue.put(None)
        for _ in self.workers:
            self.job_queue.put(None)
        for worker in self.workers:
            worker.join(timeout=5)
        self.cancel_sender.join(timeout=5)
        for cinfo in self.connections:
            try:
                if cinfo.conn is not None:
                    cinfo.conn.close()
            except Exception:
                pass

    # ---- submission / cancel -------------------------------------------------

    def submit(self, job: Job) -> bool:
        if config.MAX_QUEUED_JOBS and self.job_queue.qsize() >= config.MAX_QUEUED_JOBS:
            return False
        self.job_queue.put(job)
        return True

    def request_cancel_send(self, job_id: str, delay_ms: int) -> None:
        self.cancel_queue.put((job_id, delay_ms))

    # ---- connection (re)build -------------------------------------------------

    def connect_with_retry(self, cinfo: ConnectionInfo) -> psycopg.Connection:
        attempt = 0
        while not self.stopping.is_set():
            try:
                conn = psycopg.connect(
                    config.DATABASE_URL,
                    autocommit=True,
                    connect_timeout=config.CONNECT_TIMEOUT_S,
                    # Never use server-side prepared statements for our own
                    # queries: cleanup runs DEALLOCATE ALL, which would
                    # invalidate psycopg's client-side prepare cache
                    # ("prepared statement _pg3_0 does not exist").
                    prepare_threshold=None,
                )
                conn.execute("SELECT 1")
                return conn
            except Exception:
                attempt += 1
                wait = min(5.0, 0.5 * attempt)
                log.warning(
                    "connection %d: connect attempt %d failed, retry in %.1fs",
                    cinfo.id,
                    attempt,
                    wait,
                    exc_info=True,
                )
                time.sleep(wait)
        raise RuntimeError("pool is stopping")

    # ---- introspection ---------------------------------------------------------

    def status(self) -> dict:
        return {
            "size": len(self.connections),
            "queue_depth": self.job_queue.qsize(),
            "connections": [c.snapshot() for c in self.connections],
        }


class _Worker(threading.Thread):
    """One worker per pooled connection; jobs are pulled from the shared queue."""

    def __init__(self, pool: Pool, cinfo: ConnectionInfo):
        super().__init__(name=f"db-worker-{cinfo.id}", daemon=True)
        self.pool = pool
        self.cinfo = cinfo

    # -- main loop -------------------------------------------------------------

    def run(self) -> None:
        cinfo = self.cinfo
        try:
            cinfo.conn = self.pool.connect_with_retry(cinfo)
        except RuntimeError:
            return
        cinfo.set_state("idle")
        log.info("connection %d ready (generation %d)", cinfo.id, cinfo.generation)
        while True:
            job = self.pool.job_queue.get()
            try:
                if job is None:  # shutdown sentinel
                    return
                self._run_job(job)
            except Exception:
                log.exception("worker %d: unexpected error", cinfo.id)
            finally:
                self.pool.job_queue.task_done()

    # -- one job -----------------------------------------------------------------

    def _run_job(self, job: Job) -> None:
        cinfo = self.cinfo
        if job.is_terminal():
            return  # cancelled while still queued; connection untouched
        cinfo.set_state("busy", current_job_id=job.id)
        if not job.mark_running(cinfo.id):
            cinfo.set_state("idle")
            return  # lost the race with a queued-cancel
        try:
            self._execute_statements(job)
        finally:
            # The connection is returned only after (1) SQL execution has
            # stopped, (2) any accepted cancel has been fully sent, and
            # (3) session cleanup has completed.
            if job.cancel_requested.is_set():
                if not job.cancel_send_done.wait(timeout=config.CANCEL_SEND_WAIT_S):
                    log.error("job %s: cancel send did not finish in time", job.id)
            cinfo.set_state("cleaning")
            try:
                self._cleanup(job)
            except Exception:
                log.exception(
                    "connection %d: session cleanup failed; retiring connection",
                    cinfo.id,
                )
                with cinfo.lock:
                    cinfo.cleanups_failed += 1
                self._rebuild()
            with cinfo.lock:
                cinfo.jobs_executed += 1
            cinfo.set_state("idle")
            job.done.set()

    def _execute_statements(self, job: Job) -> None:
        conn = self.cinfo.conn
        assert conn is not None
        for rec in job.records:
            idx = rec["index"]
            if job.cancel_requested.is_set():
                job.skip_from(idx)
                job.finish("cancelled", "cancel accepted; remaining statements skipped")
                return
            job.stmt_running(idx)
            t0 = time.monotonic()
            try:
                with conn.cursor() as cur:
                    cur.execute(rec["sql"])
                    if cur.description is not None:
                        columns = [d.name for d in cur.description]
                        rows = cur.fetchmany(config.ROW_LIMIT + 1)
                        truncated = len(rows) > config.ROW_LIMIT
                        rows = rows[: config.ROW_LIMIT]
                        data = [[_jsonable(v) for v in row] for row in rows]
                        rowcount = cur.rowcount if cur.rowcount is not None else None
                    else:
                        columns = None
                        data = None
                        truncated = False
                        rowcount = (
                            cur.rowcount
                            if cur.rowcount is not None and cur.rowcount >= 0
                            else None
                        )
                job.stmt_ok(idx, columns, data, rowcount, truncated, _elapsed_ms(t0))
            except QueryCanceled as exc:
                job.stmt_cancelled(idx, _err_text(exc), _elapsed_ms(t0))
                job.skip_from(idx + 1)
                job.finish("cancelled", "running statement cancelled by request")
                return
            except Exception as exc:
                job.stmt_error(idx, _err_text(exc), _elapsed_ms(t0))
                job.skip_from(idx + 1)
                job.finish("failed", _err_text(exc))
                return
        job.finish("succeeded")

    # -- session cleanup -----------------------------------------------------------

    def _cleanup(self, job: Job) -> None:
        if job.break_connection_after:
            # Test hook: simulate a connection failure right after the job.
            # The cleanup below then fails and the pool retires the
            # connection and builds a replacement.
            log.warning(
                "connection %d: break_connection_after hook for job %s",
                self.cinfo.id,
                job.id,
            )
            try:
                self.cinfo.conn.close()
            except Exception:
                pass
        delay_ms = (
            job.cleanup_delay_ms
            if job.cleanup_delay_ms is not None
            else config.CLEANUP_DELAY_MS
        )
        if delay_ms:
            log.info(
                "connection %d: cleanup delay hook %d ms", self.cinfo.id, delay_ms
            )
            time.sleep(delay_ms / 1000.0)
        conn = self.cinfo.conn
        assert conn is not None
        for sql in CLEANUP_SQL:
            conn.execute(sql)

    def _rebuild(self) -> None:
        cinfo = self.cinfo
        cinfo.set_state("reconnecting")
        try:
            if cinfo.conn is not None:
                cinfo.conn.close()
        except Exception:
            pass
        conn = self.pool.connect_with_retry(cinfo)
        with cinfo.lock:
            cinfo.conn = conn
            cinfo.generation += 1
        log.info(
            "connection %d rebuilt (generation %d)", cinfo.id, cinfo.generation
        )


class _CancelSender(threading.Thread):
    """Serializes cancel sends; supports the configurable send-delay hook."""

    def __init__(self, pool: Pool):
        super().__init__(name="cancel-sender", daemon=True)
        self.pool = pool

    def run(self) -> None:
        while True:
            item = self.pool.cancel_queue.get()
            if item is None:
                return
            job_id, delay_ms = item
            job = self.pool.store.get(job_id)
            try:
                if delay_ms:
                    time.sleep(delay_ms / 1000.0)
                self._send(job_id)
            except Exception:
                log.exception("cancel sender: error for job %s", job_id)
            finally:
                if job is not None:
                    job.cancel_send_done.set()
                self.pool.cancel_queue.task_done()

    def _send(self, job_id: str) -> None:
        job = self.pool.store.get(job_id)
        if job is None:
            return
        with job.lock:
            conn_id = job.connection_id
        if conn_id is None:
            return
        cinfo = self.pool.connections[conn_id]
        with cinfo.lock:
            # Guard against the late-cancel race: only transmit while this
            # job is still the connection's current, non-terminal job.
            if cinfo.current_job_id != job_id or job.is_terminal():
                log.info(
                    "cancel for job %s not sent: job no longer current "
                    "(late cancel ignored, next job unaffected)",
                    job_id,
                )
                return
            if cinfo.conn is None:
                return
            log.info("sending cancel for job %s on connection %d", job_id, conn_id)
            cinfo.conn.cancel()
