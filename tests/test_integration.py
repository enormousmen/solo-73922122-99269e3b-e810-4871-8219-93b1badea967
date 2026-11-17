"""Integration tests: require a running API (API_BASE) and, for the
out-of-band connection-kill test, direct DB access (TEST_DATABASE_URL).

Environment note: PostgreSQL query cancellation is delivered via SIGINT to
the backend.  Sandboxes that block signal delivery cannot interrupt a
running statement; the tests below are written to pass in both worlds
(they assert the cooperative cancel semantics, which hold either way).

Run:
    API_BASE=http://127.0.0.1:8000 \
    TEST_DATABASE_URL=postgresql://postgres@/postgres?host=/tmp/diag-pgdata \
    pytest tests/test_integration.py -v
"""
from __future__ import annotations

import os
import time
from datetime import datetime

import httpx
import pytest

API_BASE = os.environ.get("API_BASE", "http://127.0.0.1:8000")
TEST_DSN = os.environ.get("TEST_DATABASE_URL")


def _api_up() -> bool:
    try:
        return httpx.get(f"{API_BASE}/health", timeout=2).status_code == 200
    except Exception:
        return False


pytestmark = pytest.mark.skipif(not _api_up(), reason="API not reachable")


@pytest.fixture()
def client():
    with httpx.Client(base_url=API_BASE, timeout=30) as c:
        yield c


# ---------------------------------------------------------------- helpers

def submit(client, statements, cleanup_delay_ms=None, break_connection_after=False):
    body = {"statements": statements}
    options = {}
    if cleanup_delay_ms is not None:
        options["cleanup_delay_ms"] = cleanup_delay_ms
    if break_connection_after:
        options["break_connection_after"] = True
    if options:
        body["options"] = options
    r = client.post("/jobs", json=body)
    assert r.status_code == 202, r.text
    return r.json()["job_id"]


def get_job(client, job_id):
    r = client.get(f"/jobs/{job_id}")
    assert r.status_code == 200, r.text
    return r.json()


def wait_job(client, job_id, timeout=40.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        job = get_job(client, job_id)
        if job["status"] in ("succeeded", "failed", "cancelled"):
            return job
        time.sleep(0.15)
    raise AssertionError(
        f"job {job_id} did not finish in {timeout}s: {get_job(client, job_id)}"
    )


def cancel(client, job_id, delay_ms=None):
    body = {} if delay_ms is None else {"delay_ms": delay_ms}
    return client.post(f"/jobs/{job_id}/cancel", json=body)


def pool(client):
    r = client.get("/pool")
    assert r.status_code == 200
    return r.json()


def wait_pool_idle(client, timeout=60.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        p = pool(client)
        if p["queue_depth"] == 0 and all(
            c["state"] == "idle" for c in p["connections"]
        ):
            return p
        time.sleep(0.2)
    raise AssertionError(f"pool did not become idle: {pool(client)}")


def occupy_one_conn(client, seconds=15):
    """Occupy one pooled connection for up to `seconds` seconds.

    Uses several short pg_sleep statements so a cancel stops the job
    cooperatively between statements even where signal delivery (and
    therefore mid-statement interruption) is unavailable.
    """
    per = max(0.5, seconds / 10.0)
    sid = submit(client, [f"SELECT pg_sleep({per})"] * 10)
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        job = get_job(client, sid)
        if job["status"] == "running" and job["connection_id"] is not None:
            return sid, job["connection_id"]
        time.sleep(0.1)
    raise AssertionError("sleeper did not start")


def ts(iso: str) -> datetime:
    return datetime.fromisoformat(iso)


# ---------------------------------------------------------------- tests

class TestBasics:
    def test_sequential_autocommit_and_per_statement_results(self, client):
        job_id = submit(
            client,
            [
                "SELECT 1 AS one",
                "CREATE TEMP TABLE t_seq (a int)",
                "INSERT INTO t_seq VALUES (1), (2) RETURNING a",
                "UPDATE t_seq SET a = a + 10",
                "SELECT count(*) AS n FROM t_seq",
            ],
        )
        job = wait_job(client, job_id)
        assert job["status"] == "succeeded"
        stmts = job["statements"]
        assert [s["status"] for s in stmts] == ["ok"] * 5
        assert stmts[0]["columns"] == ["one"]
        assert stmts[0]["rows"] == [[1]]
        assert stmts[2]["rows"] == [[1], [2]]  # INSERT ... RETURNING
        assert stmts[3]["rowcount"] == 2  # DML rowcount
        # statement 5 sees the committed effects of 2-4 (same connection)
        assert stmts[4]["rows"] == [[2]]
        for s in stmts:
            assert s["duration_ms"] is not None

    def test_row_limit_truncation(self, client):
        job_id = submit(client, ["SELECT generate_series(1, 500) AS g"])
        job = wait_job(client, job_id)
        assert job["status"] == "succeeded"
        s0 = job["statements"][0]
        assert s0["truncated"] is True
        assert len(s0["rows"]) == 200  # default ROW_LIMIT
        assert s0["rowcount"] == 500

    def test_failed_statement_stops_job(self, client):
        job_id = submit(
            client,
            ["SELECT 1", "SELECT * FROM no_such_table_xyz", "SELECT 3"],
        )
        job = wait_job(client, job_id)
        assert job["status"] == "failed"
        stmts = job["statements"]
        assert stmts[0]["status"] == "ok"
        assert stmts[1]["status"] == "error"
        assert "no_such_table_xyz" in (stmts[1]["error"] or "")
        assert stmts[2]["status"] == "skipped"

    def test_validation_rejected(self, client):
        for body, code in [
            ({"statements": ["BEGIN"]}, 422),
            ({"statements": ["SELECT 1", "COMMIT"]}, 422),
            ({"statements": ["SAVEPOINT s"]}, 422),
            ({"statements": ["SET TRANSACTION READ ONLY"]}, 422),
            ({"statements": ["SELECT 1; SELECT 2"]}, 422),
            ({"statements": []}, 422),
            ({"statements": ["SELECT 1"] * 11}, 422),
            ({"statements": ["-- only a comment"]}, 422),
        ]:
            r = client.post("/jobs", json=body)
            assert r.status_code == code, (body, r.status_code, r.text)

    def test_unknown_job(self, client):
        assert client.get("/jobs/doesnotexist").status_code == 404
        assert client.post("/jobs/doesnotexist/cancel", json={}).status_code == 404


class TestSessionCleanup:
    def test_session_state_does_not_leak(self, client):
        sleeper, busy_conn = occupy_one_conn(client, seconds=15)
        try:
            # job1 runs on the *other* connection and dirties the session
            job1 = submit(
                client,
                [
                    "SET application_name = 'leaked_app'",
                    "SET work_mem = '31MB'",
                    "CREATE TEMP TABLE leak_t (x int)",
                    "INSERT INTO leak_t VALUES (1)",
                    "SELECT pg_advisory_lock(424242)",
                    "PREPARE leak_plan AS SELECT 1",
                ],
            )
            j1 = wait_job(client, job1)
            assert j1["status"] == "succeeded"
            conn_id = j1["connection_id"]
            assert conn_id != busy_conn

            # job2 lands on the same connection (the other one is busy)
            job2 = submit(
                client,
                [
                    "SHOW application_name",
                    "SELECT count(*) AS n FROM pg_class "
                    "WHERE relnamespace = pg_my_temp_schema()",
                    "SELECT pg_try_advisory_lock(424242) AS got",
                    "SELECT count(*) AS n FROM pg_prepared_statements",
                    "SHOW work_mem",
                ],
            )
            j2 = wait_job(client, job2)
            assert j2["status"] == "succeeded"
            assert j2["connection_id"] == conn_id  # same connection, clean session
            s = j2["statements"]
            assert s[0]["rows"][0][0] != "leaked_app", "SET leaked to next job"
            assert s[1]["rows"][0][0] == 0, "temp table leaked to next job"
            assert s[2]["rows"][0][0] is True, "advisory lock leaked to next job"
            assert s[3]["rows"][0][0] == 0, "prepared statement leaked to next job"
            assert s[4]["rows"][0][0] != "31MB", "SET work_mem leaked"

            # also after a FAILED job the session must be cleaned
            job3 = submit(client, ["SET application_name = 'leak2'", "SELECT 1 / 0"])
            j3 = wait_job(client, job3)
            assert j3["status"] == "failed"
            assert j3["connection_id"] == conn_id
            job4 = submit(client, ["SHOW application_name"])
            j4 = wait_job(client, job4)
            assert j4["connection_id"] == conn_id
            assert j4["statements"][0]["rows"][0][0] != "leak2"
        finally:
            cancel(client, sleeper)
            wait_pool_idle(client)


class TestCancel:
    def test_cancel_running_job(self, client):
        sleeper, busy_conn = occupy_one_conn(client, seconds=15)
        try:
            job_id = submit(
                client,
                [
                    "CREATE TABLE IF NOT EXISTS public.cancel_keep (id int)",
                    "INSERT INTO public.cancel_keep VALUES (1)",
                    "SELECT pg_sleep(2)",
                    "SELECT 'never runs'",
                ],
            )
            # wait until statement 3 is actually running
            deadline = time.monotonic() + 10
            while True:
                j = get_job(client, job_id)
                if j["status"] == "running" and j["statements"][2]["status"] == "running":
                    break
                assert time.monotonic() < deadline, j
                time.sleep(0.1)
            t0 = time.monotonic()
            r = cancel(client, job_id)
            accept_s = time.monotonic() - t0
            assert r.status_code == 202, r.text
            assert accept_s < 2.0, "cancel must be accepted promptly"

            job = wait_job(client, job_id)
            assert job["status"] == "cancelled"
            stmts = job["statements"]
            assert stmts[0]["status"] == "ok"  # committed before cancel
            assert stmts[1]["status"] == "ok"  # committed before cancel
            # interrupted mid-flight where signals work, else runs out and
            # the cancel stops the job before the next statement:
            assert stmts[2]["status"] in ("cancelled", "ok")
            assert stmts[3]["status"] == "skipped"
            # committed effects survive the cancel
            check = wait_job(
                client, submit(client, ["SELECT count(*) AS n FROM public.cancel_keep"])
            )
            assert check["statements"][0]["rows"] == [[1]]
            # connection is usable again afterwards
            again = wait_job(client, submit(client, ["SELECT 1"]))
            assert again["status"] == "succeeded"
        finally:
            cancel(client, sleeper)
            wait_pool_idle(client)
            wait_job(client, submit(client, ["DROP TABLE IF EXISTS public.cancel_keep"]))

    def test_cancel_queued_job(self, client):
        s1, _ = occupy_one_conn(client, seconds=15)
        s2, _ = occupy_one_conn(client, seconds=15)
        try:
            job_id = submit(client, ["CREATE TABLE public.should_not_exist_xyz (id int)"])
            time.sleep(0.5)
            j = get_job(client, job_id)
            assert j["status"] == "queued"
            assert pool(client)["queue_depth"] >= 1
            r = cancel(client, job_id)
            assert r.status_code == 202
            job = wait_job(client, job_id)
            assert job["status"] == "cancelled"
            assert job["statements"][0]["status"] in ("pending", "skipped")
        finally:
            cancel(client, s1)
            cancel(client, s2)
            wait_pool_idle(client)
        # the cancelled queued job must never have executed
        check = wait_job(
            client,
            submit(client, ["SELECT to_regclass('public.should_not_exist_xyz') AS t"]),
        )
        assert check["statements"][0]["rows"] == [[None]]

    def test_late_cancel_after_finish_is_noop(self, client):
        job_id = submit(client, ["SELECT 1"])
        job = wait_job(client, job_id)
        assert job["status"] == "succeeded"
        r = cancel(client, job_id)
        assert r.status_code == 409

    def test_delayed_cancel_send_does_not_hit_next_job(self, client):
        """Cancel send delayed beyond the natural end of the job: the stray
        cancel must not stop the next job on the same connection, and the
        connection is handed over only after the cancel send resolved."""
        sleeper, busy_conn = occupy_one_conn(client, seconds=15)
        try:
            job_a = submit(client, ["SELECT pg_sleep(1.5)"])
            # cancel immediately, but the send is delayed 4s (test hook):
            # the job finishes naturally long before the cancel is sent.
            r = cancel(client, job_a, delay_ms=4000)
            assert r.status_code == 202
            ja = wait_job(client, job_a, timeout=30)
            # all statements completed before the cancel could act
            assert ja["status"] == "succeeded"
            assert ja["statements"][0]["status"] == "ok"
            conn_id = ja["connection_id"]
            # next job on the same connection must not be disturbed
            job_b = submit(client, ["SELECT pg_sleep(0.5)", "SELECT 2 AS two"])
            jb = wait_job(client, job_b, timeout=30)
            assert jb["connection_id"] == conn_id
            assert jb["status"] == "succeeded", jb
            assert jb["statements"][1]["rows"] == [[2]]
            # the connection was held until the delayed cancel send resolved
            # (accept+4s), only then could B start
            held = (ts(jb["started_at"]) - ts(ja["finished_at"])).total_seconds()
            assert held >= 2.0, f"connection returned before cancel send done: {held}s"
        finally:
            cancel(client, sleeper)
            wait_pool_idle(client)


class TestNonBlocking:
    def test_slow_job_does_not_block_other_connection(self, client):
        slow = submit(client, ["SELECT pg_sleep(3)", "SELECT 2"])
        try:
            t0 = time.monotonic()
            fast = submit(client, ["SELECT 1"])
            jf = wait_job(client, fast, timeout=5)
            assert jf["status"] == "succeeded"
            assert time.monotonic() - t0 < 5.0
            # status queries respond while the slow job runs
            t0 = time.monotonic()
            j = get_job(client, slow)
            assert j["status"] == "running"
            assert time.monotonic() - t0 < 2.0
            # cancel with a slow send (2s hook) is still accepted promptly
            t0 = time.monotonic()
            r = cancel(client, slow, delay_ms=2000)
            assert r.status_code == 202
            assert time.monotonic() - t0 < 1.5
            js = wait_job(client, slow, timeout=30)
            assert js["status"] == "cancelled"
            assert js["statements"][1]["status"] == "skipped"
        finally:
            cancel(client, slow)
            wait_pool_idle(client)

    def test_slow_cleanup_does_not_block_other_connection(self, client):
        sleeper, busy_conn = occupy_one_conn(client, seconds=15)
        try:
            # job with 4s cleanup delay on the free connection
            job_c = submit(client, ["SELECT 1"], cleanup_delay_ms=4000)
            jc = wait_job(client, job_c)
            assert jc["status"] == "succeeded"
            conn_id = jc["connection_id"]
            # while that connection is cleaning, the pool shows it ...
            time.sleep(0.3)
            states = {c["id"]: c["state"] for c in pool(client)["connections"]}
            assert states[conn_id] == "cleaning", states
            # ... and the next job for it only starts after cleanup finished
            job_d = submit(client, ["SELECT 2"])
            jd = wait_job(client, job_d, timeout=30)
            assert jd["connection_id"] == conn_id
            assert jd["status"] == "succeeded"
            gap = (ts(jd["started_at"]) - ts(jc["finished_at"])).total_seconds()
            assert gap >= 3.0, f"cleanup delay not observed: {gap}s"
        finally:
            cancel(client, sleeper)
            wait_pool_idle(client)


class TestConnectionRebuild:
    def test_cleanup_failure_retires_and_rebuilds_connection(self, client):
        """break_connection_after hook: cleanup fails -> retire -> rebuild."""
        sleeper, busy_conn = occupy_one_conn(client, seconds=15)
        try:
            job_b = submit(client, ["SELECT 1"], break_connection_after=True)
            jb = wait_job(client, job_b)
            assert jb["status"] == "succeeded"  # the job itself was fine
            conn_id = jb["connection_id"]
            assert conn_id != busy_conn
            gen_before = next(
                c for c in pool(client)["connections"] if c["id"] == conn_id
            )["generation"]
            deadline = time.monotonic() + 30
            while time.monotonic() < deadline:
                info = next(
                    c for c in pool(client)["connections"] if c["id"] == conn_id
                )
                if info["generation"] > gen_before and info["state"] == "idle":
                    break
                time.sleep(0.2)
            else:
                raise AssertionError(f"connection not rebuilt: {pool(client)}")
            assert info["cleanups_failed"] >= 1
            # the rebuilt connection serves new jobs
            job_r = submit(client, ["SELECT 3 AS three"])
            jr = wait_job(client, job_r)
            assert jr["connection_id"] == conn_id
            assert jr["status"] == "succeeded"
            assert jr["statements"][0]["rows"] == [[3]]
            assert len(pool(client)["connections"]) == 2
        finally:
            cancel(client, sleeper)
            wait_pool_idle(client)

    @pytest.mark.skipif(not TEST_DSN, reason="TEST_DATABASE_URL not set")
    def test_killed_backend_retires_and_rebuilds_connection(self, client):
        """Real-world variant: kill the backend out-of-band."""
        import psycopg

        # probe whether backend termination works in this environment
        with psycopg.connect(TEST_DSN, autocommit=True) as c:
            pid = c.execute("SELECT pg_backend_pid()").fetchone()[0]
            if not c.execute("SELECT pg_terminate_backend(%s)", (pid,)).fetchone()[0]:
                pytest.skip("pg_terminate_backend not effective here")

        sleeper, busy_conn = occupy_one_conn(client, seconds=15)
        try:
            job_p = submit(client, ["SELECT pg_backend_pid() AS pid"])
            jp = wait_job(client, job_p)
            conn_id = jp["connection_id"]
            assert conn_id != busy_conn
            pid = jp["statements"][0]["rows"][0][0]
            gen_before = next(
                c for c in pool(client)["connections"] if c["id"] == conn_id
            )["generation"]
            with psycopg.connect(TEST_DSN, autocommit=True) as c:
                assert c.execute(
                    "SELECT pg_terminate_backend(%s)", (pid,)
                ).fetchone()[0]
            # next job on that connection fails; cleanup then fails too and
            # the connection must be retired and rebuilt
            job_q = submit(client, ["SELECT 1"])
            jq = wait_job(client, job_q)
            assert jq["connection_id"] == conn_id
            assert jq["status"] == "failed"
            deadline = time.monotonic() + 30
            while time.monotonic() < deadline:
                info = next(
                    c for c in pool(client)["connections"] if c["id"] == conn_id
                )
                if info["generation"] > gen_before and info["state"] == "idle":
                    break
                time.sleep(0.2)
            else:
                raise AssertionError(f"connection not rebuilt: {pool(client)}")
            assert info["cleanups_failed"] >= 1
            job_r = submit(client, ["SELECT 3 AS three"])
            jr = wait_job(client, job_r)
            assert jr["status"] == "succeeded"
            assert len(pool(client)["connections"]) == 2
        finally:
            cancel(client, sleeper)
            wait_pool_idle(client)
