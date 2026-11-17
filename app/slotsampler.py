"""Replication-slot WAL retention sampling.

The on-call DBA needs to know whether a *named* replication slot is falling
behind and, when a WAL retention limit is configured, roughly when it would
hit that limit.  The endpoint takes two samples, ``interval`` seconds apart,
of:

* the cluster's current WAL insert position (``pg_current_wal_lsn()``);
* the slot's ``restart_lsn`` / ``wal_status`` / ``safe_wal_size``
  (``pg_replication_slots``).

Retained bytes are ``current_wal_lsn - restart_lsn`` (both parsed from the
canonical ``X/XXXXXXXX`` text form, never coerced through NULL).

A remaining-time estimate is produced only when *all* of the following hold:

1. both samples found the slot with a non-NULL ``restart_lsn``;
2. the two samples are comparable (same slot identity; positions and the
   restart LSN moved forward monotonically; the status never became
   ``unreserved``/``lost``);
3. the retained amount had a **net increase** over the window -- when the
   consumer advances and the retained amount falls, no countdown is reported;
4. a finite retention limit exists (``safe_wal_size`` is a real number on the
   second sample).

The estimate is a *short-window linear extrapolation* and is labelled as
such; anything else yields a machine-readable ``reason`` explaining why no
estimate is possible.

Connection policy: this module opens its own short-lived connection per
request.  It never touches the two fixed diagnostic long connections in
:mod:`app.pool`, so queued/running jobs and cancels keep working while a
sampling request sleeps between its two probes.
"""
from __future__ import annotations

import logging
import time
from dataclasses import dataclass

import psycopg

from . import config

log = logging.getLogger("diag.slots")

# wal_status values that mean the retained WAL is no longer protected.
_BAD_WAL_STATUS = frozenset({"unreserved", "lost"})

# One single statement per sample: current WAL position plus the slot row.
# The slot name is passed as a parameter, never interpolated.
_SAMPLE_SQL = """
SELECT pg_current_wal_lsn()::text AS current_wal_lsn,
       s.slot_name,
       s.slot_type,
       s.active,
       s.restart_lsn::text AS restart_lsn,
       s.wal_status,
       s.safe_wal_size
FROM pg_replication_slots AS s
WHERE s.slot_name = %s
"""


class SampleError(RuntimeError):
    """A database-side failure while taking a sample (connect/query)."""


@dataclass(frozen=True)
class SlotSample:
    """One raw observation of the cluster position and one slot's state."""

    taken_at: str  # ISO-8601 UTC timestamp
    current_wal_lsn: str
    slot_name: str
    slot_type: str
    active: bool
    restart_lsn: str | None
    wal_status: str | None
    safe_wal_size: int | None

    def as_dict(self) -> dict:
        return {
            "taken_at": self.taken_at,
            "current_wal_lsn": self.current_wal_lsn,
            "slot_name": self.slot_name,
            "slot_type": self.slot_type,
            "active": self.active,
            "restart_lsn": self.restart_lsn,
            "wal_status": self.wal_status,
            "safe_wal_size": self.safe_wal_size,
        }


def parse_lsn(text: str) -> int:
    """Parse PostgreSQL's ``X/XXXXXXXX`` LSN text into a byte offset.

    Raises ``ValueError`` on anything that is not a well-formed LSN so a
    malformed value can never silently be treated as zero.
    """
    if not isinstance(text, str) or "/" not in text:
        raise ValueError(f"invalid LSN text: {text!r}")
    high, low = text.split("/", 1)
    if not high or not low:
        raise ValueError(f"invalid LSN text: {text!r}")
    return (int(high, 16) << 32) | int(low, 16)


def _utc_iso() -> str:
    from datetime import datetime, timezone

    return datetime.now(timezone.utc).isoformat()


def _take_one(conn: psycopg.Connection, slot_name: str) -> SlotSample | None:
    """Run one probe.  Returns ``None`` when the slot does not exist."""
    row = conn.execute(_SAMPLE_SQL, (slot_name,)).fetchone()
    if row is None:
        return None
    (
        current_lsn,
        found_name,
        slot_type,
        active,
        restart_lsn,
        wal_status,
        safe_wal_size,
    ) = row
    return SlotSample(
        taken_at=_utc_iso(),
        current_wal_lsn=current_lsn,
        slot_name=found_name,
        slot_type=slot_type,
        active=bool(active),
        restart_lsn=restart_lsn,  # NULL stays None; it is never turned into 0
        wal_status=wal_status,
        safe_wal_size=safe_wal_size,
    )


def _retained_bytes(sample: SlotSample) -> int:
    """current_wal_lsn - restart_lsn; caller guarantees a non-NULL LSN."""
    return parse_lsn(sample.current_wal_lsn) - parse_lsn(sample.restart_lsn)


def _comparability_failure(s1: SlotSample, s2: SlotSample) -> str | None:
    """Return a reason code if the two samples cannot be compared, else None."""
    # Identity guard: the row must describe the same slot throughout.
    if (s1.slot_name, s1.slot_type) != (s2.slot_name, s2.slot_type):
        return "slot_identity_changed"
    # A status flip to unreserved/lost means retained WAL was already removed.
    if s2.wal_status in _BAD_WAL_STATUS or s1.wal_status in _BAD_WAL_STATUS:
        return f"wal_status_{s2.wal_status or s1.wal_status}"
    try:
        cur1, cur2 = parse_lsn(s1.current_wal_lsn), parse_lsn(s2.current_wal_lsn)
        rs1, rs2 = parse_lsn(s1.restart_lsn), parse_lsn(s2.restart_lsn)
    except ValueError:
        return "malformed_lsn"
    # Positions only move forward in a healthy cluster; a regression means
    # something changed underneath the sampler (e.g. recovery/timeline switch).
    if cur2 < cur1:
        return "current_lsn_moved_backward"
    if rs2 < rs1:
        return "restart_lsn_moved_backward"
    return None


def build_observation(
    slot_name: str,
    sample1: SlotSample | None,
    sample2: SlotSample | None,
    interval_s: float,
) -> dict:
    """Pure logic: assemble the response body from the two raw samples.

    Kept free of I/O so the estimation/decision rules are unit-testable
    without a database.
    """
    if sample1 is None or sample2 is None:
        return {
            "slot_name": slot_name,
            "status": "not_estimable",
            "reason": "slot_not_found",
            "detail": f"replication slot {slot_name!r} does not exist "
            "(absent in at least one of the two samples)",
            "interval_seconds": round(interval_s, 3),
            "samples": [
                None if s is None else s.as_dict() for s in (sample1, sample2)
            ],
        }

    samples = [sample1.as_dict(), sample2.as_dict()]
    base: dict = {
        "slot_name": slot_name,
        "interval_seconds": round(interval_s, 3),
        "samples": samples,
    }

    # Per-sample NULL checks -- NULL is reported, never treated as zero.
    if sample1.restart_lsn is None or sample2.restart_lsn is None:
        which = 1 if sample1.restart_lsn is None else 2
        return {
            **base,
            "status": "not_estimable",
            "reason": "restart_lsn_null",
            "detail": f"sample {which}: restart_lsn is NULL (the slot has "
            "never reserved WAL); retained bytes cannot be computed",
        }
    if sample1.wal_status in _BAD_WAL_STATUS:
        status = sample1.wal_status
        return {
            **base,
            "status": "not_estimable",
            "reason": f"wal_status_{status}",
            "detail": f"sample 1: wal_status={status!r}; required WAL is "
            "not retained by the server",
        }
    if sample2.wal_status in _BAD_WAL_STATUS:
        status = sample2.wal_status
        return {
            **base,
            "status": "not_estimable",
            "reason": f"wal_status_{status}",
            "detail": f"sample 2: wal_status={status!r}; required WAL is "
            "not retained by the server",
        }

    failure = _comparability_failure(sample1, sample2)
    if failure is not None:
        return {
            **base,
            "status": "not_estimable",
            "reason": failure,
            "detail": _REASON_DETAIL.get(failure, failure),
        }

    retained1 = _retained_bytes(sample1)
    retained2 = _retained_bytes(sample2)
    delta_retained = retained2 - retained1
    current1 = parse_lsn(sample1.current_wal_lsn)
    current2 = parse_lsn(sample2.current_wal_lsn)
    restart1 = parse_lsn(sample1.restart_lsn)
    restart2 = parse_lsn(sample2.restart_lsn)
    base["lag"] = {
        "retained_bytes_sample_1": retained1,
        "retained_bytes_sample_2": retained2,
        "delta_retained_bytes": delta_retained,
        "current_wal_lsn_advanced_bytes": current2 - current1,
        "restart_lsn_advanced_bytes": restart2 - restart1,
    }

    # Net growth is mandatory for a countdown.  Consumer progress that shrinks
    # the retained amount (delta <= 0) must never produce one.
    if delta_retained <= 0:
        return {
            **base,
            "status": "not_estimable",
            "reason": "no_net_growth",
            "detail": "retained WAL did not grow during the window "
            f"(net change {delta_retained} bytes); the consumer is keeping "
            "up or catching up, so no remaining-time countdown is reported",
        }

    if sample2.safe_wal_size is None:
        return {
            **base,
            "status": "not_estimable",
            "reason": "no_retention_limit",
            "detail": "safe_wal_size is NULL: max_slot_wal_keep_size is "
            "unlimited (-1), so there is no finite retention limit to count "
            "down to",
        }

    # Finite limit + positive net growth -> short-window linear extrapolation.
    growth_rate = delta_retained / interval_s  # bytes/second
    remaining_seconds = sample2.safe_wal_size / growth_rate
    estimate = {
        "estimation": "short_window_extrapolation",
        "caveat": "linear extrapolation of the net growth observed over a "
        f"{interval_s:g}s window only; real WAL/consumer rates vary, treat "
        "as an indicative countdown, not a guarantee",
        "net_growth_bytes_per_second": round(growth_rate, 3),
        "safe_wal_size_bytes": sample2.safe_wal_size,
        "estimated_remaining_seconds": round(remaining_seconds, 3),
        "estimated_remaining_human": _human_duration(remaining_seconds),
    }
    return {**base, "status": "estimated", "reason": None, "estimate": estimate}


_REASON_DETAIL = {
    "slot_identity_changed": "the slot's identity (name/type) changed "
    "between the two samples",
    "current_lsn_moved_backward": "current WAL position moved backward "
    "between samples (unexpected cluster/timeline change)",
    "restart_lsn_moved_backward": "restart_lsn moved backward between "
    "samples; the samples are not comparable",
    "malformed_lsn": "a returned LSN was not in the expected X/XXXXXXXX form",
}


def _human_duration(seconds: float) -> str:
    if seconds < 60:
        return f"{seconds:.1f}s"
    minutes, sec = divmod(seconds, 60)
    if minutes < 60:
        return f"{int(minutes)}m{int(sec)}s"
    hours, minutes = divmod(minutes, 60)
    return f"{int(hours)}h{int(minutes)}m{int(sec)}s"


def sample_slot(slot_name: str, interval_s: float) -> dict:
    """Take two probes ``interval_s`` seconds apart on a dedicated connection.

    Uses one short-lived connection for both probes (opened and closed inside
    this call) so the fixed diagnostic pool is never occupied and job
    submission/cancellation remain available throughout.
    """
    t0 = time.monotonic()
    try:
        conn = psycopg.connect(
            config.DATABASE_URL,
            autocommit=True,
            connect_timeout=config.SAMPLE_CONNECT_TIMEOUT_S,
            prepare_threshold=None,
        )
    except Exception as exc:
        raise SampleError(f"could not open sampling connection: {exc}") from exc
    try:
        # SET cannot take a bind parameter; the value comes from our own
        # integer config, so an explicit ms literal is safe and unambiguous.
        conn.execute(
            "SET statement_timeout = "
            f"{int(config.SAMPLE_STATEMENT_TIMEOUT_MS)}"
        )
        try:
            sample1 = _take_one(conn, slot_name)
        except Exception as exc:
            raise SampleError(f"first sampling query failed: {exc}") from exc

        # The wait happens on this request's own connection; the two pooled
        # diagnostic connections are untouched.
        time.sleep(interval_s)

        try:
            sample2 = _take_one(conn, slot_name)
        except Exception as exc:
            raise SampleError(f"second sampling query failed: {exc}") from exc
    finally:
        try:
            conn.close()
        except Exception:
            log.warning("failed to close sampling connection", exc_info=True)

    elapsed = time.monotonic() - t0
    observation = build_observation(slot_name, sample1, sample2, interval_s)
    observation["elapsed_seconds"] = round(elapsed, 3)
    return observation
