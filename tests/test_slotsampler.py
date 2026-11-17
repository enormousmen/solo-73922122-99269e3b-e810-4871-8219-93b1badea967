"""Unit tests for the replication-slot lag sampling decision logic.

These cover the pure estimation/classification rules in
``app.slotsampler.build_observation`` with fabricated samples -- no database
is required.
"""
from __future__ import annotations

import pytest

from app.slotsampler import SlotSample, build_observation, parse_lsn


def _sample(
    current="0/1000000",
    restart="0/0F00000",
    wal_status="reserved",
    safe=60_000_000,
    slot_type="logical",
    active=False,
    name="slot1",
):
    return SlotSample(
        taken_at="2026-11-17T00:00:00+00:00",
        current_wal_lsn=current,
        slot_name=name,
        slot_type=slot_type,
        active=active,
        restart_lsn=restart,
        wal_status=wal_status,
        safe_wal_size=safe,
    )


class TestParseLsn:
    def test_basic(self):
        assert parse_lsn("0/1") == 1
        assert parse_lsn("1/0") == 1 << 32
        assert parse_lsn("0/15348E0") == 0x15348E0

    def test_high_part(self):
        assert parse_lsn("2/FFFFFFFF") == (2 << 32) + 0xFFFFFFFF

    @pytest.mark.parametrize("bad", [None, "", "garbage", "/1", "1/", "x/y"])
    def test_malformed_raises(self, bad):
        with pytest.raises(ValueError):
            parse_lsn(bad)


class TestBuildObservation:
    def test_missing_slot_first_sample(self):
        out = build_observation("nope", None, _sample(), 2.0)
        assert out["status"] == "not_estimable"
        assert out["reason"] == "slot_not_found"
        assert out["samples"][0] is None
        assert out["samples"][1] is not None

    def test_missing_slot_second_sample(self):
        out = build_observation("nope", _sample(), None, 2.0)
        assert out["reason"] == "slot_not_found"
        assert out["samples"][0] is not None
        assert out["samples"][1] is None

    def test_null_restart_lsn_is_not_zero(self):
        s1 = _sample(restart=None, wal_status=None)
        s2 = _sample(restart=None, wal_status=None)
        out = build_observation("phys", s1, s2, 2.0)
        assert out["status"] == "not_estimable"
        assert out["reason"] == "restart_lsn_null"
        # the raw samples preserve NULL instead of a fabricated zero LSN
        assert out["samples"][0]["restart_lsn"] is None
        assert "lag" not in out

    def test_wal_status_lost(self):
        s1 = _sample(wal_status="reserved")
        s2 = _sample(current="0/1100000", wal_status="lost", safe=None)
        out = build_observation("slot1", s1, s2, 2.0)
        assert out["reason"] == "wal_status_lost"
        assert out["status"] == "not_estimable"

    def test_wal_status_unreserved_first_sample(self):
        s1 = _sample(wal_status="unreserved")
        s2 = _sample(current="0/1100000", wal_status="unreserved")
        out = build_observation("slot1", s1, s2, 2.0)
        assert out["reason"] == "wal_status_unreserved"

    def test_net_growth_with_finite_limit_estimates(self):
        # retained: 0x1000000-0x0F00000 = 1 MiB ; then 3 MiB -> delta 2 MiB
        s1 = _sample(current="0/1000000", restart="0/0F00000", safe=50_000_000)
        s2 = _sample(current="0/1300000", restart="0/1000000", safe=47_919_104)
        out = build_observation("slot1", s1, s2, 2.0)
        assert out["status"] == "estimated"
        assert out["reason"] is None
        lag = out["lag"]
        assert lag["retained_bytes_sample_1"] == 0x100000
        assert lag["retained_bytes_sample_2"] == 0x300000
        assert lag["delta_retained_bytes"] == 0x200000
        assert lag["restart_lsn_advanced_bytes"] == 0x100000
        est = out["estimate"]
        assert est["estimation"] == "short_window_extrapolation"
        assert "extrapolation" in est["caveat"]
        # 2 MiB / 2s = 1 MiB/s; remaining = safe / rate
        assert est["net_growth_bytes_per_second"] == pytest.approx(0x200000 / 2)
        assert est["estimated_remaining_seconds"] == pytest.approx(
            47_919_104 / (0x200000 / 2), abs=1e-3
        )
        assert est["safe_wal_size_bytes"] == 47_919_104
        assert est["estimated_remaining_human"].endswith("s")

    def test_shrinking_lag_no_countdown(self):
        # retained shrinks from 3 MiB to 1 MiB (consumer advancing faster)
        s1 = _sample(current="0/1300000", restart="0/1000000")
        s2 = _sample(current="0/1400000", restart="0/1300000")
        out = build_observation("slot1", s1, s2, 2.0)
        assert out["status"] == "not_estimable"
        assert out["reason"] == "no_net_growth"
        assert out["lag"]["delta_retained_bytes"] < 0
        assert "estimate" not in out

    def test_zero_net_change_no_countdown(self):
        s1 = _sample(current="0/1000000", restart="0/0F00000")
        # both positions advance by the same amount -> retained unchanged
        s2 = _sample(current="0/1100000", restart="0/1000000")
        out = build_observation("slot1", s1, s2, 2.0)
        assert out["reason"] == "no_net_growth"
        assert out["lag"]["delta_retained_bytes"] == 0

    def test_unlimited_safe_wal_size_no_countdown(self):
        s1 = _sample()
        s2 = _sample(current="0/1300000", restart="0/1000000", safe=None)
        out = build_observation("slot1", s1, s2, 2.0)
        assert out["reason"] == "no_retention_limit"
        assert out["lag"]["delta_retained_bytes"] > 0
        assert "estimate" not in out

    def test_current_lsn_moved_backward(self):
        s1 = _sample(current="0/2000000")
        s2 = _sample(current="0/1000000", restart="0/1000000")
        out = build_observation("slot1", s1, s2, 2.0)
        assert out["reason"] == "current_lsn_moved_backward"
        assert "lag" not in out

    def test_restart_lsn_moved_backward(self):
        s1 = _sample(current="0/1000000", restart="0/0F00000")
        s2 = _sample(current="0/1300000", restart="0/0E00000")
        out = build_observation("slot1", s1, s2, 2.0)
        assert out["reason"] == "restart_lsn_moved_backward"

    def test_slot_identity_changed(self):
        s1 = _sample(name="slot1", slot_type="logical")
        s2 = _sample(name="slot1", slot_type="physical")
        out = build_observation("slot1", s1, s2, 2.0)
        assert out["reason"] == "slot_identity_changed"

    def test_growth_rate_uses_actual_interval(self):
        s1 = _sample()
        s2 = _sample(current="0/1200000", restart="0/1000000", safe=10_000_000)
        out_1s = build_observation("slot1", s1, s2, 1.0)
        out_3s = build_observation("slot1", s1, s2, 3.0)
        assert out_1s["estimate"]["net_growth_bytes_per_second"] == pytest.approx(
            3 * out_3s["estimate"]["net_growth_bytes_per_second"]
        )
        # shorter window -> higher rate -> less remaining time
        assert (
            out_1s["estimate"]["estimated_remaining_seconds"]
            < out_3s["estimate"]["estimated_remaining_seconds"]
        )

    def test_interval_echoed_and_samples_kept(self):
        s1 = _sample()
        s2 = _sample(current="0/1300000", restart="0/1000000")
        out = build_observation("slot1", s1, s2, 1.5)
        assert out["interval_seconds"] == 1.5
        assert len(out["samples"]) == 2
        assert out["samples"][0]["current_wal_lsn"] == "0/1000000"
