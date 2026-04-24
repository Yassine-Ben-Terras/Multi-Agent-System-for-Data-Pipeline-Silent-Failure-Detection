"""
Unit tests for SignalWindow — bucketing, expiry, sweep, flush.
Time is controlled via monkeypatching datetime.
"""

from __future__ import annotations

import time
from datetime import datetime, timezone, timedelta
from unittest.mock import MagicMock, patch

import pytest

from agents.orchestrator.window import SignalBucket, SignalWindow, WINDOW_SECONDS
from config.schemas import AnomalySignal, AnomalyType, PipelineStage, Severity


def make_signal(
    run_id: str = "run-001",
    anomaly_type: AnomalyType = AnomalyType.ROW_COUNT,
    severity: Severity = Severity.HIGH,
    model: str = "fct_orders",
) -> AnomalySignal:
    return AnomalySignal(
        anomaly_type=anomaly_type,
        source_agent="ingestion_monitor",
        pipeline_run_id=run_id,
        model_name=model,
        severity=severity,
        confidence=0.9,
        details={"z_score": -3.5},
    )


class TestSignalBucket:

    def test_anomaly_types_deduplicated(self):
        b = SignalBucket(run_id="r")
        b.signals = [
            make_signal(anomaly_type=AnomalyType.ROW_COUNT),
            make_signal(anomaly_type=AnomalyType.ROW_COUNT),
            make_signal(anomaly_type=AnomalyType.SCHEMA_DRIFT),
        ]
        types = b.anomaly_types()
        assert len(types) == 2
        assert "row_count_anomaly" in types
        assert "schema_drift" in types

    def test_highest_severity(self):
        b = SignalBucket(run_id="r")
        b.signals = [
            make_signal(severity=Severity.LOW),
            make_signal(severity=Severity.CRITICAL),
            make_signal(severity=Severity.MEDIUM),
        ]
        assert b.highest_severity() == Severity.CRITICAL

    def test_is_expired_false_when_fresh(self):
        b = SignalBucket(run_id="r")
        assert not b.is_expired()

    def test_is_expired_true_when_old(self):
        b = SignalBucket(run_id="r")
        old_time = datetime.now(timezone.utc) - timedelta(seconds=WINDOW_SECONDS + 1)
        b.opened_at = old_time
        assert b.is_expired()


class TestSignalWindow:

    def test_add_creates_bucket(self):
        cb = MagicMock()
        w = SignalWindow(on_bucket_expired=cb)
        w.add(make_signal(run_id="run-001"))
        assert w.active_buckets == 1

    def test_add_same_run_id_same_bucket(self):
        cb = MagicMock()
        w = SignalWindow(on_bucket_expired=cb)
        w.add(make_signal(run_id="run-001"))
        w.add(make_signal(run_id="run-001", anomaly_type=AnomalyType.SCHEMA_DRIFT))
        assert w.active_buckets == 1

    def test_add_different_run_ids_different_buckets(self):
        cb = MagicMock()
        w = SignalWindow(on_bucket_expired=cb)
        w.add(make_signal(run_id="run-001"))
        w.add(make_signal(run_id="run-002"))
        assert w.active_buckets == 2

    def test_sweep_expires_old_buckets(self):
        dispatched = []
        w = SignalWindow(on_bucket_expired=lambda b: dispatched.append(b))

        # Add a signal then manually age its bucket
        w.add(make_signal(run_id="run-old"))
        with w._lock:
            w._buckets["run-old"].opened_at = (
                datetime.now(timezone.utc) - timedelta(seconds=WINDOW_SECONDS + 1)
            )

        w.sweep()

        assert w.active_buckets == 0
        assert len(dispatched) == 1
        assert dispatched[0].run_id == "run-old"

    def test_sweep_keeps_fresh_buckets(self):
        cb = MagicMock()
        w = SignalWindow(on_bucket_expired=cb)
        w.add(make_signal(run_id="run-fresh"))
        w.sweep()
        assert w.active_buckets == 1
        cb.assert_not_called()

    def test_flush_all_dispatches_everything(self):
        dispatched = []
        w = SignalWindow(on_bucket_expired=lambda b: dispatched.append(b))
        w.add(make_signal(run_id="run-001"))
        w.add(make_signal(run_id="run-002"))
        w.flush_all()
        assert w.active_buckets == 0
        assert len(dispatched) == 2

    def test_bucket_contains_all_signals(self):
        captured = []
        w = SignalWindow(on_bucket_expired=lambda b: captured.append(b))
        w.add(make_signal(run_id="run-001", anomaly_type=AnomalyType.ROW_COUNT))
        w.add(make_signal(run_id="run-001", anomaly_type=AnomalyType.SCHEMA_DRIFT))
        w.flush_all()
        assert len(captured[0].signals) == 2

    def test_callback_error_does_not_crash_sweep(self):
        """Error in callback must not stop the sweep loop."""
        def bad_callback(b):
            raise RuntimeError("Callback error")

        w = SignalWindow(on_bucket_expired=bad_callback)
        w.add(make_signal(run_id="run-001"))
        with w._lock:
            w._buckets["run-001"].opened_at = (
                datetime.now(timezone.utc) - timedelta(seconds=WINDOW_SECONDS + 1)
            )
        w.sweep()   # Should not raise
        assert w.active_buckets == 0
