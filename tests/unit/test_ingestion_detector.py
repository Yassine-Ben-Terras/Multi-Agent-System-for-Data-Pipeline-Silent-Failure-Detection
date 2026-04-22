"""
Unit tests for the Ingestion Monitor — IngestionDetector

All external dependencies (BaselineManager, Kafka) are mocked.
Tests cover: normal signal, row count anomaly, zero rows, checksum mismatch,
insufficient baseline, and severity mapping.
"""

from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest

from agents.ingestion_monitor.baseline import Baseline
from agents.ingestion_monitor.detector import IngestionDetector, _checksum_cache
from config.schemas import AnomalyType, PipelineSignal, PipelineStage, Severity


# ── Fixtures ──────────────────────────────────────────────────

def make_signal(
    row_count: int = 100_000,
    checksum: str = "abc123",
    source: str = "orders_api",
    run_id: str = "run-001",
    ts: datetime = None,
) -> PipelineSignal:
    return PipelineSignal(
        stage=PipelineStage.INGESTION,
        source=source,
        metrics={"row_count": row_count, "checksum": checksum, "duration_ms": 1200},
        pipeline_run_id=run_id,
        ts=ts or datetime(2026, 4, 22, 3, 0, 0, tzinfo=timezone.utc),
    )


def make_baseline(mean: float, std_dev: float, n: int = 10) -> Baseline:
    return Baseline(
        model_name="orders_api",
        source="orders_api",
        metric="row_count",
        mean=mean,
        std_dev=std_dev,
        sample_count=n,
    )


@pytest.fixture
def mock_baselines():
    return MagicMock()


@pytest.fixture
def detector(mock_baselines):
    return IngestionDetector(baseline_manager=mock_baselines, zscore_threshold=2.5)


@pytest.fixture(autouse=True)
def clear_checksum_cache():
    """Clear the module-level checksum cache between tests."""
    _checksum_cache.clear()
    yield
    _checksum_cache.clear()


# ── Row Count Tests ───────────────────────────────────────────

class TestRowCountDetection:

    def test_normal_signal_no_anomaly(self, detector, mock_baselines):
        """Signal within 2.5 std devs → no anomaly."""
        mock_baselines.get.return_value = make_baseline(mean=100_000, std_dev=5_000)
        mock_baselines.upsert.return_value = make_baseline(100_000, 5_000)

        result = detector.analyze(make_signal(row_count=102_000))

        assert not result.has_anomaly
        assert result.anomalies == []

    def test_row_count_anomaly_high_severity(self, detector, mock_baselines):
        """z-score of -4.0 (> 3.5) → HIGH severity."""
        mock_baselines.get.return_value = make_baseline(mean=100_000, std_dev=5_000)
        mock_baselines.upsert.return_value = make_baseline(100_000, 5_000)

        # 80_000 → z = (80000 - 100000) / 5000 = -4.0
        result = detector.analyze(make_signal(row_count=80_000))

        assert result.has_anomaly
        assert len(result.anomalies) == 1
        anomaly = result.anomalies[0]
        assert anomaly.anomaly_type == AnomalyType.ROW_COUNT
        assert anomaly.severity == Severity.HIGH
        assert anomaly.details["z_score"] == pytest.approx(-4.0, abs=0.01)

    def test_row_count_anomaly_critical_severity(self, detector, mock_baselines):
        """z-score of -6.0 (>= 5.0) → CRITICAL severity."""
        mock_baselines.get.return_value = make_baseline(mean=100_000, std_dev=5_000)
        mock_baselines.upsert.return_value = make_baseline(100_000, 5_000)

        # 70_000 → z = -6.0
        result = detector.analyze(make_signal(row_count=70_000))

        assert result.has_anomaly
        assert result.anomalies[0].severity == Severity.CRITICAL

    def test_row_count_anomaly_medium_severity(self, detector, mock_baselines):
        """z-score of -2.8 (2.5–3.5) → MEDIUM severity."""
        mock_baselines.get.return_value = make_baseline(mean=100_000, std_dev=5_000)
        mock_baselines.upsert.return_value = make_baseline(100_000, 5_000)

        # 86_000 → z = -2.8
        result = detector.analyze(make_signal(row_count=86_000))

        assert result.has_anomaly
        assert result.anomalies[0].severity == Severity.MEDIUM

    def test_no_baseline_returns_no_anomaly(self, detector, mock_baselines):
        """No baseline yet → skip detection (not enough history)."""
        mock_baselines.get.return_value = None
        mock_baselines.upsert.return_value = make_baseline(50_000, 0, n=1)

        result = detector.analyze(make_signal(row_count=50_000))

        assert not result.has_anomaly

    def test_insufficient_baseline_samples(self, detector, mock_baselines):
        """Fewer than 3 samples → skip detection."""
        mock_baselines.get.return_value = make_baseline(mean=100_000, std_dev=5_000, n=2)
        mock_baselines.upsert.return_value = make_baseline(100_000, 5_000, n=3)

        result = detector.analyze(make_signal(row_count=50_000))

        assert not result.has_anomaly

    def test_zero_std_dev_returns_no_anomaly(self, detector, mock_baselines):
        """Zero std dev (all historical values identical) → z-score undefined, no anomaly."""
        mock_baselines.get.return_value = make_baseline(mean=100_000, std_dev=0.0, n=10)
        mock_baselines.upsert.return_value = make_baseline(100_000, 0.0, n=11)

        result = detector.analyze(make_signal(row_count=99_000))

        assert not result.has_anomaly

    def test_baseline_always_updated(self, detector, mock_baselines):
        """Baseline upsert is called even on non-anomalous signals."""
        mock_baselines.get.return_value = make_baseline(mean=100_000, std_dev=5_000)
        mock_baselines.upsert.return_value = make_baseline(100_000, 5_000)

        detector.analyze(make_signal(row_count=101_000))

        mock_baselines.upsert.assert_called_once_with(
            model_name="orders_api",
            source="orders_api",
            metric="row_count",
            new_value=101_000.0,
        )

    def test_confidence_proportional_to_zscore(self, detector, mock_baselines):
        """Higher z-score → higher confidence (capped at 1.0)."""
        mock_baselines.get.return_value = make_baseline(mean=100_000, std_dev=5_000)
        mock_baselines.upsert.return_value = make_baseline(100_000, 5_000)

        # z = -6.0, confidence = min(1.0, 6.0 / 5.0) = 1.0
        result = detector.analyze(make_signal(row_count=70_000))
        assert result.anomalies[0].confidence == 1.0


# ── Zero Row Tests ────────────────────────────────────────────

class TestZeroRowDetection:

    def test_zero_rows_always_critical(self, detector, mock_baselines):
        """Zero rows → CRITICAL, confidence 1.0, no baseline check needed."""
        result = detector.analyze(make_signal(row_count=0))

        assert result.has_anomaly
        anomaly = result.anomalies[0]
        assert anomaly.severity == Severity.CRITICAL
        assert anomaly.confidence == 1.0
        assert anomaly.anomaly_type == AnomalyType.ROW_COUNT
        # Should NOT call baseline for zero-row ingestion
        mock_baselines.upsert.assert_not_called()

    def test_zero_rows_skips_baseline_upsert(self, detector, mock_baselines):
        """Zero rows should not update the baseline (would corrupt it)."""
        detector.analyze(make_signal(row_count=0))
        mock_baselines.upsert.assert_not_called()


# ── Checksum Tests ────────────────────────────────────────────

class TestChecksumDetection:

    def test_first_time_partition_no_anomaly(self, detector, mock_baselines):
        """First time seeing a (source, date) partition → no anomaly."""
        mock_baselines.get.return_value = make_baseline(100_000, 5_000)
        mock_baselines.upsert.return_value = make_baseline(100_000, 5_000)

        result = detector.analyze(make_signal(checksum="abc123"))
        checksum_anomalies = [a for a in result.anomalies if a.anomaly_type == AnomalyType.CHECKSUM_MISMATCH]
        assert checksum_anomalies == []

    def test_same_checksum_no_anomaly(self, detector, mock_baselines):
        """Same checksum on re-run → idempotent, no anomaly."""
        mock_baselines.get.return_value = make_baseline(100_000, 5_000)
        mock_baselines.upsert.return_value = make_baseline(100_000, 5_000)

        ts = datetime(2026, 4, 22, 3, 0, 0, tzinfo=timezone.utc)
        detector.analyze(make_signal(checksum="abc123", ts=ts))
        result = detector.analyze(make_signal(checksum="abc123", ts=ts))

        checksum_anomalies = [a for a in result.anomalies if a.anomaly_type == AnomalyType.CHECKSUM_MISMATCH]
        assert checksum_anomalies == []

    def test_changed_checksum_is_anomaly(self, detector, mock_baselines):
        """Different checksum for same source + partition → CHECKSUM_MISMATCH HIGH."""
        mock_baselines.get.return_value = make_baseline(100_000, 5_000)
        mock_baselines.upsert.return_value = make_baseline(100_000, 5_000)

        ts = datetime(2026, 4, 22, 3, 0, 0, tzinfo=timezone.utc)
        detector.analyze(make_signal(checksum="abc123", ts=ts))
        result = detector.analyze(make_signal(checksum="xyz999", ts=ts))

        checksum_anomalies = [a for a in result.anomalies if a.anomaly_type == AnomalyType.CHECKSUM_MISMATCH]
        assert len(checksum_anomalies) == 1
        assert checksum_anomalies[0].severity == Severity.HIGH
        assert checksum_anomalies[0].confidence == 1.0

    def test_different_day_same_checksum_no_anomaly(self, detector, mock_baselines):
        """Same source but different day = different partition → no mismatch."""
        mock_baselines.get.return_value = make_baseline(100_000, 5_000)
        mock_baselines.upsert.return_value = make_baseline(100_000, 5_000)

        ts1 = datetime(2026, 4, 21, 3, 0, 0, tzinfo=timezone.utc)
        ts2 = datetime(2026, 4, 22, 3, 0, 0, tzinfo=timezone.utc)

        detector.analyze(make_signal(checksum="abc123", ts=ts1))
        result = detector.analyze(make_signal(checksum="xyz999", ts=ts2))

        checksum_anomalies = [a for a in result.anomalies if a.anomaly_type == AnomalyType.CHECKSUM_MISMATCH]
        assert checksum_anomalies == []


# ── Severity Mapping Tests ────────────────────────────────────

class TestSeverityMapping:

    @pytest.mark.parametrize("z_score, expected_severity", [
        (2.6, Severity.MEDIUM),
        (-2.6, Severity.MEDIUM),
        (3.6, Severity.HIGH),
        (-3.6, Severity.HIGH),
        (5.1, Severity.CRITICAL),
        (-5.1, Severity.CRITICAL),
    ])
    def test_zscore_to_severity(self, z_score, expected_severity):
        severity = IngestionDetector._zscore_to_severity(z_score)
        assert severity == expected_severity
