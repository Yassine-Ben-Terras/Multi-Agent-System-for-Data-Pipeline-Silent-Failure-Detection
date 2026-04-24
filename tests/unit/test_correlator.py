"""
Unit tests for Correlator — rule matching, suppression, novel patterns.
"""

from __future__ import annotations

import pytest
from datetime import datetime, timezone

from agents.orchestrator.correlator import Correlator
from agents.orchestrator.window import SignalBucket
from config.schemas import AnomalySignal, AnomalyType, Severity


def make_signal(
    anomaly_type: AnomalyType = AnomalyType.ROW_COUNT,
    severity: Severity = Severity.HIGH,
    confidence: float = 0.9,
    model: str = "fct_orders",
    run_id: str = "run-001",
) -> AnomalySignal:
    return AnomalySignal(
        anomaly_type=anomaly_type,
        source_agent="ingestion_monitor",
        pipeline_run_id=run_id,
        model_name=model,
        severity=severity,
        confidence=confidence,
        details={},
    )


def make_bucket(*signals: AnomalySignal, run_id: str = "run-001") -> SignalBucket:
    b = SignalBucket(run_id=run_id)
    b.signals = list(signals)
    return b


@pytest.fixture
def correlator():
    return Correlator()


# ── Single signal ─────────────────────────────────────────────

class TestSingleSignal:

    def test_single_signal_root_cause_is_type(self, correlator):
        bucket = make_bucket(make_signal(anomaly_type=AnomalyType.ROW_COUNT))
        result = correlator.correlate(bucket)
        assert result.root_cause == AnomalyType.ROW_COUNT.value
        assert result.needs_llm is False

    def test_single_signal_confidence_from_signal(self, correlator):
        bucket = make_bucket(make_signal(confidence=0.87))
        result = correlator.correlate(bucket)
        assert result.confidence == 0.87

    def test_single_signal_severity_from_signal(self, correlator):
        bucket = make_bucket(make_signal(severity=Severity.CRITICAL))
        result = correlator.correlate(bucket)
        assert result.severity == Severity.CRITICAL


# ── Rule-based correlation ─────────────────────────────────────

class TestRuleBasedCorrelation:

    def test_schema_drift_plus_row_count(self, correlator):
        bucket = make_bucket(
            make_signal(AnomalyType.SCHEMA_DRIFT),
            make_signal(AnomalyType.ROW_COUNT),
        )
        result = correlator.correlate(bucket)
        assert result.root_cause == "schema_drift"
        assert result.confidence >= 0.90
        assert result.needs_llm is False

    def test_schema_drift_plus_quality(self, correlator):
        bucket = make_bucket(
            make_signal(AnomalyType.SCHEMA_DRIFT),
            make_signal(AnomalyType.QUALITY_FAILURE),
        )
        result = correlator.correlate(bucket)
        assert result.root_cause == "schema_drift"
        assert result.needs_llm is False

    def test_schema_drift_plus_row_count_plus_quality(self, correlator):
        bucket = make_bucket(
            make_signal(AnomalyType.SCHEMA_DRIFT),
            make_signal(AnomalyType.ROW_COUNT),
            make_signal(AnomalyType.QUALITY_FAILURE),
        )
        result = correlator.correlate(bucket)
        assert result.root_cause == "schema_drift"
        assert result.confidence >= 0.95

    def test_checksum_mismatch_alone(self, correlator):
        bucket = make_bucket(make_signal(AnomalyType.CHECKSUM_MISMATCH))
        result = correlator.correlate(bucket)
        # Single signal path — root_cause = type value
        assert "checksum" in result.root_cause

    def test_freshness_plus_row_count(self, correlator):
        bucket = make_bucket(
            make_signal(AnomalyType.FRESHNESS_VIOLATION),
            make_signal(AnomalyType.ROW_COUNT),
        )
        result = correlator.correlate(bucket)
        assert result.root_cause == "freshness_violation"
        assert result.needs_llm is False

    def test_severity_is_highest_across_signals(self, correlator):
        bucket = make_bucket(
            make_signal(AnomalyType.SCHEMA_DRIFT, severity=Severity.MEDIUM),
            make_signal(AnomalyType.ROW_COUNT,    severity=Severity.CRITICAL),
        )
        result = correlator.correlate(bucket)
        assert result.severity == Severity.CRITICAL

    def test_affected_models_deduplicated(self, correlator):
        bucket = make_bucket(
            make_signal(model="fct_orders"),
            make_signal(model="fct_orders"),
            make_signal(model="fct_revenue"),
        )
        result = correlator.correlate(bucket)
        assert len(result.affected_models) == 2
        assert "fct_orders" in result.affected_models


# ── Novel pattern → needs LLM ─────────────────────────────────

class TestNovelPattern:

    def test_unknown_combination_needs_llm(self, correlator):
        """QUALITY_FAILURE + FRESHNESS_VIOLATION has no rule → needs LLM."""
        bucket = make_bucket(
            make_signal(AnomalyType.QUALITY_FAILURE),
            make_signal(AnomalyType.FRESHNESS_VIOLATION),
        )
        result = correlator.correlate(bucket)
        assert result.needs_llm is True
        assert result.root_cause == "unknown"
        assert result.confidence == 0.5


# ── Suppression ───────────────────────────────────────────────

class TestSuppression:

    def test_suppress_single_low_confidence_new_column(self, correlator):
        bucket = make_bucket(
            make_signal(
                anomaly_type=AnomalyType.SCHEMA_DRIFT,
                severity=Severity.LOW,
                confidence=0.5,
            )
        )
        assert correlator.should_suppress(bucket) is True

    def test_suppress_all_low_confidence(self, correlator):
        bucket = make_bucket(
            make_signal(severity=Severity.LOW, confidence=0.5),
            make_signal(severity=Severity.LOW, confidence=0.4),
        )
        assert correlator.should_suppress(bucket) is True

    def test_no_suppress_high_confidence(self, correlator):
        bucket = make_bucket(make_signal(severity=Severity.LOW, confidence=0.9))
        assert correlator.should_suppress(bucket) is False

    def test_no_suppress_high_severity(self, correlator):
        bucket = make_bucket(make_signal(severity=Severity.CRITICAL, confidence=0.3))
        assert correlator.should_suppress(bucket) is False

    def test_no_suppress_mixed_confidence(self, correlator):
        """One low + one high confidence → don't suppress."""
        bucket = make_bucket(
            make_signal(severity=Severity.LOW, confidence=0.3),
            make_signal(severity=Severity.HIGH, confidence=0.95),
        )
        assert correlator.should_suppress(bucket) is False
