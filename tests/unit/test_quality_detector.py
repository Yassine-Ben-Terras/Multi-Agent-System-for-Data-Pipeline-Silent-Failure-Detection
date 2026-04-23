"""
Unit tests for QualityDetector.
FlakinessTracker is mocked — pure detection logic tests.
"""

from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest

from agents.quality_auditor.detector import QualityDetector
from agents.quality_auditor.flakiness import TestHistory
from agents.quality_auditor.parser import RunResults, TestResult
from config.schemas import AnomalyType, Severity


# ── Helpers ───────────────────────────────────────────────────

def make_test(
    model: str = "fct_orders",
    test_name: str = "not_null",
    status: str = "fail",
    failures: int = 100,
    agent_severity: str = None,
    blast_radius: str = None,
    oncall_team: str = None,
    column: str = "amount",
) -> TestResult:
    return TestResult(
        unique_id=f"test.project.{test_name}_{model}_{column}",
        test_name=test_name,
        model_name=model,
        column_name=column,
        status=status,
        failures=failures,
        execution_time=1.0,
        message=f"Got {failures} results",
        agent_severity=agent_severity,
        blast_radius=blast_radius,
        oncall_team=oncall_team,
    )


def make_run(tests: list[TestResult], run_id: str = "run-001") -> RunResults:
    return RunResults(
        generated_at=datetime(2026, 4, 22, tzinfo=timezone.utc),
        dbt_version="1.8.0",
        run_id=run_id,
        results=tests,
    )


def clean_history(model: str = "fct_orders", test: str = "not_null") -> TestHistory:
    return TestHistory(model_name=model, test_name=test, total_runs=20, failure_count=0)


def flaky_history(model: str = "fct_orders", test: str = "not_null") -> TestHistory:
    """Flaky: 30% failure rate over 20 runs."""
    return TestHistory(model_name=model, test_name=test, total_runs=20, failure_count=6)


def new_test_history(model: str = "fct_orders", test: str = "not_null") -> TestHistory:
    """Not enough runs to judge flakiness."""
    return TestHistory(model_name=model, test_name=test, total_runs=2, failure_count=1)


@pytest.fixture
def mock_tracker():
    return MagicMock()


@pytest.fixture
def detector(mock_tracker):
    return QualityDetector(flakiness_tracker=mock_tracker)


# ── Clean runs ────────────────────────────────────────────────

class TestCleanRun:

    def test_all_passing_no_signals(self, detector, mock_tracker):
        mock_tracker.get_history.return_value = clean_history()
        run = make_run([make_test(status="pass", failures=0)])
        result = detector.analyze(run)
        assert not result.has_failures
        assert result.signals == []

    def test_all_history_recorded(self, detector, mock_tracker):
        """record() must be called for every test, pass or fail."""
        mock_tracker.get_history.return_value = clean_history()
        tests = [
            make_test(status="pass", failures=0),
            make_test(status="fail", failures=10),
        ]
        detector.analyze(make_run(tests))
        assert mock_tracker.record.call_count == 2


# ── Severity from failure count ───────────────────────────────

class TestSeverityFromCount:

    @pytest.mark.parametrize("failures, expected", [
        (1,       Severity.LOW),
        (99,      Severity.LOW),
        (100,     Severity.MEDIUM),
        (9_999,   Severity.MEDIUM),
        (10_000,  Severity.HIGH),
        (99_999,  Severity.HIGH),
        (100_000, Severity.CRITICAL),
    ])
    def test_severity_thresholds(self, failures, expected):
        from agents.quality_auditor.detector import QualityDetector as QD
        assert QD._severity_from_count(failures) == expected


# ── Meta annotation severity ──────────────────────────────────

class TestMetaSeverity:

    def test_meta_severity_overrides_count(self, detector, mock_tracker):
        """agent_severity=critical in dbt meta → CRITICAL regardless of row count."""
        mock_tracker.get_history.return_value = clean_history()
        test = make_test(failures=1, agent_severity="critical")  # only 1 failure row
        result = detector.analyze(make_run([test]))
        assert result.signals[0].severity == Severity.CRITICAL

    def test_meta_severity_low(self, detector, mock_tracker):
        mock_tracker.get_history.return_value = clean_history()
        test = make_test(failures=200_000, agent_severity="low")
        result = detector.analyze(make_run([test]))
        assert result.signals[0].severity == Severity.LOW


# ── Flakiness downgrade ───────────────────────────────────────

class TestFlakinessDowngrade:

    def test_flaky_high_downgraded_to_medium(self, detector, mock_tracker):
        mock_tracker.get_history.return_value = flaky_history()
        test = make_test(failures=10_000)  # would be HIGH without flakiness
        result = detector.analyze(make_run([test]))
        assert result.signals[0].severity == Severity.MEDIUM

    def test_flaky_low_not_downgraded(self, detector, mock_tracker):
        """LOW stays LOW even for flaky tests."""
        mock_tracker.get_history.return_value = flaky_history()
        test = make_test(failures=1)  # LOW
        result = detector.analyze(make_run([test]))
        assert result.signals[0].severity == Severity.LOW

    def test_new_test_not_treated_as_flaky(self, detector, mock_tracker):
        """< 5 runs → not enough history, no flakiness penalty."""
        mock_tracker.get_history.return_value = new_test_history()
        test = make_test(failures=10_000)  # HIGH
        result = detector.analyze(make_run([test]))
        assert result.signals[0].severity == Severity.HIGH


# ── Aggregation by model ──────────────────────────────────────

class TestModelAggregation:

    def test_one_signal_per_model(self, detector, mock_tracker):
        mock_tracker.get_history.return_value = clean_history()
        tests = [
            make_test(model="fct_orders",  test_name="not_null",  failures=100),
            make_test(model="fct_orders",  test_name="unique",    failures=50),
            make_test(model="fct_revenue", test_name="not_null",  failures=200),
        ]
        result = detector.analyze(make_run(tests))
        assert len(result.signals) == 2
        models = {s.model_name for s in result.signals}
        assert "fct_orders" in models
        assert "fct_revenue" in models

    def test_model_signal_severity_is_highest(self, detector, mock_tracker):
        """fct_orders has LOW + HIGH tests → signal severity = HIGH."""
        mock_tracker.get_history.return_value = clean_history()
        tests = [
            make_test(model="fct_orders", failures=1,      test_name="not_null"),   # LOW
            make_test(model="fct_orders", failures=10_000, test_name="unique"),     # HIGH
        ]
        result = detector.analyze(make_run(tests))
        orders_signal = next(s for s in result.signals if s.model_name == "fct_orders")
        assert orders_signal.severity == Severity.HIGH

    def test_signal_details_flaky_vs_genuine(self, detector, mock_tracker):
        """Signal details correctly splits genuine vs flaky failures."""
        def history_side_effect(model_name, test_name):
            if test_name == "not_null":
                return flaky_history(model=model_name, test=test_name)
            return clean_history(model=model_name, test=test_name)

        mock_tracker.get_history.side_effect = history_side_effect

        tests = [
            make_test(model="fct_orders", test_name="not_null", failures=100),   # flaky
            make_test(model="fct_orders", test_name="unique",   failures=100),   # genuine
        ]
        result = detector.analyze(make_run(tests))
        details = result.signals[0].details
        assert details["genuine_failures"] == 1
        assert details["flaky_failures"]   == 1

    def test_confidence_lower_when_all_flaky(self, detector, mock_tracker):
        """All failures are flaky → confidence = 0.5."""
        mock_tracker.get_history.return_value = flaky_history()
        test = make_test(failures=100)
        result = detector.analyze(make_run([test]))
        assert result.signals[0].confidence == 0.5

    def test_confidence_high_when_genuine(self, detector, mock_tracker):
        """Genuine failures → confidence = 0.95."""
        mock_tracker.get_history.return_value = clean_history()
        test = make_test(failures=100)
        result = detector.analyze(make_run([test]))
        assert result.signals[0].confidence == 0.95

    def test_signal_anomaly_type(self, detector, mock_tracker):
        mock_tracker.get_history.return_value = clean_history()
        result = detector.analyze(make_run([make_test(failures=100)]))
        assert result.signals[0].anomaly_type == AnomalyType.QUALITY_FAILURE
