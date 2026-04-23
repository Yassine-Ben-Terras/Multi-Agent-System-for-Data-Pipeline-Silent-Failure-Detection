"""
Quality Auditor — Detection Engine

Analyses parsed dbt RunResults and decides which failures
are genuine anomalies worth signalling.

Decision logic:
  1. Skip passing tests immediately
  2. Record every result in flakiness history
  3. For each failed test:
     a. Check if historically flaky → downgrade severity + flag as flaky
     b. Apply agent_severity from dbt meta if present
     c. Default severity based on failure count thresholds
  4. Return one AnomalySignal per failing model (aggregated, not per-test)
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Dict, List, Optional

from config.schemas import AnomalySignal, AnomalyType, Severity
from agents.quality_auditor.flakiness import FlakinessTracker, TestHistory
from agents.quality_auditor.parser import RunResults, TestResult

logger = logging.getLogger(__name__)

# How many failing rows before escalating severity
_FAILURE_COUNT_THRESHOLDS = {
    Severity.LOW:      1,
    Severity.MEDIUM:   100,
    Severity.HIGH:     10_000,
    Severity.CRITICAL: 100_000,
}

# Map dbt meta agent_severity strings → Severity enum
_META_SEVERITY_MAP = {
    "critical": Severity.CRITICAL,
    "high":     Severity.HIGH,
    "medium":   Severity.MEDIUM,
    "low":      Severity.LOW,
}


@dataclass
class FailedTestContext:
    """Enriched context for one failing dbt test."""
    test: TestResult
    history: TestHistory
    computed_severity: Severity
    is_flaky: bool
    is_new_test: bool


@dataclass
class QualityAnalysisResult:
    """Output of one quality analysis pass for a dbt run."""
    run_id: str
    total_tests: int
    failed_tests: int
    signals: List[AnomalySignal] = field(default_factory=list)

    @property
    def has_failures(self) -> bool:
        return bool(self.signals)


class QualityDetector:
    """
    Stateless detection logic for the Quality Auditor Agent.
    Depends on FlakinessTracker for history; everything else is pure.
    """

    def __init__(self, flakiness_tracker: FlakinessTracker):
        self._flakiness = flakiness_tracker

    # ── Public entry point ────────────────────────────────────

    def analyze(self, run_results: RunResults) -> QualityAnalysisResult:
        """
        Analyse a full dbt RunResults object.
        Returns one AnomalySignal per model that has genuine failures.
        """
        # Step 1 — Record all results in history (pass and fail)
        for test in run_results.results:
            self._flakiness.record(
                model_name=test.model_name,
                test_name=test.test_name,
                run_id=run_results.run_id,
                passed=not test.failed,
                failure_count=test.failures,
            )

        # Step 2 — Enrich each failed test with history + severity
        failed_contexts: List[FailedTestContext] = []
        for test in run_results.failed_tests:
            history = self._flakiness.get_history(test.model_name, test.test_name)
            ctx = self._enrich(test, history)
            failed_contexts.append(ctx)

        # Step 3 — Group by model and emit one signal per model
        signals = self._aggregate_by_model(failed_contexts, run_results.run_id)

        result = QualityAnalysisResult(
            run_id=run_results.run_id,
            total_tests=len(run_results.results),
            failed_tests=len(run_results.failed_tests),
            signals=signals,
        )

        if result.has_failures:
            logger.warning(
                "quality_failures_detected | run=%s total=%s failed=%s signals=%s",
                run_results.run_id, result.total_tests, result.failed_tests, len(signals),
            )
        else:
            logger.info(
                "quality_run_clean | run=%s total_tests=%s",
                run_results.run_id, result.total_tests,
            )

        return result

    # ── Enrichment ────────────────────────────────────────────

    def _enrich(self, test: TestResult, history: TestHistory) -> FailedTestContext:
        """Compute severity for one failed test, incorporating history and meta."""
        is_flaky    = history.is_flaky
        is_new_test = history.is_new_test

        # Priority 1: explicit severity from dbt meta annotation
        if test.agent_severity and test.agent_severity in _META_SEVERITY_MAP:
            severity = _META_SEVERITY_MAP[test.agent_severity]
        else:
            # Priority 2: derive from failure count
            severity = self._severity_from_count(test.failures)

        # Downgrade if historically flaky (avoid alert fatigue)
        if is_flaky and severity in (Severity.HIGH, Severity.CRITICAL):
            logger.info(
                "severity_downgraded_flaky_test | model=%s test=%s original=%s rate=%.3f",
                test.model_name, test.test_name, severity.value, history.flakiness_rate,
            )
            severity = Severity.MEDIUM

        return FailedTestContext(
            test=test,
            history=history,
            computed_severity=severity,
            is_flaky=is_flaky,
            is_new_test=is_new_test,
        )

    def _aggregate_by_model(
        self,
        contexts: List[FailedTestContext],
        run_id: str,
    ) -> List[AnomalySignal]:
        """
        Group failed tests by model_name.
        Emit one AnomalySignal per model, severity = highest across its tests.
        """
        by_model: Dict[str, List[FailedTestContext]] = {}
        for ctx in contexts:
            by_model.setdefault(ctx.test.model_name, []).append(ctx)

        signals = []
        for model_name, model_contexts in by_model.items():
            signal = self._build_signal(model_name, model_contexts, run_id)
            signals.append(signal)

        return signals

    def _build_signal(
        self,
        model_name: str,
        contexts: List[FailedTestContext],
        run_id: str,
    ) -> AnomalySignal:
        severity_order = [Severity.LOW, Severity.MEDIUM, Severity.HIGH, Severity.CRITICAL]
        overall = max(contexts, key=lambda c: severity_order.index(c.computed_severity)).computed_severity

        flaky_tests  = [c for c in contexts if c.is_flaky]
        genuine_tests = [c for c in contexts if not c.is_flaky]

        details = {
            "total_failed_tests": len(contexts),
            "genuine_failures":   len(genuine_tests),
            "flaky_failures":     len(flaky_tests),
            "tests": [
                {
                    "test_name":    c.test.test_name,
                    "column":       c.test.column_name,
                    "failures":     c.test.failures,
                    "severity":     c.computed_severity.value,
                    "is_flaky":     c.is_flaky,
                    "is_new_test":  c.is_new_test,
                    "blast_radius": c.test.blast_radius,
                    "oncall_team":  c.test.oncall_team,
                    "message":      c.test.message,
                }
                for c in contexts
            ],
        }

        return AnomalySignal(
            anomaly_type=AnomalyType.QUALITY_FAILURE,
            source_agent="quality_auditor",
            pipeline_run_id=run_id,
            model_name=model_name,
            severity=overall,
            confidence=0.95 if genuine_tests else 0.5,
            details=details,
        )

    # ── Helpers ───────────────────────────────────────────────

    @staticmethod
    def _severity_from_count(failure_count: int) -> Severity:
        """Map raw dbt failure row count to a severity level."""
        if failure_count >= _FAILURE_COUNT_THRESHOLDS[Severity.CRITICAL]:
            return Severity.CRITICAL
        elif failure_count >= _FAILURE_COUNT_THRESHOLDS[Severity.HIGH]:
            return Severity.HIGH
        elif failure_count >= _FAILURE_COUNT_THRESHOLDS[Severity.MEDIUM]:
            return Severity.MEDIUM
        return Severity.LOW
