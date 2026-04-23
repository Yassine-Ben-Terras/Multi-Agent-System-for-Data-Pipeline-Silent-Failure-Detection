"""
Unit tests for FlakinessTracker — flakiness rate and is_flaky logic.
DB is mocked.
"""

from __future__ import annotations
import pytest
from agents.quality_auditor.flakiness import TestHistory, FLAKINESS_THRESHOLD


class TestTestHistory:

    def test_flakiness_rate_zero(self):
        h = TestHistory("m", "t", total_runs=20, failure_count=0)
        assert h.flakiness_rate == 0.0

    def test_flakiness_rate_calculation(self):
        h = TestHistory("m", "t", total_runs=20, failure_count=4)
        assert h.flakiness_rate == pytest.approx(0.20)

    def test_is_flaky_above_threshold(self):
        # 30% failure rate, 20 runs → flaky
        h = TestHistory("m", "t", total_runs=20, failure_count=6)
        assert h.is_flaky is True

    def test_is_flaky_at_threshold_not_flaky(self):
        # Exactly 20% → not flaky (threshold is strict >)
        h = TestHistory("m", "t", total_runs=20, failure_count=4)
        assert h.is_flaky is False

    def test_is_flaky_insufficient_runs(self):
        # Only 4 runs — not enough to judge flakiness
        h = TestHistory("m", "t", total_runs=4, failure_count=4)
        assert h.is_flaky is False

    def test_is_new_test(self):
        h = TestHistory("m", "t", total_runs=3, failure_count=0)
        assert h.is_new_test is True

    def test_is_not_new_test(self):
        h = TestHistory("m", "t", total_runs=5, failure_count=0)
        assert h.is_new_test is False

    def test_zero_runs_rate(self):
        h = TestHistory("m", "t", total_runs=0, failure_count=0)
        assert h.flakiness_rate == 0.0
        assert h.is_flaky is False
