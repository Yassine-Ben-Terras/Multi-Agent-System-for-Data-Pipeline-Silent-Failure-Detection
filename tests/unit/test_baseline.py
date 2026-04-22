"""
Unit tests for BaselineManager — Welford online algorithm and z-score computation.
DB is mocked: tests validate the math, not the SQL.
"""

from __future__ import annotations

import math
from unittest.mock import MagicMock, patch

import pytest

from agents.ingestion_monitor.baseline import Baseline, BaselineManager


# ── Baseline z-score tests ────────────────────────────────────

class TestBaseline:

    def test_z_score_positive(self):
        b = Baseline("m", "s", "row_count", mean=100.0, std_dev=10.0, sample_count=10)
        assert b.z_score(120.0) == pytest.approx(2.0)

    def test_z_score_negative(self):
        b = Baseline("m", "s", "row_count", mean=100.0, std_dev=10.0, sample_count=10)
        assert b.z_score(80.0) == pytest.approx(-2.0)

    def test_z_score_zero_std_dev(self):
        b = Baseline("m", "s", "row_count", mean=100.0, std_dev=0.0, sample_count=5)
        assert b.z_score(120.0) is None

    def test_is_anomalous_above_threshold(self):
        b = Baseline("m", "s", "row_count", mean=100.0, std_dev=10.0, sample_count=10)
        assert b.is_anomalous(130.0, threshold=2.5) is True

    def test_is_anomalous_below_threshold(self):
        b = Baseline("m", "s", "row_count", mean=100.0, std_dev=10.0, sample_count=10)
        assert b.is_anomalous(110.0, threshold=2.5) is False


# ── BaselineManager Welford algorithm tests ───────────────────

class TestBaselineManagerWelford:
    """
    Test the online mean/variance update (Welford's algorithm) in isolation.
    We mock the DB _persist and get methods to test pure math.
    """

    def _make_manager(self):
        mgr = BaselineManager.__new__(BaselineManager)
        mgr._engine = MagicMock()
        return mgr

    def test_first_observation_initialises_baseline(self):
        mgr = self._make_manager()

        with patch.object(mgr, "get", return_value=None), \
             patch.object(mgr, "_persist") as mock_persist:

            result = mgr.upsert("model", "source", "row_count", 100_000.0)

        assert result.mean == pytest.approx(100_000.0)
        assert result.std_dev == 0.0
        assert result.sample_count == 1
        mock_persist.assert_called_once()

    def test_second_observation_updates_mean(self):
        mgr = self._make_manager()
        from agents.ingestion_monitor.baseline import Baseline as B

        existing = B("m", "s", "row_count", mean=100_000.0, std_dev=0.0, sample_count=1)

        with patch.object(mgr, "get", return_value=existing), \
             patch.object(mgr, "_persist"):

            result = mgr.upsert("m", "s", "row_count", 110_000.0)

        # After 2 observations [100k, 110k]: mean = 105k
        assert result.mean == pytest.approx(105_000.0)
        assert result.sample_count == 2

    def test_welford_converges_to_correct_stddev(self):
        """
        Feed 5 known values and verify the final std dev matches numpy.
        Values: [10, 20, 30, 40, 50] → mean=30, std=std([10,20,30,40,50])
        """
        import numpy as np
        mgr = self._make_manager()
        values = [10.0, 20.0, 30.0, 40.0, 50.0]
        expected_std = float(np.std(values, ddof=1))  # sample std dev

        # Simulate sequential upserts
        baseline = None
        for v in values:
            with patch.object(mgr, "get", return_value=baseline), \
                 patch.object(mgr, "_persist"):
                baseline = mgr.upsert("m", "s", "row_count", v)

        assert baseline.mean == pytest.approx(30.0)
        assert baseline.std_dev == pytest.approx(expected_std, rel=1e-5)
        assert baseline.sample_count == 5
