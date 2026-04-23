"""
Quality Auditor — Flakiness Tracker

Tracks historical dbt test results in PostgreSQL to distinguish
genuine failures from historically flaky tests.

A test is considered "flaky" if it has failed in more than
FLAKINESS_THRESHOLD of its recent runs (default: 20%).

This context is passed to the Orchestrator so it can deprioritise
alerts for known-flaky tests and avoid paging on-call for noise.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Optional

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

logger = logging.getLogger(__name__)

# A test is flaky if it failed in > this fraction of recent runs
FLAKINESS_THRESHOLD = 0.20
# How many recent runs to consider for flakiness computation
FLAKINESS_WINDOW = 20


@dataclass
class TestHistory:
    model_name: str
    test_name: str
    total_runs: int
    failure_count: int

    @property
    def flakiness_rate(self) -> float:
        if self.total_runs == 0:
            return 0.0
        return self.failure_count / self.total_runs

    @property
    def is_flaky(self) -> bool:
        return (
            self.total_runs >= 5           # need at least 5 runs to judge
            and self.flakiness_rate > FLAKINESS_THRESHOLD
        )

    @property
    def is_new_test(self) -> bool:
        return self.total_runs < 5


class FlakinessTracker:
    """
    Reads and writes test run history to the PostgreSQL state store.
    Uses the existing test_history table from init.sql.
    """

    def __init__(self, db_url: str):
        self._engine: Engine = create_engine(db_url, pool_pre_ping=True)

    # ── Read ──────────────────────────────────────────────────

    def get_history(self, model_name: str, test_name: str) -> TestHistory:
        """
        Return flakiness stats for a (model, test) pair
        over the last FLAKINESS_WINDOW runs.
        """
        sql = text("""
            SELECT
                COUNT(*)                          AS total_runs,
                SUM(CASE WHEN passed = FALSE THEN 1 ELSE 0 END) AS failure_count
            FROM (
                SELECT passed
                FROM test_history
                WHERE model_name = :model AND test_name = :test
                ORDER BY run_at DESC
                LIMIT :window
            ) recent
        """)
        with self._engine.connect() as conn:
            row = conn.execute(sql, {
                "model": model_name,
                "test": test_name,
                "window": FLAKINESS_WINDOW,
            }).fetchone()

        total    = int(row.total_runs)    if row and row.total_runs    else 0
        failures = int(row.failure_count) if row and row.failure_count else 0

        return TestHistory(
            model_name=model_name,
            test_name=test_name,
            total_runs=total,
            failure_count=failures,
        )

    # ── Write ─────────────────────────────────────────────────

    def record(
        self,
        model_name: str,
        test_name: str,
        run_id: str,
        passed: bool,
        failure_count: int = 0,
    ) -> None:
        """Append one test run to the history table."""
        sql = text("""
            INSERT INTO test_history
                (model_name, test_name, run_id, passed, failure_rate, run_at)
            VALUES
                (:model, :test, :run_id, :passed, :rate, NOW())
        """)
        with self._engine.begin() as conn:
            conn.execute(sql, {
                "model": model_name,
                "test": test_name,
                "run_id": run_id,
                "passed": passed,
                "rate": float(failure_count),
            })
        logger.debug(
            "test_history_recorded",
            model=model_name,
            test=test_name,
            passed=passed,
        )
