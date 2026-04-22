"""
Ingestion Monitor — Baseline Manager

Manages the rolling 7-day statistical baselines for each (model, source, metric)
combination. Baselines are persisted in PostgreSQL and used to compute z-scores
for anomaly detection.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Optional

import numpy as np
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

logger = logging.getLogger(__name__)


@dataclass
class Baseline:
    model_name: str
    source: str
    metric: str
    mean: float
    std_dev: float
    sample_count: int
    window_days: int = 7

    def z_score(self, value: float) -> Optional[float]:
        """Return z-score of value against baseline. None if std_dev is zero."""
        if self.std_dev == 0 or self.std_dev is None:
            return None
        return (value - self.mean) / self.std_dev

    def is_anomalous(self, value: float, threshold: float = 2.5) -> bool:
        """Return True if abs(z-score) exceeds threshold."""
        z = self.z_score(value)
        if z is None:
            return False
        return abs(z) > threshold


class BaselineManager:
    """
    Reads and updates statistical baselines in the PostgreSQL state store.

    Schema: model_baselines(model_name, source, metric, mean, std_dev, sample_count, ...)
    """

    def __init__(self, db_url: str):
        self._engine: Engine = create_engine(db_url, pool_pre_ping=True)

    # ── Read ──────────────────────────────────────────────────

    def get(self, model_name: str, source: str, metric: str) -> Optional[Baseline]:
        """Retrieve baseline for a (model, source, metric) triple. Returns None if not found."""
        sql = text("""
            SELECT model_name, source, metric, mean, std_dev, sample_count, window_days
            FROM model_baselines
            WHERE model_name = :model AND source = :source AND metric = :metric
        """)
        with self._engine.connect() as conn:
            row = conn.execute(sql, {"model": model_name, "source": source, "metric": metric}).fetchone()

        if row is None:
            return None

        return Baseline(
            model_name=row.model_name,
            source=row.source,
            metric=row.metric,
            mean=row.mean,
            std_dev=row.std_dev,
            sample_count=row.sample_count,
            window_days=row.window_days,
        )

    # ── Update ────────────────────────────────────────────────

    def upsert(self, model_name: str, source: str, metric: str, new_value: float) -> Baseline:
        """
        Welford's online algorithm for incrementally updating mean and variance.
        Upserts the baseline in the state store and returns the updated Baseline.
        """
        existing = self.get(model_name, source, metric)

        if existing is None:
            # First observation — initialise baseline with no variance
            new_baseline = Baseline(
                model_name=model_name,
                source=source,
                metric=metric,
                mean=new_value,
                std_dev=0.0,
                sample_count=1,
            )
        else:
            # Welford's online update
            n = existing.sample_count + 1
            delta = new_value - existing.mean
            new_mean = existing.mean + delta / n
            delta2 = new_value - new_mean

            # Running sum of squares of differences
            # We store std_dev; reconstruct M2 = std_dev^2 * (n-1)
            old_m2 = (existing.std_dev ** 2) * (existing.sample_count - 1) if existing.sample_count > 1 else 0.0
            new_m2 = old_m2 + delta * delta2
            new_std = float(np.sqrt(new_m2 / (n - 1))) if n > 1 else 0.0

            new_baseline = Baseline(
                model_name=model_name,
                source=source,
                metric=metric,
                mean=new_mean,
                std_dev=new_std,
                sample_count=n,
            )

        self._persist(new_baseline)
        logger.debug(
            "baseline_updated",
            extra={
                "model": model_name,
                "source": source,
                "metric": metric,
                "mean": new_baseline.mean,
                "std_dev": new_baseline.std_dev,
                "n": new_baseline.sample_count,
            },
        )
        return new_baseline

    def _persist(self, b: Baseline) -> None:
        sql = text("""
            INSERT INTO model_baselines (model_name, source, metric, mean, std_dev, sample_count, last_updated)
            VALUES (:model, :source, :metric, :mean, :std_dev, :n, NOW())
            ON CONFLICT (model_name, source, metric)
            DO UPDATE SET
                mean         = EXCLUDED.mean,
                std_dev      = EXCLUDED.std_dev,
                sample_count = EXCLUDED.sample_count,
                last_updated = NOW()
        """)
        with self._engine.begin() as conn:
            conn.execute(sql, {
                "model": b.model_name,
                "source": b.source,
                "metric": b.metric,
                "mean": b.mean,
                "std_dev": b.std_dev,
                "n": b.sample_count,
            })
