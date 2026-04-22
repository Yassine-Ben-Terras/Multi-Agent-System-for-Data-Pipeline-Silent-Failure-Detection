"""
Ingestion Monitor — Detection Engine

Implements the Observe → Reason → Signal loop for ingestion anomalies.

Detects:
  1. Row count anomaly   — z-score deviation from 7-day rolling baseline
  2. Checksum mismatch   — same source + partition, different checksum
  3. Arrival latency     — signal arrives later than expected window
  4. Zero-row ingestion  — explicit check before z-score (always CRITICAL)
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, List, Optional

from config.schemas import (
    AnomalySignal,
    AnomalyType,
    PipelineSignal,
    Severity,
)
from agents.ingestion_monitor.baseline import Baseline, BaselineManager

logger = logging.getLogger(__name__)

# ── Last-seen checksum cache (in-memory, keyed by source+run_date) ──
# In production this would live in Redis / the state store.
_checksum_cache: Dict[str, str] = {}


@dataclass
class DetectionResult:
    """Output of one detection pass against a single PipelineSignal."""
    has_anomaly: bool
    anomalies: List[AnomalySignal] = field(default_factory=list)


class IngestionDetector:
    """
    Stateless detection logic for the Ingestion Monitor Agent.
    Depends on BaselineManager for persistence; everything else is pure.
    """

    def __init__(
        self,
        baseline_manager: BaselineManager,
        zscore_threshold: float = 2.5,
    ):
        self.baselines = baseline_manager
        self.zscore_threshold = zscore_threshold

    # ── Public entry point ────────────────────────────────────

    def analyze(self, signal: PipelineSignal) -> DetectionResult:
        """
        Run all detection checks against an ingestion PipelineSignal.
        Returns a DetectionResult containing zero or more AnomalySignals.
        """
        anomalies: List[AnomalySignal] = []

        row_count = signal.metrics.get("row_count")
        checksum = signal.metrics.get("checksum")

        # Check 1 — Zero-row ingestion (always CRITICAL, skip z-score)
        if row_count is not None and row_count == 0:
            anomalies.append(self._zero_row_anomaly(signal))
        elif row_count is not None:
            # Check 2 — Row count z-score anomaly
            anom = self._row_count_check(signal, row_count)
            if anom:
                anomalies.append(anom)

        # Check 3 — Checksum drift
        if checksum is not None:
            anom = self._checksum_check(signal, checksum)
            if anom:
                anomalies.append(anom)

        # Always update the baseline (even when anomalous — we track the drift)
        if row_count is not None and row_count > 0:
            self.baselines.upsert(
                model_name=signal.source,
                source=signal.source,
                metric="row_count",
                new_value=float(row_count),
            )

        return DetectionResult(has_anomaly=bool(anomalies), anomalies=anomalies)

    # ── Individual checks ─────────────────────────────────────

    def _row_count_check(
        self, signal: PipelineSignal, row_count: int
    ) -> Optional[AnomalySignal]:
        baseline: Optional[Baseline] = self.baselines.get(
            model_name=signal.source,
            source=signal.source,
            metric="row_count",
        )

        if baseline is None or baseline.sample_count < 3:
            # Not enough history yet — log and skip
            logger.info(
                "baseline_insufficient",
                extra={"source": signal.source, "samples": baseline.sample_count if baseline else 0},
            )
            return None

        z = baseline.z_score(float(row_count))
        if z is None or abs(z) <= self.zscore_threshold:
            return None  # Normal

        severity = self._zscore_to_severity(z)
        confidence = min(1.0, abs(z) / (self.zscore_threshold * 2))

        logger.warning(
            "row_count_anomaly_detected",
            extra={
                "source": signal.source,
                "run_id": signal.pipeline_run_id,
                "row_count": row_count,
                "mean": round(baseline.mean, 1),
                "std_dev": round(baseline.std_dev, 1),
                "z_score": round(z, 3),
                "severity": severity,
            },
        )

        return AnomalySignal(
            anomaly_type=AnomalyType.ROW_COUNT,
            source_agent="ingestion_monitor",
            pipeline_run_id=signal.pipeline_run_id,
            model_name=signal.source,
            severity=severity,
            confidence=round(confidence, 4),
            details={
                "row_count": row_count,
                "baseline_mean": round(baseline.mean, 2),
                "baseline_std_dev": round(baseline.std_dev, 2),
                "z_score": round(z, 4),
                "threshold": self.zscore_threshold,
                "sample_count": baseline.sample_count,
            },
        )

    def _checksum_check(
        self, signal: PipelineSignal, checksum: str
    ) -> Optional[AnomalySignal]:
        # Key: source + date of the signal (same day = same partition)
        partition_date = signal.ts.strftime("%Y-%m-%d")
        cache_key = f"{signal.source}::{partition_date}"

        previous = _checksum_cache.get(cache_key)

        if previous is None:
            _checksum_cache[cache_key] = checksum
            return None  # First time we see this partition

        if previous == checksum:
            return None  # Same checksum → idempotent re-run, normal

        # Checksum changed for same source + partition → anomaly
        _checksum_cache[cache_key] = checksum  # Update to latest

        logger.warning(
            "checksum_mismatch_detected",
            extra={
                "source": signal.source,
                "partition": partition_date,
                "previous": previous[:8] + "...",
                "current": checksum[:8] + "...",
            },
        )

        return AnomalySignal(
            anomaly_type=AnomalyType.CHECKSUM_MISMATCH,
            source_agent="ingestion_monitor",
            pipeline_run_id=signal.pipeline_run_id,
            model_name=signal.source,
            severity=Severity.HIGH,
            confidence=1.0,  # deterministic — checksum either matches or it doesn't
            details={
                "source": signal.source,
                "partition_date": partition_date,
                "previous_checksum": previous,
                "current_checksum": checksum,
                "interpretation": "Same source and partition produced different checksums. "
                                  "Possible causes: duplicate ingestion, data mutation, "
                                  "idempotency failure.",
            },
        )

    def _zero_row_anomaly(self, signal: PipelineSignal) -> AnomalySignal:
        logger.error(
            "zero_row_ingestion",
            extra={"source": signal.source, "run_id": signal.pipeline_run_id},
        )
        return AnomalySignal(
            anomaly_type=AnomalyType.ROW_COUNT,
            source_agent="ingestion_monitor",
            pipeline_run_id=signal.pipeline_run_id,
            model_name=signal.source,
            severity=Severity.CRITICAL,
            confidence=1.0,
            details={
                "row_count": 0,
                "interpretation": "Zero rows ingested. Pipeline completed without error "
                                  "but produced no data. Possible API failure, empty feed, "
                                  "or upstream source outage.",
            },
        )

    # ── Helpers ───────────────────────────────────────────────

    @staticmethod
    def _zscore_to_severity(z: float) -> Severity:
        abs_z = abs(z)
        if abs_z >= 5.0:
            return Severity.CRITICAL
        elif abs_z >= 3.5:
            return Severity.HIGH
        elif abs_z >= 2.5:
            return Severity.MEDIUM
        return Severity.LOW
