"""
Orchestrator — Correlator

Analyses a SignalBucket (all signals for one pipeline_run_id within the
5-minute window) and determines:

  1. Root cause  — the most likely upstream cause
  2. Severity    — highest across all signals (may be escalated by blast radius)
  3. Confidence  — how certain we are about the root cause

Correlation rules (priority order):
  SCHEMA_DRIFT + ROW_COUNT   → root cause = schema_drift
  SCHEMA_DRIFT + QUALITY     → root cause = schema_drift
  CHECKSUM_MISMATCH + *      → root cause = checksum_mismatch
  QUALITY_FAILURE alone      → root cause = quality_failure
  ROW_COUNT alone            → root cause = row_count_anomaly
  FRESHNESS_VIOLATION + *    → root cause = freshness_violation
  Single signal              → root cause = that signal's type

The LLM is called only when:
  - Multiple anomaly types are present AND rules are inconclusive
  - The pattern is novel (not seen in recent history)
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import List, Optional, Set

from config.schemas import AnomalySignal, AnomalyType, Severity
from agents.orchestrator.window import SignalBucket

logger = logging.getLogger(__name__)


# ── Correlation rules ─────────────────────────────────────────

# (frozenset of anomaly types present) → root cause string
# Evaluated in order; first match wins.
CORRELATION_RULES: List[tuple] = [
    # Schema drift explains both row count drop and quality failures
    (
        {AnomalyType.SCHEMA_DRIFT, AnomalyType.ROW_COUNT},
        "schema_drift",
        0.95,
    ),
    (
        {AnomalyType.SCHEMA_DRIFT, AnomalyType.QUALITY_FAILURE},
        "schema_drift",
        0.92,
    ),
    (
        {AnomalyType.SCHEMA_DRIFT, AnomalyType.ROW_COUNT, AnomalyType.QUALITY_FAILURE},
        "schema_drift",
        0.97,
    ),
    # Checksum mismatch → data mutation / idempotency failure
    (
        {AnomalyType.CHECKSUM_MISMATCH},
        "checksum_mismatch",
        0.99,
    ),
    # Freshness violation often explains row count drops
    (
        {AnomalyType.FRESHNESS_VIOLATION, AnomalyType.ROW_COUNT},
        "freshness_violation",
        0.90,
    ),
]


@dataclass
class CorrelationResult:
    root_cause: str
    confidence: float
    needs_llm: bool          # True → Orchestrator should call LLM for hypothesis
    severity: Severity
    signals: List[AnomalySignal]
    affected_models: List[str]


class Correlator:
    """
    Pure correlation logic. No I/O, no LLM calls.
    LLM call decision is returned as needs_llm flag.
    """

    def correlate(self, bucket: SignalBucket) -> CorrelationResult:
        signals = bucket.signals
        types_present: Set[AnomalyType] = {s.anomaly_type for s in signals}
        severity = bucket.highest_severity()
        affected_models = list({s.model_name for s in signals if s.model_name})

        # Single signal — trivial case
        if len(signals) == 1:
            s = signals[0]
            return CorrelationResult(
                root_cause=s.anomaly_type.value,
                confidence=s.confidence,
                needs_llm=False,
                severity=severity,
                signals=signals,
                affected_models=affected_models,
            )

        # Try rule-based correlation first
        for rule_types, root_cause, confidence in CORRELATION_RULES:
            # Match if rule types are a subset of what we observed
            if frozenset(rule_types).issubset(types_present):
                logger.info(
                    "correlation_rule_matched | root_cause=%s confidence=%.2f types=%s",
                    root_cause, confidence,
                    [t.value for t in types_present],
                )
                return CorrelationResult(
                    root_cause=root_cause,
                    confidence=confidence,
                    needs_llm=False,
                    severity=severity,
                    signals=signals,
                    affected_models=affected_models,
                )

        # No rule matched — novel pattern, needs LLM
        logger.info(
            "no_correlation_rule_matched | types=%s → flagging for LLM",
            [t.value for t in types_present],
        )
        return CorrelationResult(
            root_cause="unknown",
            confidence=0.5,
            needs_llm=True,
            severity=severity,
            signals=signals,
            affected_models=affected_models,
        )

    def should_suppress(self, bucket: SignalBucket) -> bool:
        """
        Return True if this bucket should be suppressed (not create an incident).
        Suppression cases:
          - All signals are LOW severity and confidence < 0.6
          - Bucket has only one signal of type NEW_COLUMN (harmless schema addition)
        """
        signals = bucket.signals

        # Suppress single LOW-confidence new column additions
        if (
            len(signals) == 1
            and signals[0].anomaly_type == AnomalyType.SCHEMA_DRIFT
            and signals[0].severity == Severity.LOW
            and signals[0].confidence < 0.6
        ):
            return True

        # Suppress if ALL signals are LOW severity with low confidence
        if all(
            s.severity == Severity.LOW and s.confidence < 0.6
            for s in signals
        ):
            return True

        return False
