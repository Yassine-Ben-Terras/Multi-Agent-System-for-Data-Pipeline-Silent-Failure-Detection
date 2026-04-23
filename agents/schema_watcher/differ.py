"""
Schema Watcher — Diff Engine

Pure comparison logic between an incoming schema snapshot
and the registered contract. No I/O, fully testable.

Detects four change types, each with a default severity:
  DROPPED_COLUMN   → CRITICAL  (data loss, downstream JOINs break)
  TYPE_CHANGED     → HIGH      (silent cast failures, precision loss)
  NULLABLE_CHANGED → MEDIUM    (NOT NULL → nullable = trust degradation)
  NEW_COLUMN       → LOW       (additive, usually safe)

Severity is escalated to CRITICAL for columns marked criticality='critical'
in the registered contract (e.g. primary keys, revenue fields).
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Dict, List, Optional

from config.schemas import AnomalySignal, AnomalyType, Severity
from agents.schema_watcher.contract_store import ColumnDef, SchemaContract

logger = logging.getLogger(__name__)

# ── Change types ──────────────────────────────────────────────

class ChangeType:
    DROPPED_COLUMN   = "dropped_column"
    TYPE_CHANGED     = "type_changed"
    NULLABLE_CHANGED = "nullable_changed"
    NEW_COLUMN       = "new_column"


# Default severity per change type
_DEFAULT_SEVERITY: Dict[str, Severity] = {
    ChangeType.DROPPED_COLUMN:   Severity.CRITICAL,
    ChangeType.TYPE_CHANGED:     Severity.HIGH,
    ChangeType.NULLABLE_CHANGED: Severity.MEDIUM,
    ChangeType.NEW_COLUMN:       Severity.LOW,
}


@dataclass
class ColumnChange:
    column_name: str
    change_type: str
    severity: Severity
    details: Dict


@dataclass
class DiffResult:
    source: str
    has_drift: bool
    changes: List[ColumnChange] = field(default_factory=list)
    overall_severity: Optional[Severity] = None

    def to_anomaly_signal(self, pipeline_run_id: str, source_agent: str = "schema_watcher") -> AnomalySignal:
        """Convert a DiffResult with drift into an AnomalySignal."""
        assert self.has_drift, "Cannot create anomaly signal from clean diff"

        # Confidence: 1.0 for deterministic schema diffs
        return AnomalySignal(
            anomaly_type=AnomalyType.SCHEMA_DRIFT,
            source_agent=source_agent,
            pipeline_run_id=pipeline_run_id,
            model_name=self.source,
            severity=self.overall_severity,
            confidence=1.0,
            details={
                "total_changes": len(self.changes),
                "changes": [
                    {
                        "column": c.column_name,
                        "change_type": c.change_type,
                        "severity": c.severity.value,
                        **c.details,
                    }
                    for c in self.changes
                ],
                "summary": self._summary(),
            },
        )

    def _summary(self) -> str:
        counts = {}
        for c in self.changes:
            counts[c.change_type] = counts.get(c.change_type, 0) + 1
        parts = [f"{v} {k.replace('_', ' ')}" for k, v in counts.items()]
        return f"Schema drift in '{self.source}': {', '.join(parts)}"


class SchemaDiffer:
    """
    Compares an incoming schema snapshot against a registered contract.
    Returns a DiffResult describing every detected change.
    """

    def diff(
        self,
        source: str,
        contract: SchemaContract,
        incoming_columns: List[ColumnDef],
    ) -> DiffResult:
        """
        Compare incoming_columns against contract.
        Returns DiffResult with all detected changes.
        """
        contract_map: Dict[str, ColumnDef] = contract.as_dict()
        incoming_map: Dict[str, ColumnDef] = {col.name: col for col in incoming_columns}

        changes: List[ColumnChange] = []

        # ── Check 1: Dropped columns (in contract, missing from incoming)
        for col_name, contract_col in contract_map.items():
            if col_name not in incoming_map:
                severity = self._escalate_if_critical(
                    ChangeType.DROPPED_COLUMN, contract_col
                )
                changes.append(ColumnChange(
                    column_name=col_name,
                    change_type=ChangeType.DROPPED_COLUMN,
                    severity=severity,
                    details={
                        "contract_type": contract_col.dtype,
                        "criticality": contract_col.criticality,
                        "impact": "downstream models referencing this column will fail silently",
                    },
                ))
                logger.warning(
                    "column_dropped",
                    extra={"source": source, "column": col_name, "severity": severity.value},
                )

        # ── Check 2: Type changes and nullable changes (present in both)
        for col_name, incoming_col in incoming_map.items():
            if col_name not in contract_map:
                continue  # New column — handled below

            contract_col = contract_map[col_name]

            # Type changed
            if self._types_differ(contract_col.dtype, incoming_col.dtype):
                severity = self._escalate_if_critical(
                    ChangeType.TYPE_CHANGED, contract_col
                )
                changes.append(ColumnChange(
                    column_name=col_name,
                    change_type=ChangeType.TYPE_CHANGED,
                    severity=severity,
                    details={
                        "contract_type": contract_col.dtype,
                        "incoming_type": incoming_col.dtype,
                        "criticality": contract_col.criticality,
                        "impact": "silent cast failures or precision loss in downstream transforms",
                    },
                ))
                logger.warning(
                    "column_type_changed",
                    extra={
                        "source": source,
                        "column": col_name,
                        "from": contract_col.dtype,
                        "to": incoming_col.dtype,
                    },
                )

            # Nullable changed (NOT NULL → nullable is a trust degradation)
            elif (not contract_col.nullable) and incoming_col.nullable:
                severity = self._escalate_if_critical(
                    ChangeType.NULLABLE_CHANGED, contract_col
                )
                changes.append(ColumnChange(
                    column_name=col_name,
                    change_type=ChangeType.NULLABLE_CHANGED,
                    severity=severity,
                    details={
                        "contract_nullable": contract_col.nullable,
                        "incoming_nullable": incoming_col.nullable,
                        "criticality": contract_col.criticality,
                        "impact": "column previously guaranteed non-null, now allows nulls",
                    },
                ))

        # ── Check 3: New columns (in incoming, missing from contract)
        for col_name, incoming_col in incoming_map.items():
            if col_name not in contract_map:
                changes.append(ColumnChange(
                    column_name=col_name,
                    change_type=ChangeType.NEW_COLUMN,
                    severity=Severity.LOW,
                    details={
                        "incoming_type": incoming_col.dtype,
                        "incoming_nullable": incoming_col.nullable,
                        "impact": "additive change — generally safe but contract should be updated",
                    },
                ))

        if not changes:
            return DiffResult(source=source, has_drift=False)

        overall = self._highest_severity(changes)
        return DiffResult(
            source=source,
            has_drift=True,
            changes=changes,
            overall_severity=overall,
        )

    # ── Helpers ───────────────────────────────────────────────

    @staticmethod
    def _escalate_if_critical(change_type: str, contract_col: ColumnDef) -> Severity:
        """Escalate to CRITICAL if the column is marked as critical in the contract."""
        base = _DEFAULT_SEVERITY[change_type]
        if contract_col.criticality == "critical" and base != Severity.CRITICAL:
            return Severity.CRITICAL
        return base


    @staticmethod
    def _types_differ(a: str, b: str) -> bool:
        """Case-insensitive type comparison, strips whitespace."""
        return a.strip().upper() != b.strip().upper()


    @staticmethod
    def _highest_severity(changes: List[ColumnChange]) -> Severity:
        order = [Severity.LOW, Severity.MEDIUM, Severity.HIGH, Severity.CRITICAL]
        return max(changes, key=lambda c: order.index(c.severity)).severity
