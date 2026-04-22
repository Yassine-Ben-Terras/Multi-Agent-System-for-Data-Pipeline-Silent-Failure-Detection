"""
Unit tests for SchemaDiffer — all change types, severity escalation,
overall severity computation, and AnomalySignal generation.
No I/O — pure logic tests.
"""

from __future__ import annotations

import pytest

from agents.schema_watcher.contract_store import ColumnDef, SchemaContract
from agents.schema_watcher.differ import ChangeType, SchemaDiffer
from config.schemas import AnomalyType, Severity


# ── Fixtures ──────────────────────────────────────────────────

def make_contract(columns: list[ColumnDef], source: str = "orders_api") -> SchemaContract:
    return SchemaContract(
        source=source,
        columns=columns,
        version_hash=SchemaContract.compute_hash(columns),
    )


def col(name: str, dtype: str = "VARCHAR", nullable: bool = True, criticality: str = "standard") -> ColumnDef:
    return ColumnDef(name=name, dtype=dtype, nullable=nullable, criticality=criticality)


@pytest.fixture
def differ():
    return SchemaDiffer()


# ── Clean diff ────────────────────────────────────────────────

class TestCleanDiff:

    def test_identical_schema_no_drift(self, differ):
        cols = [col("order_id", "VARCHAR", False), col("amount", "FLOAT")]
        contract = make_contract(cols)
        result = differ.diff("orders_api", contract, cols)
        assert not result.has_drift
        assert result.changes == []

    def test_column_order_does_not_matter(self, differ):
        """Schema diff is order-independent."""
        contract = make_contract([col("a"), col("b"), col("c")])
        incoming  = [col("c"), col("a"), col("b")]
        result = differ.diff("src", contract, incoming)
        assert not result.has_drift


# ── Dropped columns ───────────────────────────────────────────

class TestDroppedColumns:

    def test_dropped_column_is_critical(self, differ):
        contract = make_contract([col("order_id"), col("amount"), col("status")])
        incoming  = [col("order_id"), col("amount")]   # status dropped

        result = differ.diff("orders_api", contract, incoming)

        assert result.has_drift
        dropped = [c for c in result.changes if c.change_type == ChangeType.DROPPED_COLUMN]
        assert len(dropped) == 1
        assert dropped[0].column_name == "status"
        assert dropped[0].severity == Severity.CRITICAL

    def test_multiple_dropped_columns(self, differ):
        contract = make_contract([col("a"), col("b"), col("c"), col("d")])
        incoming  = [col("a")]

        result = differ.diff("src", contract, incoming)

        dropped = [c for c in result.changes if c.change_type == ChangeType.DROPPED_COLUMN]
        assert len(dropped) == 3

    def test_overall_severity_critical_when_column_dropped(self, differ):
        contract = make_contract([col("id"), col("val")])
        incoming  = [col("id")]

        result = differ.diff("src", contract, incoming)
        assert result.overall_severity == Severity.CRITICAL


# ── Type changes ──────────────────────────────────────────────

class TestTypeChanges:

    def test_type_change_is_high(self, differ):
        contract = make_contract([col("amount", "INTEGER")])
        incoming  = [col("amount", "VARCHAR")]

        result = differ.diff("src", contract, incoming)

        type_changes = [c for c in result.changes if c.change_type == ChangeType.TYPE_CHANGED]
        assert len(type_changes) == 1
        assert type_changes[0].severity == Severity.HIGH
        assert type_changes[0].details["contract_type"] == "INTEGER"
        assert type_changes[0].details["incoming_type"] == "VARCHAR"

    def test_type_comparison_case_insensitive(self, differ):
        """varchar == VARCHAR == Varchar — no drift."""
        contract = make_contract([col("name", "VARCHAR")])
        incoming  = [col("name", "varchar")]

        result = differ.diff("src", contract, incoming)
        assert not result.has_drift

    def test_critical_column_type_change_escalates_to_critical(self, differ):
        contract = make_contract([col("revenue_usd", "FLOAT", criticality="critical")])
        incoming  = [col("revenue_usd", "INTEGER")]

        result = differ.diff("src", contract, incoming)

        type_changes = [c for c in result.changes if c.change_type == ChangeType.TYPE_CHANGED]
        assert type_changes[0].severity == Severity.CRITICAL


# ── Nullable changes ──────────────────────────────────────────

class TestNullableChanges:

    def test_not_null_to_nullable_is_medium(self, differ):
        contract = make_contract([col("order_id", nullable=False)])
        incoming  = [col("order_id", nullable=True)]

        result = differ.diff("src", contract, incoming)

        nullable_changes = [c for c in result.changes if c.change_type == ChangeType.NULLABLE_CHANGED]
        assert len(nullable_changes) == 1
        assert nullable_changes[0].severity == Severity.MEDIUM

    def test_nullable_to_not_null_is_not_flagged(self, differ):
        """Tightening constraints (nullable → not null) is safe — not a drift."""
        contract = make_contract([col("order_id", nullable=True)])
        incoming  = [col("order_id", nullable=False)]

        result = differ.diff("src", contract, incoming)
        nullable_changes = [c for c in result.changes if c.change_type == ChangeType.NULLABLE_CHANGED]
        assert nullable_changes == []

    def test_critical_column_nullable_change_escalates(self, differ):
        contract = make_contract([col("user_id", nullable=False, criticality="critical")])
        incoming  = [col("user_id", nullable=True)]

        result = differ.diff("src", contract, incoming)
        nullable_changes = [c for c in result.changes if c.change_type == ChangeType.NULLABLE_CHANGED]
        assert nullable_changes[0].severity == Severity.CRITICAL


# ── New columns ───────────────────────────────────────────────

class TestNewColumns:

    def test_new_column_is_low(self, differ):
        contract = make_contract([col("id"), col("name")])
        incoming  = [col("id"), col("name"), col("email")]  # email is new

        result = differ.diff("src", contract, incoming)

        new_cols = [c for c in result.changes if c.change_type == ChangeType.NEW_COLUMN]
        assert len(new_cols) == 1
        assert new_cols[0].column_name == "email"
        assert new_cols[0].severity == Severity.LOW

    def test_multiple_new_columns(self, differ):
        contract = make_contract([col("id")])
        incoming  = [col("id"), col("a"), col("b"), col("c")]

        result = differ.diff("src", contract, incoming)
        new_cols = [c for c in result.changes if c.change_type == ChangeType.NEW_COLUMN]
        assert len(new_cols) == 3


# ── Combined changes ──────────────────────────────────────────

class TestCombinedChanges:

    def test_mixed_changes_overall_severity_is_highest(self, differ):
        """Dropped column (CRITICAL) + new column (LOW) → overall = CRITICAL."""
        contract = make_contract([col("id"), col("amount"), col("status")])
        incoming  = [col("id"), col("amount"), col("new_field")]  # status dropped, new_field added

        result = differ.diff("src", contract, incoming)

        assert result.has_drift
        assert result.overall_severity == Severity.CRITICAL
        change_types = {c.change_type for c in result.changes}
        assert ChangeType.DROPPED_COLUMN in change_types
        assert ChangeType.NEW_COLUMN in change_types

    def test_only_new_columns_overall_severity_is_low(self, differ):
        contract = make_contract([col("id")])
        incoming  = [col("id"), col("extra")]

        result = differ.diff("src", contract, incoming)
        assert result.overall_severity == Severity.LOW


# ── AnomalySignal generation ──────────────────────────────────

class TestAnomalySignalGeneration:

    def test_to_anomaly_signal_correct_type(self, differ):
        contract = make_contract([col("id"), col("amount")])
        incoming  = [col("id")]

        result = differ.diff("orders_api", contract, incoming)
        signal = result.to_anomaly_signal("run-001")

        assert signal.anomaly_type == AnomalyType.SCHEMA_DRIFT
        assert signal.source_agent == "schema_watcher"
        assert signal.confidence == 1.0
        assert signal.model_name == "orders_api"
        assert signal.pipeline_run_id == "run-001"

    def test_to_anomaly_signal_details_structure(self, differ):
        contract = make_contract([col("id"), col("amount")])
        incoming  = [col("id"), col("new_col")]

        result = differ.diff("src", contract, incoming)
        signal = result.to_anomaly_signal("run-002")

        assert "changes" in signal.details
        assert "total_changes" in signal.details
        assert signal.details["total_changes"] == 2   # dropped + new

    def test_cannot_create_signal_from_clean_diff(self, differ):
        contract = make_contract([col("id")])
        result = differ.diff("src", contract, [col("id")])

        with pytest.raises(AssertionError):
            result.to_anomaly_signal("run-003")
