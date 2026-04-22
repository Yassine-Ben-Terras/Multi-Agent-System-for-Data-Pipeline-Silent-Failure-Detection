"""
Unit tests for core signal schemas.
"""

import pytest
from datetime import datetime
from config.schemas import (
    PipelineSignal,
    PipelineStage,
    AnomalySignal,
    AnomalyType,
    Severity,
    SchemaSnapshot,
    AgentHeartbeat,
)


class TestPipelineSignal:
    def test_create_valid_signal(self):
        signal = PipelineSignal(
            stage=PipelineStage.INGESTION,
            source="orders_api",
            metrics={"row_count": 142830, "checksum": "a3f9c2", "duration_ms": 4210},
            pipeline_run_id="run-2026-04-22-001",
        )
        assert signal.stage == PipelineStage.INGESTION
        assert signal.metrics["row_count"] == 142830
        assert signal.event_id is not None
        assert signal.event_type == "pipeline.stage.completed"

    def test_signal_with_schema_snapshot(self):
        snapshot = SchemaSnapshot(
            columns=[{"name": "order_id", "type": "VARCHAR", "nullable": False}],
            version_hash="b7c2e1",
        )
        signal = PipelineSignal(
            stage=PipelineStage.TRANSFORMATION,
            source="dbt_run",
            metrics={"row_count": 5000},
            pipeline_run_id="run-001",
            schema_snapshot=snapshot,
        )
        assert signal.schema_snapshot.version_hash == "b7c2e1"

    def test_signal_auto_timestamp(self):
        signal = PipelineSignal(
            stage=PipelineStage.SERVING,
            source="serving_layer",
            metrics={},
            pipeline_run_id="run-001",
        )
        assert isinstance(signal.ts, datetime)


class TestAnomalySignal:
    def test_create_anomaly_signal(self):
        signal = AnomalySignal(
            anomaly_type=AnomalyType.ROW_COUNT,
            source_agent="ingestion_monitor",
            pipeline_run_id="run-001",
            model_name="fct_orders",
            severity=Severity.HIGH,
            confidence=0.92,
            details={"z_score": -3.4, "threshold": 2.5},
        )
        assert signal.severity == Severity.HIGH
        assert signal.confidence == 0.92

    def test_confidence_bounds(self):
        with pytest.raises(Exception):
            AnomalySignal(
                anomaly_type=AnomalyType.SCHEMA_DRIFT,
                source_agent="schema_watcher",
                pipeline_run_id="run-001",
                severity=Severity.LOW,
                confidence=1.5,  # Invalid: > 1.0
                details={},
            )


class TestAgentHeartbeat:
    def test_default_status(self):
        hb = AgentHeartbeat(agent_name="ingestion_monitor")
        assert hb.status == "alive"
        assert isinstance(hb.ts, datetime)
