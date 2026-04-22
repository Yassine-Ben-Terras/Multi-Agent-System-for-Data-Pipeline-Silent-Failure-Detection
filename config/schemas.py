"""
MAS Pipeline Sentinel — Core Signal Schema
Pydantic models for all messages flowing through the event bus.
Every producer and consumer in the system uses these models.
"""

from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional
from uuid import uuid4

from pydantic import BaseModel, Field


# ── Enums ────────────────────────────────────────────────────

class PipelineStage(str, Enum):
    INGESTION = "ingestion"
    TRANSFORMATION = "transformation"
    SERVING = "serving"


class Severity(str, Enum):
    LOW = "LOW"
    MEDIUM = "MEDIUM"
    HIGH = "HIGH"
    CRITICAL = "CRITICAL"


class AgentMode(str, Enum):
    SHADOW = "shadow"          # observe & log only
    ALERT_ONLY = "alert_only"  # fire alerts, no actions
    SUPERVISED = "supervised"  # auto on LOW, human on HIGH/CRITICAL
    FULL_AUTONOMY = "full_autonomy"


# ── Pipeline Signal (produced by pipeline stages) ────────────

class SchemaSnapshot(BaseModel):
    columns: List[Dict[str, Any]]   # [{name, type, nullable}, ...]
    version_hash: str


class PipelineSignal(BaseModel):
    """
    Standardised signal envelope emitted by every pipeline stage.
    Topic: pipeline.signals.raw
    """
    event_id: str = Field(default_factory=lambda: str(uuid4()))
    event_type: str = "pipeline.stage.completed"
    stage: PipelineStage
    source: str                         # e.g. "orders_api", "dbt_run", "kafka_consumer"
    metrics: Dict[str, Any]             # row_count, checksum, duration_ms, ...
    schema_snapshot: Optional[SchemaSnapshot] = None
    pipeline_run_id: str
    ts: datetime = Field(default_factory=datetime.utcnow)


# ── Agent Anomaly Signals (produced by specialist agents) ─────

class AnomalyType(str, Enum):
    ROW_COUNT = "row_count_anomaly"
    SCHEMA_DRIFT = "schema_drift"
    QUALITY_FAILURE = "quality_failure"
    FRESHNESS_VIOLATION = "freshness_violation"
    CHECKSUM_MISMATCH = "checksum_mismatch"


class AnomalySignal(BaseModel):
    """
    Emitted by specialist agents when an anomaly is detected.
    Topic: agents.anomalies
    """
    signal_id: str = Field(default_factory=lambda: str(uuid4()))
    anomaly_type: AnomalyType
    source_agent: str                   # e.g. "ingestion_monitor"
    pipeline_run_id: str
    model_name: Optional[str] = None
    severity: Severity
    confidence: float = Field(ge=0.0, le=1.0)
    details: Dict[str, Any]             # agent-specific details
    ts: datetime = Field(default_factory=datetime.utcnow)


# ── Confirmed Incident (produced by Orchestrator) ─────────────

class BlastRadius(BaseModel):
    affected_models: List[str]
    dashboards: List[str] = []
    ml_features: List[str] = []
    next_refresh_time: Optional[datetime] = None
    sla_impact: Optional[str] = None


class ConfirmedIncident(BaseModel):
    """
    Produced by the Orchestrator after correlating multiple anomaly signals.
    Topic: agents.confirmed_incidents
    """
    incident_id: str = Field(default_factory=lambda: str(uuid4()))
    root_cause: str
    severity: Severity
    pipeline_run_id: str
    correlated_signals: List[AnomalySignal]
    blast_radius: Optional[BlastRadius] = None
    llm_hypothesis: Optional[str] = None   # LLM-generated root cause narrative
    ts: datetime = Field(default_factory=datetime.utcnow)


# ── Remediation Action (produced by Remediation Agent) ────────

class ActionType(str, Enum):
    QUARANTINE_PARTITION = "quarantine_partition"
    TRIGGER_BACKFILL = "trigger_backfill"
    PAUSE_DOWNSTREAM_DAGS = "pause_downstream_dags"
    CREATE_PAGERDUTY = "create_pagerduty"
    CREATE_JIRA = "create_jira"
    NOTIFY_SLACK = "notify_slack"


class RemediationAction(BaseModel):
    """
    Audit record for every action taken by the Remediation Agent.
    Topic: agents.actions_taken
    """
    action_id: str = Field(default_factory=lambda: str(uuid4()))
    incident_id: str
    action_type: ActionType
    target: str                         # model name, DAG id, partition key
    payload: Dict[str, Any] = {}
    success: bool = False
    error_message: Optional[str] = None
    ts: datetime = Field(default_factory=datetime.utcnow)


# ── Agent Heartbeat ───────────────────────────────────────────

class AgentHeartbeat(BaseModel):
    """
    Published by every agent every 60 seconds.
    Topic: agents.heartbeats
    """
    agent_name: str
    status: str = "alive"              # alive | degraded | error
    metadata: Dict[str, Any] = {}
    ts: datetime = Field(default_factory=datetime.utcnow)
