"""
Unit tests for Playbook — action selection for all severities,
root causes, and agent modes.
"""

from __future__ import annotations

import pytest
from datetime import datetime, timezone
from unittest.mock import MagicMock

from agents.remediation.playbook import Playbook, PlannedAction
from config.schemas import (
    ActionType, AgentMode, AnomalySignal, AnomalyType,
    BlastRadius, ConfirmedIncident, Severity,
)


# ── Fixtures ──────────────────────────────────────────────────

def make_signal(
    model: str = "fct_orders",
    severity: Severity = Severity.HIGH,
    anomaly_type: AnomalyType = AnomalyType.ROW_COUNT,
) -> AnomalySignal:
    return AnomalySignal(
        anomaly_type=anomaly_type,
        source_agent="ingestion_monitor",
        pipeline_run_id="run-001",
        model_name=model,
        severity=severity,
        confidence=0.9,
        details={},
    )


def make_incident(
    severity: Severity = Severity.HIGH,
    root_cause: str = "row_count_anomaly",
    model: str = "fct_orders",
    blast_radius: BlastRadius = None,
    hypothesis: str = None,
) -> ConfirmedIncident:
    return ConfirmedIncident(
        root_cause=root_cause,
        severity=severity,
        pipeline_run_id="run-001",
        correlated_signals=[make_signal(model=model, severity=severity)],
        blast_radius=blast_radius,
        llm_hypothesis=hypothesis,
    )


@pytest.fixture
def playbook():
    return Playbook()


# ── LOW severity ──────────────────────────────────────────────

class TestLowSeverity:

    def test_low_only_slack(self, playbook):
        incident = make_incident(severity=Severity.LOW)
        actions = playbook.select(incident, AgentMode.FULL_AUTONOMY)
        types = [a.action_type for a in actions]
        assert ActionType.NOTIFY_SLACK in types
        assert ActionType.QUARANTINE_PARTITION not in types
        assert ActionType.TRIGGER_BACKFILL not in types

    def test_medium_only_slack(self, playbook):
        incident = make_incident(severity=Severity.MEDIUM)
        actions = playbook.select(incident, AgentMode.FULL_AUTONOMY)
        types = [a.action_type for a in actions]
        assert ActionType.NOTIFY_SLACK in types
        assert ActionType.QUARANTINE_PARTITION not in types


# ── HIGH severity ─────────────────────────────────────────────

class TestHighSeverity:

    def test_high_includes_quarantine(self, playbook):
        incident = make_incident(severity=Severity.HIGH)
        actions = playbook.select(incident, AgentMode.FULL_AUTONOMY)
        types = [a.action_type for a in actions]
        assert ActionType.QUARANTINE_PARTITION in types
        assert ActionType.NOTIFY_SLACK in types

    def test_high_no_backfill(self, playbook):
        """HIGH → quarantine only, not backfill (that's CRITICAL)."""
        incident = make_incident(severity=Severity.HIGH)
        actions = playbook.select(incident, AgentMode.FULL_AUTONOMY)
        types = [a.action_type for a in actions]
        assert ActionType.TRIGGER_BACKFILL not in types

    def test_high_supervised_quarantine_requires_approval(self, playbook):
        incident = make_incident(severity=Severity.HIGH)
        actions = playbook.select(incident, AgentMode.SUPERVISED)
        quarantine = next(a for a in actions if a.action_type == ActionType.QUARANTINE_PARTITION)
        assert quarantine.requires_approval is True

    def test_high_full_autonomy_no_approval_needed(self, playbook):
        incident = make_incident(severity=Severity.HIGH)
        actions = playbook.select(incident, AgentMode.FULL_AUTONOMY)
        quarantine = next(a for a in actions if a.action_type == ActionType.QUARANTINE_PARTITION)
        assert quarantine.requires_approval is False


# ── CRITICAL severity ─────────────────────────────────────────

class TestCriticalSeverity:

    def test_critical_includes_all_actions(self, playbook):
        incident = make_incident(severity=Severity.CRITICAL)
        actions = playbook.select(incident, AgentMode.FULL_AUTONOMY)
        types = [a.action_type for a in actions]
        assert ActionType.NOTIFY_SLACK in types
        assert ActionType.QUARANTINE_PARTITION in types
        assert ActionType.TRIGGER_BACKFILL in types
        assert ActionType.CREATE_PAGERDUTY in types

    def test_critical_pagerduty_never_requires_approval(self, playbook):
        """PagerDuty always fires — even in supervised mode."""
        incident = make_incident(severity=Severity.CRITICAL)
        actions = playbook.select(incident, AgentMode.SUPERVISED)
        pagerduty = next(a for a in actions if a.action_type == ActionType.CREATE_PAGERDUTY)
        assert pagerduty.requires_approval is False

    def test_critical_schema_drift_pauses_dags(self, playbook):
        br = BlastRadius(
            affected_models=["fct_revenue", "dim_customers"],
            dashboards=[], ml_features=[], sla_impact=None,
        )
        incident = make_incident(
            severity=Severity.CRITICAL,
            root_cause="schema_drift",
            blast_radius=br,
        )
        actions = playbook.select(incident, AgentMode.FULL_AUTONOMY)
        types = [a.action_type for a in actions]
        assert ActionType.PAUSE_DOWNSTREAM_DAGS in types

    def test_critical_non_schema_drift_no_dag_pause(self, playbook):
        incident = make_incident(
            severity=Severity.CRITICAL,
            root_cause="row_count_anomaly",
        )
        actions = playbook.select(incident, AgentMode.FULL_AUTONOMY)
        types = [a.action_type for a in actions]
        assert ActionType.PAUSE_DOWNSTREAM_DAGS not in types

    def test_critical_backfill_full_refresh_for_schema_drift(self, playbook):
        incident = make_incident(
            severity=Severity.CRITICAL,
            root_cause="schema_drift",
        )
        actions = playbook.select(incident, AgentMode.FULL_AUTONOMY)
        backfill = next(a for a in actions if a.action_type == ActionType.TRIGGER_BACKFILL)
        assert backfill.payload["full_refresh"] is True

    def test_critical_backfill_no_full_refresh_for_row_count(self, playbook):
        incident = make_incident(
            severity=Severity.CRITICAL,
            root_cause="row_count_anomaly",
        )
        actions = playbook.select(incident, AgentMode.FULL_AUTONOMY)
        backfill = next(a for a in actions if a.action_type == ActionType.TRIGGER_BACKFILL)
        assert backfill.payload["full_refresh"] is False

    def test_dag_pause_capped_at_5_models(self, playbook):
        """Never pause more than 5 DAGs in one incident."""
        br = BlastRadius(
            affected_models=[f"model_{i}" for i in range(10)],
            dashboards=[], ml_features=[], sla_impact=None,
        )
        incident = make_incident(
            severity=Severity.CRITICAL,
            root_cause="schema_drift",
            blast_radius=br,
        )
        actions = playbook.select(incident, AgentMode.FULL_AUTONOMY)
        dag_pauses = [a for a in actions if a.action_type == ActionType.PAUSE_DOWNSTREAM_DAGS]
        assert len(dag_pauses) <= 5


# ── Slack payload ─────────────────────────────────────────────

class TestSlackPayload:

    def test_slack_payload_has_required_fields(self, playbook):
        incident = make_incident(
            severity=Severity.HIGH,
            hypothesis="Possible upstream API schema change.",
        )
        actions = playbook.select(incident, AgentMode.FULL_AUTONOMY)
        slack = next(a for a in actions if a.action_type == ActionType.NOTIFY_SLACK)
        assert "incident_id" in slack.payload
        assert "root_cause" in slack.payload
        assert "severity" in slack.payload
        assert "model" in slack.payload
        assert slack.payload["hypothesis"] == "Possible upstream API schema change."

    def test_slack_never_requires_approval(self, playbook):
        incident = make_incident(severity=Severity.CRITICAL)
        actions = playbook.select(incident, AgentMode.SUPERVISED)
        slack = next(a for a in actions if a.action_type == ActionType.NOTIFY_SLACK)
        assert slack.requires_approval is False


# ── Action ordering ───────────────────────────────────────────

class TestActionOrdering:

    def test_slack_is_first_action(self, playbook):
        """Slack notification always comes first."""
        incident = make_incident(severity=Severity.CRITICAL)
        actions = playbook.select(incident, AgentMode.FULL_AUTONOMY)
        assert actions[0].action_type == ActionType.NOTIFY_SLACK

    def test_quarantine_before_backfill(self, playbook):
        """Quarantine before backfill — safe order."""
        incident = make_incident(severity=Severity.CRITICAL)
        actions = playbook.select(incident, AgentMode.FULL_AUTONOMY)
        types = [a.action_type for a in actions]
        q_idx = types.index(ActionType.QUARANTINE_PARTITION)
        b_idx = types.index(ActionType.TRIGGER_BACKFILL)
        assert q_idx < b_idx
