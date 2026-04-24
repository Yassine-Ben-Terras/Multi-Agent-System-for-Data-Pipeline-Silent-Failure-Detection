"""
Remediation Agent — Playbook

Maps (severity, root_cause) → ordered list of RemediationActions to execute.

Design principles:
  - Never auto-delete data — quarantine only (rename/tag partition)
  - Always notify before acting (Slack first, then warehouse actions)
  - LOW/MEDIUM → full auto in all modes except shadow
  - HIGH       → auto in full_autonomy, human approval in supervised
  - CRITICAL   → auto in full_autonomy, human approval in supervised
  - Quarantine always comes before backfill (safe order)
  - Every action is idempotent (safe to re-run)
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from config.schemas import ActionType, AgentMode, ConfirmedIncident, Severity


@dataclass
class PlannedAction:
    """A single action the Remediation Agent will attempt."""
    action_type: ActionType
    target: str
    payload: Dict[str, Any] = field(default_factory=dict)
    requires_approval: bool = False   # True → skip unless full_autonomy
    description: str = ""


class Playbook:
    """
    Selects an ordered list of PlannedActions for a confirmed incident.

    Selection logic:
      1. Always notify Slack (all severities)
      2. LOW/MEDIUM  → no warehouse action needed (notification only)
      3. HIGH        → quarantine affected partition
      4. CRITICAL    → quarantine + trigger backfill + page PagerDuty
      5. schema_drift-specific → also pause downstream DAGs
    """

    def select(
        self,
        incident: ConfirmedIncident,
        mode: AgentMode,
    ) -> List[PlannedAction]:
        severity   = incident.severity
        root_cause = incident.root_cause
        model      = self._primary_model(incident)
        run_id     = incident.pipeline_run_id

        actions: List[PlannedAction] = []

        # ── Always: Slack notification ─────────────────────────
        actions.append(PlannedAction(
            action_type=ActionType.NOTIFY_SLACK,
            target="data-platform",
            payload={
                "incident_id":  incident.incident_id,
                "root_cause":   root_cause,
                "severity":     severity.value,
                "model":        model,
                "run_id":       run_id,
                "signals":      len(incident.correlated_signals),
                "hypothesis":   incident.llm_hypothesis,
                "blast_radius": self._blast_summary(incident),
            },
            requires_approval=False,
            description=f"Notify #data-platform about {severity.value} incident",
        ))

        # ── LOW / MEDIUM: notify only ──────────────────────────
        if severity in (Severity.LOW, Severity.MEDIUM):
            return actions

        # ── HIGH: quarantine ───────────────────────────────────
        if severity == Severity.HIGH:
            actions.append(PlannedAction(
                action_type=ActionType.QUARANTINE_PARTITION,
                target=model,
                payload={
                    "partition_run_id": run_id,
                    "reason":           f"{root_cause} detected — severity {severity.value}",
                    "incident_id":      incident.incident_id,
                },
                requires_approval=(mode == AgentMode.SUPERVISED),
                description=f"Quarantine {model} partition for run {run_id}",
            ))
            return actions

        # ── CRITICAL: quarantine + backfill + PagerDuty ────────
        if severity == Severity.CRITICAL:
            actions.append(PlannedAction(
                action_type=ActionType.QUARANTINE_PARTITION,
                target=model,
                payload={
                    "partition_run_id": run_id,
                    "reason":           f"CRITICAL: {root_cause}",
                    "incident_id":      incident.incident_id,
                },
                requires_approval=(mode == AgentMode.SUPERVISED),
                description=f"Quarantine {model} partition",
            ))

            # schema_drift → pause downstream DAGs first (prevent further propagation)
            if root_cause == "schema_drift":
                affected = self._affected_models(incident)
                for downstream_model in affected[:5]:   # cap at 5 to avoid runaway
                    actions.append(PlannedAction(
                        action_type=ActionType.PAUSE_DOWNSTREAM_DAGS,
                        target=downstream_model,
                        payload={"incident_id": incident.incident_id},
                        requires_approval=(mode == AgentMode.SUPERVISED),
                        description=f"Pause DAG for downstream model {downstream_model}",
                    ))

            actions.append(PlannedAction(
                action_type=ActionType.TRIGGER_BACKFILL,
                target=model,
                payload={
                    "dbt_select":       model,
                    "full_refresh":     (root_cause in ("schema_drift", "checksum_mismatch")),
                    "incident_id":      incident.incident_id,
                },
                requires_approval=(mode == AgentMode.SUPERVISED),
                description=f"Trigger dbt backfill for {model}",
            ))

            actions.append(PlannedAction(
                action_type=ActionType.CREATE_PAGERDUTY,
                target="data-platform-oncall",
                payload={
                    "incident_id":   incident.incident_id,
                    "title":         f"CRITICAL data pipeline failure: {root_cause} in {model}",
                    "severity":      "critical",
                    "root_cause":    root_cause,
                    "blast_radius":  self._blast_summary(incident),
                    "hypothesis":    incident.llm_hypothesis,
                    "run_id":        run_id,
                },
                requires_approval=False,   # Always page — even in supervised mode
                description="Create PagerDuty incident for on-call",
            ))

        return actions

    # ── Helpers ───────────────────────────────────────────────

    @staticmethod
    def _primary_model(incident: ConfirmedIncident) -> str:
        if incident.correlated_signals:
            return incident.correlated_signals[0].model_name or "unknown"
        return "unknown"

    @staticmethod
    def _affected_models(incident: ConfirmedIncident) -> List[str]:
        if incident.blast_radius and incident.blast_radius.affected_models:
            return incident.blast_radius.affected_models
        return []

    @staticmethod
    def _blast_summary(incident: ConfirmedIncident) -> str:
        if not incident.blast_radius:
            return "blast radius unknown"
        br = incident.blast_radius
        parts = [f"{len(br.affected_models)} models"]
        if br.dashboards:
            parts.append(f"{len(br.dashboards)} dashboards")
        if br.sla_impact:
            parts.append(f"SLA: {br.sla_impact}")
        return ", ".join(parts)
