"""
Lineage & Impact Agent — Blast Radius Calculator

Given a failed model name and a DbtDag, computes the full blast radius:
  - All downstream models (transitive)
  - All exposures (dashboards, ML features) that depend on those models
  - SLA impact assessment
  - Human-readable impact summary for incident narratives
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import List, Optional

from config.schemas import BlastRadius
from agents.lineage_impact.manifest_parser import DbtDag, ExposureNode, ModelNode

logger = logging.getLogger(__name__)


@dataclass
class BlastRadiusReport:
    """
    Full impact report for one failed model.
    Extends the config.schemas.BlastRadius with richer detail.
    """
    failed_model_name: str
    failed_model_uid: Optional[str]

    # Downstream models
    downstream_models: List[str]          # short names e.g. ["fct_revenue", "dim_customers"]
    downstream_model_uids: List[str]      # full unique_ids

    # Downstream exposures
    affected_exposures: List[ExposureNode]

    # Derived
    dashboards: List[str]                 # exposure names of type=dashboard
    ml_features: List[str]               # exposure names of type=ml
    sla_breaches: List[str]              # exposures with SLA metadata
    severity_multiplier: float           # 1.0–3.0 based on blast radius size

    def to_blast_radius(self) -> BlastRadius:
        """Convert to the BlastRadius schema used in ConfirmedIncident."""
        return BlastRadius(
            affected_models=self.downstream_models,
            dashboards=self.dashboards,
            ml_features=self.ml_features,
            sla_impact=self.sla_impact_summary(),
        )

    def sla_impact_summary(self) -> Optional[str]:
        if not self.sla_breaches:
            return None
        return f"SLA at risk: {', '.join(self.sla_breaches)}"

    def summary(self) -> str:
        parts = [f"Model '{self.failed_model_name}' failure affects:"]
        parts.append(f"  • {len(self.downstream_models)} downstream models")
        if self.dashboards:
            parts.append(f"  • Dashboards: {', '.join(self.dashboards)}")
        if self.ml_features:
            parts.append(f"  • ML features: {', '.join(self.ml_features)}")
        if self.sla_breaches:
            parts.append(f"  • SLA at risk: {', '.join(self.sla_breaches)}")
        return "\n".join(parts)


class BlastRadiusCalculator:
    """
    Computes blast radius for a given model name using a DbtDag.
    Pure logic — no I/O.
    """

    def calculate(self, model_name: str, dag: DbtDag) -> BlastRadiusReport:
        """
        Main entry point. Returns a BlastRadiusReport for the given model.
        If the model is not in the DAG, returns an empty report.
        """
        model_node = dag.get_model_by_name(model_name)

        if model_node is None:
            logger.warning(
                "model_not_in_dag | model=%s (returning empty blast radius)", model_name
            )
            return self._empty_report(model_name)

        # BFS downstream traversal
        downstream_uids = dag.downstream_models(model_node.unique_id)

        # Resolve short names
        downstream_names = [
            dag.models[uid].name
            for uid in downstream_uids
            if uid in dag.models
        ]

        # Include the failed model itself in the full uid set for exposure matching
        all_affected_uids = downstream_uids | {model_node.unique_id}
        affected_exposures = dag.exposures_for_models(all_affected_uids)

        dashboards = [
            e.name for e in affected_exposures
            if e.exposure_type in ("dashboard", "analysis")
        ]
        ml_features = [
            e.name for e in affected_exposures
            if e.exposure_type == "ml"
        ]
        sla_breaches = [
            f"{e.name} ({e.sla})"
            for e in affected_exposures
            if e.sla
        ]

        severity_multiplier = self._compute_multiplier(
            downstream_count=len(downstream_names),
            dashboard_count=len(dashboards),
            sla_count=len(sla_breaches),
        )

        report = BlastRadiusReport(
            failed_model_name=model_name,
            failed_model_uid=model_node.unique_id,
            downstream_models=sorted(downstream_names),
            downstream_model_uids=sorted(downstream_uids),
            affected_exposures=affected_exposures,
            dashboards=sorted(dashboards),
            ml_features=sorted(ml_features),
            sla_breaches=sorted(sla_breaches),
            severity_multiplier=severity_multiplier,
        )

        logger.info(
            "blast_radius_computed | model=%s downstream=%s dashboards=%s ml=%s sla=%s",
            model_name,
            len(downstream_names),
            len(dashboards),
            len(ml_features),
            len(sla_breaches),
        )

        return report

    # ── Helpers ───────────────────────────────────────────────

    @staticmethod
    def _empty_report(model_name: str) -> BlastRadiusReport:
        return BlastRadiusReport(
            failed_model_name=model_name,
            failed_model_uid=None,
            downstream_models=[],
            downstream_model_uids=[],
            affected_exposures=[],
            dashboards=[],
            ml_features=[],
            sla_breaches=[],
            severity_multiplier=1.0,
        )

    @staticmethod
    def _compute_multiplier(
        downstream_count: int,
        dashboard_count: int,
        sla_count: int,
    ) -> float:
        """
        Severity multiplier for the Orchestrator to use when escalating.
        Range: 1.0 (no blast) → 3.0 (massive blast with SLA breaches).
        """
        score = 1.0
        if downstream_count >= 10:
            score += 1.0
        elif downstream_count >= 3:
            score += 0.5
        if dashboard_count >= 1:
            score += 0.5
        if sla_count >= 1:
            score += 0.5
        return min(score, 3.0)
