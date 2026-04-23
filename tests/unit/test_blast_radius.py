"""
Unit tests for BlastRadiusCalculator.

Uses the DbtDag fixture built from manifest_sample.json.

DAG reminder:
  stg_orders → fct_orders → fct_revenue → rpt_weekly_sales
                          → dim_customers

Exposures:
  executive_revenue_dashboard → fct_revenue, fct_orders (SLA: daily_08:00_UTC)
  churn_model_features        → dim_customers, fct_orders (no SLA, type=ml)
"""

from __future__ import annotations

from pathlib import Path
import pytest

from agents.lineage_impact.calculator import BlastRadiusCalculator
from agents.lineage_impact.manifest_parser import ManifestParser

FIXTURE = Path(__file__).parent.parent / "fixtures" / "manifest_sample.json"


@pytest.fixture
def dag():
    return ManifestParser().parse_file(FIXTURE)


@pytest.fixture
def calc():
    return BlastRadiusCalculator()


# ── Basic blast radius ────────────────────────────────────────

class TestBlastRadiusCalculator:

    def test_unknown_model_returns_empty(self, calc, dag):
        report = calc.calculate("nonexistent_model", dag)
        assert report.downstream_models == []
        assert report.dashboards == []
        assert report.severity_multiplier == 1.0

    def test_fct_orders_downstream_models(self, calc, dag):
        report = calc.calculate("fct_orders", dag)
        assert "fct_revenue" in report.downstream_models
        assert "dim_customers" in report.downstream_models
        assert "rpt_weekly_sales" in report.downstream_models

    def test_fct_orders_excludes_self(self, calc, dag):
        report = calc.calculate("fct_orders", dag)
        assert "fct_orders" not in report.downstream_models

    def test_leaf_model_no_downstream(self, calc, dag):
        report = calc.calculate("rpt_weekly_sales", dag)
        assert report.downstream_models == []

    def test_fct_orders_dashboards(self, calc, dag):
        report = calc.calculate("fct_orders", dag)
        assert "executive_revenue_dashboard" in report.dashboards

    def test_fct_orders_ml_features(self, calc, dag):
        report = calc.calculate("fct_orders", dag)
        assert "churn_model_features" in report.ml_features

    def test_fct_orders_sla_breach(self, calc, dag):
        """executive_revenue_dashboard has SLA → should appear in sla_breaches."""
        report = calc.calculate("fct_orders", dag)
        assert len(report.sla_breaches) == 1
        assert "executive_revenue_dashboard" in report.sla_breaches[0]
        assert "daily_08:00_UTC" in report.sla_breaches[0]

    def test_dim_customers_no_sla(self, calc, dag):
        """dim_customers → churn_model_features (no SLA) → no sla_breaches."""
        report = calc.calculate("dim_customers", dag)
        assert report.sla_breaches == []

    def test_stg_orders_full_blast(self, calc, dag):
        """stg_orders is root → all models downstream."""
        report = calc.calculate("stg_orders", dag)
        assert len(report.downstream_models) == 4
        assert len(report.dashboards) >= 1

    def test_model_uid_captured(self, calc, dag):
        report = calc.calculate("fct_orders", dag)
        assert report.failed_model_uid == "model.project.fct_orders"


# ── to_blast_radius conversion ────────────────────────────────

class TestToBlastRadius:

    def test_to_blast_radius_fields(self, calc, dag):
        report = calc.calculate("fct_orders", dag)
        br = report.to_blast_radius()
        assert "fct_revenue" in br.affected_models
        assert "executive_revenue_dashboard" in br.dashboards
        assert "churn_model_features" in br.ml_features
        assert br.sla_impact is not None

    def test_empty_report_blast_radius(self, calc, dag):
        report = calc.calculate("nonexistent", dag)
        br = report.to_blast_radius()
        assert br.affected_models == []
        assert br.dashboards == []
        assert br.sla_impact is None


# ── Severity multiplier ───────────────────────────────────────

class TestSeverityMultiplier:

    @pytest.mark.parametrize("downstream, dashboards, sla, expected_min", [
        (0,  0, 0, 1.0),
        (3,  0, 0, 1.5),
        (3,  1, 0, 2.0),
        (3,  1, 1, 2.5),
        (10, 1, 1, 3.0),
    ])
    def test_multiplier_thresholds(self, downstream, dashboards, sla, expected_min):
        result = BlastRadiusCalculator._compute_multiplier(downstream, dashboards, sla)
        assert result == pytest.approx(expected_min)

    def test_multiplier_capped_at_3(self):
        result = BlastRadiusCalculator._compute_multiplier(100, 10, 10)
        assert result == 3.0

    def test_fct_orders_multiplier_above_1(self, calc, dag):
        """fct_orders has 3 downstream + dashboards + SLA → multiplier > 1."""
        report = calc.calculate("fct_orders", dag)
        assert report.severity_multiplier > 1.0


# ── Summary ───────────────────────────────────────────────────

class TestSummary:

    def test_summary_contains_model_name(self, calc, dag):
        report = calc.calculate("fct_orders", dag)
        assert "fct_orders" in report.summary()

    def test_summary_contains_downstream_count(self, calc, dag):
        report = calc.calculate("fct_orders", dag)
        assert "3" in report.summary()

    def test_empty_summary_no_crash(self, calc, dag):
        report = calc.calculate("nonexistent", dag)
        assert "nonexistent" in report.summary()
