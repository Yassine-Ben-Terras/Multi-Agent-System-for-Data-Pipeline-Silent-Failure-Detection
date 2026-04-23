"""
Unit tests for ManifestParser and DbtDag.

DAG structure in fixture (manifest_sample.json):

  stg_orders
      └── fct_orders
              ├── fct_revenue
              │       └── rpt_weekly_sales
              └── dim_customers

Exposures:
  executive_revenue_dashboard → fct_revenue, fct_orders  (SLA: daily_08:00_UTC)
  churn_model_features        → dim_customers, fct_orders (no SLA)
"""

from __future__ import annotations

import json
import pytest
from pathlib import Path

from agents.lineage_impact.manifest_parser import DbtDag, ManifestParser

FIXTURE = Path(__file__).parent.parent / "fixtures" / "manifest_sample.json"


@pytest.fixture
def dag() -> DbtDag:
    return ManifestParser().parse_file(FIXTURE)


@pytest.fixture
def raw_data():
    with open(FIXTURE) as f:
        return json.load(f)


# ── Parser ────────────────────────────────────────────────────

class TestManifestParser:

    def test_parses_models_only(self, dag):
        """Test nodes should be excluded — only model nodes parsed."""
        for uid in dag.models:
            assert uid.startswith("model.")

    def test_correct_model_count(self, dag):
        """Fixture has 5 model nodes and 1 test node → 5 models."""
        assert len(dag.models) == 5

    def test_correct_exposure_count(self, dag):
        assert len(dag.exposures) == 2

    def test_model_fields(self, dag):
        model = dag.models["model.project.fct_orders"]
        assert model.name == "fct_orders"
        assert model.schema == "marts"
        assert model.database == "analytics"
        assert "finance" in model.tags

    def test_exposure_fields(self, dag):
        exp = dag.exposures["exposure.project.executive_revenue_dashboard"]
        assert exp.name == "executive_revenue_dashboard"
        assert exp.exposure_type == "dashboard"
        assert exp.sla == "daily_08:00_UTC"
        assert exp.owner == "data-platform"

    def test_exposure_depends_on_models(self, dag):
        exp = dag.exposures["exposure.project.executive_revenue_dashboard"]
        assert "model.project.fct_revenue" in exp.depends_on_models
        assert "model.project.fct_orders" in exp.depends_on_models

    def test_missing_file_raises(self):
        with pytest.raises(FileNotFoundError):
            ManifestParser().parse_file("/nonexistent/manifest.json")

    def test_child_map_loaded(self, dag):
        children = dag.child_map.get("model.project.fct_orders", [])
        assert "model.project.fct_revenue" in children
        assert "model.project.dim_customers" in children

    def test_build_child_map_from_parent_map(self, raw_data):
        """If child_map is absent, build it from parent_map."""
        data = {**raw_data}
        del data["child_map"]   # force rebuild from parent_map
        dag = ManifestParser().parse_dict(data)
        children = dag.child_map.get("model.project.fct_orders", [])
        assert "model.project.fct_revenue" in children
        assert "model.project.dim_customers" in children


# ── DbtDag traversal ─────────────────────────────────────────

class TestDbtDagTraversal:

    def test_get_model_by_name(self, dag):
        model = dag.get_model_by_name("fct_orders")
        assert model is not None
        assert model.unique_id == "model.project.fct_orders"

    def test_get_model_by_name_not_found(self, dag):
        assert dag.get_model_by_name("nonexistent_model") is None

    def test_downstream_models_direct(self, dag):
        """fct_orders direct children: fct_revenue, dim_customers."""
        downstream = dag.downstream_models("model.project.fct_orders")
        assert "model.project.fct_revenue" in downstream
        assert "model.project.dim_customers" in downstream

    def test_downstream_models_transitive(self, dag):
        """fct_orders transitive: fct_revenue + dim_customers + rpt_weekly_sales."""
        downstream = dag.downstream_models("model.project.fct_orders")
        assert "model.project.rpt_weekly_sales" in downstream

    def test_downstream_excludes_self(self, dag):
        downstream = dag.downstream_models("model.project.fct_orders")
        assert "model.project.fct_orders" not in downstream

    def test_downstream_excludes_test_nodes(self, dag):
        """Test nodes should never appear in downstream model results."""
        downstream = dag.downstream_models("model.project.stg_orders")
        for uid in downstream:
            assert uid.startswith("model.")

    def test_leaf_node_has_no_downstream(self, dag):
        downstream = dag.downstream_models("model.project.rpt_weekly_sales")
        assert downstream == set()

    def test_stg_orders_full_blast(self, dag):
        """stg_orders → all 4 other models downstream."""
        downstream = dag.downstream_models("model.project.stg_orders")
        assert len(downstream) == 4

    def test_exposures_for_models(self, dag):
        """fct_orders is in both exposures."""
        exps = dag.exposures_for_models({"model.project.fct_orders"})
        names = {e.name for e in exps}
        assert "executive_revenue_dashboard" in names
        assert "churn_model_features" in names

    def test_exposures_for_unrelated_model(self, dag):
        """rpt_weekly_sales has no direct exposure."""
        exps = dag.exposures_for_models({"model.project.rpt_weekly_sales"})
        assert exps == []

    def test_no_cycle_in_traversal(self, dag):
        """BFS should terminate even if child_map had a cycle (defensive)."""
        # Manually add a cycle to the DAG for this test
        dag.child_map["model.project.fct_revenue"].append("model.project.fct_orders")
        # Should not raise or loop forever
        downstream = dag.downstream_models("model.project.fct_orders")
        assert isinstance(downstream, set)
