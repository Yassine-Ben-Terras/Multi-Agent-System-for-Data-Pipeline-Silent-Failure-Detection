"""
Unit tests for DbtArtifactParser.
Uses the fixture file tests/fixtures/run_results_sample.json.
"""

from __future__ import annotations

import json
import pytest
from pathlib import Path

from agents.quality_auditor.parser import DbtArtifactParser, RunResults, TestResult

FIXTURE_PATH = Path(__file__).parent.parent / "fixtures" / "run_results_sample.json"


@pytest.fixture
def parser():
    return DbtArtifactParser()


@pytest.fixture
def sample_data():
    with open(FIXTURE_PATH) as f:
        return json.load(f)


class TestDbtArtifactParser:

    def test_parse_file(self, parser):
        result = parser.parse_file(FIXTURE_PATH)
        assert isinstance(result, RunResults)

    def test_filters_model_nodes(self, parser, sample_data):
        """Model nodes (resource_type=model) should be excluded — tests only."""
        result = parser.parse_dict(sample_data)
        for r in result.results:
            assert r.unique_id.startswith("test.")

    def test_correct_test_count(self, parser, sample_data):
        """Fixture has 3 test nodes and 1 model node → 3 parsed."""
        result = parser.parse_dict(sample_data)
        assert len(result.results) == 3

    def test_correct_failure_count(self, parser, sample_data):
        result = parser.parse_dict(sample_data)
        assert len(result.failed_tests) == 2

    def test_pass_result_parsed(self, parser, sample_data):
        result = parser.parse_dict(sample_data)
        passing = [r for r in result.results if not r.failed]
        assert len(passing) == 1
        assert passing[0].test_name == "not_null"
        assert passing[0].model_name == "fct_orders"
        assert passing[0].failures == 0

    def test_fail_result_parsed(self, parser, sample_data):
        result = parser.parse_dict(sample_data)
        failing = [r for r in result.failed_tests if r.model_name == "fct_orders"]
        assert len(failing) == 1
        assert failing[0].column_name == "amount"
        assert failing[0].failures == 142

    def test_meta_annotations_parsed(self, parser, sample_data):
        """agent_severity, blast_radius, oncall_team from dbt meta."""
        result = parser.parse_dict(sample_data)
        annotated = [r for r in result.results if r.agent_severity == "critical"]
        assert len(annotated) == 1
        assert annotated[0].blast_radius == "executive_dashboard"
        assert annotated[0].oncall_team == "data-platform"

    def test_model_name_extracted_from_depends_on(self, parser, sample_data):
        result = parser.parse_dict(sample_data)
        revenue_test = next(r for r in result.results if "revenue" in r.model_name)
        assert revenue_test.model_name == "fct_revenue"

    def test_run_id_derived_from_timestamp(self, parser, sample_data):
        result = parser.parse_dict(sample_data)
        assert result.run_id.startswith("dbt-run-")

    def test_models_with_failures(self, parser, sample_data):
        result = parser.parse_dict(sample_data)
        models = result.models_with_failures
        assert "fct_orders" in models
        assert "fct_revenue" in models

    def test_missing_file_raises(self, parser):
        with pytest.raises(FileNotFoundError):
            parser.parse_file("/nonexistent/path/run_results.json")

    def test_failed_property(self):
        t = TestResult(
            unique_id="test.x", test_name="not_null", model_name="m",
            column_name="c", status="fail", failures=10,
            execution_time=1.0, message=None,
            agent_severity=None, blast_radius=None, oncall_team=None,
        )
        assert t.failed is True

    def test_passed_property(self):
        t = TestResult(
            unique_id="test.x", test_name="not_null", model_name="m",
            column_name="c", status="pass", failures=0,
            execution_time=1.0, message=None,
            agent_severity=None, blast_radius=None, oncall_team=None,
        )
        assert t.failed is False
