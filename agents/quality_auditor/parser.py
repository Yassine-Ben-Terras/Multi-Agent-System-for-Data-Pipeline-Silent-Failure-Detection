"""
Quality Auditor — dbt Artifact Parser

Parses dbt run_results.json and extracts per-model test results.

dbt run_results.json structure (simplified):
{
  "metadata": { "generated_at": "...", "dbt_version": "..." },
  "results": [
    {
      "unique_id": "test.project.not_null_fct_orders_order_id.abc123",
      "status": "pass" | "fail" | "warn" | "error",
      "failures": 142,        # number of failing rows
      "message": "Got 142 results, configured to fail if != 0",
      "execution_time": 1.23,
      "node": {
        "name": "not_null_fct_orders_order_id",
        "resource_type": "test",
        "depends_on": { "nodes": ["model.project.fct_orders"] },
        "test_metadata": {
          "name": "not_null",
          "kwargs": { "column_name": "order_id", "model": "ref('fct_orders')" }
        },
        "config": {
          "severity": "ERROR",
          "meta": {
            "agent_severity": "critical",
            "blast_radius": "executive_dashboard",
            "oncall_team": "data-platform"
          }
        }
      }
    }
  ]
}
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


@dataclass
class TestResult:
    """Parsed result for a single dbt test."""
    unique_id: str
    test_name: str           # e.g. "not_null", "unique", "accepted_values"
    model_name: str          # e.g. "fct_orders"
    column_name: Optional[str]
    status: str              # pass | fail | warn | error
    failures: int            # number of failing rows (0 = pass)
    execution_time: float
    message: Optional[str]

    # From meta annotations (schema.yml)
    agent_severity: Optional[str]   # critical | high | medium | low
    blast_radius: Optional[str]     # e.g. "executive_dashboard"
    oncall_team: Optional[str]

    @property
    def failed(self) -> bool:
        return self.status in ("fail", "error")

    @property
    def failure_rate(self) -> float:
        """
        dbt reports failures as a row count, not a rate.
        We store it as-is; the auditor computes rate relative to model size
        when available, or uses raw count otherwise.
        """
        return float(self.failures)


@dataclass
class RunResults:
    """Parsed dbt run_results.json."""
    generated_at: datetime
    dbt_version: str
    run_id: str                        # derived from generated_at timestamp
    results: List[TestResult] = field(default_factory=list)

    @property
    def failed_tests(self) -> List[TestResult]:
        return [r for r in self.results if r.failed]

    @property
    def models_with_failures(self) -> List[str]:
        return list({r.model_name for r in self.failed_tests})


class DbtArtifactParser:
    """
    Parses dbt run_results.json from a file path or dict.
    Filters to test nodes only (skips model/snapshot results).
    """

    def parse_file(self, path: str | Path) -> RunResults:
        path = Path(path)
        if not path.exists():
            raise FileNotFoundError(f"run_results.json not found at: {path}")

        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)

        logger.info("parsing_run_results", path=str(path))
        return self.parse_dict(data)

    def parse_dict(self, data: Dict[str, Any]) -> RunResults:
        metadata = data.get("metadata", {})
        generated_at_raw = metadata.get("generated_at", "")

        try:
            generated_at = datetime.fromisoformat(
                generated_at_raw.replace("Z", "+00:00")
            )
        except (ValueError, AttributeError):
            generated_at = datetime.utcnow()

        run_id = f"dbt-run-{generated_at.strftime('%Y%m%d%H%M%S')}"

        results = []
        for raw in data.get("results", []):
            parsed = self._parse_result(raw)
            if parsed is not None:
                results.append(parsed)

        logger.info(
            "run_results_parsed",
            run_id=run_id,
            total=len(data.get("results", [])),
            tests=len(results),
            failures=sum(1 for r in results if r.failed),
        )

        return RunResults(
            generated_at=generated_at,
            dbt_version=metadata.get("dbt_version", "unknown"),
            run_id=run_id,
            results=results,
        )

    def _parse_result(self, raw: Dict[str, Any]) -> Optional[TestResult]:
        """Parse one result entry. Returns None if not a test node."""
        unique_id = raw.get("unique_id", "")

        # Only process test nodes
        if not unique_id.startswith("test."):
            return None

        node = raw.get("node", {})
        config = node.get("config", {})
        meta = config.get("meta", {})
        test_meta = node.get("test_metadata", {})
        kwargs = test_meta.get("kwargs", {})

        # Extract model name from depends_on
        model_name = self._extract_model_name(node)

        return TestResult(
            unique_id=unique_id,
            test_name=test_meta.get("name", node.get("name", "unknown")),
            model_name=model_name,
            column_name=kwargs.get("column_name"),
            status=raw.get("status", "unknown"),
            failures=int(raw.get("failures", 0) or 0),
            execution_time=float(raw.get("execution_time", 0.0)),
            message=raw.get("message"),
            agent_severity=meta.get("agent_severity"),
            blast_radius=meta.get("blast_radius"),
            oncall_team=meta.get("oncall_team"),
        )

    @staticmethod
    def _extract_model_name(node: Dict[str, Any]) -> str:
        """
        Extract the parent model name from depends_on.nodes.
        e.g. "model.project.fct_orders" → "fct_orders"
        Falls back to parsing the test unique_id.
        """
        depends_on = node.get("depends_on", {}).get("nodes", [])
        for dep in depends_on:
            if dep.startswith("model."):
                return dep.split(".")[-1]

        # Fallback: parse from node name
        # "not_null_fct_orders_order_id" → best effort
        name = node.get("name", "unknown")
        return name
