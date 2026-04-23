"""
Lineage & Impact Agent — dbt Manifest Parser

Parses dbt manifest.json and builds an in-memory DAG of all models,
their dependencies, exposures (dashboards), and sources.

manifest.json key sections used:
  nodes      → all models, tests, snapshots (we use models only)
  sources    → raw source tables
  exposures  → downstream consumers (dashboards, ML features, apps)
  parent_map → precomputed {node_id: [parent_ids]} (dbt >= 1.5)
  child_map  → precomputed {node_id: [child_ids]}  (dbt >= 1.5)

If child_map is absent (older dbt), we build it from parent_map.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Set

logger = logging.getLogger(__name__)


@dataclass
class ModelNode:
    unique_id: str          # e.g. "model.project.fct_orders"
    name: str               # e.g. "fct_orders"
    schema: str
    database: str
    description: str
    tags: List[str]
    meta: Dict[str, Any]   # arbitrary metadata from schema.yml


@dataclass
class ExposureNode:
    unique_id: str
    name: str               # e.g. "executive_revenue_dashboard"
    exposure_type: str      # dashboard | ml | application | analysis
    description: str
    depends_on_models: List[str]   # model unique_ids this exposure depends on
    owner: Optional[str]
    url: Optional[str]
    sla: Optional[str]      # from meta: "daily_08:00_UTC"


@dataclass
class DbtDag:
    """
    In-memory DAG built from dbt manifest.json.

    child_map: model_unique_id → list of direct child unique_ids
    models:    unique_id → ModelNode
    exposures: unique_id → ExposureNode
    """
    models: Dict[str, ModelNode]
    exposures: Dict[str, ExposureNode]
    child_map: Dict[str, List[str]]   # downstream edges
    parent_map: Dict[str, List[str]]  # upstream edges

    def get_model_by_name(self, name: str) -> Optional[ModelNode]:
        """Look up a model by its short name (e.g. 'fct_orders')."""
        for node in self.models.values():
            if node.name == name:
                return node
        return None

    def downstream_models(self, unique_id: str) -> Set[str]:
        """
        BFS traversal: return all transitive downstream model unique_ids
        starting from unique_id (exclusive — does not include start node).
        """
        visited: Set[str] = set()
        queue = list(self.child_map.get(unique_id, []))

        while queue:
            current = queue.pop(0)
            if current in visited:
                continue
            visited.add(current)
            queue.extend(self.child_map.get(current, []))

        # Filter to model nodes only (exclude test/snapshot children)
        return {uid for uid in visited if uid.startswith("model.")}

    def exposures_for_models(self, model_unique_ids: Set[str]) -> List[ExposureNode]:
        """Return all exposures that depend on any of the given model unique_ids."""
        result = []
        for exposure in self.exposures.values():
            if any(m in model_unique_ids for m in exposure.depends_on_models):
                result.append(exposure)
        return result


class ManifestParser:
    """Parses dbt manifest.json into a DbtDag."""

    def parse_file(self, path: str | Path) -> DbtDag:
        path = Path(path)
        if not path.exists():
            raise FileNotFoundError(f"manifest.json not found at: {path}")

        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)

        logger.info("parsing_manifest", path=str(path))
        return self.parse_dict(data)

    def parse_dict(self, data: Dict[str, Any]) -> DbtDag:
        models    = self._parse_models(data.get("nodes", {}))
        exposures = self._parse_exposures(data.get("exposures", {}))

        # Use precomputed maps if available (dbt >= 1.5)
        child_map  = {k: list(v) for k, v in data.get("child_map", {}).items()}
        parent_map = {k: list(v) for k, v in data.get("parent_map", {}).items()}

        # Build child_map from parent_map if absent
        if not child_map and parent_map:
            child_map = self._build_child_map(parent_map)

        # Ensure every model has an entry in child_map (even if no children)
        for uid in models:
            child_map.setdefault(uid, [])

        logger.info(
            "manifest_parsed | models=%s exposures=%s",
            len(models), len(exposures),
        )

        return DbtDag(
            models=models,
            exposures=exposures,
            child_map=child_map,
            parent_map=parent_map,
        )

    # ── Parsers ───────────────────────────────────────────────

    def _parse_models(self, nodes: Dict[str, Any]) -> Dict[str, ModelNode]:
        models = {}
        for uid, node in nodes.items():
            if node.get("resource_type") != "model":
                continue
            models[uid] = ModelNode(
                unique_id=uid,
                name=node.get("name", ""),
                schema=node.get("schema", ""),
                database=node.get("database", ""),
                description=node.get("description", ""),
                tags=node.get("tags", []),
                meta=node.get("config", {}).get("meta", {}),
            )
        return models

    def _parse_exposures(self, exposures: Dict[str, Any]) -> Dict[str, ExposureNode]:
        result = {}
        for uid, exp in exposures.items():
            depends_on = exp.get("depends_on", {}).get("nodes", [])
            meta = exp.get("meta", {})
            result[uid] = ExposureNode(
                unique_id=uid,
                name=exp.get("name", ""),
                exposure_type=exp.get("type", "unknown"),
                description=exp.get("description", ""),
                depends_on_models=[n for n in depends_on if n.startswith("model.")],
                owner=exp.get("owner", {}).get("name") if isinstance(exp.get("owner"), dict) else exp.get("owner"),
                url=exp.get("url"),
                sla=meta.get("sla"),
            )
        return result

    @staticmethod
    def _build_child_map(parent_map: Dict[str, List[str]]) -> Dict[str, List[str]]:
        """Invert parent_map → child_map."""
        child_map: Dict[str, List[str]] = {}
        for child, parents in parent_map.items():
            for parent in parents:
                child_map.setdefault(parent, [])
                if child not in child_map[parent]:
                    child_map[parent].append(child)
        return child_map
