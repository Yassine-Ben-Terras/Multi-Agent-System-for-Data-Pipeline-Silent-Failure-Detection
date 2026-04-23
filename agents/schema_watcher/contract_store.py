"""
Schema Watcher — Contract Store

Manages registered schema contracts in PostgreSQL.
A "contract" is the expected schema for a given source — the
version we trust and compare all incoming snapshots against.

Tables used:
  schema_contracts  — registered (source, column, type, nullable, criticality)
  schema_history    — full history of every schema version seen per source
"""

from __future__ import annotations

import hashlib
import json
import logging
from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List, Optional


from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

logger = logging.getLogger(__name__)

@dataclass
class ColumnDef:
    name: str
    dtype: str
    nullable: bool
    criticality: str = "standard"   # critical | standard | low

@dataclass
class SchemaContract:
    source: str
    columns: List[ColumnDef]
    version_hash: str
    registered_at: Optional[datetime] = None

    def as_dict(self) -> Dict[str, ColumnDef]:
        """Return columns indexed by name for O(1) lookup."""
        return {col.name: col for col in self.columns}

    @staticmethod
    def compute_hash(columns: List[ColumnDef]) -> str:
        payload = json.dumps(
            [{"name": c.name, "dtype": c.dtype, "nullable": c.nullable} for c in columns],
            sort_keys=True,
        )
        return hashlib.sha256(payload.encode()).hexdigest()[:16]


class ContractStore:
    """
    Reads and writes schema contracts to PostgreSQL.

    The contract for a source is updated whenever the Schema Watcher
    receives an operator instruction to 'accept' a new schema version.
    On first sight of a source, the incoming schema is auto-registered
    as the initial contract (bootstrap mode).
    """

    def __init__(self, db_url: str):
        self._engine: Engine = create_engine(db_url, pool_pre_ping=True)
        self._ensure_tables()

    # ── Read ──────────────────────────────────────────────────

    def get_contract(self, source: str) -> Optional[SchemaContract]:
        sql = text("""
            SELECT column_name, dtype, nullable, criticality
            FROM schema_contracts
            WHERE source = :source
            ORDER BY column_name
        """)
        with self._engine.connect() as conn:
            rows = conn.execute(sql, {"source": source}).fetchall()

        if not rows:
            return None

        columns = [
            ColumnDef(
                name=row.column_name,
                dtype=row.dtype,
                nullable=row.nullable,
                criticality=row.criticality,
            )
            for row in rows
        ]
        version_hash = SchemaContract.compute_hash(columns)
        return SchemaContract(source=source, columns=columns, version_hash=version_hash)

    # ── Write ─────────────────────────────────────────────────

    def register_contract(self, source: str, columns: List[ColumnDef]) -> SchemaContract:
        """
        Register or overwrite the schema contract for a source.
        Called on bootstrap (first sight) or when an operator accepts a new schema.
        """
        # Delete existing contract for this source
        with self._engine.begin() as conn:
            conn.execute(text("DELETE FROM schema_contracts WHERE source = :source"), {"source": source})
            for col in columns:
                conn.execute(text("""
                    INSERT INTO schema_contracts (source, column_name, dtype, nullable, criticality)
                    VALUES (:source, :name, :dtype, :nullable, :criticality)
                """), {
                    "source": source,
                    "name": col.name,
                    "dtype": col.dtype,
                    "nullable": col.nullable,
                    "criticality": col.criticality,
                })

        version_hash = SchemaContract.compute_hash(columns)
        logger.info(
            "contract_registered",
            extra={"source": source, "columns": len(columns), "hash": version_hash},
        )
        return SchemaContract(source=source, columns=columns, version_hash=version_hash)

    def record_history(
        self,
        source: str,
        version_hash: str,
        pipeline_run_id: str,
        columns: List[ColumnDef],
    ) -> None:
        """Append a schema snapshot to the history table."""
        with self._engine.begin() as conn:
            conn.execute(text("""
                INSERT INTO schema_history
                    (source, version_hash, pipeline_run_id, columns_json, seen_at)
                VALUES (:source, :hash, :run_id, :columns, NOW())
                ON CONFLICT (source, version_hash) DO NOTHING
            """), {
                "source": source,
                "hash": version_hash,
                "run_id": pipeline_run_id,
                "columns": json.dumps([
                    {"name": c.name, "dtype": c.dtype, "nullable": c.nullable}
                    for c in columns
                ]),
            })

    # ── Schema setup ──────────────────────────────────────────

    def _ensure_tables(self) -> None:
        with self._engine.begin() as conn:
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS schema_contracts (
                    id          SERIAL PRIMARY KEY,
                    source      VARCHAR(256) NOT NULL,
                    column_name VARCHAR(256) NOT NULL,
                    dtype       VARCHAR(64)  NOT NULL,
                    nullable    BOOLEAN      NOT NULL DEFAULT TRUE,
                    criticality VARCHAR(32)  NOT NULL DEFAULT 'standard',
                    registered_at TIMESTAMPTZ DEFAULT NOW(),
                    UNIQUE (source, column_name)
                )
            """))
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS schema_history (
                    id              SERIAL PRIMARY KEY,
                    source          VARCHAR(256) NOT NULL,
                    version_hash    VARCHAR(32)  NOT NULL,
                    pipeline_run_id VARCHAR(64),
                    columns_json    JSONB,
                    seen_at         TIMESTAMPTZ DEFAULT NOW(),
                    UNIQUE (source, version_hash)
                )
            """))
        logger.debug("schema_tables_ensured")
