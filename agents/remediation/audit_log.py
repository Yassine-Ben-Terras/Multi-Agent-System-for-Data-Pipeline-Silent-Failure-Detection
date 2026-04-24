"""
Remediation Agent — Audit Log

Persists every action taken by the Remediation Agent
to the remediation_actions table in PostgreSQL.

This is the system's paper trail:
  - What action was taken
  - Against which model/partition
  - Whether it succeeded
  - Full payload (for replay/debugging)
  - Timestamp

The audit log is append-only. Actions are never deleted.
"""

from __future__ import annotations

import json
import logging
from typing import Optional

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from agents.remediation.playbook import PlannedAction
from config.schemas import RemediationAction

logger = logging.getLogger(__name__)


class RemediationAuditLog:
    """Appends RemediationAction records to PostgreSQL."""

    def __init__(self, db_url: str):
        self._engine: Engine = create_engine(db_url, pool_pre_ping=True)

    def record(
        self,
        incident_id: str,
        action: PlannedAction,
        success: bool,
        error_message: Optional[str] = None,
    ) -> RemediationAction:
        """
        Persist one action result. Returns the RemediationAction schema object.
        """
        record = RemediationAction(
            incident_id=incident_id,
            action_type=action.action_type,
            target=action.target,
            payload=action.payload,
            success=success,
            error_message=error_message,
        )

        sql = text("""
            INSERT INTO remediation_actions
                (incident_id, action_type, target, payload, success, error_message, executed_at)
            VALUES
                (:incident_id, :action_type, :target, :payload, :success, :error, NOW())
        """)

        with self._engine.begin() as conn:
            conn.execute(sql, {
                "incident_id":  incident_id,
                "action_type":  action.action_type.value,
                "target":       action.target,
                "payload":      json.dumps(action.payload),
                "success":      success,
                "error":        error_message,
            })

        level = logging.INFO if success else logging.ERROR
        logger.log(
            level,
            "action_recorded | type=%s target=%s success=%s",
            action.action_type.value,
            action.target,
            success,
        )

        return record

    def get_incident_actions(self, incident_id: str) -> list[dict]:
        """Retrieve all actions taken for a given incident (for debugging/UI)."""
        sql = text("""
            SELECT action_type, target, success, error_message, executed_at
            FROM remediation_actions
            WHERE incident_id = :incident_id
            ORDER BY executed_at ASC
        """)
        with self._engine.connect() as conn:
            rows = conn.execute(sql, {"incident_id": incident_id}).fetchall()
        return [
            {
                "action_type":    r.action_type,
                "target":         r.target,
                "success":        r.success,
                "error_message":  r.error_message,
                "executed_at":    str(r.executed_at),
            }
            for r in rows
        ]
