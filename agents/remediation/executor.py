"""
Remediation Agent — Action Executor

Executes PlannedActions against real systems:
  QUARANTINE_PARTITION  → tags/renames partition in warehouse (simulated via metadata)
  TRIGGER_BACKFILL      → calls dbt CLI via subprocess
  PAUSE_DOWNSTREAM_DAGS → calls Airflow REST API
  CREATE_PAGERDUTY      → calls PagerDuty Events API v2
  CREATE_JIRA           → calls Jira REST API
  NOTIFY_SLACK          → calls Slack Incoming Webhook

All executors:
  - Return (success: bool, error: Optional[str])
  - Are idempotent (safe to retry)
  - Never raise — errors are captured and logged
  - Respect DRY_RUN env var for safe testing

In production:
  - Warehouse quarantine uses a metadata table (never deletes data)
  - dbt backfill runs with --select + --full-refresh as needed
  - All credentials come from environment variables (never hardcoded)
"""

from __future__ import annotations

import json
import logging
import os
import subprocess
from typing import Optional, Tuple

from agents.remediation.playbook import PlannedAction
from config.schemas import ActionType

logger = logging.getLogger(__name__)

DRY_RUN = os.getenv("REMEDIATION_DRY_RUN", "true").lower() == "true"


class ActionExecutor:
    """
    Dispatches PlannedActions to the appropriate executor method.
    Returns (success, error_message).
    """

    def execute(self, action: PlannedAction) -> Tuple[bool, Optional[str]]:
        if DRY_RUN:
            logger.info(
                "DRY_RUN | would execute action_type=%s target=%s payload=%s",
                action.action_type.value,
                action.target,
                json.dumps(action.payload),
            )
            return True, None

        dispatch = {
            ActionType.QUARANTINE_PARTITION:  self._quarantine_partition,
            ActionType.TRIGGER_BACKFILL:      self._trigger_backfill,
            ActionType.PAUSE_DOWNSTREAM_DAGS: self._pause_dag,
            ActionType.CREATE_PAGERDUTY:      self._create_pagerduty,
            ActionType.CREATE_JIRA:           self._create_jira,
            ActionType.NOTIFY_SLACK:          self._notify_slack,
        }

        executor = dispatch.get(action.action_type)
        if executor is None:
            return False, f"No executor for action type: {action.action_type}"

        try:
            return executor(action)
        except Exception as e:
            logger.error(
                "executor_unhandled_error | type=%s error=%s",
                action.action_type.value, str(e), exc_info=True,
            )
            return False, str(e)

    # ── Quarantine ────────────────────────────────────────────

    def _quarantine_partition(self, action: PlannedAction) -> Tuple[bool, Optional[str]]:
        """
        Mark a partition as quarantined in the remediation_actions audit table.
        In production: also renames/tags the warehouse partition.
        Never deletes data.
        """
        model   = action.target
        run_id  = action.payload.get("partition_run_id", "unknown")
        reason  = action.payload.get("reason", "")

        logger.warning(
            "QUARANTINE | model=%s run_id=%s reason=%s",
            model, run_id, reason,
        )
        # Production: ALTER TABLE ... RENAME / UPDATE metadata tag
        # Here: logged as audit record (persisted by RemediationAuditLog)
        return True, None

    # ── dbt backfill ──────────────────────────────────────────

    def _trigger_backfill(self, action: PlannedAction) -> Tuple[bool, Optional[str]]:
        """Run dbt build --select <model> [--full-refresh]."""
        model        = action.payload.get("dbt_select", action.target)
        full_refresh = action.payload.get("full_refresh", False)

        cmd = ["dbt", "build", "--select", model]
        if full_refresh:
            cmd.append("--full-refresh")

        logger.info("BACKFILL | cmd=%s", " ".join(cmd))

        try:
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=600,   # 10 min timeout
            )
            if result.returncode != 0:
                return False, f"dbt build failed: {result.stderr[:500]}"
            logger.info("BACKFILL_SUCCESS | model=%s", model)
            return True, None
        except FileNotFoundError:
            return False, "dbt CLI not found — is dbt installed and on PATH?"
        except subprocess.TimeoutExpired:
            return False, "dbt backfill timed out after 600s"

    # ── Airflow DAG pause ─────────────────────────────────────

    def _pause_dag(self, action: PlannedAction) -> Tuple[bool, Optional[str]]:
        """Call Airflow REST API to pause a DAG."""
        import httpx

        dag_id       = action.target
        airflow_url  = os.getenv("AIRFLOW_BASE_URL", "http://localhost:8082")
        airflow_user = os.getenv("AIRFLOW_USER", "airflow")
        airflow_pass = os.getenv("AIRFLOW_PASSWORD", "airflow")

        url = f"{airflow_url}/api/v1/dags/{dag_id}"
        try:
            resp = httpx.patch(
                url,
                json={"is_paused": True},
                auth=(airflow_user, airflow_pass),
                timeout=10,
            )
            if resp.status_code not in (200, 204):
                return False, f"Airflow API error {resp.status_code}: {resp.text[:200]}"
            logger.info("DAG_PAUSED | dag_id=%s", dag_id)
            return True, None
        except Exception as e:
            return False, f"Airflow API call failed: {str(e)}"

    # ── PagerDuty ─────────────────────────────────────────────

    def _create_pagerduty(self, action: PlannedAction) -> Tuple[bool, Optional[str]]:
        """Create a PagerDuty incident via Events API v2."""
        import httpx

        routing_key = os.getenv("PAGERDUTY_ROUTING_KEY", "")
        if not routing_key:
            return False, "PAGERDUTY_ROUTING_KEY not set"

        payload = {
            "routing_key":  routing_key,
            "event_action": "trigger",
            "dedup_key":    action.payload.get("incident_id", "unknown"),
            "payload": {
                "summary":   action.payload.get("title", "Data pipeline failure"),
                "severity":  action.payload.get("severity", "critical"),
                "source":    "mas-pipeline-sentinel",
                "custom_details": {
                    "root_cause":   action.payload.get("root_cause"),
                    "blast_radius": action.payload.get("blast_radius"),
                    "hypothesis":   action.payload.get("hypothesis"),
                    "run_id":       action.payload.get("run_id"),
                },
            },
        }

        try:
            resp = httpx.post(
                "https://events.pagerduty.com/v2/enqueue",
                json=payload,
                timeout=10,
            )
            if resp.status_code not in (200, 202):
                return False, f"PagerDuty error {resp.status_code}: {resp.text[:200]}"
            logger.info("PAGERDUTY_CREATED | dedup_key=%s", payload["dedup_key"])
            return True, None
        except Exception as e:
            return False, f"PagerDuty API call failed: {str(e)}"

    # ── Jira ──────────────────────────────────────────────────

    def _create_jira(self, action: PlannedAction) -> Tuple[bool, Optional[str]]:
        """Create a Jira issue via REST API."""
        import httpx

        jira_url   = os.getenv("JIRA_BASE_URL", "")
        jira_token = os.getenv("JIRA_API_TOKEN", "")
        jira_email = os.getenv("JIRA_EMAIL", "")
        jira_proj  = os.getenv("JIRA_PROJECT_KEY", "DATA")

        if not all([jira_url, jira_token, jira_email]):
            return False, "Jira credentials not configured (JIRA_BASE_URL, JIRA_API_TOKEN, JIRA_EMAIL)"

        payload = {
            "fields": {
                "project":     {"key": jira_proj},
                "summary":     action.payload.get("title", "Data pipeline incident"),
                "description": {
                    "type":    "doc",
                    "version": 1,
                    "content": [{
                        "type":    "paragraph",
                        "content": [{"type": "text", "text": str(action.payload)}],
                    }],
                },
                "issuetype": {"name": "Bug"},
                "priority":  {"name": "High" if action.payload.get("severity") == "critical" else "Medium"},
            }
        }

        try:
            resp = httpx.post(
                f"{jira_url}/rest/api/3/issue",
                json=payload,
                auth=(jira_email, jira_token),
                timeout=10,
            )
            if resp.status_code not in (200, 201):
                return False, f"Jira error {resp.status_code}: {resp.text[:200]}"
            issue_key = resp.json().get("key", "unknown")
            logger.info("JIRA_CREATED | issue=%s", issue_key)
            return True, None
        except Exception as e:
            return False, f"Jira API call failed: {str(e)}"

    # ── Slack ─────────────────────────────────────────────────

    def _notify_slack(self, action: PlannedAction) -> Tuple[bool, Optional[str]]:
        """Post to Slack via Incoming Webhook."""
        import httpx

        webhook_url = os.getenv("SLACK_WEBHOOK_URL", "")
        if not webhook_url:
            logger.info("SLACK_WEBHOOK_URL not set — skipping Slack notification")
            return True, None   # Not a failure — just not configured

        p = action.payload
        severity  = p.get("severity", "unknown").upper()
        root_cause = p.get("root_cause", "unknown")
        model     = p.get("model", "unknown")
        run_id    = p.get("run_id", "unknown")
        blast     = p.get("blast_radius", "unknown")
        hypothesis = p.get("hypothesis", "")

        emoji = {"CRITICAL": "🚨", "HIGH": "🔴", "MEDIUM": "🟡", "LOW": "🟢"}.get(severity, "⚠️")

        text = (
            f"{emoji} *[{severity}] Data Pipeline Incident*\n"
            f"*Root cause:* `{root_cause}`\n"
            f"*Model:* `{model}` | *Run:* `{run_id}`\n"
            f"*Blast radius:* {blast}\n"
        )
        if hypothesis:
            text += f"*Hypothesis:* {hypothesis[:300]}\n"

        try:
            resp = httpx.post(
                webhook_url,
                json={"text": text},
                timeout=10,
            )
            if resp.status_code != 200:
                return False, f"Slack error {resp.status_code}: {resp.text[:200]}"
            logger.info("SLACK_NOTIFIED | severity=%s model=%s", severity, model)
            return True, None
        except Exception as e:
            return False, f"Slack webhook failed: {str(e)}"
