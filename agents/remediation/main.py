"""
Remediation Agent — Main Entrypoint

Observe → Reason → Signal loop:

  1. OBSERVE   Poll agents.confirmed_incidents for ConfirmedIncident
  2. REASON    Playbook.select() → ordered list of PlannedActions
               Skip actions that require_approval in supervised mode
  3. ACT       Execute each action via ActionExecutor
               Record every result in audit log (PostgreSQL + Kafka)

Agent modes and their effect on remediation:
  shadow       → no actions executed, playbook logged only
  alert_only   → Slack notification only (no warehouse actions)
  supervised   → LOW/MEDIUM auto, HIGH/CRITICAL require human approval
  full_autonomy→ all actions executed automatically

Usage:
  python -m agents.remediation.main
  AGENT_MODE=supervised python -m agents.remediation.main
"""

from __future__ import annotations

import logging
import os
import signal as os_signal
import threading
import time

import structlog

from config.schemas import AgentHeartbeat, AgentMode, ConfirmedIncident
from agents.remediation.audit_log import RemediationAuditLog
from agents.remediation.executor import ActionExecutor
from agents.remediation.kafka_io import ActionProducer, IncidentConsumer
from agents.remediation.playbook import Playbook

structlog.configure(
    processors=[
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.stdlib.add_log_level,
        structlog.dev.ConsoleRenderer(),
    ],
    wrapper_class=structlog.stdlib.BoundLogger,
    context_class=dict,
    logger_factory=structlog.stdlib.LoggerFactory(),
)
logger = structlog.get_logger("remediation")

# ── Config ────────────────────────────────────────────────────
BOOTSTRAP_SERVERS  = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
DB_URL = (
    f"postgresql://{os.getenv('POSTGRES_USER', 'mas_user')}"
    f":{os.getenv('POSTGRES_PASSWORD', 'changeme')}"
    f"@{os.getenv('POSTGRES_HOST', 'localhost')}"
    f":{os.getenv('POSTGRES_PORT', '5432')}"
    f"/{os.getenv('POSTGRES_DB', 'mas_sentinel')}"
)
AGENT_MODE         = AgentMode(os.getenv("AGENT_MODE", "shadow"))
HEARTBEAT_INTERVAL = int(os.getenv("HEARTBEAT_INTERVAL_SECONDS", "60"))
POLL_TIMEOUT       = 1.0


class RemediationAgent:

    AGENT_NAME = "remediation"

    def __init__(self):
        self._running  = False
        self._mode     = AGENT_MODE

        self._playbook  = Playbook()
        self._executor  = ActionExecutor()
        self._audit     = RemediationAuditLog(db_url=DB_URL)
        self._consumer  = IncidentConsumer(bootstrap_servers=BOOTSTRAP_SERVERS)
        self._producer  = ActionProducer(bootstrap_servers=BOOTSTRAP_SERVERS)

        # Stats
        self._incidents_received  = 0
        self._actions_attempted   = 0
        self._actions_succeeded   = 0
        self._actions_failed      = 0
        self._actions_skipped     = 0   # requires_approval in supervised mode

        logger.info(
            "agent_initialised",
            agent=self.AGENT_NAME,
            mode=self._mode.value,
            dry_run=os.getenv("REMEDIATION_DRY_RUN", "true"),
        )

    # ── Lifecycle ─────────────────────────────────────────────

    def start(self) -> None:
        self._running = True
        os_signal.signal(os_signal.SIGINT,  self._handle_shutdown)
        os_signal.signal(os_signal.SIGTERM, self._handle_shutdown)

        threading.Thread(
            target=self._heartbeat_loop, daemon=True, name="heartbeat"
        ).start()

        logger.info("agent_starting", agent=self.AGENT_NAME, mode=self._mode.value)
        self._poll_loop()

    def stop(self) -> None:
        logger.info("agent_stopping", agent=self.AGENT_NAME)
        self._running = False
        self._consumer.close()
        self._producer.flush()
        self._log_stats()
        logger.info("agent_stopped", agent=self.AGENT_NAME)

    # ── Main loop ─────────────────────────────────────────────

    def _poll_loop(self) -> None:
        while self._running:
            try:
                incident = self._consumer.poll(timeout=POLL_TIMEOUT)
                if incident is None:
                    continue
                self._incidents_received += 1
                self._handle_incident(incident)
            except Exception as e:
                logger.error("poll_loop_error", error=str(e), exc_info=True)
                time.sleep(2)

    def _handle_incident(self, incident: ConfirmedIncident) -> None:
        logger.warning(
            "incident_received",
            incident_id=incident.incident_id[:8],
            severity=incident.severity.value,
            root_cause=incident.root_cause,
            signals=len(incident.correlated_signals),
        )

        # Shadow mode — log only, no actions
        if self._mode == AgentMode.SHADOW:
            actions = self._playbook.select(incident, self._mode)
            logger.info(
                "shadow_mode | would execute %s actions: %s",
                len(actions),
                [a.action_type.value for a in actions],
            )
            return

        # Select actions from playbook
        actions = self._playbook.select(incident, self._mode)

        logger.info(
            "playbook_selected",
            incident_id=incident.incident_id[:8],
            actions=len(actions),
            action_types=[a.action_type.value for a in actions],
        )

        # Execute each action in order
        for action in actions:

            # Supervised mode — skip actions that need approval
            if action.requires_approval and self._mode == AgentMode.SUPERVISED:
                self._actions_skipped += 1
                logger.info(
                    "action_skipped_needs_approval | type=%s target=%s",
                    action.action_type.value, action.target,
                )
                continue

            # Alert-only mode — only Slack notifications
            if self._mode == AgentMode.ALERT_ONLY:
                from config.schemas import ActionType
                if action.action_type != ActionType.NOTIFY_SLACK:
                    self._actions_skipped += 1
                    continue

            self._actions_attempted += 1
            logger.info(
                "executing_action",
                type=action.action_type.value,
                target=action.target,
                description=action.description,
            )

            success, error = self._executor.execute(action)

            # Persist to audit log
            audit_record = self._audit.record(
                incident_id=incident.incident_id,
                action=action,
                success=success,
                error_message=error,
            )

            # Emit to agents.actions_taken
            self._producer.emit_action(audit_record)

            if success:
                self._actions_succeeded += 1
                logger.info(
                    "action_succeeded | type=%s target=%s",
                    action.action_type.value, action.target,
                )
            else:
                self._actions_failed += 1
                logger.error(
                    "action_failed | type=%s target=%s error=%s",
                    action.action_type.value, action.target, error,
                )

    # ── Heartbeat ─────────────────────────────────────────────

    def _heartbeat_loop(self) -> None:
        while self._running:
            try:
                hb = AgentHeartbeat(
                    agent_name=self.AGENT_NAME,
                    status="alive",
                    metadata={
                        "mode":                self._mode.value,
                        "incidents_received":  self._incidents_received,
                        "actions_attempted":   self._actions_attempted,
                        "actions_succeeded":   self._actions_succeeded,
                        "actions_failed":      self._actions_failed,
                        "actions_skipped":     self._actions_skipped,
                        "dry_run":             os.getenv("REMEDIATION_DRY_RUN", "true"),
                    },
                )
                self._producer.emit_heartbeat(hb)
            except Exception as e:
                logger.error("heartbeat_error | error=%s", str(e))
            time.sleep(HEARTBEAT_INTERVAL)

    def _log_stats(self) -> None:
        logger.info(
            "agent_stats",
            agent=self.AGENT_NAME,
            incidents_received=self._incidents_received,
            actions_attempted=self._actions_attempted,
            actions_succeeded=self._actions_succeeded,
            actions_failed=self._actions_failed,
            actions_skipped=self._actions_skipped,
        )

    def _handle_shutdown(self, signum, frame) -> None:
        logger.info("shutdown_signal_received | signal=%s", signum)
        self.stop()


def main():
    RemediationAgent().start()


if __name__ == "__main__":
    main()
