"""
Lineage & Impact Agent — Main Entrypoint

Observe → Reason → Signal loop:

  1. OBSERVE   Poll agents.anomalies for any AnomalySignal
  2. REASON    Look up the failed model in the DAG
               BFS traverse all downstream models
               Find all exposures (dashboards, ML) affected
               Compute blast radius + severity multiplier
  3. SIGNAL    Emit ConfirmedIncident to agents.confirmed_incidents
               (unless shadow mode)

DAG refresh:
  The manifest.json is loaded on startup and reloaded whenever
  the artifact watcher detects a new version (same watcher as
  quality_auditor, watching for manifest.json instead).

Usage:
  python -m agents.lineage_impact.main
  AGENT_MODE=shadow python -m agents.lineage_impact.main
"""

from __future__ import annotations
import logging
import os
import signal as os_signal
import threading
import time
from typing import Optional
import structlog

from config.schemas import (
    AgentHeartbeat,
    AgentMode,
    AnomalySignal,
    ConfirmedIncident,
    Severity,
)
from agents.lineage_impact.calculator import BlastRadiusCalculator
from agents.lineage_impact.kafka_io import AnomalyConsumer, IncidentProducer
from agents.lineage_impact.manifest_parser import DbtDag, ManifestParser

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
logger = structlog.get_logger("lineage_impact")

# ── Config ────────────────────────────────────────────────────
BOOTSTRAP_SERVERS  = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
AGENT_MODE         = AgentMode(os.getenv("AGENT_MODE", "shadow"))
ARTIFACTS_PATH     = os.getenv("DBT_ARTIFACTS_PATH", "./dbt_integration/artifacts")
HEARTBEAT_INTERVAL = int(os.getenv("HEARTBEAT_INTERVAL_SECONDS", "60"))
MANIFEST_PATH      = os.path.join(ARTIFACTS_PATH, "latest", "manifest.json")
POLL_TIMEOUT       = 1.0


class LineageImpactAgent:

    AGENT_NAME = "lineage_impact"

    def __init__(self):
        self._running    = False
        self._mode       = AGENT_MODE
        self._dag: Optional[DbtDag] = None
        self._dag_lock   = threading.Lock()

        self._parser     = ManifestParser()
        self._calculator = BlastRadiusCalculator()
        self._consumer   = AnomalyConsumer(bootstrap_servers=BOOTSTRAP_SERVERS)
        self._producer   = IncidentProducer(bootstrap_servers=BOOTSTRAP_SERVERS)

        # Stats
        self._signals_received  = 0
        self._incidents_emitted = 0
        self._dag_refreshes     = 0

        # Load DAG on startup
        self._load_dag()

        logger.info(
            "agent_initialised",
            agent=self.AGENT_NAME,
            mode=self._mode.value,
            dag_loaded=self._dag is not None,
        )

    # ── Lifecycle ──────────────────────────────────────────

    def start(self) -> None:
        self._running = True
        os_signal.signal(os_signal.SIGINT,  self._handle_shutdown)
        os_signal.signal(os_signal.SIGTERM, self._handle_shutdown)

        threading.Thread(target=self._heartbeat_loop,   daemon=True, name="heartbeat").start()
        threading.Thread(target=self._dag_refresh_loop, daemon=True, name="dag-refresh").start()

        logger.info("agent_starting", agent=self.AGENT_NAME, mode=self._mode.value)
        self._poll_loop()

    def stop(self) -> None:
        logger.info("agent_stopping", agent=self.AGENT_NAME)
        self._running = False
        self._consumer.close()
        self._producer.flush()
        self._log_stats()

    # ── Main loop ──────────────────────────────────────────

    def _poll_loop(self) -> None:
        while self._running:
            try:
                signal = self._consumer.poll(timeout=POLL_TIMEOUT)
                if signal is None:
                    continue
                self._signals_received += 1
                self._process_signal(signal)
            except Exception as e:
                logger.error("poll_loop_error", error=str(e), exc_info=True)
                time.sleep(2)

    def _process_signal(self, signal: AnomalySignal) -> None:
        model_name = signal.model_name
        if not model_name:
            logger.debug("signal_has_no_model_name", signal_id=signal.signal_id)
            return

        with self._dag_lock:
            dag = self._dag

        if dag is None:
            logger.warning("dag_not_loaded | skipping signal for model=%s", model_name)
            return

        # Compute blast radius
        report = self._calculator.calculate(model_name, dag)

        logger.info(
            "blast_radius_computed",
            model=model_name,
            downstream=len(report.downstream_models),
            dashboards=len(report.dashboards),
            ml_features=len(report.ml_features),
            sla_breaches=len(report.sla_breaches),
            multiplier=report.severity_multiplier,
        )

        # Build ConfirmedIncident
        incident = ConfirmedIncident(
            root_cause=signal.anomaly_type.value,
            severity=self._apply_multiplier(signal.severity, report.severity_multiplier),
            pipeline_run_id=signal.pipeline_run_id,
            correlated_signals=[signal],
            blast_radius=report.to_blast_radius(),
            llm_hypothesis=None,   # Orchestrator will enrich with LLM later
        )

        if self._mode == AgentMode.SHADOW:
            logger.info(
                "shadow_mode_suppressed_emit",
                incident_id=incident.incident_id,
                model=model_name,
                summary=report.summary(),
            )
            return

        self._producer.emit_incident(incident)
        self._incidents_emitted += 1

    # ── DAG management ────────────────────────────────────────

    def _load_dag(self) -> None:
        try:
            self._dag = self._parser.parse_file(MANIFEST_PATH)
            self._dag_refreshes += 1
            logger.info(
                "dag_loaded",
                models=len(self._dag.models),
                exposures=len(self._dag.exposures),
                path=MANIFEST_PATH,
            )
        except FileNotFoundError:
            logger.warning("manifest_not_found | path=%s (DAG will be None until file appears)", MANIFEST_PATH)
            self._dag = None
        except Exception as e:
            logger.error("dag_load_error | error=%s", str(e), exc_info=True)
            self._dag = None

    def _dag_refresh_loop(self) -> None:
        """Check for a new manifest.json every 5 minutes and reload if changed."""
        import hashlib
        last_hash = ""

        while self._running:
            time.sleep(300)  # check every 5 minutes
            try:
                import os
                if not os.path.exists(MANIFEST_PATH):
                    continue
                with open(MANIFEST_PATH, "rb") as f:
                    current_hash = hashlib.md5(f.read()).hexdigest()
                if current_hash != last_hash:
                    last_hash = current_hash
                    with self._dag_lock:
                        self._load_dag()
                    logger.info("dag_refreshed")
            except Exception as e:
                logger.error("dag_refresh_error | error=%s", str(e))

    # ── Helpers ───────────────────────────────────────────────

    @staticmethod
    def _apply_multiplier(severity: Severity, multiplier: float) -> Severity:
        """
        Escalate severity based on blast radius size.
        Multiplier >= 2.0 escalates by one level.
        """
        if multiplier < 2.0:
            return severity
        order = [Severity.LOW, Severity.MEDIUM, Severity.HIGH, Severity.CRITICAL]
        idx = order.index(severity)
        return order[min(idx + 1, len(order) - 1)]

    def _heartbeat_loop(self) -> None:
        while self._running:
            try:
                with self._dag_lock:
                    dag_size = len(self._dag.models) if self._dag else 0
                hb = AgentHeartbeat(
                    agent_name=self.AGENT_NAME,
                    status="alive",
                    metadata={
                        "mode":               self._mode.value,
                        "signals_received":   self._signals_received,
                        "incidents_emitted":  self._incidents_emitted,
                        "dag_model_count":    dag_size,
                        "dag_refreshes":      self._dag_refreshes,
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
            signals_received=self._signals_received,
            incidents_emitted=self._incidents_emitted,
            dag_refreshes=self._dag_refreshes,
        )

    def _handle_shutdown(self, signum, frame) -> None:
        logger.info("shutdown_signal_received | signal=%s", signum)
        self.stop()


def main():
    LineageImpactAgent().start()


if __name__ == "__main__":
    main()
