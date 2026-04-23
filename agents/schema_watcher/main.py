"""
Schema Watcher Agent — Main Entrypoint

Observe → Reason → Signal loop:

  1. OBSERVE   Poll pipeline.signals.raw for any stage that includes a schema_snapshot
  2. REASON    Diff incoming schema against registered contract via SchemaDiffer
               Bootstrap: if no contract exists, register the first snapshot as the contract
  3. SIGNAL    Emit AnomalySignal(SCHEMA_DRIFT) to agents.anomalies (unless shadow mode)

Usage:
  python -m agents.schema_watcher.main
  AGENT_MODE=shadow python -m agents.schema_watcher.main
"""

from __future__ import annotations

import logging
import os
import signal as os_signal
import threading
import time

import structlog

from config.schemas import AgentHeartbeat, AgentMode, PipelineSignal
from agents.schema_watcher.contract_store import ColumnDef, ContractStore, SchemaContract
from agents.schema_watcher.differ import SchemaDiffer
from agents.schema_watcher.kafka_io import SchemaSignalConsumer, SchemaDriftProducer

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
logger = structlog.get_logger("schema_watcher")

# ── Config ────────────────────────────────────────────────────
BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
DB_URL = (
    f"postgresql://{os.getenv('POSTGRES_USER', 'mas_user')}"
    f":{os.getenv('POSTGRES_PASSWORD', 'changeme')}"
    f"@{os.getenv('POSTGRES_HOST', 'localhost')}"
    f":{os.getenv('POSTGRES_PORT', '5432')}"
    f"/{os.getenv('POSTGRES_DB', 'mas_sentinel')}"
)
AGENT_MODE      = AgentMode(os.getenv("AGENT_MODE", "shadow"))
HEARTBEAT_INTERVAL = int(os.getenv("HEARTBEAT_INTERVAL_SECONDS", "60"))
POLL_TIMEOUT    = 1.0


class SchemaWatcherAgent:
    """
    The Schema Watcher Agent.

    On first sight of a source: auto-registers its schema as the contract (bootstrap).
    On subsequent signals: diffs against the registered contract and emits drift signals.
    """

    AGENT_NAME = "schema_watcher"

    def __init__(self):
        self._running   = False
        self._mode      = AGENT_MODE

        self._contracts  = ContractStore(db_url=DB_URL)
        self._differ     = SchemaDiffer()
        self._consumer   = SchemaSignalConsumer(bootstrap_servers=BOOTSTRAP_SERVERS)
        self._producer   = SchemaDriftProducer(bootstrap_servers=BOOTSTRAP_SERVERS)

        # Stats
        self._signals_processed = 0
        self._drifts_detected   = 0
        self._drifts_emitted    = 0
        self._bootstraps        = 0

        logger.info("agent_initialised", agent=self.AGENT_NAME, mode=self._mode.value)

    # ── Lifecycle ─────────────────────────────────────────────

    def start(self) -> None:
        self._running = True
        os_signal.signal(os_signal.SIGINT,  self._handle_shutdown)
        os_signal.signal(os_signal.SIGTERM, self._handle_shutdown)

        threading.Thread(target=self._heartbeat_loop, daemon=True, name="heartbeat").start()
        logger.info("agent_starting", agent=self.AGENT_NAME)
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
                signal = self._consumer.poll(timeout=POLL_TIMEOUT)
                if signal is None:
                    continue
                self._signals_processed += 1
                self._process_signal(signal)
            except Exception as e:
                logger.error("poll_loop_error", error=str(e), exc_info=True)
                time.sleep(2)

    def _process_signal(self, signal: PipelineSignal) -> None:
        snapshot = signal.schema_snapshot
        if snapshot is None:
            return  # No schema in this signal — skip

        source = signal.source
        incoming_cols = self._parse_snapshot(snapshot)

        # Record this version in history
        self._contracts.record_history(
            source=source,
            version_hash=snapshot.version_hash,
            pipeline_run_id=signal.pipeline_run_id,
            columns=incoming_cols,
        )

        # Bootstrap: register first-ever schema as the contract
        contract = self._contracts.get_contract(source)
        if contract is None:
            self._contracts.register_contract(source, incoming_cols)
            self._bootstraps += 1
            logger.info(
                "contract_bootstrapped",
                source=source,
                columns=len(incoming_cols),
                run_id=signal.pipeline_run_id,
            )
            return

        # Hash match — no diff needed
        if contract.version_hash == snapshot.version_hash:
            logger.debug("schema_match", source=source, hash=snapshot.version_hash)
            return

        # Run the diff
        result = self._differ.diff(
            source=source,
            contract=contract,
            incoming_columns=incoming_cols,
        )

        if not result.has_drift:
            return

        self._drifts_detected += 1
        anomaly = result.to_anomaly_signal(pipeline_run_id=signal.pipeline_run_id)

        logger.warning(
            "schema_drift_detected",
            source=source,
            severity=anomaly.severity.value,
            changes=len(result.changes),
            summary=result.changes[0].change_type if result.changes else "unknown",
        )

        if self._mode == AgentMode.SHADOW:
            logger.info("shadow_mode_suppressed_emit", signal_id=anomaly.signal_id)
            return

        self._producer.emit_drift(anomaly)
        self._drifts_emitted += 1

    # ── Helpers ───────────────────────────────────────────────

    @staticmethod
    def _parse_snapshot(snapshot) -> list[ColumnDef]:
        """Convert raw schema_snapshot.columns list into ColumnDef objects."""
        cols = []
        for col in snapshot.columns:
            cols.append(ColumnDef(
                name=col.get("name", ""),
                dtype=col.get("type", col.get("dtype", "VARCHAR")),
                nullable=col.get("nullable", True),
                criticality=col.get("criticality", "standard"),
            ))
        return cols

    def _heartbeat_loop(self) -> None:
        while self._running:
            try:
                hb = AgentHeartbeat(
                    agent_name=self.AGENT_NAME,
                    status="alive",
                    metadata={
                        "mode": self._mode.value,
                        "signals_processed": self._signals_processed,
                        "drifts_detected":   self._drifts_detected,
                        "drifts_emitted":    self._drifts_emitted,
                        "bootstraps":        self._bootstraps,
                    },
                )
                self._producer.emit_heartbeat(hb)
            except Exception as e:
                logger.error("heartbeat_error", error=str(e))
            time.sleep(HEARTBEAT_INTERVAL)

    def _log_stats(self) -> None:
        logger.info(
            "agent_stats",
            agent=self.AGENT_NAME,
            signals_processed=self._signals_processed,
            drifts_detected=self._drifts_detected,
            drifts_emitted=self._drifts_emitted,
        )

    def _handle_shutdown(self, signum, frame) -> None:
        logger.info("shutdown_signal_received", signal=signum)
        self.stop()


def main():
    SchemaWatcherAgent().start()


if __name__ == "__main__":
    main()
