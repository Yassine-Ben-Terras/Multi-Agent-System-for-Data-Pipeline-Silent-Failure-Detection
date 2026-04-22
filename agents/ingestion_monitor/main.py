"""
Ingestion Monitor Agent — Main Entrypoint

The complete Observe → Reason → Signal loop:

  1. OBSERVE   Poll pipeline.signals.raw for ingestion events
  2. REASON    Run row-count z-score + checksum checks via IngestionDetector
  3. SIGNAL    Emit AnomalySignals to agents.anomalies (unless shadow mode)

Agent modes:
  shadow        → detect and log; never emit to Kafka
  alert_only    → emit anomalies; no automated remediation downstream
  supervised    → full emit; remediation limited to LOW severity
  full_autonomy → full emit; remediation handles all severities

Usage:
  python -m agents.ingestion_monitor.main
  AGENT_MODE=shadow python -m agents.ingestion_monitor.main
"""

from __future__ import annotations

import logging
import os
import signal as os_signal
import threading
import time
from datetime import datetime, timezone

import structlog

from config.schemas import AgentHeartbeat, AgentMode, PipelineStage
from agents.ingestion_monitor.baseline import BaselineManager
from agents.ingestion_monitor.detector import IngestionDetector
from agents.ingestion_monitor.kafka_io import AnomalyProducer, SignalConsumer

# ── Logging setup ─────────────────────────────────────────────
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
logger = structlog.get_logger("ingestion_monitor")


# ── Configuration from environment ───────────────────────────
BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
DB_URL = (
    f"postgresql://{os.getenv('POSTGRES_USER', 'mas_user')}"
    f":{os.getenv('POSTGRES_PASSWORD', 'changeme')}"
    f"@{os.getenv('POSTGRES_HOST', 'localhost')}"
    f":{os.getenv('POSTGRES_PORT', '5432')}"
    f"/{os.getenv('POSTGRES_DB', 'mas_sentinel')}"
)
AGENT_MODE = AgentMode(os.getenv("AGENT_MODE", "shadow"))
ZSCORE_THRESHOLD = float(os.getenv("ANOMALY_ZSCORE_THRESHOLD", "2.5"))
HEARTBEAT_INTERVAL = int(os.getenv("HEARTBEAT_INTERVAL_SECONDS", "60"))
POLL_TIMEOUT = 1.0  # seconds


class IngestionMonitorAgent:
    """
    The Ingestion Monitor Agent.

    Lifecycle:
      start()   → begins polling loop + heartbeat thread
      stop()    → graceful shutdown (flushes Kafka producer)
    """

    AGENT_NAME = "ingestion_monitor"

    def __init__(self):
        self._running = False
        self._mode = AGENT_MODE

        # Core components
        self._baseline_mgr = BaselineManager(db_url=DB_URL)
        self._detector = IngestionDetector(
            baseline_manager=self._baseline_mgr,
            zscore_threshold=ZSCORE_THRESHOLD,
        )
        self._consumer = SignalConsumer(bootstrap_servers=BOOTSTRAP_SERVERS)
        self._producer = AnomalyProducer(bootstrap_servers=BOOTSTRAP_SERVERS)

        # Stats
        self._signals_processed = 0
        self._anomalies_detected = 0
        self._anomalies_emitted = 0

        logger.info(
            "agent_initialised",
            agent=self.AGENT_NAME,
            mode=self._mode.value,
            zscore_threshold=ZSCORE_THRESHOLD,
            bootstrap=BOOTSTRAP_SERVERS,
        )

    # ── Lifecycle ─────────────────────────────────────────────

    def start(self) -> None:
        self._running = True
        logger.info("agent_starting", agent=self.AGENT_NAME, mode=self._mode.value)

        # Register OS signal handlers for graceful shutdown
        os_signal.signal(os_signal.SIGINT, self._handle_shutdown)
        os_signal.signal(os_signal.SIGTERM, self._handle_shutdown)

        # Start heartbeat thread
        heartbeat_thread = threading.Thread(
            target=self._heartbeat_loop, daemon=True, name="heartbeat"
        )
        heartbeat_thread.start()

        # Main polling loop
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
        logger.info("poll_loop_started", agent=self.AGENT_NAME)

        while self._running:
            try:
                signal = self._consumer.poll(timeout=POLL_TIMEOUT)

                if signal is None:
                    continue

                self._signals_processed += 1
                self._process_signal(signal)

            except Exception as e:
                logger.error(
                    "poll_loop_error",
                    agent=self.AGENT_NAME,
                    error=str(e),
                    exc_info=True,
                )
                time.sleep(2)  # Back off briefly before retrying

    def _process_signal(self, signal) -> None:
        """Core processing: run detection, decide whether to emit."""
        logger.debug(
            "processing_signal",
            source=signal.source,
            run_id=signal.pipeline_run_id,
            row_count=signal.metrics.get("row_count"),
        )

        result = self._detector.analyze(signal)

        if not result.has_anomaly:
            logger.debug("signal_clean", source=signal.source, run_id=signal.pipeline_run_id)
            return

        self._anomalies_detected += len(result.anomalies)

        for anomaly in result.anomalies:
            logger.warning(
                "anomaly_detected",
                type=anomaly.anomaly_type.value,
                severity=anomaly.severity.value,
                confidence=anomaly.confidence,
                model=anomaly.model_name,
                details=anomaly.details,
            )

            if self._mode == AgentMode.SHADOW:
                logger.info(
                    "shadow_mode_suppressed_emit",
                    signal_id=anomaly.signal_id,
                    type=anomaly.anomaly_type.value,
                )
                continue

            # Emit to agents.anomalies
            self._producer.emit_anomaly(anomaly)
            self._anomalies_emitted += 1

    # ── Heartbeat ─────────────────────────────────────────────

    def _heartbeat_loop(self) -> None:
        while self._running:
            try:
                hb = AgentHeartbeat(
                    agent_name=self.AGENT_NAME,
                    status="alive",
                    metadata={
                        "mode": self._mode.value,
                        "signals_processed": self._signals_processed,
                        "anomalies_detected": self._anomalies_detected,
                        "anomalies_emitted": self._anomalies_emitted,
                    },
                )
                self._producer.emit_heartbeat(hb)
                logger.debug("heartbeat_sent", agent=self.AGENT_NAME)
            except Exception as e:
                logger.error("heartbeat_error", error=str(e))

            time.sleep(HEARTBEAT_INTERVAL)

    # ── Helpers ───────────────────────────────────────────────

    def _log_stats(self) -> None:
        logger.info(
            "agent_stats",
            agent=self.AGENT_NAME,
            signals_processed=self._signals_processed,
            anomalies_detected=self._anomalies_detected,
            anomalies_emitted=self._anomalies_emitted,
        )

    def _handle_shutdown(self, signum, frame) -> None:
        logger.info("shutdown_signal_received", signal=signum)
        self.stop()


# ── Entrypoint ────────────────────────────────────────────────

def main():
    agent = IngestionMonitorAgent()
    agent.start()


if __name__ == "__main__":
    main()
