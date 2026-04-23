"""
Quality Auditor Agent — Main Entrypoint

Triggered by artifact watcher (not Kafka) → runs detector → emits signals.

Loop:
  1. OBSERVE   LocalArtifactWatcher / S3ArtifactWatcher detects new run_results.json
  2. REASON    DbtArtifactParser parses it → QualityDetector analyses failures
               FlakinessTracker provides history context per test
  3. SIGNAL    Emit AnomalySignal(QUALITY_FAILURE) per failing model (unless shadow)

Usage:
  python -m agents.quality_auditor.main
  AGENT_MODE=shadow python -m agents.quality_auditor.main
"""

from __future__ import annotations

import logging
import os
import signal as os_signal
import threading
import time

import structlog

from config.schemas import AgentHeartbeat, AgentMode
from agents.quality_auditor.detector import QualityDetector
from agents.quality_auditor.flakiness import FlakinessTracker
from agents.quality_auditor.kafka_io import QualityAnomalyProducer
from agents.quality_auditor.parser import DbtArtifactParser
from agents.quality_auditor.watcher import build_watcher

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
logger = structlog.get_logger("quality_auditor")

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
ARTIFACTS_PATH     = os.getenv("DBT_ARTIFACTS_PATH", "./dbt_integration/artifacts")
POLL_INTERVAL      = int(os.getenv("ARTIFACT_POLL_INTERVAL_SECONDS", "30"))
HEARTBEAT_INTERVAL = int(os.getenv("HEARTBEAT_INTERVAL_SECONDS", "60"))


class QualityAuditorAgent:

    AGENT_NAME = "quality_auditor"

    def __init__(self):
        self._running = False
        self._mode    = AGENT_MODE

        self._parser   = DbtArtifactParser()
        self._tracker  = FlakinessTracker(db_url=DB_URL)
        self._detector = QualityDetector(flakiness_tracker=self._tracker)
        self._producer = QualityAnomalyProducer(bootstrap_servers=BOOTSTRAP_SERVERS)
        self._watcher  = build_watcher(ARTIFACTS_PATH, poll_interval=POLL_INTERVAL)

        # Stats
        self._runs_processed   = 0
        self._signals_emitted  = 0
        self._failures_detected = 0

        logger.info(
            "agent_initialised",
            agent=self.AGENT_NAME,
            mode=self._mode.value,
            artifacts_path=ARTIFACTS_PATH,
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

        # Blocking — watcher runs the loop
        self._watcher.watch(on_new_artifact=self._on_new_artifact)

    def stop(self) -> None:
        logger.info("agent_stopping", agent=self.AGENT_NAME)
        self._running = False
        self._producer.flush()
        self._log_stats()
        logger.info("agent_stopped", agent=self.AGENT_NAME)

    # ── Artifact handler ──────────────────────────────────────

    def _on_new_artifact(self, path: str) -> None:
        """Called by the watcher whenever a new run_results.json is detected."""
        try:
            run_results = self._parser.parse_file(path)
            self._runs_processed += 1

            analysis = self._detector.analyze(run_results)
            self._failures_detected += analysis.failed_tests

            if not analysis.has_failures:
                logger.info(
                    "run_clean",
                    run_id=run_results.run_id,
                    tests=analysis.total_tests,
                )
                return

            for signal in analysis.signals:
                logger.warning(
                    "quality_failure",
                    model=signal.model_name,
                    severity=signal.severity.value,
                    confidence=signal.confidence,
                    failed_tests=signal.details.get("total_failed_tests"),
                    genuine=signal.details.get("genuine_failures"),
                    flaky=signal.details.get("flaky_failures"),
                )

                if self._mode == AgentMode.SHADOW:
                    logger.info(
                        "shadow_mode_suppressed_emit",
                        signal_id=signal.signal_id,
                        model=signal.model_name,
                    )
                    continue

                self._producer.emit_quality_failure(signal)
                self._signals_emitted += 1

        except Exception as e:
            logger.error("artifact_processing_error", error=str(e), path=path, exc_info=True)

    # ── Heartbeat ─────────────────────────────────────────────

    def _heartbeat_loop(self) -> None:
        while self._running:
            try:
                hb = AgentHeartbeat(
                    agent_name=self.AGENT_NAME,
                    status="alive",
                    metadata={
                        "mode":               self._mode.value,
                        "runs_processed":     self._runs_processed,
                        "failures_detected":  self._failures_detected,
                        "signals_emitted":    self._signals_emitted,
                        "artifacts_path":     ARTIFACTS_PATH,
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
            runs_processed=self._runs_processed,
            failures_detected=self._failures_detected,
            signals_emitted=self._signals_emitted,
        )

    def _handle_shutdown(self, signum, frame) -> None:
        logger.info("shutdown_signal_received", signal=signum)
        self.stop()


def main():
    QualityAuditorAgent().start()


if __name__ == "__main__":
    main()
