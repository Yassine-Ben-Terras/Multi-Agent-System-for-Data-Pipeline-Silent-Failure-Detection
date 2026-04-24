"""
Orchestrator Agent — Main Entrypoint

The central coordinator of the MAS. Full loop:

  1. OBSERVE   Poll agents.anomalies → add each signal to the 5-min window
  2. REASON    On window expiry:
               a. Suppress low-value buckets (all LOW + low confidence)
               b. Correlate signals → root cause via rules
               c. If inconclusive → call Claude for hypothesis
  3. SIGNAL    Emit ConfirmedIncident to agents.confirmed_incidents
               (Lineage Agent and Remediation Agent consume from this topic)

Key responsibilities:
  - Alert storm prevention: N signals for one run → 1 incident
  - LLM cost control: rule-first, LLM only when rules fail
  - Severity management: highest across all correlated signals

Usage:
  python -m agents.orchestrator.main
  AGENT_MODE=shadow python -m agents.orchestrator.main
"""

from __future__ import annotations

import logging
import os
import signal as os_signal
import threading
import time

import structlog

from config.schemas import AgentHeartbeat, AgentMode, ConfirmedIncident
from agents.orchestrator.correlator import Correlator
from agents.orchestrator.kafka_io import OrchestratorConsumer, OrchestratorProducer
from agents.orchestrator.llm_client import LLMClient
from agents.orchestrator.window import SignalBucket, SignalWindow

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
logger = structlog.get_logger("orchestrator")

# ── Config ────────────────────────────────────────────────────
BOOTSTRAP_SERVERS  = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
AGENT_MODE         = AgentMode(os.getenv("AGENT_MODE", "shadow"))
HEARTBEAT_INTERVAL = int(os.getenv("HEARTBEAT_INTERVAL_SECONDS", "60"))
POLL_TIMEOUT       = 1.0


class OrchestratorAgent:

    AGENT_NAME = "orchestrator"

    def __init__(self):
        self._running   = False
        self._mode      = AGENT_MODE

        self._correlator = Correlator()
        self._llm        = LLMClient()
        self._consumer   = OrchestratorConsumer(bootstrap_servers=BOOTSTRAP_SERVERS)
        self._producer   = OrchestratorProducer(bootstrap_servers=BOOTSTRAP_SERVERS)
        self._window     = SignalWindow(on_bucket_expired=self._on_bucket_expired)

        # Stats
        self._signals_received    = 0
        self._buckets_processed   = 0
        self._buckets_suppressed  = 0
        self._incidents_emitted   = 0
        self._llm_calls           = 0

        logger.info(
            "agent_initialised",
            agent=self.AGENT_NAME,
            mode=self._mode.value,
            llm_enabled=self._llm._enabled,
        )

    # ── Lifecycle ─────────────────────────────────────────────

    def start(self) -> None:
        self._running = True
        os_signal.signal(os_signal.SIGINT,  self._handle_shutdown)
        os_signal.signal(os_signal.SIGTERM, self._handle_shutdown)

        self._window.start_sweep_thread()
        threading.Thread(
            target=self._heartbeat_loop, daemon=True, name="heartbeat"
        ).start()

        logger.info("agent_starting", agent=self.AGENT_NAME, mode=self._mode.value)
        self._poll_loop()

    def stop(self) -> None:
        logger.info("agent_stopping", agent=self.AGENT_NAME)
        self._running = False
        self._window.flush_all()     # process any remaining buckets
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
                self._signals_received += 1
                self._window.add(signal)
                logger.debug(
                    "signal_windowed",
                    type=signal.anomaly_type.value,
                    model=signal.model_name,
                    run_id=signal.pipeline_run_id,
                    active_buckets=self._window.active_buckets,
                )
            except Exception as e:
                logger.error("poll_loop_error", error=str(e), exc_info=True)
                time.sleep(2)

    # ── Bucket processing (called by SignalWindow on expiry) ──

    def _on_bucket_expired(self, bucket: SignalBucket) -> None:
        self._buckets_processed += 1

        # Step 1 — Suppression check
        if self._correlator.should_suppress(bucket):
            self._buckets_suppressed += 1
            logger.info(
                "bucket_suppressed | run_id=%s signals=%s",
                bucket.run_id, len(bucket.signals),
            )
            return

        # Step 2 — Rule-based correlation
        result = self._correlator.correlate(bucket)

        # Step 3 — LLM hypothesis (only when rules inconclusive)
        llm_hypothesis = None
        if result.needs_llm:
            self._llm_calls += 1
            logger.info(
                "calling_llm_for_hypothesis | run_id=%s types=%s",
                bucket.run_id, bucket.anomaly_types(),
            )
            llm_hypothesis = self._llm.generate_hypothesis(result.signals)

            # Update root cause from LLM if we got a useful response
            if llm_hypothesis:
                # Try to extract root_cause from LLM (it may have updated it)
                import json as _json
                try:
                    parsed = _json.loads(llm_hypothesis)
                    result.root_cause = parsed.get("root_cause", result.root_cause)
                    result.confidence = parsed.get("confidence", result.confidence)
                    llm_hypothesis    = parsed.get("explanation", llm_hypothesis)
                except Exception:
                    pass  # LLM returned plain text, use as-is

        # Step 4 — Build ConfirmedIncident
        incident = ConfirmedIncident(
            root_cause=result.root_cause,
            severity=result.severity,
            pipeline_run_id=bucket.run_id,
            correlated_signals=result.signals,
            blast_radius=None,    # Lineage Agent will enrich this
            llm_hypothesis=llm_hypothesis,
        )

        logger.warning(
            "incident_confirmed",
            run_id=bucket.run_id,
            root_cause=result.root_cause,
            severity=result.severity.value,
            confidence=result.confidence,
            signals=len(result.signals),
            models=result.affected_models,
            llm_used=result.needs_llm,
        )

        if self._mode == AgentMode.SHADOW:
            logger.info(
                "shadow_mode_suppressed_emit | incident_id=%s root_cause=%s",
                incident.incident_id, incident.root_cause,
            )
            return

        self._producer.emit_incident(incident)
        self._incidents_emitted += 1

    # ── Heartbeat ─────────────────────────────────────────────

    def _heartbeat_loop(self) -> None:
        while self._running:
            try:
                hb = AgentHeartbeat(
                    agent_name=self.AGENT_NAME,
                    status="alive",
                    metadata={
                        "mode":                self._mode.value,
                        "signals_received":    self._signals_received,
                        "buckets_processed":   self._buckets_processed,
                        "buckets_suppressed":  self._buckets_suppressed,
                        "incidents_emitted":   self._incidents_emitted,
                        "llm_calls":           self._llm_calls,
                        "active_buckets":      self._window.active_buckets,
                        "llm_enabled":         self._llm._enabled,
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
            buckets_processed=self._buckets_processed,
            buckets_suppressed=self._buckets_suppressed,
            incidents_emitted=self._incidents_emitted,
            llm_calls=self._llm_calls,
        )

    def _handle_shutdown(self, signum, frame) -> None:
        logger.info("shutdown_signal_received | signal=%s", signum)
        self.stop()


def main():
    OrchestratorAgent().start()


if __name__ == "__main__":
    main()
