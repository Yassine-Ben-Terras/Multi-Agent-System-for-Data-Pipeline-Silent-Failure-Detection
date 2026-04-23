"""
Quality Auditor — Kafka Producer

Produces quality failure anomaly signals and heartbeats.
The Quality Auditor only produces (no Kafka consumer —
it is triggered by the artifact watcher, not Kafka events).
"""

from __future__ import annotations

import logging

from confluent_kafka import Producer

from config.schemas import AgentHeartbeat, AnomalySignal

logger = logging.getLogger(__name__)

TOPIC_ANOMALIES  = "agents.anomalies"
TOPIC_HEARTBEATS = "agents.heartbeats"


class QualityAnomalyProducer:

    def __init__(self, bootstrap_servers: str):
        self._producer = Producer({
            "bootstrap.servers": bootstrap_servers,
            "acks": "all",
            "retries": 3,
            "retry.backoff.ms": 500,
        })

    def emit_quality_failure(self, signal: AnomalySignal) -> None:
        self._producer.produce(
            topic=TOPIC_ANOMALIES,
            key=signal.pipeline_run_id.encode("utf-8"),
            value=signal.model_dump_json().encode("utf-8"),
            on_delivery=self._delivery_report,
        )
        self._producer.poll(0)
        logger.info(
            "quality_failure_emitted",
            model=signal.model_name,
            severity=signal.severity.value,
            run_id=signal.pipeline_run_id,
        )

    def emit_heartbeat(self, hb: AgentHeartbeat) -> None:
        self._producer.produce(
            topic=TOPIC_HEARTBEATS,
            key=hb.agent_name.encode("utf-8"),
            value=hb.model_dump_json().encode("utf-8"),
        )
        self._producer.poll(0)

    def flush(self) -> None:
        self._producer.flush(timeout=10)

    @staticmethod
    def _delivery_report(err, msg) -> None:
        if err:
            logger.error("delivery_failed", error=str(err), topic=msg.topic())
        else:
            logger.debug("delivery_ok", topic=msg.topic(), offset=msg.offset())
