"""
Ingestion Monitor — Kafka I/O

Handles:
  - Consuming PipelineSignal messages from pipeline.signals.raw
  - Producing AnomalySignal messages to agents.anomalies
  - Producing AgentHeartbeat messages to agents.heartbeats
"""

from __future__ import annotations

import json
import logging
from typing import Callable, Optional

from confluent_kafka import Consumer, KafkaError, KafkaException, Producer
from confluent_kafka.admin import AdminClient, NewTopic

from config.schemas import AgentHeartbeat, AnomalySignal, PipelineSignal

logger = logging.getLogger(__name__)

TOPIC_RAW_SIGNALS = "pipeline.signals.raw"
TOPIC_ANOMALIES = "agents.anomalies"
TOPIC_HEARTBEATS = "agents.heartbeats"


class SignalConsumer:
    """
    Kafka consumer for pipeline.signals.raw.
    Filters to stage='ingestion' messages only.
    """

    def __init__(self, bootstrap_servers: str, group_id: str = "ingestion-monitor"):
        self._consumer = Consumer({
            "bootstrap.servers": bootstrap_servers,
            "group.id": group_id,
            "auto.offset.reset": "latest",
            "enable.auto.commit": True,
            "session.timeout.ms": 30000,
        })
        self._consumer.subscribe([TOPIC_RAW_SIGNALS])
        logger.info("consumer_subscribed", extra={"topic": TOPIC_RAW_SIGNALS})

    def poll(self, timeout: float = 1.0) -> Optional[PipelineSignal]:
        """
        Poll for one message. Returns a parsed PipelineSignal if available
        and stage == 'ingestion', otherwise None.
        """
        msg = self._consumer.poll(timeout=timeout)

        if msg is None:
            return None

        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                return None
            raise KafkaException(msg.error())

        try:
            data = json.loads(msg.value().decode("utf-8"))
            signal = PipelineSignal(**data)

            # Filter: only ingestion stage
            if signal.stage.value != "ingestion":
                return None

            logger.debug(
                "signal_received",
                extra={"source": signal.source, "run_id": signal.pipeline_run_id},
            )
            return signal

        except Exception as e:
            logger.error("signal_parse_error", extra={"error": str(e), "raw": msg.value()[:200]})
            return None

    def close(self) -> None:
        self._consumer.close()
        logger.info("consumer_closed")


class AnomalyProducer:
    """
    Kafka producer for agents.anomalies and agents.heartbeats.
    """

    def __init__(self, bootstrap_servers: str):
        self._producer = Producer({
            "bootstrap.servers": bootstrap_servers,
            "acks": "all",
            "retries": 3,
            "retry.backoff.ms": 500,
        })

    def emit_anomaly(self, signal: AnomalySignal) -> None:
        payload = signal.model_dump_json().encode("utf-8")
        self._producer.produce(
            topic=TOPIC_ANOMALIES,
            key=signal.pipeline_run_id.encode("utf-8"),
            value=payload,
            on_delivery=self._delivery_report,
        )
        self._producer.poll(0)
        logger.info(
            "anomaly_emitted",
            extra={
                "type": signal.anomaly_type,
                "severity": signal.severity,
                "model": signal.model_name,
                "confidence": signal.confidence,
            },
        )

    def emit_heartbeat(self, heartbeat: AgentHeartbeat) -> None:
        payload = heartbeat.model_dump_json().encode("utf-8")
        self._producer.produce(
            topic=TOPIC_HEARTBEATS,
            key=heartbeat.agent_name.encode("utf-8"),
            value=payload,
            on_delivery=self._delivery_report,
        )
        self._producer.poll(0)

    def flush(self) -> None:
        self._producer.flush(timeout=10)

    @staticmethod
    def _delivery_report(err, msg) -> None:
        if err:
            logger.error("kafka_delivery_failed", extra={"error": str(err), "topic": msg.topic()})
        else:
            logger.debug("kafka_delivery_ok", extra={"topic": msg.topic(), "offset": msg.offset()})
