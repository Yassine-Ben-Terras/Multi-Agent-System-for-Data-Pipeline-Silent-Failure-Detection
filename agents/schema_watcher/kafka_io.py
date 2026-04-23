"""
Schema Watcher — Kafka I/O

Consumes from pipeline.signals.raw (all stages — schema can drift at any stage).
Produces to agents.anomalies and agents.heartbeats.
"""


from __future__ import annotations

import json
import logging
from typing import Optional

from confluent_kafka import Consumer, KafkaError, KafkaException, Producer

from config.schemas import AgentHeartbeat, AnomalySignal, PipelineSignal

logger = logging.getLogger(__name__)

TOPIC_RAW_SIGNALS = "pipeline.signals.raw"
TOPIC_ANOMALIES   = "agents.anomalies"
TOPIC_HEARTBEATS  = "agents.heartbeats"

class SchemaSignalConsumer:
    """Consumes all stages from pipeline.signals.raw (schema can drift at ingestion or transformation)."""

    def __init__(self, bootstrap_servers: str, group_id: str = "schema-watcher"):
        self._consumer = Consumer({
            "bootstrap.servers": bootstrap_servers,
            "group.id": group_id,
            "auto.offset.reset": "latest",
            "enable.auto.commit": True,
            "session.timeout.ms": 30000,
        })
        self._consumer.subscribe([TOPIC_RAW_SIGNALS])
        logger.info("schema_consumer_subscribed", topic=TOPIC_RAW_SIGNALS)

    def poll(self, timeout: float = 1.0) -> Optional[PipelineSignal]:
        msg = self._consumer.poll(timeout=timeout)
        if msg is None:
            return None
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                return None
            raise KafkaException(msg.error())
        try:
            data = json.loads(msg.value().decode("utf-8"))
            return PipelineSignal(**data)
        except Exception as e:
            logger.error("schema_signal_parse_error", error=str(e))
            return None

    def close(self) -> None:
        self._consumer.close()

class SchemaDriftProducer:
    """Produces schema drift anomalies and heartbeats."""

    def __init__(self, bootstrap_servers: str):
        self._producer = Producer({
            "bootstrap.servers": bootstrap_servers,
            "acks": "all",
            "retries": 3,
        })

    def emit_drift(self, signal: AnomalySignal) -> None:
        self._producer.produce(
            topic=TOPIC_ANOMALIES,
            key=signal.pipeline_run_id.encode("utf-8"),
            value=signal.model_dump_json().encode("utf-8"),
            on_delivery=self._delivery_report,
        )
        self._producer.poll(0)

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
