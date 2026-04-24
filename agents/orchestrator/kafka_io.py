"""
Orchestrator — Kafka I/O

Consumes:  agents.anomalies            → AnomalySignal  (from all specialist agents)
Produces:  agents.confirmed_incidents  → ConfirmedIncident
           agents.heartbeats           → AgentHeartbeat
"""

from __future__ import annotations

import json
import logging
from typing import Optional

from confluent_kafka import Consumer, KafkaError, KafkaException, Producer

from config.schemas import AgentHeartbeat, AnomalySignal, ConfirmedIncident

logger = logging.getLogger(__name__)

TOPIC_ANOMALIES  = "agents.anomalies"
TOPIC_INCIDENTS  = "agents.confirmed_incidents"
TOPIC_HEARTBEATS = "agents.heartbeats"


class OrchestratorConsumer:
    """Consumes all AnomalySignals from agents.anomalies."""

    def __init__(self, bootstrap_servers: str, group_id: str = "orchestrator"):
        self._consumer = Consumer({
            "bootstrap.servers": bootstrap_servers,
            "group.id": group_id,
            "auto.offset.reset": "latest",
            "enable.auto.commit": True,
            "session.timeout.ms": 30000,
        })
        self._consumer.subscribe([TOPIC_ANOMALIES])
        logger.info("orchestrator_consumer_subscribed | topic=%s", TOPIC_ANOMALIES)

    def poll(self, timeout: float = 1.0) -> Optional[AnomalySignal]:
        msg = self._consumer.poll(timeout=timeout)
        if msg is None:
            return None
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                return None
            raise KafkaException(msg.error())
        try:
            data = json.loads(msg.value().decode("utf-8"))
            return AnomalySignal(**data)
        except Exception as e:
            logger.error("anomaly_parse_error | error=%s", str(e))
            return None

    def close(self) -> None:
        self._consumer.close()


class OrchestratorProducer:
    """Produces ConfirmedIncidents and heartbeats."""

    def __init__(self, bootstrap_servers: str):
        self._producer = Producer({
            "bootstrap.servers": bootstrap_servers,
            "acks": "all",
            "retries": 3,
            "retry.backoff.ms": 500,
        })

    def emit_incident(self, incident: ConfirmedIncident) -> None:
        self._producer.produce(
            topic=TOPIC_INCIDENTS,
            key=incident.incident_id.encode("utf-8"),
            value=incident.model_dump_json().encode("utf-8"),
            on_delivery=self._delivery_report,
        )
        self._producer.poll(0)
        logger.info(
            "incident_emitted | id=%s severity=%s root_cause=%s models=%s",
            incident.incident_id[:8],
            incident.severity.value,
            incident.root_cause,
            len(incident.correlated_signals),
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
            logger.error("delivery_failed | error=%s topic=%s", str(err), msg.topic())
        else:
            logger.debug("delivery_ok | topic=%s offset=%s", msg.topic(), msg.offset())
