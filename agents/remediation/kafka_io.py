"""
Remediation Agent — Kafka I/O

Consumes:  agents.confirmed_incidents → ConfirmedIncident
Produces:  agents.actions_taken       → RemediationAction (audit)
           agents.heartbeats          → AgentHeartbeat
"""

from __future__ import annotations

import json
import logging
from typing import Optional

from confluent_kafka import Consumer, KafkaError, KafkaException, Producer

from config.schemas import AgentHeartbeat, ConfirmedIncident, RemediationAction

logger = logging.getLogger(__name__)

TOPIC_INCIDENTS  = "agents.confirmed_incidents"
TOPIC_ACTIONS    = "agents.actions_taken"
TOPIC_HEARTBEATS = "agents.heartbeats"


class IncidentConsumer:
    """Consumes ConfirmedIncident messages from agents.confirmed_incidents."""

    def __init__(self, bootstrap_servers: str, group_id: str = "remediation"):
        self._consumer = Consumer({
            "bootstrap.servers": bootstrap_servers,
            "group.id": group_id,
            "auto.offset.reset": "latest",
            "enable.auto.commit": True,
            "session.timeout.ms": 30000,
        })
        self._consumer.subscribe([TOPIC_INCIDENTS])
        logger.info("remediation_consumer_subscribed | topic=%s", TOPIC_INCIDENTS)

    def poll(self, timeout: float = 1.0) -> Optional[ConfirmedIncident]:
        msg = self._consumer.poll(timeout=timeout)
        if msg is None:
            return None
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                return None
            raise KafkaException(msg.error())
        try:
            data = json.loads(msg.value().decode("utf-8"))
            return ConfirmedIncident(**data)
        except Exception as e:
            logger.error("incident_parse_error | error=%s", str(e))
            return None

    def close(self) -> None:
        self._consumer.close()


class ActionProducer:
    """Produces RemediationAction audit records and heartbeats."""

    def __init__(self, bootstrap_servers: str):
        self._producer = Producer({
            "bootstrap.servers": bootstrap_servers,
            "acks": "all",
            "retries": 3,
            "retry.backoff.ms": 500,
        })

    def emit_action(self, action: RemediationAction) -> None:
        self._producer.produce(
            topic=TOPIC_ACTIONS,
            key=action.incident_id.encode("utf-8"),
            value=action.model_dump_json().encode("utf-8"),
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
            logger.error("delivery_failed | error=%s topic=%s", str(err), msg.topic())
        else:
            logger.debug("delivery_ok | topic=%s offset=%s", msg.topic(), msg.offset())
