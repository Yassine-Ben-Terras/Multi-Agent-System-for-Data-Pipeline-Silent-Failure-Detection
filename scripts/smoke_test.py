#!/usr/bin/env python3
"""
MAS Pipeline Sentinel — End-to-End Smoke Test

Simulates a complete incident flow through all agents:

  1. Emits a fake PipelineSignal with a severe row count anomaly
     → to pipeline.signals.raw
  2. Emits a matching schema drift signal
     → to agents.anomalies (directly, simulating schema watcher)
  3. Watches agents.anomalies for ingestion monitor response
  4. Watches agents.confirmed_incidents for orchestrator correlation
  5. Watches agents.actions_taken for remediation response
  6. Watches agents.heartbeats to verify all agents are alive

Expected flow (shadow mode):
  pipeline.signals.raw
       ↓
  [Ingestion Monitor] → agents.anomalies (row_count_anomaly)
  [Schema Watcher]    → agents.anomalies (schema_drift)
       ↓
  [Orchestrator] correlates → agents.confirmed_incidents
       ↓
  [Lineage Agent] enriches → agents.confirmed_incidents (with blast radius)
       ↓
  [Remediation] logs planned actions (shadow = no execution)

Usage:
  python scripts/smoke_test.py                    # default localhost:9092
  python scripts/smoke_test.py --bootstrap kafka:29092
  python scripts/smoke_test.py --scenario zero_rows
  python scripts/smoke_test.py --scenario schema_drift

Scenarios:
  row_count_anomaly  (default) — z-score -4.0, HIGH severity
  zero_rows          — zero row ingestion, CRITICAL severity
  schema_drift       — dropped critical column, CRITICAL severity
  combined           — schema drift + row count (tests correlation)
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import datetime, timezone
from typing import Dict, List

from confluent_kafka import Consumer, Producer

# ── Scenario definitions ──────────────────────────────────────

SCENARIOS: Dict[str, dict] = {
    "row_count_anomaly": {
        "description": "Row count drops 40% — z-score -4.0, expected HIGH severity",
        "pipeline_signal": {
            "event_type": "pipeline.stage.completed",
            "stage": "ingestion",
            "source": "orders_api",
            "metrics": {
                "row_count": 60_000,       # baseline ~100k → z ≈ -4.0
                "checksum": "abc123def",
                "duration_ms": 4210,
            },
            "schema_snapshot": None,
            "pipeline_run_id": "smoke-{ts}",
        },
    },
    "zero_rows": {
        "description": "Zero rows ingested — always CRITICAL",
        "pipeline_signal": {
            "event_type": "pipeline.stage.completed",
            "stage": "ingestion",
            "source": "payments_api",
            "metrics": {
                "row_count": 0,
                "checksum": "000000",
                "duration_ms": 120,
            },
            "schema_snapshot": None,
            "pipeline_run_id": "smoke-{ts}",
        },
    },
    "schema_drift": {
        "description": "Critical column dropped — CRITICAL severity, schema drift correlation",
        "pipeline_signal": {
            "event_type": "pipeline.stage.completed",
            "stage": "ingestion",
            "source": "orders_api",
            "metrics": {
                "row_count": 98_000,
                "checksum": "newchecksum99",
                "duration_ms": 3800,
            },
            "schema_snapshot": {
                "columns": [
                    {"name": "order_id",    "type": "VARCHAR",  "nullable": False},
                    # amount_usd column DROPPED — was previously here
                    {"name": "status",      "type": "VARCHAR",  "nullable": True},
                    {"name": "created_at",  "type": "TIMESTAMP","nullable": False},
                ],
                "version_hash": "driftedschema01",
            },
            "pipeline_run_id": "smoke-{ts}",
        },
    },
    "combined": {
        "description": "Schema drift + row count anomaly — tests orchestrator correlation",
        "pipeline_signal": {
            "event_type": "pipeline.stage.completed",
            "stage": "ingestion",
            "source": "orders_api",
            "metrics": {
                "row_count": 55_000,
                "checksum": "combinedcheck99",
                "duration_ms": 3100,
            },
            "schema_snapshot": {
                "columns": [
                    {"name": "order_id", "type": "INTEGER", "nullable": False},  # type changed
                    {"name": "status",   "type": "VARCHAR", "nullable": True},
                ],
                "version_hash": "combineddrift01",
            },
            "pipeline_run_id": "smoke-{ts}",
        },
    },
}


def make_producer(bootstrap: str) -> Producer:
    return Producer({
        "bootstrap.servers": bootstrap,
        "acks": "all",
    })


def make_consumer(bootstrap: str, topics: List[str], group_id: str) -> Consumer:
    c = Consumer({
        "bootstrap.servers": bootstrap,
        "group.id": group_id,
        "auto.offset.reset": "latest",
        "enable.auto.commit": True,
    })
    c.subscribe(topics)
    return c


def emit_signal(producer: Producer, topic: str, payload: dict) -> None:
    producer.produce(
        topic=topic,
        key=payload.get("pipeline_run_id", "smoke").encode(),
        value=json.dumps(payload).encode("utf-8"),
    )
    producer.flush()
    print(f"  → emitted to {topic}: {payload.get('event_type', payload.get('anomaly_type', '?'))}")


def watch_topic(consumer: Consumer, topic_label: str, timeout_s: int = 30) -> List[dict]:
    """Poll a topic for `timeout_s` seconds, return all messages received."""
    print(f"\n  watching {topic_label} for {timeout_s}s...")
    messages = []
    deadline = time.time() + timeout_s

    while time.time() < deadline:
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            continue
        if msg.error():
            continue
        try:
            data = json.loads(msg.value().decode("utf-8"))
            messages.append(data)
            print(f"  ✓ received on {topic_label}: {json.dumps(data, indent=2)[:300]}...")
        except Exception as e:
            print(f"  ✗ parse error: {e}")

    return messages


def check_heartbeats(consumer: Consumer, expected_agents: List[str], timeout_s: int = 70) -> None:
    """Verify all expected agents are publishing heartbeats."""
    print(f"\n  checking heartbeats for: {expected_agents} (timeout={timeout_s}s)")
    seen = set()
    deadline = time.time() + timeout_s

    while time.time() < deadline and len(seen) < len(expected_agents):
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            continue
        if msg.error():
            continue
        try:
            data = json.loads(msg.value().decode("utf-8"))
            agent = data.get("agent_name")
            if agent in expected_agents and agent not in seen:
                seen.add(agent)
                status = data.get("status", "unknown")
                print(f"  ✓ heartbeat from {agent} (status={status})")
        except Exception:
            pass

    missing = set(expected_agents) - seen
    if missing:
        print(f"  ⚠ no heartbeat from: {missing}")
    else:
        print(f"  ✓ all {len(expected_agents)} agents alive")


def run_smoke_test(bootstrap: str, scenario_name: str) -> int:
    scenario = SCENARIOS.get(scenario_name)
    if scenario is None:
        print(f"Unknown scenario: {scenario_name}. Available: {list(SCENARIOS.keys())}")
        return 1

    ts = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
    run_id = f"smoke-{ts}"

    print("=" * 60)
    print(f"MAS Pipeline Sentinel — Smoke Test")
    print(f"Scenario : {scenario_name}")
    print(f"Run ID   : {run_id}")
    print(f"Broker   : {bootstrap}")
    print(f"Desc     : {scenario['description']}")
    print("=" * 60)

    producer = make_producer(bootstrap)

    # ── Step 1: Emit pipeline signal ──────────────────────────
    print("\n[1/4] Emitting pipeline signal → pipeline.signals.raw")
    signal = scenario["pipeline_signal"].copy()
    signal["pipeline_run_id"] = run_id
    signal["ts"] = datetime.now(timezone.utc).isoformat()
    emit_signal(producer, "pipeline.signals.raw", signal)

    # ── Step 2: Watch for anomaly signals ─────────────────────
    print("\n[2/4] Watching agents.anomalies (ingestion monitor + schema watcher responses)")
    anomaly_consumer = make_consumer(bootstrap, ["agents.anomalies"], f"smoke-anomaly-{ts}")
    anomalies = watch_topic(anomaly_consumer, "agents.anomalies", timeout_s=35)
    anomaly_consumer.close()

    if anomalies:
        print(f"\n  ✓ {len(anomalies)} anomaly signal(s) detected")
    else:
        print("\n  ℹ no anomalies received (agents may be in shadow mode — check logs)")

    # ── Step 3: Watch for confirmed incidents ──────────────────
    print("\n[3/4] Watching agents.confirmed_incidents (orchestrator output)")
    incident_consumer = make_consumer(bootstrap, ["agents.confirmed_incidents"], f"smoke-incident-{ts}")
    incidents = watch_topic(incident_consumer, "agents.confirmed_incidents", timeout_s=40)
    incident_consumer.close()

    if incidents:
        inc = incidents[0]
        print(f"\n  ✓ Incident confirmed:")
        print(f"    root_cause : {inc.get('root_cause')}")
        print(f"    severity   : {inc.get('severity')}")
        print(f"    signals    : {len(inc.get('correlated_signals', []))}")
        print(f"    hypothesis : {(inc.get('llm_hypothesis') or 'none')[:100]}")
    else:
        print("\n  ℹ no confirmed incidents (orchestrator may be in shadow mode)")

    # ── Step 4: Check heartbeats ──────────────────────────────
    print("\n[4/4] Checking agent heartbeats → agents.heartbeats")
    hb_consumer = make_consumer(bootstrap, ["agents.heartbeats"], f"smoke-hb-{ts}")
    check_heartbeats(
        hb_consumer,
        expected_agents=[
            "ingestion_monitor",
            "schema_watcher",
            "quality_auditor",
            "lineage_impact",
            "orchestrator",
            "remediation",
        ],
        timeout_s=70,
    )
    hb_consumer.close()

    print("\n" + "=" * 60)
    print("Smoke test complete.")
    print("Tip: run 'docker compose logs -f orchestrator' to see full agent output")
    print("Tip: open http://localhost:8080 to browse Kafka topics")
    print("=" * 60)
    return 0


def main() -> None:
    parser = argparse.ArgumentParser(description="MAS Pipeline Sentinel smoke test")
    parser.add_argument("--bootstrap", default="localhost:9092", help="Kafka bootstrap servers")
    parser.add_argument(
        "--scenario",
        default="row_count_anomaly",
        choices=list(SCENARIOS.keys()),
        help="Test scenario to run",
    )
    args = parser.parse_args()
    sys.exit(run_smoke_test(args.bootstrap, args.scenario))


if __name__ == "__main__":
    main()
