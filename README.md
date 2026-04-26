<div align="center">

# 🛡️ MAS Pipeline Sentinel

**Multi-Agent System for Data Pipeline Silent Failure Detection**

[![Python](https://img.shields.io/badge/Python-3.11+-3776AB?style=flat&logo=python&logoColor=white)](https://python.org)
[![LangGraph](https://img.shields.io/badge/LangGraph-0.2+-1C3C3C?style=flat)](https://github.com/langchain-ai/langgraph)
[![Kafka](https://img.shields.io/badge/Apache%20Kafka-7.6-231F20?style=flat&logo=apachekafka&logoColor=white)](https://kafka.apache.org)
[![dbt](https://img.shields.io/badge/dbt-1.8-FF694B?style=flat&logo=dbt&logoColor=white)](https://getdbt.com)
[![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?style=flat&logo=docker&logoColor=white)](https://docker.com)
[![License](https://img.shields.io/badge/License-MIT-lightgrey?style=flat)](LICENSE)

*A production-grade AI-powered monitoring system that autonomously detects, diagnoses, and remediates silent failures across modern data pipelines ,before they reach your dashboards.*

</div>

---

## 🚨 The Problem

Silent failures are the most dangerous failures in data engineering. A pipeline completes successfully — no exception raised, no alert fired — yet the data it produced is **wrong, incomplete, or corrupted**. Downstream consumers silently operate on bad data for hours, sometimes days.

```
orders_api → [dbt run] → fct_orders ✅ → revenue dashboard ❌
                              ↑
                    Zero error. Zero alert.
                    But amount_usd column was NULL for 12 hours.
```

> *No single monitoring point has full context. Ingestion doesn't know what the dashboard expects. Transformation doesn't know what the source intended.*

---

## 🧠 The Solution

A **five-agent system** where each agent owns a distinct observability domain. Agents communicate through a shared Kafka event bus, correlate signals through a central Orchestrator, and collaborate to build a **causal chain** — not just a symptom report.

```
┌──────────────────────────────────────────────────────────────────┐
│                         ORCHESTRATOR                              │
│     5-min tumbling window · rule correlation · Claude LLM         │
└────────┬──────────┬──────────┬──────────┬───────────────────────┘
         │          │          │          │
┌────────▼──┐ ┌─────▼────┐ ┌──▼──────┐ ┌─▼──────────┐
│ Ingestion │ │  Schema  │ │Quality  │ │ Lineage &  │
│  Monitor  │ │  Watcher │ │ Auditor │ │   Impact   │
│           │ │          │ │         │ │            │
│ z-score   │ │ contract │ │ dbt     │ │ DAG BFS    │
│ checksum  │ │ diff     │ │ flakiness│ │ blast radius│
└─────┬─────┘ └─────┬────┘ └──┬──────┘ └─────┬──────┘
      │             │          │               │
      └─────────────┴──────────┘               │
                    │                           │
              agents.anomalies                  │
                    │                           │
            ┌───────▼────────────────────────── ▼──────┐
            │              ORCHESTRATOR                  │
            │    correlates → confirmed_incident         │
            └───────────────────┬────────────────────────┘
                                │
                    ┌───────────▼────────────┐
                    │    REMEDIATION AGENT    │
                    │  quarantine · backfill  │
                    │  PagerDuty · Slack      │
                    └────────────────────────┘
```

### The Five Specialist Agents

| Agent | Observability Domain | Key Detections |
|---|---|---|
| **Ingestion Monitor** | Source systems | Row count z-score (2.5σ), zero-row ingestion, checksum drift |
| **Schema Watcher** | Schema contracts | Dropped columns (CRITICAL), type changes (HIGH), nullable drift (MEDIUM) |
| **Quality Auditor** | dbt test results | Failure rates, flakiness history, dbt meta severity routing |
| **Lineage & Impact** | dbt DAG | Transitive blast radius, dashboard SLA exposure, ML feature impact |
| **Remediation** | Automated response | Quarantine, dbt backfill, Slack/PagerDuty/Jira incident creation |

> **Architectural principle:** Agents reason on **metadata only** — row counts, schema snapshots, dbt artifacts. Raw data never leaves the warehouse. The system is secure, lightweight, and warehouse-agnostic.

---

## 🏗️ Tech Stack

| Layer | Technology | Purpose |
|---|---|---|
| **Agent Framework** | LangGraph 0.2 | Stateful agent graphs, retry logic |
| **LLM** | Anthropic Claude | Root cause hypothesis (Sonnet), classification (Haiku) |
| **Message Bus** | Apache Kafka | Agent-to-agent communication, 5 topics |
| **State Store** | PostgreSQL 15 | Baselines, incidents, test history, audit log |
| **Artifact Store** | Local / S3 | dbt manifest.json, run_results.json, sources.json |
| **dbt Integration** | dbt Core 1.8 | DAG traversal, test results, schema contracts |
| **Containerisation** | Docker Compose | Full local stack, one command startup |
| **Observability** | structlog + OpenTelemetry | Structured agent logs, trace propagation |

---

## 📁 Project Structure

```
mas-pipeline-sentinel/
├── agents/
│   ├── ingestion_monitor/     # Row count anomaly · checksum · z-score baseline
│   │   ├── baseline.py        # Welford online algorithm (incremental mean/variance)
│   │   ├── detector.py        # Detection engine (pure logic, fully testable)
│   │   ├── kafka_io.py        # Consumer (pipeline.signals.raw) + producer
│   │   └── main.py            # Poll loop · heartbeat thread · shadow mode
│   ├── schema_watcher/        # Schema contract drift
│   │   ├── contract_store.py  # PostgreSQL contract registry + history
│   │   ├── differ.py          # Column diff engine (drop/type/nullable/new)
│   │   └── ...
│   ├── quality_auditor/       # dbt test failure analysis
│   │   ├── parser.py          # run_results.json parser
│   │   ├── flakiness.py       # Historical flakiness tracker
│   │   ├── detector.py        # Severity routing + model aggregation
│   │   └── watcher.py         # Local/S3 artifact watcher
│   ├── lineage_impact/        # dbt DAG blast radius
│   │   ├── manifest_parser.py # manifest.json → DbtDag (BFS-ready)
│   │   └── calculator.py      # Downstream traversal + exposure enrichment
│   ├── orchestrator/          # Signal correlation + LLM
│   │   ├── window.py          # 5-min tumbling window (thread-safe)
│   │   ├── correlator.py      # Rule-based correlation + suppression
│   │   └── llm_client.py      # Claude API (tiered: Haiku/Sonnet)
│   └── remediation/           # Automated response
│       ├── playbook.py        # Action selection matrix
│       ├── executor.py        # dbt · Airflow · PagerDuty · Jira · Slack
│       └── audit_log.py       # Append-only PostgreSQL action history
├── config/
│   └── schemas.py             # All Pydantic models (PipelineSignal, AnomalySignal, ...)
├── dbt_integration/
│   ├── schema_example.yml     # dbt meta annotations (agent_severity, blast_radius)
│   └── publish_artifacts.py   # CI/CD artifact publisher (local + S3)
├── infra/
│   ├── kafka/                 # Topic creation scripts
│   └── postgres/init.sql      # State store schema (6 tables)
├── scripts/
│   └── smoke_test.py          # E2E smoke test (4 scenarios)
├── tests/
│   ├── fixtures/              # Sample dbt artifacts (manifest, run_results)
│   └── unit/                  # 188 tests across all agents
├── docs/
│   ├── architecture.md        # System design + ADRs
│   ├── agents.md              # Per-agent reference
│   ├── event-schema.md        # Kafka topic + message schemas
│   └── runbooks.md            # Incident response playbooks
└── docker-compose.yml         # Full local stack (infra + all 6 agents)
```

---

## 🚀 Quickstart

### Prerequisites
- Docker & Docker Compose
- Python 3.11+
- Anthropic API key (optional — system works without it, LLM enrichment disabled)

### 1. Clone and configure

```bash
git clone https://github.com/Yassine-Ben-Terras/Multi-Agent-System-for-Data-Pipeline-Silent-Failure-Detection.git
cd mas-pipeline-sentinel
cp .env.example .env
# Edit .env → add ANTHROPIC_API_KEY (optional)
```

### 2. Start the full stack

```bash
make up
```

This starts:
- Kafka + ZooKeeper + Kafka UI (http://localhost:8080)
- PostgreSQL state store
- All 6 agents in **shadow mode** (observe and log — no automated actions)

### 3. Run the smoke test

```bash
# Test: row count drops 40% — should trigger HIGH anomaly
make smoke

# Test: zero rows ingested — always CRITICAL
make smoke-zero

# Test: critical column dropped — schema drift + orchestrator correlation
make smoke-drift

# Test: schema drift + row count — tests 5-min window correlation
make smoke-combined
```

### 4. Watch the agents

```bash
docker compose logs -f orchestrator      # see signal correlation
docker compose logs -f ingestion-monitor # see z-score checks
docker compose logs -f remediation       # see planned actions (shadow mode)
```

### 5. Browse Kafka topics

Open **http://localhost:8080** → Kafka UI → Topics → browse messages flowing through:
- `pipeline.signals.raw` — raw pipeline events
- `agents.anomalies` — detected anomalies from all agents
- `agents.confirmed_incidents` — correlated incidents from orchestrator
- `agents.actions_taken` — remediation audit log
- `agents.heartbeats` — agent liveness

---

## ⚙️ Agent Modes

The system has four operating modes, controlled by `AGENT_MODE` in `.env`:

| Mode | Behaviour | When to use |
|---|---|---|
| `shadow` | Detect and log only. **No Kafka emit, no actions.** | First 2 weeks — calibrate baselines |
| `alert_only` | Emit anomalies + Slack notifications. No warehouse actions. | Weeks 3–4 — validate alert quality |
| `supervised` | AUTO on LOW/MEDIUM. Human approval for HIGH/CRITICAL. | Weeks 5–6 — build trust |
| `full_autonomy` | All actions automated within playbooks. | Week 7+ — production |

```bash
# Switch mode without rebuilding images
AGENT_MODE=alert_only docker compose up -d
```

---

## 📐 Event Bus Topics

| Topic | Producer | Consumer(s) | Retention |
|---|---|---|---|
| `pipeline.signals.raw` | Your pipelines | Ingestion Monitor, Schema Watcher | 24h |
| `agents.anomalies` | Specialist agents | Orchestrator | 7 days |
| `agents.confirmed_incidents` | Orchestrator | Lineage Agent, Remediation Agent | 30 days |
| `agents.actions_taken` | Remediation Agent | Audit log | 90 days |
| `agents.heartbeats` | All agents (60s) | Health monitor | 1h |

---

## 🔌 Instrumenting Your Pipeline

Add this signal emission to every pipeline stage:

```python
import json
from datetime import datetime, timezone
from confluent_kafka import Producer

producer = Producer({"bootstrap.servers": "localhost:9092"})

signal = {
    "event_type": "pipeline.stage.completed",
    "stage": "ingestion",                        # ingestion | transformation | serving
    "source": "orders_api",
    "metrics": {
        "row_count": 142830,
        "checksum": "a3f9c2...",
        "duration_ms": 4210,
    },
    "schema_snapshot": {
        "columns": [
            {"name": "order_id", "type": "VARCHAR", "nullable": False, "criticality": "critical"},
            {"name": "amount",   "type": "FLOAT",   "nullable": True},
        ],
        "version_hash": "b7c2e1...",
    },
    "pipeline_run_id": "run-2026-04-22-001",
    "ts": datetime.now(timezone.utc).isoformat(),
}

producer.produce("pipeline.signals.raw", value=json.dumps(signal).encode())
producer.flush()
```

---

## 🧪 Testing

```bash
# Run all 188 unit tests
make test

# Run with coverage
make test-cov

# Run specific agent tests
python -m pytest tests/unit/test_ingestion_detector.py -v
python -m pytest tests/unit/test_correlator.py -v
python -m pytest tests/unit/test_playbook.py -v
```

Test coverage by agent:

| Agent | Test File | Tests |
|---|---|---|
| Ingestion Monitor | test_ingestion_detector.py, test_baseline.py | 38 |
| Schema Watcher | test_schema_differ.py, test_contract_store.py | 26 |
| Quality Auditor | test_artifact_parser.py, test_quality_detector.py, test_flakiness.py | 41 |
| Lineage & Impact | test_manifest_parser.py, test_blast_radius.py | 42 |
| Orchestrator | test_signal_window.py, test_correlator.py | 34 |
| Remediation | test_playbook.py, test_executor.py | 28 |
| Core schemas | test_schemas.py | 3 |
| **Total** | | **188** |

---

## ☁️ Production Deployment (AWS)

```bash
# Orchestrator — long-running ECS Fargate task
aws ecs create-service \
  --cluster mas-cluster \
  --task-def orchestrator:latest \
  --desired-count 1 \
  --launch-type FARGATE

# Specialist agents — Lambda functions triggered by EventBridge
aws lambda create-function \
  --function-name ingestion-monitor \
  --runtime python3.12 \
  --handler agent.handler \
  --timeout 60

# Wire EventBridge rule: pipeline signals → ingestion monitor
aws events put-rule \
  --name pipeline-signals \
  --event-pattern '{"source":["pipeline"]}'
```

For full deployment guide see [`docs/architecture.md`](docs/architecture.md).

---

## 📖 Documentation

| Document | Contents |
|---|---|
| [`docs/architecture.md`](docs/architecture.md) | System design, event-driven architecture, ADRs |
| [`docs/agents.md`](docs/agents.md) | Per-agent reference: config, inputs, outputs |
| [`docs/event-schema.md`](docs/event-schema.md) | Full Kafka message schemas with examples |
| [`docs/runbooks.md`](docs/runbooks.md) | Incident response by severity level |

---

## 📄 License

MIT License — see [LICENSE](LICENSE)

---

<div align="center">

*Built as a portfolio project demonstrating production-grade Multi-Agent System design for data engineering.*

**[Architecture](docs/architecture.md) · [Agents](docs/agents.md) · [Event Schema](docs/event-schema.md) · [Runbooks](docs/runbooks.md)**

</div>
