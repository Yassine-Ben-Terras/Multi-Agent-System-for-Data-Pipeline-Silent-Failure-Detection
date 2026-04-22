# 🛡️ MAS Pipeline Sentinel

> **Multi-Agent System for Data Pipeline Silent Failure Detection**

A production-grade, AI-powered Multi-Agent System (MAS) that autonomously **detects**, **diagnoses**, and **remediates** silent failures across modern data engineering pipelines — before they reach dashboards, ML models, or finance reports.

---

##  The Problem

Silent failures are the most dangerous failures in data engineering. A pipeline completes successfully, no exception is raised, no alert fires — yet the data it produced is **wrong, incomplete, or corrupted**. Downstream consumers operate on bad data for hours, sometimes days.

> No single monitoring point has full context. Ingestion doesn't know what the dashboard expects. Transformation doesn't know what the source intended.

---

##  The Solution

A five-agent system where each agent owns a distinct observability domain, communicates through a shared event bus, and collaborates through a central orchestrator to build a **causal chain** — not just a symptom report.

```
┌─────────────────────────────────────────────────────────┐
│                    ORCHESTRATOR                          │
│        (correlates signals · manages severity)          │
└──────────┬──────────┬──────────┬──────────┬─────────────┘
           │          │          │          │
    ┌──────▼──┐  ┌────▼────┐ ┌──▼──────┐ ┌─▼──────────┐
    │Ingestion│  │ Schema  │ │Quality  │ │ Lineage &  │
    │ Monitor │  │ Watcher │ │Auditor  │ │   Impact   │
    └─────────┘  └─────────┘ └─────────┘ └─────────────┘
                                                │
                                         ┌──────▼──────┐
                                         │Remediation  │
                                         │   Agent     │
                                         └─────────────┘
```

### The Five Agents

| Agent | Domain | Key Signals |
|---|---|---|
| **Ingestion Monitor** | Source systems | Row count anomaly, arrival latency, checksum drift |
| **Schema Watcher** | Schema contracts | Column drops, type coercions, semantic drift |
| **Quality Auditor** | dbt test results | Failure rates, NULL rates, distribution shifts |
| **Lineage & Impact** | dbt DAG traversal | Blast radius: models, dashboards, SLA impact |
| **Remediation** | Automated response | Quarantine, backfill, PagerDuty/Jira incidents |

> **Key architectural principle:** agents reason on **metadata only** — row counts, schema snapshots, statistical summaries, dbt artifacts. Raw data never leaves its warehouse.

---

##  Tech Stack

| Layer | Technology |
|---|---|
| Agent Framework | [LangGraph](https://github.com/langchain-ai/langgraph) |
| LLM Backend | Anthropic Claude (Sonnet for reasoning, Haiku for classification) |
| Message Bus | Apache Kafka (Confluent Cloud or self-hosted) |
| Agent State Store | PostgreSQL |
| Metadata Store | S3 / Local (dbt artifacts: `manifest.json`, `run_results.json`) |
| dbt Integration | dbt Core |
| Orchestration | Apache Airflow |
| Containerization | Docker + Docker Compose |
| Observability | OpenTelemetry |
| Cloud | AWS (Lambda, ECS Fargate, EventBridge, S3) |

---

##  Project Structure

```
mas-pipeline-sentinel/
├── agents/
│   ├── ingestion_monitor/     # Row count anomaly, checksum drift detection
│   ├── schema_watcher/        # Schema contract validation
│   ├── quality_auditor/       # dbt test result parsing & analysis
│   ├── lineage_impact/        # dbt DAG traversal & blast radius
│   ├── remediation/           # Quarantine, backfill, incident creation
│   └── orchestrator/          # Signal correlation & severity escalation
├── infra/
│   ├── kafka/                 # Kafka topic definitions & configs
│   └── postgres/              # State store schema & migrations
├── dbt_integration/           # dbt artifact consumers & schema.yml helpers
├── tests/
│   ├── unit/                  # Per-agent unit tests
│   └── integration/           # End-to-end pipeline tests
├── config/                    # Environment configs (topics, thresholds, playbooks)
├── scripts/                   # Setup, seed, and utility scripts
├── docs/                      # Architecture diagrams & ADRs
├── docker-compose.yml         # Full local stack
├── Makefile                   # Developer shortcuts
└── requirements.txt
```

---

##  Quickstart (Local)

### Prerequisites
- Docker & Docker Compose
- Python 3.11+
- Anthropic API key

### 1. Clone & configure
```bash
git clone https://github.com/YOUR_USERNAME/mas-pipeline-sentinel.git
cd mas-pipeline-sentinel
cp .env.example .env
# Edit .env → add your ANTHROPIC_API_KEY
```

### 2. Start the local stack
```bash
make up
```

### 3. Run in shadow mode (observe only, no actions)
```bash
make shadow-mode
```

---

##  Deployment Phases

| Phase | What's Live | Success Criteria |
|---|---|---|
| **Shadow Mode** (Wk 1–2) | All agents observe & log only | Zero false positives on known-good runs |
| **Alert-Only** (Wk 3–4) | Agents fire Slack/email alerts | >80% of alerts are actionable |
| **Supervised Remediation** (Wk 5–6) | AUTO on LOW severity only | MTTD decreases >50% |
| **Full Autonomy** (Wk 7+) | All severity levels autonomous | MTTD <5 min, false positive rate <5% |

---

##  Documentation

- [`docs/architecture.md`](docs/architecture.md) — Full system design & ADRs
- [`docs/agents.md`](docs/agents.md) — Per-agent design & observe→reason→signal loop
- [`docs/event-schema.md`](docs/event-schema.md) — Event bus topic definitions & message schemas
- [`docs/runbooks.md`](docs/runbooks.md) — Incident response & remediation playbooks

---

##  Roadmap

- [x] Project scaffold & environment setup
- [ ] Kafka + PostgreSQL local infrastructure
- [ ] Ingestion Monitor Agent (baseline)
- [ ] Schema Watcher Agent
- [ ] Quality Auditor Agent (dbt integration)
- [ ] Lineage & Impact Agent (manifest.json traversal)
- [ ] Orchestrator correlation logic
- [ ] Remediation Agent with playbooks
- [ ] Shadow mode deployment
- [ ] OpenTelemetry observability
- [ ] AWS production deployment

---

##  License

MIT License — see [LICENSE](LICENSE)

---

*Built as a portfolio project demonstrating production-grade Multi-Agent System design for data engineering pipelines.*
