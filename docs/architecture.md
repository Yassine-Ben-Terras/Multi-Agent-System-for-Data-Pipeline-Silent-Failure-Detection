# Architecture — MAS Pipeline Sentinel

## System Design

### Core Principle

Agents reason on **metadata only**. Raw data never leaves the warehouse. Every agent consumes:
- Row counts, checksums, arrival timestamps
- Schema snapshots (column names, types, version hashes)
- dbt artifacts (`manifest.json`, `run_results.json`, `sources.json`)
- Statistical summaries (mean, std dev, NULL rates)

This makes the system **secure, lightweight, and warehouse-agnostic**.

---

## Event-Driven Architecture

```
Pipeline Stage
     │
     │  PipelineSignal (JSON)
     ▼
┌────────────────────────┐
│  pipeline.signals.raw  │  ← Kafka topic (24h retention)
└────────┬───────────────┘
         │
    ┌────┴──────────────────────────────┐
    │           │           │           │
    ▼           ▼           ▼           ▼
Ingestion   Schema      Quality     (future:
Monitor     Watcher     Auditor      Serving
                                     Monitor)
    │           │           │
    └───────────┴───────────┘
                │
         AnomalySignal
                │
                ▼
┌──────────────────────────┐
│    agents.anomalies      │  ← Kafka topic (7d retention)
└────────────┬─────────────┘
             │
             ▼
        ORCHESTRATOR
        (correlation,
         5-min window)
             │
             ▼
┌──────────────────────────────┐
│  agents.confirmed_incidents  │  ← Kafka topic (30d retention)
└────────────┬─────────────────┘
             │
    ┌────────┴──────────┐
    ▼                   ▼
Lineage Agent      Remediation
(blast radius)         Agent
                        │
                        ▼
              ┌──────────────────────┐
              │  agents.actions_taken│  ← Kafka (90d retention)
              └──────────────────────┘
```

---

## Agent Observe → Reason → Signal Loop

Every agent follows the same three-phase loop:

```
┌─────────────────────────────────────────────────────┐
│  OBSERVE    Subscribe to event bus topic(s)          │
│             Pull dbt artifacts from metadata store   │
├─────────────────────────────────────────────────────┤
│  REASON     Rule-based checks first (fast, cheap)    │
│             LLM call only if rules are inconclusive  │
│             Compare against baselines in state store  │
├─────────────────────────────────────────────────────┤
│  SIGNAL     Emit AnomalySignal to agents.anomalies   │
│             Update heartbeat in agents.heartbeats    │
│             Write metrics to state store             │
└─────────────────────────────────────────────────────┘
```

---

## LLM Call Strategy (Tiered)

```
Signal arrives
     │
     ▼
Rule-based check ──► PASS → no LLM call needed
     │
    FAIL / UNCERTAIN
     │
     ▼
Is it a known pattern? ──► YES → use cached LLM response
     │
     NO
     │
     ▼
Classification task? ──► claude-haiku (fast, cheap)
     │
     NO (complex root cause, novel pattern)
     │
     ▼
claude-sonnet (full reasoning)
```

---

## State Store Schema (PostgreSQL)

| Table | Purpose |
|---|---|
| `incidents` | Open/resolved incidents with full causal chain |
| `model_baselines` | 7-day rolling mean/stddev per model metric |
| `agent_heartbeats` | Liveness tracking for all agents |
| `test_history` | Historical dbt test results for flakiness detection |
| `alert_feedback` | On-call engineer feedback for threshold calibration |
| `remediation_actions` | Full audit log of every automated action taken |

---

## Deployment Models

| Model | Agent Host | LLM | Message Bus |
|---|---|---|---|
| Cloud-Native (AWS) | Lambda + ECS Fargate | Anthropic API | EventBridge |
| On-Premise | Docker / Kubernetes | Ollama (LLaMA/Mistral) | Redis Streams |
| Hybrid | Mixed | Anthropic API (metadata only) | Kafka |

---

## ADR-001: Metadata-Only Architecture

**Decision:** Agents consume only metadata, never raw warehouse rows.

**Rationale:**
- Security: no PII or sensitive business data leaves the warehouse
- Performance: metadata is orders of magnitude smaller than raw data
- Portability: works with any warehouse (Snowflake, BigQuery, Redshift, DuckDB)

**Trade-off:** Some anomaly patterns (e.g., value distribution drift) require statistical summaries to be pre-computed by the pipeline and included in the signal envelope.

---

## ADR-002: LangGraph for Agent Framework

**Decision:** Use LangGraph for agent orchestration.

**Rationale:**
- Stateful graph execution — agents maintain state across signal processing cycles
- Production-grade retry and error handling
- Fine-grained control over agent reasoning steps
- Strong Anthropic/LangChain integration

**Trade-off:** Higher initial complexity vs CrewAI. Worth it for production reliability.
