# Agent Reference Guide

Each agent follows the same **Observe → Reason → Signal** loop and shares a common lifecycle:
- Subscribes to one or more Kafka topics
- Publishes heartbeats every 60 seconds to `agents.heartbeats`
- Respects `AGENT_MODE` (shadow / alert_only / supervised / full_autonomy)
- Exposes structured logs via `structlog`

---

## Ingestion Monitor

**File:** `agents/ingestion_monitor/`
**Consumes:** `pipeline.signals.raw` (stage = `ingestion`)
**Produces:** `agents.anomalies` (AnomalySignal)

### What it detects

| Detection | Mechanism | Default Threshold |
|---|---|---|
| Row count anomaly | Welford z-score vs 7-day rolling baseline | ±2.5σ |
| Zero-row ingestion | Explicit check before z-score | row_count == 0 |
| Checksum drift | Per (source, partition_date) hash comparison | Any change |

### Severity mapping

| Condition | Severity |
|---|---|
| `row_count == 0` | CRITICAL |
| `\|z\| >= 5.0` | CRITICAL |
| `\|z\| >= 3.5` | HIGH |
| `\|z\| >= 2.5` | MEDIUM |

### Configuration

```env
ANOMALY_ZSCORE_THRESHOLD=2.5      # z-score threshold (default: 2.5)
HEARTBEAT_INTERVAL_SECONDS=60
```

### Baseline algorithm

Uses **Welford's online algorithm** — updates mean and variance incrementally without storing all historical values. Minimum 3 samples required before anomaly detection activates.

---

## Schema Watcher

**File:** `agents/schema_watcher/`
**Consumes:** `pipeline.signals.raw` (all stages with schema_snapshot)
**Produces:** `agents.anomalies` (AnomalySignal)

### What it detects

| Change Type | Default Severity | Escalated (critical column) |
|---|---|---|
| Dropped column | CRITICAL | CRITICAL |
| Type changed | HIGH | CRITICAL |
| NOT NULL → nullable | MEDIUM | CRITICAL |
| New column | LOW | LOW |

### Bootstrap mode

On first sight of a source, the incoming schema is **auto-registered** as the trusted contract — no manual setup required. Subsequent signals are diffed against this contract.

### Marking critical columns

In your pipeline signal's `schema_snapshot`, add `criticality: "critical"` to key columns:

```json
{
  "columns": [
    {"name": "order_id",    "type": "VARCHAR", "nullable": false, "criticality": "critical"},
    {"name": "amount_usd",  "type": "FLOAT",   "nullable": true,  "criticality": "critical"},
    {"name": "status",      "type": "VARCHAR", "nullable": true}
  ],
  "version_hash": "b7c2e1..."
}
```

---

## Quality Auditor

**File:** `agents/quality_auditor/`
**Trigger:** Artifact watcher (polls for new `run_results.json` every 30s)
**Produces:** `agents.anomalies` (AnomalySignal)

### What it detects

- dbt test failures — any test with `status: fail | error`
- Severity from dbt `meta.agent_severity` annotation (highest priority)
- Severity from failure row count (fallback):

| Failure rows | Severity |
|---|---|
| 1–99 | LOW |
| 100–9,999 | MEDIUM |
| 10,000–99,999 | HIGH |
| 100,000+ | CRITICAL |

- Historically **flaky tests** (>20% failure rate in last 20 runs) are downgraded one level
- One `AnomalySignal` emitted **per model** (aggregated, not per-test)

### dbt meta annotations

```yaml
# models/schema.yml
- name: fct_orders
  columns:
    - name: amount_usd
      tests:
        - not_null:
            meta:
              agent_severity: critical        # override severity
              blast_radius: executive_dashboard
              oncall_team: data-platform
```

### Configuration

```env
DBT_ARTIFACTS_PATH=./dbt_integration/artifacts   # local path or s3://bucket
ARTIFACT_POLL_INTERVAL_SECONDS=30
```

---

## Lineage & Impact

**File:** `agents/lineage_impact/`
**Consumes:** `agents.anomalies`
**Produces:** `agents.confirmed_incidents` (ConfirmedIncident with blast_radius)

### What it computes

For each anomaly signal received:

1. Looks up the failed model in `manifest.json`
2. BFS traversal — finds all **transitive downstream models**
3. Finds all **exposures** (dashboards, ML features) that depend on affected models
4. Computes **severity multiplier** (1.0–3.0) based on blast size
5. Emits a `ConfirmedIncident` with full `BlastRadius`

### Blast radius example

```json
{
  "affected_models": ["fct_revenue", "dim_customers", "rpt_weekly_sales"],
  "dashboards": ["executive_revenue_dashboard"],
  "ml_features": ["churn_model_features"],
  "sla_impact": "SLA at risk: executive_revenue_dashboard (daily_08:00_UTC)"
}
```

### DAG refresh

Reloads `manifest.json` every 5 minutes via MD5 hash comparison. Thread-safe — DAG access is protected by `threading.Lock`.

---

## Orchestrator

**File:** `agents/orchestrator/`
**Consumes:** `agents.anomalies`
**Produces:** `agents.confirmed_incidents`

### Signal correlation (5-minute tumbling window)

Signals are grouped by `pipeline_run_id`. When the 5-minute window closes, the Orchestrator correlates all signals in the group into a single root cause.

### Correlation rules (priority order)

| Signals Present | Root Cause | Confidence |
|---|---|---|
| SCHEMA_DRIFT + ROW_COUNT + QUALITY | schema_drift | 0.97 |
| SCHEMA_DRIFT + ROW_COUNT | schema_drift | 0.95 |
| SCHEMA_DRIFT + QUALITY_FAILURE | schema_drift | 0.92 |
| FRESHNESS_VIOLATION + ROW_COUNT | freshness_violation | 0.90 |
| No rule matches | unknown → LLM called | 0.50 |

### LLM call strategy

| Task | Model | When |
|---|---|---|
| Root cause classification | claude-haiku | Known pattern, fast confirmation |
| Full hypothesis generation | claude-sonnet | Novel/unknown pattern |
| No LLM | — | Rule matched with confidence ≥ 0.90 |

### Suppression

Buckets are suppressed (no incident created) when:
- All signals are LOW severity with confidence < 0.6
- Single schema drift signal with LOW severity and confidence < 0.6

---

## Remediation

**File:** `agents/remediation/`
**Consumes:** `agents.confirmed_incidents`
**Produces:** `agents.actions_taken`

### Action playbook

| Severity | Actions (in order) |
|---|---|
| LOW / MEDIUM | Slack notification |
| HIGH | Slack → quarantine partition |
| CRITICAL | Slack → quarantine → [pause DAGs if schema_drift] → dbt backfill → PagerDuty |

### Mode behaviour

| Mode | LOW/MEDIUM | HIGH | CRITICAL |
|---|---|---|---|
| `shadow` | log only | log only | log only |
| `alert_only` | Slack only | Slack only | Slack only |
| `supervised` | auto | requires approval | requires approval (PagerDuty always fires) |
| `full_autonomy` | auto | auto | auto |

### Safety rules

- **Never auto-delete** — quarantine only (rename/tag partition)
- **Quarantine before backfill** — safe ordering guaranteed
- **`REMEDIATION_DRY_RUN=true`** by default — logs all actions without executing
- Full audit log in `remediation_actions` table and `agents.actions_taken` topic

### Configuration

```env
REMEDIATION_DRY_RUN=true             # set false for real execution
SLACK_WEBHOOK_URL=https://hooks.slack.com/...
PAGERDUTY_ROUTING_KEY=your-key
JIRA_BASE_URL=https://company.atlassian.net
JIRA_API_TOKEN=your-token
JIRA_EMAIL=your@email.com
AIRFLOW_BASE_URL=http://airflow:8082
```
