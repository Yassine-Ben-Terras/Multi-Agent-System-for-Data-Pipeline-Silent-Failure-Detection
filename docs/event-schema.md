# Event Bus — Topic Definitions & Message Schemas

## Topics

| Topic | Producer | Consumer(s) | Retention |
|---|---|---|---|
| `pipeline.signals.raw` | Pipeline stages | Ingestion Monitor, Schema Watcher | 24h |
| `agents.anomalies` | Specialist agents | Orchestrator | 7 days |
| `agents.confirmed_incidents` | Orchestrator | Lineage Agent, Remediation Agent | 30 days |
| `agents.actions_taken` | Remediation Agent | Audit log, Incident system | 90 days |
| `agents.heartbeats` | All agents (60s) | Orchestrator health monitor | 1h |

---

## `pipeline.signals.raw` — PipelineSignal

```json
{
  "event_id": "uuid-v4",
  "event_type": "pipeline.stage.completed",
  "stage": "ingestion | transformation | serving",
  "source": "orders_api | dbt_run | kafka_consumer",
  "metrics": {
    "row_count": 142830,
    "checksum": "a3f9c2...",
    "duration_ms": 4210,
    "null_rate": 0.002
  },
  "schema_snapshot": {
    "columns": [
      {"name": "order_id", "type": "VARCHAR", "nullable": false},
      {"name": "amount_usd", "type": "FLOAT", "nullable": true}
    ],
    "version_hash": "b7c2e1..."
  },
  "pipeline_run_id": "run-2026-04-22-001",
  "ts": "2026-04-22T03:14:00Z"
}
```

---

## `agents.anomalies` — AnomalySignal

```json
{
  "signal_id": "uuid-v4",
  "anomaly_type": "row_count_anomaly | schema_drift | quality_failure | freshness_violation | checksum_mismatch",
  "source_agent": "ingestion_monitor",
  "pipeline_run_id": "run-2026-04-22-001",
  "model_name": "fct_orders",
  "severity": "LOW | MEDIUM | HIGH | CRITICAL",
  "confidence": 0.92,
  "details": {
    "expected_row_count_mean": 143000,
    "actual_row_count": 95000,
    "z_score": -3.4,
    "threshold": 2.5
  },
  "ts": "2026-04-22T03:15:12Z"
}
```

---

## `agents.confirmed_incidents` — ConfirmedIncident

```json
{
  "incident_id": "uuid-v4",
  "root_cause": "schema_drift",
  "severity": "HIGH",
  "pipeline_run_id": "run-2026-04-22-001",
  "correlated_signals": ["signal_id_1", "signal_id_2"],
  "blast_radius": {
    "affected_models": ["fct_orders", "fct_revenue", "dim_customers"],
    "dashboards": ["Executive Revenue Dashboard", "Finance Weekly Report"],
    "ml_features": ["churn_model_feature_order_count"],
    "next_refresh_time": "2026-04-22T06:00:00Z",
    "sla_impact": "Finance report due at 08:00 UTC — 4h window"
  },
  "llm_hypothesis": "The column 'payment_method' was dropped from the orders_api source, causing JOIN failures in fct_orders which propagated to all downstream revenue models.",
  "ts": "2026-04-22T03:15:45Z"
}
```

---

## `agents.actions_taken` — RemediationAction

```json
{
  "action_id": "uuid-v4",
  "incident_id": "incident-uuid",
  "action_type": "quarantine_partition | trigger_backfill | pause_downstream_dags | create_pagerduty | notify_slack",
  "target": "fct_orders__2026-04-22",
  "payload": {
    "partition_key": "2026-04-22",
    "reason": "Row count anomaly: z-score -3.4 (threshold 2.5)"
  },
  "success": true,
  "error_message": null,
  "ts": "2026-04-22T03:16:02Z"
}
```
