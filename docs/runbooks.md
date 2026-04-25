# Incident Response Runbooks

This document describes what the system does automatically at each severity level,
and what on-call engineers should do when they receive an alert.

---

## Severity Levels

| Severity | MTTD Target | Auto-Remediation | On-Call Paged |
|---|---|---|---|
| LOW | < 30 min | Never | No |
| MEDIUM | < 15 min | Never | No |
| HIGH | < 10 min | Quarantine (full_autonomy) | No |
| CRITICAL | < 5 min | Quarantine + backfill + Slack | Yes (PagerDuty) |

---

## Runbook: Row Count Anomaly

**Signal:** `anomaly_type: row_count_anomaly`
**Typical root causes:** API pagination truncation, upstream source outage, partition filter bug

### Automated actions (by severity)

**HIGH (|z| ≥ 3.5):**
1. Slack alert to `#data-platform`
2. Quarantine affected partition (full_autonomy mode)

**CRITICAL (zero rows or |z| ≥ 5.0):**
1. Slack alert
2. Quarantine partition
3. Trigger dbt backfill
4. Page on-call via PagerDuty

### On-call checklist

```
[ ] Check source system status (API health page, vendor status)
[ ] Verify partition filter — is the date range correct?
[ ] Check API pagination — did it truncate at page limit?
[ ] Review upstream dependency — did a feed go silent?
[ ] If data is recoverable: approve backfill (supervised mode) or confirm auto-backfill
[ ] Update incident with root cause in Jira/PagerDuty
[ ] Resolve quarantine once data is validated
```

---

## Runbook: Schema Drift

**Signal:** `anomaly_type: schema_drift`
**Typical root causes:** Upstream API schema change, vendor feed format change, migration without coordination

### Automated actions (by severity)

**MEDIUM (new nullable column):**
1. Slack alert — informational

**HIGH (type change):**
1. Slack alert
2. Quarantine affected partition

**CRITICAL (dropped column, especially criticality=critical):**
1. Slack alert
2. Quarantine partition
3. Pause downstream DAGs (up to 5 models)
4. Trigger dbt backfill with `--full-refresh`
5. Page on-call via PagerDuty

### On-call checklist

```
[ ] Identify which column changed (see incident details.changes[])
[ ] Contact upstream team — was this intentional?
[ ] If intentional: update schema contract (ContractStore.register_contract)
    and update dbt schema.yml to reflect new schema
[ ] If unintentional: work with upstream to revert or provide compatible feed
[ ] Once schema is stable: unpause DAGs and validate data
[ ] Run dbt test to confirm all assertions pass
[ ] Update blast_radius models — check dashboards are serving correct data
```

---

## Runbook: Quality Failure

**Signal:** `anomaly_type: quality_failure`
**Typical root causes:** Data quality regression, logic bug in transformation, upstream data issue

### Automated actions

**CRITICAL (from dbt meta agent_severity=critical):**
1. Slack alert with blast_radius and oncall_team
2. PagerDuty page to oncall_team specified in dbt meta

**Others:**
1. Slack alert only

### On-call checklist

```
[ ] Check incident details.tests[] for which dbt tests failed
[ ] Note is_flaky flag — if true, may be noise (but investigate anyway)
[ ] Check blast_radius.dashboards — are executive reports affected?
[ ] Run failing dbt tests manually: dbt test --select <model>
[ ] If logic bug: fix transformation, rerun dbt build --select <model>
[ ] If upstream data: quarantine and await upstream fix
[ ] Rate the alert (actionable/noise) in Slack for threshold calibration
```

---

## Runbook: Freshness Violation

**Signal:** `anomaly_type: freshness_violation`
**Typical root causes:** Upstream feed delayed, ingestion job failure, scheduler issue

### On-call checklist

```
[ ] Check dbt source freshness: dbt source freshness
[ ] Check ingestion job logs — did it run? Did it complete?
[ ] Check upstream SLA — is the vendor late?
[ ] If ingestion failed: rerun ingestion job, then dbt build
[ ] If vendor delayed: note expected delay, snooze alerts for SLA window
```

---

## Runbook: Checksum Mismatch

**Signal:** `anomaly_type: checksum_mismatch`
**Typical root causes:** Idempotency failure (double-write), data mutation in source, re-ingestion without deduplication

### On-call checklist

```
[ ] Check ingestion job — did it run twice for same partition?
[ ] Check source system — was data mutated after first ingestion?
[ ] Review deduplication logic in pipeline
[ ] Quarantine suspect partition and re-ingest from source
[ ] Run reconciliation: compare row counts source vs warehouse
```

---

## Checking Agent Health

```bash
# View all agent heartbeats
docker compose logs ingestion-monitor | grep heartbeat
docker compose logs orchestrator | grep heartbeat

# Check PostgreSQL for agent status
psql -U mas_user -d mas_sentinel -c "SELECT agent_name, last_seen, status FROM agent_heartbeats;"

# Check open incidents
psql -U mas_user -d mas_sentinel -c "SELECT incident_id, root_cause, severity, status, created_at FROM incidents WHERE status = 'open';"

# Check recent remediation actions
psql -U mas_user -d mas_sentinel -c "SELECT action_type, target, success, executed_at FROM remediation_actions ORDER BY executed_at DESC LIMIT 20;"
```

---

## Escalation Path

```
LOW / MEDIUM → Slack #data-platform → Data Engineer (pipeline owner)
HIGH         → Slack #data-platform → Data Platform Engineer
CRITICAL     → Slack + PagerDuty   → Data Platform On-Call → Data Team Lead
```

---

## Alert Feedback (Threshold Calibration)

Every Slack alert has a 👍 (actionable) / 👎 (noise) reaction.
The Quality Auditor reads this weekly and adjusts per-model thresholds.

```sql
-- View recent feedback
SELECT i.root_cause, f.feedback, COUNT(*) 
FROM alert_feedback f
JOIN incidents i ON f.incident_id = i.incident_id
WHERE f.created_at > NOW() - INTERVAL '7 days'
GROUP BY i.root_cause, f.feedback;
```
