-- ─────────────────────────────────────────────────────────────
-- MAS Pipeline Sentinel — Agent State Store Schema
-- Runs automatically on first postgres container start
-- ─────────────────────────────────────────────────────────────

-- ── Incidents ────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS incidents (
    id              SERIAL PRIMARY KEY,
    incident_id     VARCHAR(64) UNIQUE NOT NULL,
    pipeline_run_id VARCHAR(64),
    root_cause      VARCHAR(128),
    severity        VARCHAR(16) CHECK (severity IN ('LOW', 'MEDIUM', 'HIGH', 'CRITICAL')),
    status          VARCHAR(32) CHECK (status IN ('open', 'investigating', 'remediating', 'resolved', 'false_positive')),
    affected_models JSONB,
    blast_radius    JSONB,
    causal_chain    JSONB,
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    updated_at      TIMESTAMPTZ DEFAULT NOW(),
    resolved_at     TIMESTAMPTZ
);

-- ── Model Baselines (for anomaly detection) ──────────────────
CREATE TABLE IF NOT EXISTS model_baselines (
    id              SERIAL PRIMARY KEY,
    model_name      VARCHAR(256) NOT NULL,
    source          VARCHAR(256),
    metric          VARCHAR(64) NOT NULL,   -- row_count, null_rate, etc.
    mean            FLOAT,
    std_dev         FLOAT,
    sample_count    INTEGER,
    window_days     INTEGER DEFAULT 7,
    last_updated    TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (model_name, source, metric)
);

-- ── Agent Heartbeats ─────────────────────────────────────────
CREATE TABLE IF NOT EXISTS agent_heartbeats (
    agent_name      VARCHAR(64) PRIMARY KEY,
    last_seen       TIMESTAMPTZ DEFAULT NOW(),
    status          VARCHAR(16) DEFAULT 'alive',
    metadata        JSONB
);

-- ── Test History (for flakiness detection) ───────────────────
CREATE TABLE IF NOT EXISTS test_history (
    id              SERIAL PRIMARY KEY,
    model_name      VARCHAR(256) NOT NULL,
    test_name       VARCHAR(256) NOT NULL,
    run_id          VARCHAR(64),
    passed          BOOLEAN,
    failure_rate    FLOAT,
    run_at          TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_test_history_model ON test_history (model_name, test_name);

-- ── Alert Feedback (for threshold calibration) ───────────────
CREATE TABLE IF NOT EXISTS alert_feedback (
    id              SERIAL PRIMARY KEY,
    incident_id     VARCHAR(64) REFERENCES incidents(incident_id),
    feedback        VARCHAR(16) CHECK (feedback IN ('actionable', 'noise', 'partial')),
    engineer        VARCHAR(128),
    notes           TEXT,
    created_at      TIMESTAMPTZ DEFAULT NOW()
);

-- ── Remediation Actions Log ──────────────────────────────────
CREATE TABLE IF NOT EXISTS remediation_actions (
    id              SERIAL PRIMARY KEY,
    incident_id     VARCHAR(64),
    action_type     VARCHAR(64),  -- quarantine, backfill, create_incident, pause_dag
    target          VARCHAR(256),
    payload         JSONB,
    success         BOOLEAN,
    error_message   TEXT,
    executed_at     TIMESTAMPTZ DEFAULT NOW()
);

-- Seed agent heartbeat rows
INSERT INTO agent_heartbeats (agent_name, status) VALUES
    ('orchestrator', 'pending'),
    ('ingestion_monitor', 'pending'),
    ('schema_watcher', 'pending'),
    ('quality_auditor', 'pending'),
    ('lineage_impact', 'pending'),
    ('remediation', 'pending')
ON CONFLICT DO NOTHING;
