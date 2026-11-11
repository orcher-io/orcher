-- ORCHER Task Orchestrator - Initial Database Schema
-- This migration creates the core infrastructure tables for Orcher.

-- ============================================================================
-- EXTENSIONS
-- ============================================================================

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pg_stat_statements";
CREATE EXTENSION IF NOT EXISTS "pg_trgm";

-- ============================================================================
-- WORKER HEARTBEATS TABLE: Worker health monitoring
-- ============================================================================

CREATE TABLE worker_heartbeats (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    worker_id VARCHAR(255) NOT NULL,

    -- Heartbeat data
    timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    status VARCHAR(50) NOT NULL,

    -- Resource metrics
    cpu_usage_percent DECIMAL(5,2),
    memory_usage_percent DECIMAL(5,2),
    disk_usage_percent DECIMAL(5,2),
    load_average DECIMAL(8,2),

    -- Task metrics
    active_tasks INTEGER DEFAULT 0,
    completed_tasks_since_start BIGINT DEFAULT 0,
    failed_tasks_since_start BIGINT DEFAULT 0,

    -- Health metrics
    uptime_seconds BIGINT,
    health_score DECIMAL(5,2),

    -- Additional metrics
    metrics JSONB DEFAULT '{}'
);

-- ============================================================================
-- SYSTEM METRICS TABLE: Orchestrator-wide metrics
-- ============================================================================

CREATE TABLE system_metrics (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    metric_name VARCHAR(255) NOT NULL,
    metric_type VARCHAR(50) NOT NULL DEFAULT 'gauge',

    -- Metric value
    value DECIMAL(20,6) NOT NULL,
    unit VARCHAR(50),

    -- Dimensions/tags
    tags JSONB DEFAULT '{}',
    dimensions JSONB DEFAULT '{}',

    -- Timing
    timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    interval_seconds INTEGER,

    -- Metadata
    source VARCHAR(255),
    component VARCHAR(255),
    metadata JSONB DEFAULT '{}',

    CONSTRAINT system_metrics_type_check CHECK (
        metric_type IN ('counter', 'gauge', 'histogram', 'timer')
    )
);

-- ============================================================================
-- TRIGGER FUNCTION: update_updated_at_column
-- ============================================================================

CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ language 'plpgsql';

-- ============================================================================
-- INDEXES
-- ============================================================================

-- System metrics indexes (V002 versions - better than V001 originals)
CREATE INDEX idx_system_metrics_timestamp ON system_metrics(timestamp DESC);
CREATE INDEX idx_system_metrics_component ON system_metrics(component, timestamp DESC);
CREATE INDEX idx_system_metrics_name ON system_metrics(metric_name);
CREATE INDEX idx_system_metrics_tags ON system_metrics USING GIN(tags);

-- Worker heartbeat indexes
CREATE INDEX idx_worker_heartbeats_worker_id ON worker_heartbeats(worker_id, timestamp DESC);
CREATE INDEX idx_worker_heartbeats_timestamp ON worker_heartbeats(timestamp DESC);

-- ============================================================================
-- COMMENTS
-- ============================================================================

COMMENT ON TABLE worker_heartbeats IS 'Worker health monitoring and metrics';
COMMENT ON TABLE system_metrics IS 'System-wide metrics and monitoring data';
COMMENT ON INDEX idx_system_metrics_timestamp IS 'Optimize system metrics time-series queries';
COMMENT ON INDEX idx_system_metrics_component IS 'Optimize per-component metric lookups';
COMMENT ON INDEX idx_worker_heartbeats_worker_id IS 'Optimize worker-specific heartbeat queries';
