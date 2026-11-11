-- ORCHER Task Orchestrator - Worker Registry
-- Migration V018: SDK worker registration and heartbeat tracking

-- ============================================================================
-- WORKERS TABLE
-- ============================================================================

CREATE TABLE workers (
    id UUID PRIMARY KEY,
    organization_id UUID NOT NULL DEFAULT '00000000-0000-0000-0000-000000000000',
    worker_id VARCHAR(255) UNIQUE NOT NULL,
    name VARCHAR(255) NOT NULL,
    worker_type VARCHAR(100) NOT NULL DEFAULT 'standard',
    status VARCHAR(50) NOT NULL DEFAULT 'idle',
    capabilities TEXT[],
    max_concurrent_tasks INTEGER NOT NULL DEFAULT 1,
    current_task_count INTEGER NOT NULL DEFAULT 0,
    cpu_cores DECIMAL(5,2),
    memory_mb INTEGER,
    disk_space_mb INTEGER,
    metadata JSONB DEFAULT '{}',
    tags TEXT[],
    host_address VARCHAR(255),
    port INTEGER,
    last_heartbeat TIMESTAMPTZ,
    health_status VARCHAR(50) DEFAULT 'unknown',
    health_details JSONB DEFAULT '{}',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    registered_at TIMESTAMPTZ DEFAULT NOW(),
    CONSTRAINT workers_status_check CHECK (status IN ('idle', 'busy', 'draining', 'offline', 'error')),
    CONSTRAINT workers_health_check CHECK (health_status IN ('healthy', 'degraded', 'unhealthy', 'unknown'))
);

-- ============================================================================
-- INDEXES
-- ============================================================================

CREATE INDEX idx_workers_worker_id ON workers(worker_id);
CREATE INDEX idx_workers_status ON workers(status);
CREATE INDEX idx_workers_organization_id ON workers(organization_id);
CREATE INDEX idx_workers_last_heartbeat ON workers(last_heartbeat DESC);

-- ============================================================================
-- COMMENTS
-- ============================================================================

COMMENT ON TABLE workers IS 'Worker registry for tracking connected SDK workers';
