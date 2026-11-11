-- ORCHER Task Orchestrator - Cluster Management
-- Migration V009: Cluster definitions, nodes, heartbeats, and events

-- ============================================================================
-- CLUSTERS TABLE: Cluster definitions and configurations
-- ============================================================================

CREATE TABLE clusters (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    name VARCHAR(255) NOT NULL UNIQUE,
    description TEXT,

    -- Cluster configuration
    cluster_type VARCHAR(50) NOT NULL DEFAULT 'standard',
    region VARCHAR(100),
    zone VARCHAR(100),

    -- Cluster settings
    max_nodes INTEGER DEFAULT 100,
    min_nodes INTEGER DEFAULT 1,
    auto_scaling_enabled BOOLEAN NOT NULL DEFAULT false,

    -- Cluster metadata
    labels JSONB DEFAULT '{}',
    annotations JSONB DEFAULT '{}',
    metadata JSONB DEFAULT '{}',

    -- Cluster status
    status VARCHAR(50) NOT NULL DEFAULT 'initializing',
    health_status VARCHAR(50) DEFAULT 'unknown',

    -- Leadership tracking
    leader_node_id UUID,
    leader_elected_at TIMESTAMPTZ,

    -- Version tracking
    version VARCHAR(50) NOT NULL DEFAULT '1.0.0',

    -- Audit fields
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_by VARCHAR(255),

    CONSTRAINT clusters_status_check CHECK (
        status IN ('initializing', 'active', 'degraded', 'maintenance', 'failed', 'terminated')
    ),
    CONSTRAINT clusters_health_check CHECK (
        health_status IN ('healthy', 'degraded', 'unhealthy', 'unknown')
    ),
    CONSTRAINT clusters_type_check CHECK (
        cluster_type IN ('standard', 'high_availability', 'development', 'production', 'testing')
    ),
    CONSTRAINT clusters_node_count_check CHECK (
        min_nodes > 0 AND (max_nodes IS NULL OR max_nodes >= min_nodes)
    )
);

-- ============================================================================
-- NODES TABLE: Individual nodes within clusters
-- ============================================================================

CREATE TABLE nodes (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    cluster_id UUID NOT NULL REFERENCES clusters(id) ON DELETE CASCADE,

    -- Node identification
    name VARCHAR(255) NOT NULL,
    node_type VARCHAR(50) NOT NULL DEFAULT 'worker',

    -- Network configuration
    bind_address INET NOT NULL,
    bind_port INTEGER NOT NULL,

    -- Node location
    region VARCHAR(100),
    zone VARCHAR(100),
    datacenter VARCHAR(100),

    -- Node capabilities
    capabilities TEXT[] DEFAULT '{}',

    -- Resource configuration
    cpu_cores DECIMAL(5,2),
    memory_mb INTEGER,
    disk_space_mb INTEGER,

    -- Node metadata
    labels JSONB DEFAULT '{}',
    metadata JSONB DEFAULT '{}',

    -- Node status
    status VARCHAR(50) NOT NULL DEFAULT 'initializing',
    health_status VARCHAR(50) DEFAULT 'unknown',

    -- Health tracking
    last_heartbeat TIMESTAMPTZ,
    heartbeat_interval_seconds INTEGER DEFAULT 30,
    heartbeat_timeout_seconds INTEGER DEFAULT 90,
    consecutive_failures INTEGER NOT NULL DEFAULT 0,

    -- Leadership eligibility
    is_leader_eligible BOOLEAN NOT NULL DEFAULT true,
    is_current_leader BOOLEAN NOT NULL DEFAULT false,

    -- Node version and build
    version VARCHAR(50),
    build_hash VARCHAR(64),

    -- Audit fields
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    registered_at TIMESTAMPTZ DEFAULT NOW(),
    deregistered_at TIMESTAMPTZ,

    CONSTRAINT nodes_status_check CHECK (
        status IN ('initializing', 'active', 'draining', 'offline', 'failed', 'maintenance', 'terminated')
    ),
    CONSTRAINT nodes_health_check CHECK (
        health_status IN ('healthy', 'degraded', 'unhealthy', 'unknown')
    ),
    CONSTRAINT nodes_type_check CHECK (
        node_type IN ('master', 'worker', 'hybrid', 'coordinator')
    ),
    CONSTRAINT nodes_port_check CHECK (
        bind_port > 0 AND bind_port <= 65535
    ),
    UNIQUE(cluster_id, name),
    UNIQUE(bind_address, bind_port)
);

-- ============================================================================
-- NODE HEARTBEATS TABLE: Node health and metrics over time
-- ============================================================================

CREATE TABLE node_heartbeats (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    node_id UUID NOT NULL REFERENCES nodes(id) ON DELETE CASCADE,
    cluster_id UUID NOT NULL REFERENCES clusters(id) ON DELETE CASCADE,

    -- Heartbeat timing
    timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    -- Resource metrics
    cpu_usage_percent DECIMAL(5,2),
    memory_usage_percent DECIMAL(5,2),
    disk_usage_percent DECIMAL(5,2),
    load_average DECIMAL(8,2),

    -- Worker metrics
    active_workers INTEGER DEFAULT 0,
    available_capacity INTEGER DEFAULT 0,

    -- Health metrics
    health_score DECIMAL(5,2),
    health_details JSONB DEFAULT '{}',

    -- Network metrics
    network_latency_ms DECIMAL(10,2),
    network_throughput_mbps DECIMAL(10,2),

    -- Additional metrics
    metrics JSONB DEFAULT '{}',

    CONSTRAINT node_heartbeats_cpu_check CHECK (
        cpu_usage_percent IS NULL OR (cpu_usage_percent >= 0 AND cpu_usage_percent <= 100)
    ),
    CONSTRAINT node_heartbeats_memory_check CHECK (
        memory_usage_percent IS NULL OR (memory_usage_percent >= 0 AND memory_usage_percent <= 100)
    ),
    CONSTRAINT node_heartbeats_disk_check CHECK (
        disk_usage_percent IS NULL OR (disk_usage_percent >= 0 AND disk_usage_percent <= 100)
    )
);

-- ============================================================================
-- CLUSTER EVENTS TABLE: Cluster lifecycle events
-- ============================================================================

CREATE TABLE cluster_events (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    cluster_id UUID NOT NULL REFERENCES clusters(id) ON DELETE CASCADE,
    node_id UUID REFERENCES nodes(id) ON DELETE SET NULL,

    -- Event details
    event_type VARCHAR(100) NOT NULL,
    event_category VARCHAR(50) NOT NULL,
    severity VARCHAR(20) NOT NULL DEFAULT 'info',

    -- Event data
    message TEXT NOT NULL,
    details JSONB DEFAULT '{}',

    -- Event context
    triggered_by VARCHAR(50) NOT NULL DEFAULT 'system',
    user_id VARCHAR(255),

    -- Event timing
    occurred_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    -- Event metadata
    tags TEXT[] DEFAULT '{}',
    metadata JSONB DEFAULT '{}',

    CONSTRAINT cluster_events_category_check CHECK (
        event_category IN ('lifecycle', 'health', 'leadership', 'scaling', 'configuration', 'error', 'security')
    ),
    CONSTRAINT cluster_events_severity_check CHECK (
        severity IN ('debug', 'info', 'warning', 'error', 'critical')
    ),
    CONSTRAINT cluster_events_triggered_by_check CHECK (
        triggered_by IN ('system', 'user', 'scheduler', 'health_check', 'auto_scaling', 'api')
    )
);

-- ============================================================================
-- FOREIGN KEY: Cluster leader reference (after nodes table exists)
-- ============================================================================

ALTER TABLE clusters
    ADD CONSTRAINT clusters_leader_node_fk
    FOREIGN KEY (leader_node_id)
    REFERENCES nodes(id)
    ON DELETE SET NULL;

-- ============================================================================
-- TRIGGERS
-- ============================================================================

CREATE TRIGGER update_clusters_updated_at
    BEFORE UPDATE ON clusters
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_nodes_updated_at
    BEFORE UPDATE ON nodes
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

-- ============================================================================
-- INDEXES
-- ============================================================================

-- Cluster indexes
CREATE INDEX idx_clusters_status ON clusters(status);
CREATE INDEX idx_clusters_health_status ON clusters(health_status);
CREATE INDEX idx_clusters_type ON clusters(cluster_type);
CREATE INDEX idx_clusters_created_at ON clusters(created_at);

-- Node indexes
CREATE INDEX idx_nodes_cluster_id ON nodes(cluster_id);
CREATE INDEX idx_nodes_status ON nodes(status);
CREATE INDEX idx_nodes_health_status ON nodes(health_status);
CREATE INDEX idx_nodes_last_heartbeat ON nodes(last_heartbeat);
CREATE INDEX idx_nodes_is_leader ON nodes(is_current_leader) WHERE is_current_leader = true;
CREATE INDEX idx_nodes_type ON nodes(node_type);
CREATE INDEX idx_nodes_active ON nodes(cluster_id, status) WHERE status = 'active';

-- Node heartbeat indexes
CREATE INDEX idx_node_heartbeats_node_id ON node_heartbeats(node_id);
CREATE INDEX idx_node_heartbeats_cluster_id ON node_heartbeats(cluster_id);
CREATE INDEX idx_node_heartbeats_timestamp ON node_heartbeats(timestamp DESC);
CREATE INDEX idx_node_heartbeats_recent ON node_heartbeats(node_id, timestamp DESC);

-- Cluster events indexes
CREATE INDEX idx_cluster_events_cluster_id ON cluster_events(cluster_id);
CREATE INDEX idx_cluster_events_node_id ON cluster_events(node_id) WHERE node_id IS NOT NULL;
CREATE INDEX idx_cluster_events_type ON cluster_events(event_type);
CREATE INDEX idx_cluster_events_category ON cluster_events(event_category);
CREATE INDEX idx_cluster_events_severity ON cluster_events(severity);
CREATE INDEX idx_cluster_events_occurred_at ON cluster_events(occurred_at DESC);

-- Composite indexes for common queries
CREATE INDEX idx_nodes_cluster_status_health ON nodes(cluster_id, status, health_status);
CREATE INDEX idx_cluster_events_cluster_severity ON cluster_events(cluster_id, severity, occurred_at DESC);

-- ============================================================================
-- COMMENTS
-- ============================================================================

COMMENT ON TABLE clusters IS 'Cluster definitions and configurations for distributed deployments';
COMMENT ON TABLE nodes IS 'Individual nodes within clusters with health and capability tracking';
COMMENT ON TABLE node_heartbeats IS 'Time-series health and metrics data for cluster nodes';
COMMENT ON TABLE cluster_events IS 'Audit trail of cluster lifecycle and operational events';

COMMENT ON COLUMN clusters.leader_node_id IS 'Current leader node for this cluster (if using leader election)';
COMMENT ON COLUMN nodes.is_leader_eligible IS 'Whether this node can participate in leader election';
COMMENT ON COLUMN nodes.is_current_leader IS 'Whether this node is currently the cluster leader';
COMMENT ON COLUMN nodes.consecutive_failures IS 'Number of consecutive health check failures';
