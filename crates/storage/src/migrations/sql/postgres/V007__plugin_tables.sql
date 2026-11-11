-- ORCHER Task Orchestrator - Plugin System
-- Migration V007: Plugin registry, configurations, executions, health checks, and API endpoints

-- ============================================================================
-- PLUGINS TABLE: Registry of all discovered and registered plugins
-- ============================================================================

CREATE TABLE plugins (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),

    -- Plugin identification
    name VARCHAR(100) UNIQUE NOT NULL,
    version VARCHAR(50) NOT NULL,
    display_name VARCHAR(150),
    description TEXT,

    -- Multi-tenancy
    organization_id UUID REFERENCES organizations(id) ON DELETE CASCADE,

    -- Plugin metadata
    author VARCHAR(100),
    license VARCHAR(50),
    repository VARCHAR(255),
    documentation VARCHAR(255),

    -- Plugin source and location
    source_type VARCHAR(50) NOT NULL DEFAULT 'file',
    source_path TEXT NOT NULL,
    checksum VARCHAR(128),

    -- Plugin state
    state VARCHAR(50) NOT NULL DEFAULT 'discovered',
    is_enabled BOOLEAN NOT NULL DEFAULT false,
    auto_start BOOLEAN NOT NULL DEFAULT false,

    -- Plugin type classification
    plugin_type VARCHAR(50) NOT NULL DEFAULT 'custom',

    -- Plugin capabilities
    capabilities JSONB DEFAULT '{}',
    supported_task_types TEXT[],
    supported_integrations TEXT[],

    -- Resource requirements
    min_memory_mb INTEGER,
    min_cpu_cores DECIMAL(3,1),
    min_disk_space_mb INTEGER,

    -- Compatibility
    min_orcher_version VARCHAR(50),
    max_orcher_version VARCHAR(50),
    required_features TEXT[],

    -- Plugin metrics
    load_count INTEGER NOT NULL DEFAULT 0,
    error_count INTEGER NOT NULL DEFAULT 0,
    last_loaded_at TIMESTAMPTZ,
    last_error_at TIMESTAMPTZ,
    last_error_message TEXT,

    -- Audit fields
    discovered_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT plugins_source_type_check CHECK (
        source_type IN ('file', 'git', 'registry', 'url', 'embedded')
    ),
    CONSTRAINT plugins_state_check CHECK (
        state IN ('discovered', 'loading', 'loaded', 'initializing', 'ready',
                  'running', 'completed', 'failed', 'paused', 'stopping',
                  'stopped', 'removed', 'unknown')
    ),
    CONSTRAINT plugins_type_check CHECK (
        plugin_type IN ('executor', 'middleware', 'integration',
                        'transform', 'trigger', 'custom')
    ),
    UNIQUE(name, version)
);

-- ============================================================================
-- PLUGIN CONFIGURATIONS TABLE
-- ============================================================================

CREATE TABLE plugin_configurations (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    plugin_id UUID NOT NULL REFERENCES plugins(id) ON DELETE CASCADE,

    -- Configuration identification
    config_name VARCHAR(100) NOT NULL DEFAULT 'default',
    description TEXT,

    -- Configuration data
    configuration JSONB NOT NULL DEFAULT '{}',

    -- Configuration state
    is_active BOOLEAN NOT NULL DEFAULT true,
    is_default BOOLEAN NOT NULL DEFAULT false,

    -- Environment targeting
    environment VARCHAR(50) DEFAULT 'production',

    -- Validation
    schema_version VARCHAR(20),
    last_validated_at TIMESTAMPTZ,
    validation_errors JSONB,

    -- Audit fields
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_by UUID REFERENCES users(id),

    UNIQUE(plugin_id, config_name, environment)
);

-- ============================================================================
-- PLUGIN DEPENDENCIES TABLE
-- ============================================================================

CREATE TABLE plugin_dependencies (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    plugin_id UUID NOT NULL REFERENCES plugins(id) ON DELETE CASCADE,

    -- Dependency information
    dependency_name VARCHAR(100) NOT NULL,
    version_requirement VARCHAR(100),
    dependency_type VARCHAR(50) NOT NULL DEFAULT 'runtime',

    -- Dependency state
    is_optional BOOLEAN NOT NULL DEFAULT false,
    is_satisfied BOOLEAN NOT NULL DEFAULT false,

    -- Resolution
    resolved_plugin_id UUID REFERENCES plugins(id),
    resolved_version VARCHAR(50),

    -- Metadata
    minimum_capabilities JSONB DEFAULT '{}',

    -- Audit fields
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT plugin_dependencies_type_check CHECK (
        dependency_type IN ('runtime', 'development', 'optional', 'peer')
    ),
    UNIQUE(plugin_id, dependency_name)
);

-- ============================================================================
-- PLUGIN PERMISSIONS TABLE
-- ============================================================================

CREATE TABLE plugin_permissions (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    plugin_id UUID NOT NULL REFERENCES plugins(id) ON DELETE CASCADE,

    -- Permission categories
    filesystem_permissions JSONB DEFAULT '{}',
    network_permissions JSONB DEFAULT '{}',
    database_permissions JSONB DEFAULT '{}',
    system_permissions JSONB DEFAULT '{}',
    api_permissions JSONB DEFAULT '{}',

    -- Resource limits
    max_memory_mb INTEGER DEFAULT 512,
    max_cpu_percent DECIMAL(5,2) DEFAULT 50.0,
    max_disk_space_mb INTEGER DEFAULT 1024,
    max_network_bandwidth_mbps DECIMAL(8,2) DEFAULT 10.0,
    max_execution_time INTERVAL DEFAULT INTERVAL '5 minutes',
    max_concurrent_tasks INTEGER DEFAULT 5,

    -- Security settings
    allow_network_access BOOLEAN NOT NULL DEFAULT false,
    allow_file_system_access BOOLEAN NOT NULL DEFAULT false,
    allow_database_access BOOLEAN NOT NULL DEFAULT false,
    allow_subprocess_execution BOOLEAN NOT NULL DEFAULT false,

    -- Sandbox configuration
    sandbox_enabled BOOLEAN NOT NULL DEFAULT true,
    sandbox_config JSONB DEFAULT '{}',

    -- Permission grants
    granted_by UUID REFERENCES users(id),
    granted_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    -- Audit fields
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    UNIQUE(plugin_id)
);

-- ============================================================================
-- PLUGIN EXECUTIONS TABLE
-- ============================================================================

CREATE TABLE plugin_executions (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    plugin_id UUID NOT NULL REFERENCES plugins(id) ON DELETE CASCADE,

    -- Execution context
    execution_id VARCHAR(255) UNIQUE NOT NULL,
    execution_type VARCHAR(50) NOT NULL,

    -- Trigger information
    triggered_by VARCHAR(50) NOT NULL,
    trigger_user_id UUID REFERENCES users(id),
    trigger_task_id UUID,
    trigger_workflow_id UUID REFERENCES workflow_executions(id),

    -- Execution details
    input_data JSONB DEFAULT '{}',
    configuration_used JSONB DEFAULT '{}',

    -- Execution state
    status VARCHAR(50) NOT NULL DEFAULT 'pending',

    -- Timing
    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at TIMESTAMPTZ,
    duration_ms BIGINT,
    timeout_at TIMESTAMPTZ,

    -- Results
    output_data JSONB,
    exit_code INTEGER,

    -- Error handling
    error_message TEXT,
    error_details JSONB,
    stack_trace TEXT,

    -- Resource usage
    peak_memory_mb INTEGER,
    cpu_time_ms BIGINT,
    disk_io_mb INTEGER,
    network_io_mb INTEGER,

    -- Metadata
    execution_metadata JSONB DEFAULT '{}',

    CONSTRAINT plugin_executions_type_check CHECK (
        execution_type IN ('task', 'webhook', 'scheduled', 'manual', 'test', 'health_check')
    ),
    CONSTRAINT plugin_executions_status_check CHECK (
        status IN ('pending', 'running', 'completed', 'failed', 'timeout', 'cancelled')
    ),
    CONSTRAINT plugin_executions_triggered_by_check CHECK (
        triggered_by IN ('user', 'system', 'scheduler', 'webhook', 'task', 'workflow', 'api')
    )
);

-- ============================================================================
-- PLUGIN HEALTH CHECKS TABLE
-- ============================================================================

CREATE TABLE plugin_health_checks (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    plugin_id UUID NOT NULL REFERENCES plugins(id) ON DELETE CASCADE,

    -- Health check details
    check_type VARCHAR(50) NOT NULL DEFAULT 'standard',

    -- Health status
    status VARCHAR(50) NOT NULL,
    health_score DECIMAL(5,2),

    -- Check results
    response_time_ms INTEGER,
    checks_performed JSONB DEFAULT '{}',
    check_results JSONB DEFAULT '{}',

    -- Error information
    error_message TEXT,
    error_count INTEGER DEFAULT 0,

    -- Timing
    checked_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    -- Metadata
    check_metadata JSONB DEFAULT '{}',

    CONSTRAINT plugin_health_checks_type_check CHECK (
        check_type IN ('standard', 'deep', 'startup', 'shutdown', 'custom')
    ),
    CONSTRAINT plugin_health_checks_status_check CHECK (
        status IN ('healthy', 'degraded', 'unhealthy', 'unknown', 'timeout', 'error')
    ),
    CONSTRAINT plugin_health_checks_score_check CHECK (
        health_score IS NULL OR (health_score >= 0.0 AND health_score <= 100.0)
    )
);

-- ============================================================================
-- PLUGIN EVENTS TABLE: Lifecycle and operational events
-- ============================================================================

CREATE TABLE plugin_events (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    plugin_id UUID NOT NULL REFERENCES plugins(id) ON DELETE CASCADE,

    -- Event identification
    event_type VARCHAR(100) NOT NULL,
    event_category VARCHAR(50) NOT NULL,

    -- Event details
    event_data JSONB DEFAULT '{}',
    message TEXT,

    -- Event context
    user_id UUID REFERENCES users(id),
    session_id UUID REFERENCES user_sessions(id),
    execution_id UUID REFERENCES plugin_executions(id),

    -- Event severity
    severity VARCHAR(20) NOT NULL DEFAULT 'info',

    -- Timing
    occurred_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    -- Correlation
    correlation_id UUID,
    parent_event_id UUID REFERENCES plugin_events(id),

    -- Metadata
    event_metadata JSONB DEFAULT '{}',

    CONSTRAINT plugin_events_category_check CHECK (
        event_category IN ('lifecycle', 'execution', 'security', 'resource', 'error', 'audit', 'custom')
    ),
    CONSTRAINT plugin_events_severity_check CHECK (
        severity IN ('debug', 'info', 'warning', 'error', 'critical')
    )
);

-- ============================================================================
-- PLUGIN RESOURCE USAGE TABLE
-- ============================================================================

CREATE TABLE plugin_resource_usage (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    plugin_id UUID NOT NULL REFERENCES plugins(id) ON DELETE CASCADE,

    -- Resource metrics
    memory_usage_mb INTEGER NOT NULL,
    cpu_usage_percent DECIMAL(5,2) NOT NULL,
    disk_usage_mb INTEGER DEFAULT 0,
    network_usage_mb DECIMAL(10,2) DEFAULT 0,

    -- Execution metrics
    active_executions INTEGER DEFAULT 0,
    total_executions INTEGER DEFAULT 0,
    failed_executions INTEGER DEFAULT 0,

    -- Performance metrics
    avg_execution_time_ms INTEGER,
    avg_response_time_ms INTEGER,

    -- Time-based aggregation
    measurement_interval INTERVAL NOT NULL DEFAULT INTERVAL '1 minute',
    measurement_start TIMESTAMPTZ NOT NULL,
    measurement_end TIMESTAMPTZ NOT NULL,

    -- Metadata
    additional_metrics JSONB DEFAULT '{}',

    CONSTRAINT plugin_resource_usage_interval_check CHECK (
        measurement_end > measurement_start
    ),
    CONSTRAINT plugin_resource_usage_cpu_check CHECK (
        cpu_usage_percent >= 0.0 AND cpu_usage_percent <= 100.0
    )
);

-- ============================================================================
-- PLUGIN SCHEDULES TABLE
-- ============================================================================

CREATE TABLE plugin_schedules (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    plugin_id UUID NOT NULL REFERENCES plugins(id) ON DELETE CASCADE,

    -- Schedule identification
    name VARCHAR(100) NOT NULL,
    description TEXT,

    -- Schedule definition
    schedule_type VARCHAR(50) NOT NULL,
    cron_expression VARCHAR(100),
    interval_seconds INTEGER,

    -- Schedule state
    is_enabled BOOLEAN NOT NULL DEFAULT true,

    -- Execution settings
    execution_timeout INTERVAL DEFAULT INTERVAL '30 minutes',
    max_concurrent_executions INTEGER DEFAULT 1,
    retry_count INTEGER DEFAULT 0,
    retry_delay INTERVAL DEFAULT INTERVAL '1 minute',

    -- Schedule timing
    next_execution TIMESTAMPTZ,
    last_execution TIMESTAMPTZ,
    last_execution_status VARCHAR(50),

    -- Input configuration
    default_input JSONB DEFAULT '{}',

    -- Metadata
    tags TEXT[],
    metadata JSONB DEFAULT '{}',

    -- Audit fields
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_by UUID REFERENCES users(id),

    CONSTRAINT plugin_schedules_type_check CHECK (
        schedule_type IN ('cron', 'interval', 'once', 'manual')
    ),
    CONSTRAINT plugin_schedules_interval_check CHECK (
        (schedule_type = 'interval' AND interval_seconds > 0) OR
        (schedule_type != 'interval' AND interval_seconds IS NULL)
    ),
    CONSTRAINT plugin_schedules_cron_check CHECK (
        (schedule_type = 'cron' AND cron_expression IS NOT NULL) OR
        (schedule_type != 'cron' AND cron_expression IS NULL)
    ),
    UNIQUE(plugin_id, name)
);

-- ============================================================================
-- PLUGIN API ENDPOINTS TABLE
-- ============================================================================

CREATE TABLE plugin_api_endpoints (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    plugin_id UUID NOT NULL REFERENCES plugins(id) ON DELETE CASCADE,

    -- Endpoint definition
    endpoint_path VARCHAR(255) NOT NULL,
    http_method VARCHAR(10) NOT NULL,

    -- Endpoint metadata
    description TEXT,
    summary VARCHAR(255),

    -- Security
    requires_authentication BOOLEAN NOT NULL DEFAULT true,
    required_permissions TEXT[],
    rate_limit_per_minute INTEGER DEFAULT 60,

    -- Request/Response specification
    request_schema JSONB,
    response_schema JSONB,

    -- Endpoint state
    is_enabled BOOLEAN NOT NULL DEFAULT true,
    is_deprecated BOOLEAN NOT NULL DEFAULT false,

    -- Usage tracking
    total_requests INTEGER DEFAULT 0,
    successful_requests INTEGER DEFAULT 0,
    failed_requests INTEGER DEFAULT 0,
    last_request_at TIMESTAMPTZ,

    -- Performance metrics
    avg_response_time_ms INTEGER,

    -- Audit fields
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT plugin_api_endpoints_method_check CHECK (
        http_method IN ('GET', 'POST', 'PUT', 'DELETE', 'PATCH', 'HEAD', 'OPTIONS')
    ),
    UNIQUE(plugin_id, endpoint_path, http_method)
);

-- ============================================================================
-- TRIGGERS
-- ============================================================================

CREATE TRIGGER update_plugins_updated_at BEFORE UPDATE ON plugins
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_plugin_configurations_updated_at BEFORE UPDATE ON plugin_configurations
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_plugin_dependencies_updated_at BEFORE UPDATE ON plugin_dependencies
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_plugin_permissions_updated_at BEFORE UPDATE ON plugin_permissions
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_plugin_schedules_updated_at BEFORE UPDATE ON plugin_schedules
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_plugin_api_endpoints_updated_at BEFORE UPDATE ON plugin_api_endpoints
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- ============================================================================
-- INDEXES
-- ============================================================================

-- Plugins indexes
CREATE INDEX idx_plugins_name ON plugins(name);
CREATE INDEX idx_plugins_state ON plugins(state, is_enabled);
CREATE INDEX idx_plugins_type ON plugins(plugin_type);
CREATE INDEX idx_plugins_capabilities ON plugins USING GIN(capabilities);
CREATE INDEX idx_plugins_task_types ON plugins USING GIN(supported_task_types);
CREATE INDEX idx_plugins_source_type ON plugins(source_type);
CREATE INDEX idx_plugins_last_loaded ON plugins(last_loaded_at);
CREATE INDEX idx_plugins_organization_id ON plugins(organization_id) WHERE organization_id IS NOT NULL;

-- Plugin configurations indexes
CREATE INDEX idx_plugin_configurations_plugin ON plugin_configurations(plugin_id);
CREATE INDEX idx_plugin_configurations_active ON plugin_configurations(plugin_id, is_active);
CREATE INDEX idx_plugin_configurations_environment ON plugin_configurations(environment, is_active);

-- Plugin dependencies indexes
CREATE INDEX idx_plugin_dependencies_plugin ON plugin_dependencies(plugin_id);
CREATE INDEX idx_plugin_dependencies_resolved ON plugin_dependencies(resolved_plugin_id) WHERE resolved_plugin_id IS NOT NULL;
CREATE INDEX idx_plugin_dependencies_satisfied ON plugin_dependencies(is_satisfied, is_optional);

-- Plugin executions indexes
CREATE INDEX idx_plugin_executions_plugin ON plugin_executions(plugin_id);
CREATE INDEX idx_plugin_executions_execution_id ON plugin_executions(execution_id);
CREATE INDEX idx_plugin_executions_status ON plugin_executions(status, started_at);
CREATE INDEX idx_plugin_executions_triggered_by ON plugin_executions(triggered_by, started_at);
CREATE INDEX idx_plugin_executions_user ON plugin_executions(trigger_user_id) WHERE trigger_user_id IS NOT NULL;
CREATE INDEX idx_plugin_executions_task ON plugin_executions(trigger_task_id) WHERE trigger_task_id IS NOT NULL;
CREATE INDEX idx_plugin_executions_workflow ON plugin_executions(trigger_workflow_id) WHERE trigger_workflow_id IS NOT NULL;

-- Plugin health checks indexes
CREATE INDEX idx_plugin_health_checks_plugin ON plugin_health_checks(plugin_id);
CREATE INDEX idx_plugin_health_checks_status ON plugin_health_checks(status, checked_at);
CREATE INDEX idx_plugin_health_checks_recent ON plugin_health_checks(plugin_id, checked_at DESC);

-- Plugin events indexes
CREATE INDEX idx_plugin_events_plugin ON plugin_events(plugin_id);
CREATE INDEX idx_plugin_events_type ON plugin_events(event_type, occurred_at);
CREATE INDEX idx_plugin_events_category ON plugin_events(event_category, occurred_at);
CREATE INDEX idx_plugin_events_severity ON plugin_events(severity, occurred_at);
CREATE INDEX idx_plugin_events_correlation ON plugin_events(correlation_id) WHERE correlation_id IS NOT NULL;
CREATE INDEX idx_plugin_events_execution ON plugin_events(execution_id) WHERE execution_id IS NOT NULL;

-- Plugin resource usage indexes
CREATE INDEX idx_plugin_resource_usage_plugin ON plugin_resource_usage(plugin_id);
CREATE INDEX idx_plugin_resource_usage_time ON plugin_resource_usage(measurement_start, measurement_end);
CREATE INDEX idx_plugin_resource_usage_memory ON plugin_resource_usage(memory_usage_mb, measurement_start);

-- Plugin schedules indexes
CREATE INDEX idx_plugin_schedules_plugin ON plugin_schedules(plugin_id);
CREATE INDEX idx_plugin_schedules_enabled ON plugin_schedules(is_enabled, next_execution);
CREATE INDEX idx_plugin_schedules_next_execution ON plugin_schedules(next_execution) WHERE next_execution IS NOT NULL;

-- Plugin API endpoints indexes
CREATE INDEX idx_plugin_api_endpoints_plugin ON plugin_api_endpoints(plugin_id);
CREATE INDEX idx_plugin_api_endpoints_path ON plugin_api_endpoints(endpoint_path, http_method);
CREATE INDEX idx_plugin_api_endpoints_enabled ON plugin_api_endpoints(is_enabled, is_deprecated);

-- Composite indexes for common query patterns
CREATE INDEX idx_plugins_enabled_state ON plugins(is_enabled, state, name);
CREATE INDEX idx_plugins_type_enabled ON plugins(plugin_type, is_enabled, state);
CREATE INDEX idx_plugin_executions_plugin_status ON plugin_executions(plugin_id, status, started_at DESC);
CREATE INDEX idx_plugin_health_checks_plugin_latest ON plugin_health_checks(plugin_id, checked_at DESC, status);
CREATE INDEX idx_plugin_events_plugin_recent ON plugin_events(plugin_id, occurred_at DESC, severity);

-- ============================================================================
-- VIEWS
-- ============================================================================

CREATE VIEW v_active_plugins AS
SELECT
    p.*,
    pc.configuration as active_config,
    pp.max_memory_mb,
    pp.max_cpu_percent,
    pp.sandbox_enabled
FROM plugins p
LEFT JOIN plugin_configurations pc ON p.id = pc.plugin_id AND pc.is_active = true AND pc.is_default = true
LEFT JOIN plugin_permissions pp ON p.id = pp.plugin_id
WHERE p.is_enabled = true AND p.state = 'running';

CREATE VIEW v_plugin_health_summary AS
SELECT
    p.id as plugin_id,
    p.name as plugin_name,
    p.state as plugin_state,
    phc.status as health_status,
    phc.health_score,
    phc.checked_at as last_health_check,
    CASE
        WHEN phc.checked_at < NOW() - INTERVAL '5 minutes' THEN 'stale'
        WHEN phc.status = 'healthy' THEN 'healthy'
        WHEN phc.status = 'degraded' THEN 'warning'
        ELSE 'critical'
    END as overall_status
FROM plugins p
LEFT JOIN LATERAL (
    SELECT * FROM plugin_health_checks
    WHERE plugin_id = p.id
    ORDER BY checked_at DESC
    LIMIT 1
) phc ON true
WHERE p.is_enabled = true;

CREATE VIEW v_plugin_execution_stats AS
SELECT
    p.id as plugin_id,
    p.name as plugin_name,
    COUNT(pe.id) as total_executions,
    COUNT(CASE WHEN pe.status = 'completed' THEN 1 END) as successful_executions,
    COUNT(CASE WHEN pe.status = 'failed' THEN 1 END) as failed_executions,
    COUNT(CASE WHEN pe.status = 'running' THEN 1 END) as running_executions,
    AVG(pe.duration_ms) as avg_duration_ms,
    MAX(pe.started_at) as last_execution
FROM plugins p
LEFT JOIN plugin_executions pe ON p.id = pe.plugin_id
WHERE p.is_enabled = true
GROUP BY p.id, p.name;

CREATE VIEW v_plugin_resource_current AS
SELECT DISTINCT ON (pru.plugin_id)
    p.id as plugin_id,
    p.name as plugin_name,
    pru.memory_usage_mb,
    pru.cpu_usage_percent,
    pru.disk_usage_mb,
    pru.active_executions,
    pru.measurement_start,
    pp.max_memory_mb,
    pp.max_cpu_percent,
    CASE
        WHEN pru.memory_usage_mb > pp.max_memory_mb * 0.9 THEN 'memory_high'
        WHEN pru.cpu_usage_percent > pp.max_cpu_percent * 0.9 THEN 'cpu_high'
        ELSE 'normal'
    END as resource_status
FROM plugins p
LEFT JOIN plugin_resource_usage pru ON p.id = pru.plugin_id
LEFT JOIN plugin_permissions pp ON p.id = pp.plugin_id
WHERE p.is_enabled = true
ORDER BY pru.plugin_id, pru.measurement_start DESC;

-- ============================================================================
-- COMMENTS
-- ============================================================================

COMMENT ON TABLE plugins IS 'Registry of all discovered and loaded plugins';
COMMENT ON TABLE plugin_configurations IS 'Plugin-specific configuration settings';
COMMENT ON TABLE plugin_dependencies IS 'Plugin dependency relationships and resolution';
COMMENT ON TABLE plugin_permissions IS 'Security permissions and resource limits for plugins';
COMMENT ON TABLE plugin_executions IS 'Individual plugin execution instances and results';
COMMENT ON TABLE plugin_health_checks IS 'Plugin health check results and monitoring';
COMMENT ON TABLE plugin_events IS 'Plugin lifecycle and operational event log';
COMMENT ON TABLE plugin_resource_usage IS 'Time-series plugin resource consumption data';
COMMENT ON TABLE plugin_schedules IS 'Scheduled plugin execution configurations';
COMMENT ON TABLE plugin_api_endpoints IS 'API endpoints exposed by plugins';

COMMENT ON COLUMN plugins.plugin_type IS 'Plugin type: executor, middleware, integration, transform, trigger, or custom';
COMMENT ON COLUMN plugins.state IS 'Plugin state: discovered, loading, loaded, initializing, ready, running, completed, failed, paused, stopping, stopped, removed, or unknown';
COMMENT ON COLUMN plugins.capabilities IS 'JSONB array of plugin capabilities';
COMMENT ON COLUMN plugins.source_type IS 'Plugin source location type: file, git, registry, url, or embedded';
