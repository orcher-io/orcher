-- ORCHER Task Orchestrator - Integrations and Webhooks System
-- Migration V008: External system integrations, webhook management, and event logging

-- ============================================================================
-- INTEGRATION PROVIDERS TABLE: Available integration types
-- ============================================================================

CREATE TABLE integration_providers (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),

    -- Provider identification
    name VARCHAR(100) UNIQUE NOT NULL,
    display_name VARCHAR(150) NOT NULL,
    description TEXT,

    -- Provider metadata
    provider_type VARCHAR(50) NOT NULL,
    version VARCHAR(50) NOT NULL DEFAULT '1.0.0',

    -- Provider capabilities
    capabilities JSONB DEFAULT '{}',
    supported_auth_types TEXT[] DEFAULT '{}',
    supported_operations TEXT[] DEFAULT '{}',

    -- Provider configuration
    config_schema JSONB DEFAULT '{}',
    default_config JSONB DEFAULT '{}',

    -- Provider status
    is_enabled BOOLEAN NOT NULL DEFAULT true,
    is_system_provider BOOLEAN NOT NULL DEFAULT false,

    -- Documentation and support
    documentation_url VARCHAR(255),
    support_url VARCHAR(255),
    icon_url VARCHAR(255),

    -- Audit fields
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_by UUID REFERENCES users(id),

    CONSTRAINT integration_providers_type_check CHECK (
        provider_type IN ('notification', 'data_source', 'data_sink', 'monitoring', 'authentication', 'storage', 'compute', 'custom')
    )
);

-- ============================================================================
-- INTEGRATIONS TABLE: Configured integration instances
-- ============================================================================

CREATE TABLE integrations (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    provider_id UUID NOT NULL REFERENCES integration_providers(id) ON DELETE RESTRICT,

    -- Integration identification
    name VARCHAR(100) NOT NULL,
    description TEXT,

    -- Integration configuration
    configuration JSONB NOT NULL DEFAULT '{}',
    environment VARCHAR(50) NOT NULL DEFAULT 'production',

    -- Authentication configuration
    auth_type VARCHAR(50) NOT NULL,
    auth_config JSONB DEFAULT '{}',

    -- Connection settings
    endpoint_url VARCHAR(500),
    timeout_seconds INTEGER DEFAULT 30,
    retry_config JSONB DEFAULT '{}',

    -- Rate limiting
    rate_limit_requests_per_minute INTEGER DEFAULT 60,
    rate_limit_requests_per_hour INTEGER DEFAULT 1000,
    rate_limit_burst_limit INTEGER DEFAULT 10,

    -- Integration state
    is_enabled BOOLEAN NOT NULL DEFAULT true,
    status VARCHAR(50) NOT NULL DEFAULT 'inactive',

    -- Health tracking
    last_health_check TIMESTAMPTZ,
    health_status VARCHAR(50) DEFAULT 'unknown',
    health_details JSONB DEFAULT '{}',

    -- Usage statistics
    total_requests INTEGER NOT NULL DEFAULT 0,
    successful_requests INTEGER NOT NULL DEFAULT 0,
    failed_requests INTEGER NOT NULL DEFAULT 0,
    last_request_at TIMESTAMPTZ,

    -- Error tracking
    last_error_at TIMESTAMPTZ,
    last_error_message TEXT,
    consecutive_failures INTEGER NOT NULL DEFAULT 0,

    -- Tags and metadata
    tags TEXT[] DEFAULT '{}',
    metadata JSONB DEFAULT '{}',

    -- Audit fields
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_by UUID REFERENCES users(id),

    CONSTRAINT integrations_auth_type_check CHECK (
        auth_type IN ('none', 'api_key', 'oauth2', 'jwt', 'basic', 'certificate', 'custom')
    ),
    CONSTRAINT integrations_status_check CHECK (
        status IN ('inactive', 'active', 'error', 'disabled', 'testing')
    ),
    CONSTRAINT integrations_health_status_check CHECK (
        health_status IN ('healthy', 'degraded', 'unhealthy', 'unknown', 'maintenance')
    ),
    UNIQUE(name, environment)
);

-- ============================================================================
-- INTEGRATION OPERATIONS TABLE: Individual operation tracking
-- ============================================================================

CREATE TABLE integration_operations (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    integration_id UUID NOT NULL REFERENCES integrations(id) ON DELETE CASCADE,

    -- Operation identification
    operation_id VARCHAR(255) UNIQUE NOT NULL,
    operation_type VARCHAR(100) NOT NULL,

    -- Request details
    request_method VARCHAR(10),
    request_url TEXT,
    request_headers JSONB DEFAULT '{}',
    request_body JSONB,

    -- Response details
    response_status INTEGER,
    response_headers JSONB DEFAULT '{}',
    response_body JSONB,
    response_time_ms INTEGER,

    -- Operation context
    triggered_by VARCHAR(50) NOT NULL,
    trigger_user_id UUID REFERENCES users(id),
    trigger_task_id UUID,
    trigger_workflow_id UUID REFERENCES workflow_executions(id),

    -- Operation status
    status VARCHAR(50) NOT NULL DEFAULT 'pending',

    -- Timing
    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at TIMESTAMPTZ,

    -- Error handling
    error_message TEXT,
    error_details JSONB,
    retry_count INTEGER NOT NULL DEFAULT 0,
    max_retries INTEGER DEFAULT 3,

    -- Rate limiting tracking
    rate_limited BOOLEAN NOT NULL DEFAULT false,
    rate_limit_reset_at TIMESTAMPTZ,

    -- Metadata
    operation_metadata JSONB DEFAULT '{}',

    CONSTRAINT integration_operations_status_check CHECK (
        status IN ('pending', 'running', 'completed', 'failed', 'timeout', 'cancelled', 'rate_limited')
    ),
    CONSTRAINT integration_operations_triggered_by_check CHECK (
        triggered_by IN ('user', 'system', 'scheduler', 'webhook', 'task', 'workflow', 'alert', 'manual')
    )
);

-- ============================================================================
-- WEBHOOK CONFIGURATIONS TABLE: Incoming webhook endpoints
-- ============================================================================

CREATE TABLE webhook_configurations (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),

    -- Webhook identification
    name VARCHAR(100) NOT NULL,
    description TEXT,

    -- Webhook endpoint
    webhook_url_path VARCHAR(255) UNIQUE NOT NULL,
    webhook_secret VARCHAR(255),

    -- Webhook settings
    is_enabled BOOLEAN NOT NULL DEFAULT true,
    requires_authentication BOOLEAN NOT NULL DEFAULT true,
    allowed_ips INET[] DEFAULT '{}',

    -- Processing configuration
    content_type VARCHAR(100) DEFAULT 'application/json',
    max_payload_size_mb INTEGER DEFAULT 10,
    timeout_seconds INTEGER DEFAULT 30,

    -- Response configuration
    success_response_code INTEGER DEFAULT 200,
    success_response_body TEXT DEFAULT 'OK',
    error_response_code INTEGER DEFAULT 400,

    -- Integration mapping
    target_integration_id UUID REFERENCES integrations(id),
    processing_plugin_id UUID REFERENCES plugins(id),

    -- Webhook filters
    header_filters JSONB DEFAULT '{}',
    payload_filters JSONB DEFAULT '{}',

    -- Rate limiting
    rate_limit_requests_per_minute INTEGER DEFAULT 60,
    rate_limit_requests_per_hour INTEGER DEFAULT 1000,

    -- Usage statistics
    total_requests INTEGER NOT NULL DEFAULT 0,
    successful_requests INTEGER NOT NULL DEFAULT 0,
    failed_requests INTEGER NOT NULL DEFAULT 0,
    last_request_at TIMESTAMPTZ,

    -- Tags and metadata
    tags TEXT[] DEFAULT '{}',
    metadata JSONB DEFAULT '{}',

    -- Audit fields
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_by UUID REFERENCES users(id),

    CONSTRAINT webhook_configurations_path_format CHECK (webhook_url_path ~ '^/[a-zA-Z0-9/_-]+$')
);

-- ============================================================================
-- WEBHOOK REQUESTS TABLE: All incoming webhook requests
-- ============================================================================

CREATE TABLE webhook_requests (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    webhook_config_id UUID NOT NULL REFERENCES webhook_configurations(id) ON DELETE CASCADE,

    -- Request identification
    request_id VARCHAR(255) UNIQUE NOT NULL,

    -- Request details
    source_ip INET NOT NULL,
    user_agent TEXT,
    request_headers JSONB DEFAULT '{}',
    request_body JSONB,
    content_type VARCHAR(100),
    content_length INTEGER,

    -- Security validation
    signature_header VARCHAR(500),
    signature_valid BOOLEAN,
    authentication_valid BOOLEAN NOT NULL DEFAULT false,

    -- Processing details
    status VARCHAR(50) NOT NULL DEFAULT 'received',
    processing_started_at TIMESTAMPTZ,
    processing_completed_at TIMESTAMPTZ,
    processing_duration_ms INTEGER,

    -- Response details
    response_status INTEGER,
    response_headers JSONB DEFAULT '{}',
    response_body TEXT,

    -- Error handling
    error_message TEXT,
    error_details JSONB,

    -- Processing results
    actions_triggered INTEGER DEFAULT 0,
    tasks_created INTEGER DEFAULT 0,
    workflows_started INTEGER DEFAULT 0,
    notifications_sent INTEGER DEFAULT 0,

    -- Rate limiting
    rate_limited BOOLEAN NOT NULL DEFAULT false,

    -- Timing
    received_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    -- Metadata
    request_metadata JSONB DEFAULT '{}',

    CONSTRAINT webhook_requests_status_check CHECK (
        status IN ('received', 'validating', 'processing', 'completed', 'failed', 'rejected', 'rate_limited')
    )
);

-- ============================================================================
-- INTEGRATION EVENTS TABLE: Event log for integration activities
-- ============================================================================

CREATE TABLE integration_events (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    integration_id UUID REFERENCES integrations(id) ON DELETE CASCADE,
    webhook_config_id UUID REFERENCES webhook_configurations(id) ON DELETE CASCADE,

    -- Event identification
    event_type VARCHAR(100) NOT NULL,
    event_category VARCHAR(50) NOT NULL,

    -- Event details
    event_data JSONB DEFAULT '{}',
    message TEXT,

    -- Event context
    user_id UUID REFERENCES users(id),
    operation_id UUID REFERENCES integration_operations(id),
    webhook_request_id UUID REFERENCES webhook_requests(id),

    -- Event severity
    severity VARCHAR(20) NOT NULL DEFAULT 'info',

    -- Timing
    occurred_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    -- Correlation
    correlation_id UUID,

    -- Metadata
    event_metadata JSONB DEFAULT '{}',

    CONSTRAINT integration_events_category_check CHECK (
        event_category IN ('connection', 'authentication', 'operation', 'webhook', 'error', 'health', 'configuration')
    ),
    CONSTRAINT integration_events_severity_check CHECK (
        severity IN ('debug', 'info', 'warning', 'error', 'critical')
    ),
    CONSTRAINT integration_events_source_check CHECK (
        (integration_id IS NOT NULL) OR (webhook_config_id IS NOT NULL)
    )
);

-- ============================================================================
-- INTEGRATION SCHEDULES TABLE: Scheduled integration operations
-- ============================================================================

CREATE TABLE integration_schedules (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    integration_id UUID NOT NULL REFERENCES integrations(id) ON DELETE CASCADE,

    -- Schedule identification
    name VARCHAR(100) NOT NULL,
    description TEXT,

    -- Schedule definition
    schedule_type VARCHAR(50) NOT NULL,
    cron_expression VARCHAR(100),
    interval_seconds INTEGER,

    -- Operation configuration
    operation_type VARCHAR(100) NOT NULL,
    operation_config JSONB DEFAULT '{}',

    -- Schedule state
    is_enabled BOOLEAN NOT NULL DEFAULT true,

    -- Execution settings
    timeout_seconds INTEGER DEFAULT 300,
    max_retries INTEGER DEFAULT 3,
    retry_delay_seconds INTEGER DEFAULT 60,

    -- Schedule timing
    next_execution TIMESTAMPTZ,
    last_execution TIMESTAMPTZ,
    last_execution_status VARCHAR(50),
    last_execution_duration_ms INTEGER,

    -- Error tracking
    consecutive_failures INTEGER NOT NULL DEFAULT 0,
    last_error_message TEXT,

    -- Metadata
    tags TEXT[] DEFAULT '{}',
    metadata JSONB DEFAULT '{}',

    -- Audit fields
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_by UUID REFERENCES users(id),

    CONSTRAINT integration_schedules_type_check CHECK (
        schedule_type IN ('cron', 'interval', 'once', 'manual')
    ),
    CONSTRAINT integration_schedules_interval_check CHECK (
        (schedule_type = 'interval' AND interval_seconds > 0) OR
        (schedule_type != 'interval' AND interval_seconds IS NULL)
    ),
    CONSTRAINT integration_schedules_cron_check CHECK (
        (schedule_type = 'cron' AND cron_expression IS NOT NULL) OR
        (schedule_type != 'cron' AND cron_expression IS NULL)
    ),
    UNIQUE(integration_id, name)
);

-- ============================================================================
-- TRIGGERS
-- ============================================================================

CREATE TRIGGER update_integration_providers_updated_at BEFORE UPDATE ON integration_providers
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_integrations_updated_at BEFORE UPDATE ON integrations
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_webhook_configurations_updated_at BEFORE UPDATE ON webhook_configurations
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_integration_schedules_updated_at BEFORE UPDATE ON integration_schedules
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- ============================================================================
-- INDEXES
-- ============================================================================

-- Integration providers indexes
CREATE INDEX idx_integration_providers_type ON integration_providers(provider_type, is_enabled);
CREATE INDEX idx_integration_providers_enabled ON integration_providers(is_enabled);

-- Integrations indexes
CREATE INDEX idx_integrations_provider ON integrations(provider_id);
CREATE INDEX idx_integrations_status ON integrations(status, is_enabled);
CREATE INDEX idx_integrations_environment ON integrations(environment, is_enabled);
CREATE INDEX idx_integrations_health ON integrations(health_status, last_health_check);
CREATE INDEX idx_integrations_tags ON integrations USING GIN(tags);
CREATE INDEX idx_integrations_last_request ON integrations(last_request_at);

-- Integration operations indexes
CREATE INDEX idx_integration_operations_integration ON integration_operations(integration_id);
CREATE INDEX idx_integration_operations_operation_id ON integration_operations(operation_id);
CREATE INDEX idx_integration_operations_status ON integration_operations(status, started_at);
CREATE INDEX idx_integration_operations_type ON integration_operations(operation_type, started_at);
CREATE INDEX idx_integration_operations_triggered_by ON integration_operations(triggered_by, started_at);
CREATE INDEX idx_integration_operations_user ON integration_operations(trigger_user_id) WHERE trigger_user_id IS NOT NULL;
CREATE INDEX idx_integration_operations_task ON integration_operations(trigger_task_id) WHERE trigger_task_id IS NOT NULL;
CREATE INDEX idx_integration_operations_workflow ON integration_operations(trigger_workflow_id) WHERE trigger_workflow_id IS NOT NULL;

-- Webhook configurations indexes
CREATE INDEX idx_webhook_configurations_enabled ON webhook_configurations(is_enabled);
CREATE INDEX idx_webhook_configurations_path ON webhook_configurations(webhook_url_path);
CREATE INDEX idx_webhook_configurations_integration ON webhook_configurations(target_integration_id) WHERE target_integration_id IS NOT NULL;
CREATE INDEX idx_webhook_configurations_plugin ON webhook_configurations(processing_plugin_id) WHERE processing_plugin_id IS NOT NULL;

-- Webhook requests indexes
CREATE INDEX idx_webhook_requests_config ON webhook_requests(webhook_config_id);
CREATE INDEX idx_webhook_requests_request_id ON webhook_requests(request_id);
CREATE INDEX idx_webhook_requests_status ON webhook_requests(status, received_at);
CREATE INDEX idx_webhook_requests_source_ip ON webhook_requests(source_ip, received_at);
CREATE INDEX idx_webhook_requests_received_at ON webhook_requests(received_at);

-- Integration events indexes
CREATE INDEX idx_integration_events_integration ON integration_events(integration_id) WHERE integration_id IS NOT NULL;
CREATE INDEX idx_integration_events_webhook ON integration_events(webhook_config_id) WHERE webhook_config_id IS NOT NULL;
CREATE INDEX idx_integration_events_type ON integration_events(event_type, occurred_at);
CREATE INDEX idx_integration_events_category ON integration_events(event_category, occurred_at);
CREATE INDEX idx_integration_events_severity ON integration_events(severity, occurred_at);
CREATE INDEX idx_integration_events_correlation ON integration_events(correlation_id) WHERE correlation_id IS NOT NULL;

-- Integration schedules indexes
CREATE INDEX idx_integration_schedules_integration ON integration_schedules(integration_id);
CREATE INDEX idx_integration_schedules_enabled ON integration_schedules(is_enabled, next_execution);
CREATE INDEX idx_integration_schedules_next_execution ON integration_schedules(next_execution) WHERE next_execution IS NOT NULL;

-- Composite indexes for common query patterns
CREATE INDEX idx_integrations_provider_status ON integrations(provider_id, status, is_enabled);
CREATE INDEX idx_integration_operations_integration_status ON integration_operations(integration_id, status, started_at DESC);
CREATE INDEX idx_webhook_requests_config_status ON webhook_requests(webhook_config_id, status, received_at DESC);
CREATE INDEX idx_integration_events_source_recent ON integration_events(integration_id, occurred_at DESC) WHERE integration_id IS NOT NULL;

-- ============================================================================
-- SEED DATA
-- ============================================================================

INSERT INTO integration_providers (name, display_name, description, provider_type, capabilities, supported_auth_types, supported_operations, is_system_provider) VALUES
('slack', 'Slack', 'Send notifications and messages to Slack channels', 'notification',
 '{"channels": true, "direct_messages": true, "file_uploads": true}',
 ARRAY['api_key', 'oauth2'], ARRAY['send_message', 'upload_file', 'get_channels'], true),

('email_smtp', 'SMTP Email', 'Send email notifications via SMTP', 'notification',
 '{"html_emails": true, "attachments": true, "templates": true}',
 ARRAY['basic', 'none'], ARRAY['send_email'], true),

('webhook_http', 'HTTP Webhook', 'Send HTTP requests to external endpoints', 'notification',
 '{"custom_headers": true, "retry_logic": true, "authentication": true}',
 ARRAY['none', 'api_key', 'basic', 'custom'], ARRAY['post_data', 'get_data'], true),

('github', 'GitHub', 'Integration with GitHub repositories and issues', 'data_source',
 '{"repositories": true, "issues": true, "pull_requests": true, "webhooks": true}',
 ARRAY['oauth2', 'api_key'], ARRAY['get_repos', 'create_issue', 'get_issues', 'webhook_handler'], true),

('jira', 'Jira', 'Integration with Atlassian Jira for issue tracking', 'data_source',
 '{"projects": true, "issues": true, "workflows": true, "custom_fields": true}',
 ARRAY['oauth2', 'api_key', 'basic'], ARRAY['get_projects', 'create_issue', 'update_issue', 'get_issues'], true),

('prometheus', 'Prometheus', 'Metrics collection and monitoring integration', 'monitoring',
 '{"metrics_collection": true, "alerting": true, "queries": true}',
 ARRAY['none', 'basic'], ARRAY['collect_metrics', 'query_metrics'], true);

INSERT INTO webhook_configurations (name, description, webhook_url_path, is_enabled, content_type, created_by) VALUES
('github_webhook', 'GitHub repository webhook endpoint', '/webhooks/github', true, 'application/json', NULL),
('gitlab_webhook', 'GitLab repository webhook endpoint', '/webhooks/gitlab', true, 'application/json', NULL),
('jira_webhook', 'Jira issue webhook endpoint', '/webhooks/jira', true, 'application/json', NULL),
('generic_webhook', 'Generic webhook endpoint for custom integrations', '/webhooks/generic', true, 'application/json', NULL);

-- ============================================================================
-- VIEWS
-- ============================================================================

CREATE VIEW v_active_integrations AS
SELECT
    i.*,
    ip.display_name as provider_display_name,
    ip.provider_type,
    ip.capabilities as provider_capabilities
FROM integrations i
JOIN integration_providers ip ON i.provider_id = ip.id
WHERE i.is_enabled = true AND i.status = 'active';

CREATE VIEW v_integration_health_summary AS
SELECT
    i.id as integration_id,
    i.name as integration_name,
    ip.display_name as provider_name,
    i.status,
    i.health_status,
    i.last_health_check,
    i.consecutive_failures,
    i.total_requests,
    i.successful_requests,
    i.failed_requests,
    CASE
        WHEN i.health_status = 'healthy' AND i.consecutive_failures = 0 THEN 'healthy'
        WHEN i.health_status = 'degraded' OR i.consecutive_failures BETWEEN 1 AND 3 THEN 'warning'
        WHEN i.health_status = 'unhealthy' OR i.consecutive_failures > 3 THEN 'critical'
        ELSE 'unknown'
    END as overall_status
FROM integrations i
JOIN integration_providers ip ON i.provider_id = ip.id
WHERE i.is_enabled = true;

CREATE VIEW v_webhook_request_summary AS
SELECT
    wc.id as webhook_config_id,
    wc.name as webhook_name,
    wc.webhook_url_path,
    COUNT(wr.id) as total_requests,
    COUNT(CASE WHEN wr.status = 'completed' THEN 1 END) as successful_requests,
    COUNT(CASE WHEN wr.status = 'failed' THEN 1 END) as failed_requests,
    COUNT(CASE WHEN wr.rate_limited = true THEN 1 END) as rate_limited_requests,
    MAX(wr.received_at) as last_request_at,
    AVG(wr.processing_duration_ms) as avg_processing_time_ms
FROM webhook_configurations wc
LEFT JOIN webhook_requests wr ON wc.id = wr.webhook_config_id
WHERE wc.is_enabled = true
GROUP BY wc.id, wc.name, wc.webhook_url_path;

CREATE VIEW v_integration_operation_stats AS
SELECT
    i.id as integration_id,
    i.name as integration_name,
    ip.display_name as provider_name,
    COUNT(io.id) as total_operations,
    COUNT(CASE WHEN io.status = 'completed' THEN 1 END) as successful_operations,
    COUNT(CASE WHEN io.status = 'failed' THEN 1 END) as failed_operations,
    COUNT(CASE WHEN io.status = 'running' THEN 1 END) as running_operations,
    AVG(io.response_time_ms) as avg_response_time_ms,
    MAX(io.started_at) as last_operation_at
FROM integrations i
JOIN integration_providers ip ON i.provider_id = ip.id
LEFT JOIN integration_operations io ON i.id = io.integration_id
WHERE i.is_enabled = true
GROUP BY i.id, i.name, ip.display_name;

-- ============================================================================
-- COMMENTS
-- ============================================================================

COMMENT ON TABLE integration_providers IS 'Available integration provider types and their capabilities';
COMMENT ON TABLE integrations IS 'Configured integration instances with external systems';
COMMENT ON TABLE integration_operations IS 'Individual operations performed through integrations';
COMMENT ON TABLE webhook_configurations IS 'Incoming webhook endpoint configurations';
COMMENT ON TABLE webhook_requests IS 'Log of all incoming webhook requests and their processing';
COMMENT ON TABLE integration_events IS 'Event log for integration and webhook activities';
COMMENT ON TABLE integration_schedules IS 'Scheduled integration operation configurations';
