-- ORCHER Task Orchestrator - Notifications and Alerts System
-- Migration V006: Notification categories, channels, user preferences, alerts, and templates

-- ============================================================================
-- NOTIFICATION CATEGORIES TABLE: Predefined notification types
-- ============================================================================

CREATE TABLE notification_categories (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    name VARCHAR(50) UNIQUE NOT NULL,
    display_name VARCHAR(100) NOT NULL,
    description TEXT,

    -- Category properties
    default_enabled BOOLEAN NOT NULL DEFAULT true,
    is_system_category BOOLEAN NOT NULL DEFAULT false,
    icon VARCHAR(50),
    color VARCHAR(7),

    -- Audit fields
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT notification_categories_name_format CHECK (name ~* '^[a-z][a-z0-9_]*$')
);

-- ============================================================================
-- NOTIFICATION CHANNELS TABLE: Delivery methods
-- ============================================================================

CREATE TABLE notification_channels (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    name VARCHAR(50) UNIQUE NOT NULL,
    display_name VARCHAR(100) NOT NULL,
    description TEXT,

    -- Channel properties
    is_enabled BOOLEAN NOT NULL DEFAULT true,
    is_realtime BOOLEAN NOT NULL DEFAULT false,
    requires_configuration BOOLEAN NOT NULL DEFAULT false,

    -- Channel settings schema
    config_schema JSONB DEFAULT '{}',

    -- Audit fields
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT notification_channels_name_format CHECK (name ~* '^[a-z][a-z0-9_]*$')
);

-- ============================================================================
-- USER NOTIFICATION PREFERENCES TABLE
-- ============================================================================

CREATE TABLE user_notification_preferences (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,

    -- Global notification settings
    email_notifications_enabled BOOLEAN NOT NULL DEFAULT true,
    push_notifications_enabled BOOLEAN NOT NULL DEFAULT true,
    websocket_notifications_enabled BOOLEAN NOT NULL DEFAULT true,

    -- Quiet hours settings
    quiet_hours_enabled BOOLEAN NOT NULL DEFAULT false,
    quiet_hours_start TIME,
    quiet_hours_end TIME,
    quiet_hours_timezone VARCHAR(50) DEFAULT 'UTC',
    quiet_hours_days INTEGER[] DEFAULT ARRAY[1,2,3,4,5,6,7],

    -- Filter settings
    include_own_actions BOOLEAN NOT NULL DEFAULT false,
    min_alert_level VARCHAR(20) DEFAULT 'info',

    -- Digest settings
    daily_digest_enabled BOOLEAN NOT NULL DEFAULT false,
    weekly_digest_enabled BOOLEAN NOT NULL DEFAULT false,
    digest_time TIME DEFAULT '09:00:00',

    -- Preferences data
    preferences JSONB DEFAULT '{}',

    -- Audit fields
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    UNIQUE(user_id),
    CONSTRAINT user_notification_preferences_alert_level_check CHECK (
        min_alert_level IN ('info', 'warning', 'error', 'critical')
    ),
    CONSTRAINT user_notification_preferences_days_check CHECK (
        array_length(quiet_hours_days, 1) <= 7 AND
        quiet_hours_days <@ ARRAY[1,2,3,4,5,6,7]
    )
);

-- ============================================================================
-- USER NOTIFICATION CATEGORY PREFERENCES TABLE
-- ============================================================================

CREATE TABLE user_notification_category_preferences (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    category_id UUID NOT NULL REFERENCES notification_categories(id) ON DELETE CASCADE,

    -- Channel preferences for this category
    email_enabled BOOLEAN NOT NULL DEFAULT true,
    push_enabled BOOLEAN NOT NULL DEFAULT true,
    websocket_enabled BOOLEAN NOT NULL DEFAULT true,

    -- Category-specific settings
    priority_threshold VARCHAR(20) DEFAULT 'normal',

    -- Audit fields
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    UNIQUE(user_id, category_id),
    CONSTRAINT user_notification_category_priority_check CHECK (
        priority_threshold IN ('low', 'normal', 'high', 'urgent')
    )
);

-- ============================================================================
-- NOTIFICATIONS TABLE: All system notifications
-- ============================================================================

CREATE TABLE notifications (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),

    -- Notification identification
    notification_type VARCHAR(100) NOT NULL,
    category_id UUID REFERENCES notification_categories(id),

    -- Targeting
    recipient_user_id UUID REFERENCES users(id) ON DELETE CASCADE,
    recipient_role VARCHAR(50),
    recipient_group VARCHAR(100),

    -- Content
    title VARCHAR(255) NOT NULL,
    message TEXT NOT NULL,

    -- Priority and urgency
    priority VARCHAR(20) NOT NULL DEFAULT 'normal',
    urgency VARCHAR(20) NOT NULL DEFAULT 'normal',

    -- Related entities
    related_task_id UUID,
    related_workflow_id UUID REFERENCES workflow_executions(id),
    related_user_id UUID REFERENCES users(id),

    -- Notification state
    status VARCHAR(50) NOT NULL DEFAULT 'pending',
    read_at TIMESTAMPTZ,
    archived_at TIMESTAMPTZ,

    -- Delivery tracking
    channels_sent TEXT[] DEFAULT '{}',
    channels_delivered TEXT[] DEFAULT '{}',
    channels_failed TEXT[] DEFAULT '{}',

    -- Metadata
    metadata JSONB DEFAULT '{}',
    actions JSONB DEFAULT '{}',

    -- Scheduling
    scheduled_at TIMESTAMPTZ,
    expires_at TIMESTAMPTZ,

    -- Audit fields
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_by UUID REFERENCES users(id),

    CONSTRAINT notifications_priority_check CHECK (
        priority IN ('low', 'normal', 'high', 'urgent')
    ),
    CONSTRAINT notifications_urgency_check CHECK (
        urgency IN ('low', 'normal', 'high', 'critical')
    ),
    CONSTRAINT notifications_status_check CHECK (
        status IN ('pending', 'scheduled', 'sending', 'sent', 'delivered', 'failed', 'cancelled', 'expired')
    )
);

-- ============================================================================
-- NOTIFICATION DELIVERIES TABLE: Delivery attempts per channel
-- ============================================================================

CREATE TABLE notification_deliveries (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    notification_id UUID NOT NULL REFERENCES notifications(id) ON DELETE CASCADE,

    -- Delivery details
    channel_name VARCHAR(50) NOT NULL,
    channel_config JSONB DEFAULT '{}',

    -- Attempt tracking
    attempt_number INTEGER NOT NULL DEFAULT 1,
    status VARCHAR(50) NOT NULL DEFAULT 'pending',

    -- Timing
    scheduled_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    attempted_at TIMESTAMPTZ,
    delivered_at TIMESTAMPTZ,

    -- Response tracking
    external_id VARCHAR(255),
    response_code INTEGER,
    response_message TEXT,

    -- Error handling
    error_message TEXT,
    retry_after TIMESTAMPTZ,

    -- Metadata
    delivery_metadata JSONB DEFAULT '{}',

    CONSTRAINT notification_deliveries_status_check CHECK (
        status IN ('pending', 'sending', 'sent', 'delivered', 'bounced', 'failed', 'cancelled')
    ),
    CONSTRAINT notification_deliveries_attempt_check CHECK (attempt_number > 0)
);

-- ============================================================================
-- SYSTEM ALERTS TABLE
-- ============================================================================

CREATE TABLE system_alerts (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),

    -- Alert identification
    alert_code VARCHAR(100) NOT NULL,
    alert_type VARCHAR(50) NOT NULL,
    severity VARCHAR(20) NOT NULL,

    -- Alert content
    title VARCHAR(255) NOT NULL,
    description TEXT NOT NULL,

    -- Source information
    source_component VARCHAR(100),
    source_host VARCHAR(255),
    source_process VARCHAR(100),

    -- Alert state
    status VARCHAR(50) NOT NULL DEFAULT 'active',
    acknowledged_at TIMESTAMPTZ,
    acknowledged_by UUID REFERENCES users(id),
    resolved_at TIMESTAMPTZ,
    resolved_by UUID REFERENCES users(id),

    -- Alert data
    alert_data JSONB DEFAULT '{}',
    thresholds JSONB DEFAULT '{}',

    -- Correlation
    correlation_id UUID,
    related_alerts UUID[],

    -- Timing
    first_occurrence TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_occurrence TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    occurrence_count INTEGER NOT NULL DEFAULT 1,

    -- Auto-resolution
    auto_resolve_after INTERVAL,
    escalation_policy VARCHAR(100),

    CONSTRAINT system_alerts_severity_check CHECK (
        severity IN ('info', 'warning', 'error', 'critical')
    ),
    CONSTRAINT system_alerts_status_check CHECK (
        status IN ('active', 'acknowledged', 'resolved', 'suppressed', 'expired')
    ),
    CONSTRAINT system_alerts_type_check CHECK (
        alert_type IN ('system', 'performance', 'security', 'resource', 'task', 'workflow', 'custom')
    )
);

-- ============================================================================
-- ALERT RULES TABLE: When alerts should be triggered
-- ============================================================================

CREATE TABLE alert_rules (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),

    -- Rule identification
    name VARCHAR(100) UNIQUE NOT NULL,
    description TEXT,

    -- Rule definition
    rule_type VARCHAR(50) NOT NULL,
    condition_expression TEXT NOT NULL,
    threshold_config JSONB DEFAULT '{}',

    -- Rule state
    is_enabled BOOLEAN NOT NULL DEFAULT true,

    -- Evaluation settings
    evaluation_interval INTERVAL NOT NULL DEFAULT INTERVAL '1 minute',
    evaluation_window INTERVAL NOT NULL DEFAULT INTERVAL '5 minutes',
    min_occurrences INTEGER NOT NULL DEFAULT 1,

    -- Alert settings
    alert_severity VARCHAR(20) NOT NULL DEFAULT 'warning',
    alert_title_template VARCHAR(255),
    alert_description_template TEXT,

    -- Notification settings
    notification_channels TEXT[] DEFAULT '{}',
    notification_users UUID[],
    notification_roles TEXT[],

    -- Suppression settings
    suppression_interval INTERVAL,
    max_alerts_per_hour INTEGER DEFAULT 10,

    -- Metadata
    tags TEXT[],
    metadata JSONB DEFAULT '{}',

    -- Audit fields
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_by UUID REFERENCES users(id),

    CONSTRAINT alert_rules_type_check CHECK (
        rule_type IN ('metric', 'log', 'event', 'threshold', 'pattern', 'custom')
    ),
    CONSTRAINT alert_rules_severity_check CHECK (
        alert_severity IN ('info', 'warning', 'error', 'critical')
    ),
    CONSTRAINT alert_rules_min_occurrences_check CHECK (min_occurrences > 0)
);

-- ============================================================================
-- ALERT RULE EVALUATIONS TABLE: Evaluation history
-- ============================================================================

CREATE TABLE alert_rule_evaluations (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    rule_id UUID NOT NULL REFERENCES alert_rules(id) ON DELETE CASCADE,

    -- Evaluation details
    evaluation_time TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    evaluation_duration_ms INTEGER,

    -- Evaluation results
    condition_met BOOLEAN NOT NULL,
    threshold_breached BOOLEAN NOT NULL DEFAULT false,

    -- Values and context
    evaluated_value DECIMAL(20,6),
    threshold_value DECIMAL(20,6),
    evaluation_context JSONB DEFAULT '{}',

    -- Alert generation
    alert_generated BOOLEAN NOT NULL DEFAULT false,
    alert_id UUID REFERENCES system_alerts(id),

    -- Error tracking
    evaluation_error TEXT,

    CONSTRAINT alert_rule_evaluations_duration_check CHECK (evaluation_duration_ms >= 0)
);

-- ============================================================================
-- NOTIFICATION TEMPLATES TABLE
-- ============================================================================

CREATE TABLE notification_templates (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),

    -- Template identification
    name VARCHAR(100) UNIQUE NOT NULL,
    category VARCHAR(50) NOT NULL,
    notification_type VARCHAR(100) NOT NULL,

    -- Template content
    title_template VARCHAR(255) NOT NULL,
    body_template TEXT NOT NULL,

    -- Channel-specific templates
    email_subject_template VARCHAR(255),
    email_html_template TEXT,
    sms_template TEXT,
    push_title_template VARCHAR(255),
    push_body_template TEXT,

    -- Template properties
    is_system_template BOOLEAN NOT NULL DEFAULT false,
    is_enabled BOOLEAN NOT NULL DEFAULT true,

    -- Template variables
    supported_variables JSONB DEFAULT '{}',
    required_variables TEXT[],

    -- Localization
    locale VARCHAR(10) DEFAULT 'en',

    -- Metadata
    description TEXT,
    tags TEXT[],

    -- Audit fields
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_by UUID REFERENCES users(id),

    CONSTRAINT notification_templates_category_check CHECK (
        category IN ('system', 'task', 'workflow', 'user', 'security', 'custom')
    )
);

-- ============================================================================
-- NOTIFICATION DIGESTS TABLE: Scheduled digest notifications
-- ============================================================================

CREATE TABLE notification_digests (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),

    -- Digest identification
    digest_type VARCHAR(50) NOT NULL,
    recipient_user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,

    -- Digest period
    period_start TIMESTAMPTZ NOT NULL,
    period_end TIMESTAMPTZ NOT NULL,

    -- Digest content
    summary JSONB NOT NULL DEFAULT '{}',
    notification_count INTEGER NOT NULL DEFAULT 0,
    alert_count INTEGER NOT NULL DEFAULT 0,

    -- Digest state
    status VARCHAR(50) NOT NULL DEFAULT 'pending',
    generated_at TIMESTAMPTZ,
    sent_at TIMESTAMPTZ,

    -- Delivery
    delivery_channel VARCHAR(50),
    delivery_status VARCHAR(50),
    delivery_error TEXT,

    CONSTRAINT notification_digests_type_check CHECK (
        digest_type IN ('daily', 'weekly', 'monthly')
    ),
    CONSTRAINT notification_digests_status_check CHECK (
        status IN ('pending', 'generating', 'ready', 'sending', 'sent', 'failed')
    ),
    CONSTRAINT notification_digests_period_check CHECK (period_end > period_start)
);

-- ============================================================================
-- TRIGGERS
-- ============================================================================

CREATE TRIGGER update_notification_categories_updated_at BEFORE UPDATE ON notification_categories
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_notification_channels_updated_at BEFORE UPDATE ON notification_channels
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_user_notification_preferences_updated_at BEFORE UPDATE ON user_notification_preferences
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_user_notification_category_preferences_updated_at BEFORE UPDATE ON user_notification_category_preferences
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_alert_rules_updated_at BEFORE UPDATE ON alert_rules
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_notification_templates_updated_at BEFORE UPDATE ON notification_templates
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- ============================================================================
-- INDEXES
-- ============================================================================

-- Notifications indexes
CREATE INDEX idx_notifications_recipient ON notifications(recipient_user_id, status);
CREATE INDEX idx_notifications_type ON notifications(notification_type, created_at);
CREATE INDEX idx_notifications_category ON notifications(category_id, created_at);
CREATE INDEX idx_notifications_status ON notifications(status, created_at);
CREATE INDEX idx_notifications_priority ON notifications(priority, created_at);
CREATE INDEX idx_notifications_scheduled ON notifications(scheduled_at) WHERE scheduled_at IS NOT NULL;
CREATE INDEX idx_notifications_expires ON notifications(expires_at) WHERE expires_at IS NOT NULL;
CREATE INDEX idx_notifications_unread ON notifications(recipient_user_id, read_at) WHERE read_at IS NULL;
CREATE INDEX idx_notifications_related_task ON notifications(related_task_id) WHERE related_task_id IS NOT NULL;
CREATE INDEX idx_notifications_related_workflow ON notifications(related_workflow_id) WHERE related_workflow_id IS NOT NULL;

-- Notification deliveries indexes
CREATE INDEX idx_notification_deliveries_notification ON notification_deliveries(notification_id);
CREATE INDEX idx_notification_deliveries_channel ON notification_deliveries(channel_name, status);
CREATE INDEX idx_notification_deliveries_status ON notification_deliveries(status, scheduled_at);
CREATE INDEX idx_notification_deliveries_retry ON notification_deliveries(retry_after) WHERE retry_after IS NOT NULL;

-- System alerts indexes
CREATE INDEX idx_system_alerts_code ON system_alerts(alert_code, status);
CREATE INDEX idx_system_alerts_type ON system_alerts(alert_type, severity);
CREATE INDEX idx_system_alerts_severity ON system_alerts(severity, status);
CREATE INDEX idx_system_alerts_status ON system_alerts(status, first_occurrence);
CREATE INDEX idx_system_alerts_source ON system_alerts(source_component, source_host);
CREATE INDEX idx_system_alerts_correlation ON system_alerts(correlation_id) WHERE correlation_id IS NOT NULL;
CREATE INDEX idx_system_alerts_occurrence ON system_alerts(last_occurrence);

-- Alert rules indexes
CREATE INDEX idx_alert_rules_enabled ON alert_rules(is_enabled, evaluation_interval);
CREATE INDEX idx_alert_rules_type ON alert_rules(rule_type, is_enabled);

-- Alert rule evaluations indexes
CREATE INDEX idx_alert_rule_evaluations_rule ON alert_rule_evaluations(rule_id, evaluation_time);
CREATE INDEX idx_alert_rule_evaluations_alert ON alert_rule_evaluations(alert_id) WHERE alert_id IS NOT NULL;

-- User preferences indexes
CREATE INDEX idx_user_notification_preferences_user ON user_notification_preferences(user_id);
CREATE INDEX idx_user_notification_category_preferences_user ON user_notification_category_preferences(user_id);
CREATE INDEX idx_user_notification_category_preferences_category ON user_notification_category_preferences(category_id);

-- Notification templates indexes
CREATE INDEX idx_notification_templates_type ON notification_templates(notification_type, is_enabled);
CREATE INDEX idx_notification_templates_category ON notification_templates(category, is_enabled);

-- Notification digests indexes
CREATE INDEX idx_notification_digests_user ON notification_digests(recipient_user_id, digest_type);
CREATE INDEX idx_notification_digests_status ON notification_digests(status, period_end);

-- Composite indexes for common query patterns
CREATE INDEX idx_notifications_user_status_created ON notifications(recipient_user_id, status, created_at DESC);
CREATE INDEX idx_system_alerts_active ON system_alerts(status, severity, last_occurrence DESC) WHERE status = 'active';
CREATE INDEX idx_notification_deliveries_pending ON notification_deliveries(status, scheduled_at) WHERE status = 'pending';

-- ============================================================================
-- VIEWS
-- ============================================================================

CREATE VIEW v_user_notification_settings AS
SELECT
    u.id as user_id,
    u.username,
    u.email,
    unp.email_notifications_enabled,
    unp.push_notifications_enabled,
    unp.websocket_notifications_enabled,
    unp.quiet_hours_enabled,
    unp.quiet_hours_start,
    unp.quiet_hours_end,
    unp.quiet_hours_timezone,
    unp.min_alert_level
FROM users u
LEFT JOIN user_notification_preferences unp ON u.id = unp.user_id
WHERE u.is_active = true;

CREATE VIEW v_active_alerts AS
SELECT
    sa.*,
    ar.name as rule_name,
    ar.description as rule_description
FROM system_alerts sa
LEFT JOIN alert_rules ar ON sa.alert_code = ar.name
WHERE sa.status = 'active'
ORDER BY sa.severity DESC, sa.last_occurrence DESC;

CREATE VIEW v_pending_notifications AS
SELECT
    n.*,
    u.username as recipient_username,
    u.email as recipient_email,
    nc.display_name as category_name
FROM notifications n
JOIN users u ON n.recipient_user_id = u.id
LEFT JOIN notification_categories nc ON n.category_id = nc.id
WHERE n.status IN ('pending', 'scheduled')
  AND (n.scheduled_at IS NULL OR n.scheduled_at <= NOW())
  AND (n.expires_at IS NULL OR n.expires_at > NOW())
ORDER BY n.priority DESC, n.created_at ASC;

-- ============================================================================
-- SEED DATA
-- ============================================================================

INSERT INTO notification_categories (name, display_name, description, default_enabled, is_system_category, icon, color) VALUES
('tasks', 'Tasks', 'Task creation, completion, and failure notifications', true, true, 'task', '#3498db'),
('workflows', 'Workflows', 'Workflow execution and status change notifications', true, true, 'workflow', '#2ecc71'),
('system', 'System', 'System alerts and maintenance notifications', true, true, 'system', '#e74c3c'),
('security', 'Security', 'Security alerts and authentication events', true, true, 'security', '#e67e22'),
('user', 'User', 'User account and profile notifications', true, true, 'user', '#9b59b6'),
('admin', 'Administration', 'Administrative notifications and alerts', false, true, 'admin', '#34495e'),
('monitoring', 'Monitoring', 'System monitoring and metrics alerts', true, true, 'monitoring', '#f39c12');

INSERT INTO notification_channels (name, display_name, description, is_enabled, is_realtime, requires_configuration) VALUES
('websocket', 'WebSocket', 'Real-time browser notifications', true, true, false),
('email', 'Email', 'Email notifications', true, false, true),
('sms', 'SMS', 'Text message notifications', false, false, true),
('push', 'Push Notifications', 'Mobile push notifications', false, false, true),
('slack', 'Slack', 'Slack channel notifications', false, false, true),
('webhook', 'Webhook', 'HTTP webhook notifications', false, false, true);

INSERT INTO notification_templates (
    name, category, notification_type, title_template, body_template,
    email_subject_template, is_system_template, supported_variables, required_variables
) VALUES
('task_created', 'task', 'TaskCreated', 'Task Created: {{task_name}}',
 'A new task "{{task_name}}" has been created.', 'ORCHER: Task Created - {{task_name}}',
 true, '{"task_name": "string", "task_id": "string", "created_by": "string"}', ARRAY['task_name']),

('task_completed', 'task', 'TaskCompleted', 'Task Completed: {{task_name}}',
 'Task "{{task_name}}" has been completed successfully.', 'ORCHER: Task Completed - {{task_name}}',
 true, '{"task_name": "string", "task_id": "string", "duration": "string"}', ARRAY['task_name']),

('task_failed', 'task', 'TaskFailed', 'Task Failed: {{task_name}}',
 'Task "{{task_name}}" has failed. Error: {{error_message}}', 'ORCHER: Task Failed - {{task_name}}',
 true, '{"task_name": "string", "task_id": "string", "error_message": "string"}', ARRAY['task_name', 'error_message']),

('workflow_started', 'workflow', 'WorkflowStarted', 'Workflow Started: {{workflow_name}}',
 'Workflow "{{workflow_name}}" has been started.', 'ORCHER: Workflow Started - {{workflow_name}}',
 true, '{"workflow_name": "string", "workflow_id": "string"}', ARRAY['workflow_name']),

('system_alert', 'system', 'SystemAlert', 'System Alert: {{alert_title}}',
 'System alert: {{alert_description}}', 'ORCHER: System Alert - {{alert_title}}',
 true, '{"alert_title": "string", "alert_description": "string", "severity": "string"}', ARRAY['alert_title']);

INSERT INTO alert_rules (
    name, description, rule_type, condition_expression, alert_severity,
    alert_title_template, alert_description_template, evaluation_interval,
    notification_channels, created_by
) VALUES
('high_task_failure_rate', 'Alert when task failure rate exceeds 10% in 5 minutes',
 'metric', 'task_failure_rate > 0.1', 'warning',
 'High Task Failure Rate', 'Task failure rate is {{value}}% over the last 5 minutes',
 INTERVAL '1 minute', ARRAY['websocket', 'email'], NULL),

('system_memory_usage', 'Alert when system memory usage exceeds 90%',
 'metric', 'memory_usage_percent > 90', 'critical',
 'High Memory Usage', 'System memory usage is {{value}}%',
 INTERVAL '30 seconds', ARRAY['websocket', 'email'], NULL),

('worker_offline', 'Alert when a worker goes offline',
 'event', 'worker_status = "offline"', 'error',
 'Worker Offline', 'Worker {{worker_id}} has gone offline',
 INTERVAL '1 minute', ARRAY['websocket'], NULL);

-- ============================================================================
-- COMMENTS
-- ============================================================================

COMMENT ON TABLE notification_categories IS 'Predefined notification categories for organization';
COMMENT ON TABLE notification_channels IS 'Available notification delivery channels';
COMMENT ON TABLE user_notification_preferences IS 'User-specific notification preferences and settings';
COMMENT ON TABLE user_notification_category_preferences IS 'User preferences per notification category';
COMMENT ON TABLE notifications IS 'All system notifications with delivery tracking';
COMMENT ON TABLE notification_deliveries IS 'Individual delivery attempts per channel';
COMMENT ON TABLE system_alerts IS 'System alerts with acknowledgment and resolution tracking';
COMMENT ON TABLE alert_rules IS 'Configurable alert rules and conditions';
COMMENT ON TABLE alert_rule_evaluations IS 'History of alert rule evaluations';
COMMENT ON TABLE notification_templates IS 'Reusable notification message templates';
COMMENT ON TABLE notification_digests IS 'Scheduled digest notifications for users';
