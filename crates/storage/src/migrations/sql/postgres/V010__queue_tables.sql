-- ORCHER Task Orchestrator - PostgreSQL-Backed Task Queue
-- Migration V010: Queue configuration, messages, statistics, consumers, and helper functions

-- ============================================================================
-- QUEUE CONFIGURATION TABLE
-- ============================================================================

CREATE TABLE queue_config (
    queue_name VARCHAR(255) PRIMARY KEY,
    max_size INTEGER NOT NULL DEFAULT 0,
    message_ttl_seconds INTEGER,
    dead_letter_enabled BOOLEAN NOT NULL DEFAULT FALSE,
    dead_letter_queue VARCHAR(255),
    durable BOOLEAN NOT NULL DEFAULT TRUE,
    auto_delete BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    CONSTRAINT fk_dead_letter_queue FOREIGN KEY (dead_letter_queue)
        REFERENCES queue_config(queue_name) ON DELETE SET NULL
);

COMMENT ON TABLE queue_config IS 'Configuration for queue instances';
COMMENT ON COLUMN queue_config.max_size IS 'Maximum queue size, 0 = unlimited';
COMMENT ON COLUMN queue_config.message_ttl_seconds IS 'Default TTL for messages in seconds';
COMMENT ON COLUMN queue_config.dead_letter_enabled IS 'Enable dead letter queue for failed messages';
COMMENT ON COLUMN queue_config.dead_letter_queue IS 'Target queue for failed messages';

-- ============================================================================
-- QUEUE MESSAGES TABLE
-- ============================================================================

CREATE TABLE queue_messages (
    message_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    queue_name VARCHAR(255) NOT NULL,
    task_data JSONB NOT NULL,
    priority SMALLINT NOT NULL DEFAULT 1,
    retry_count INTEGER NOT NULL DEFAULT 0,
    max_retries INTEGER NOT NULL DEFAULT 3,
    status VARCHAR(50) NOT NULL DEFAULT 'pending',

    -- Metadata
    headers JSONB,

    -- Timing
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    scheduled_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    expires_at TIMESTAMP WITH TIME ZONE,
    consumed_at TIMESTAMP WITH TIME ZONE,
    completed_at TIMESTAMP WITH TIME ZONE,

    -- Consumer tracking
    consumer_id VARCHAR(255),
    last_error TEXT,

    CONSTRAINT fk_queue_config FOREIGN KEY (queue_name)
        REFERENCES queue_config(queue_name) ON DELETE CASCADE,
    CONSTRAINT chk_status CHECK (status IN ('pending', 'processing', 'completed', 'failed', 'dead_letter')),
    CONSTRAINT chk_priority CHECK (priority BETWEEN 0 AND 3)
);

COMMENT ON TABLE queue_messages IS 'Queue messages for task dispatch';
COMMENT ON COLUMN queue_messages.status IS 'Message status: pending, processing, completed, failed, dead_letter';
COMMENT ON COLUMN queue_messages.priority IS 'Message priority: 0=low, 1=normal, 2=high, 3=critical';
COMMENT ON COLUMN queue_messages.task_data IS 'Serialized task submission data';
COMMENT ON COLUMN queue_messages.retry_count IS 'Current retry attempt number';
COMMENT ON COLUMN queue_messages.consumer_id IS 'ID of consumer currently processing this message';

-- ============================================================================
-- QUEUE STATISTICS TABLE
-- ============================================================================

CREATE TABLE queue_stats (
    queue_name VARCHAR(255) PRIMARY KEY,
    total_messages BIGINT NOT NULL DEFAULT 0,
    in_flight BIGINT NOT NULL DEFAULT 0,
    consumers INTEGER NOT NULL DEFAULT 0,
    published_total BIGINT NOT NULL DEFAULT 0,
    consumed_total BIGINT NOT NULL DEFAULT 0,
    failed_total BIGINT NOT NULL DEFAULT 0,
    avg_age_seconds NUMERIC(10, 2) NOT NULL DEFAULT 0,
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    CONSTRAINT fk_queue_stats_config FOREIGN KEY (queue_name)
        REFERENCES queue_config(queue_name) ON DELETE CASCADE
);

COMMENT ON TABLE queue_stats IS 'Real-time statistics for each queue';
COMMENT ON COLUMN queue_stats.in_flight IS 'Messages currently being processed';
COMMENT ON COLUMN queue_stats.consumers IS 'Number of active consumers';

-- ============================================================================
-- CONSUMER REGISTRY TABLE
-- ============================================================================

CREATE TABLE queue_consumers (
    consumer_id VARCHAR(255) PRIMARY KEY,
    consumer_group VARCHAR(255) NOT NULL,
    last_heartbeat TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    capabilities JSONB NOT NULL DEFAULT '[]',
    subscribed_queues JSONB NOT NULL DEFAULT '[]',
    capacity INTEGER NOT NULL DEFAULT 10,
    current_load INTEGER NOT NULL DEFAULT 0,
    status VARCHAR(50) NOT NULL DEFAULT 'active',
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    CONSTRAINT chk_consumer_status CHECK (status IN ('active', 'idle', 'stopped', 'error'))
);

COMMENT ON TABLE queue_consumers IS 'Registry of active queue consumers (workers)';
COMMENT ON COLUMN queue_consumers.consumer_group IS 'Consumer group for load balancing';
COMMENT ON COLUMN queue_consumers.capabilities IS 'Worker capabilities (JSON array)';
COMMENT ON COLUMN queue_consumers.subscribed_queues IS 'Queues this consumer is subscribed to';
COMMENT ON COLUMN queue_consumers.capacity IS 'Maximum concurrent tasks';
COMMENT ON COLUMN queue_consumers.current_load IS 'Current number of tasks being processed';

-- ============================================================================
-- TRIGGERS
-- ============================================================================

CREATE OR REPLACE FUNCTION update_queue_config_timestamp()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_queue_config_updated
    BEFORE UPDATE ON queue_config
    FOR EACH ROW
    EXECUTE FUNCTION update_queue_config_timestamp();

-- ============================================================================
-- INDEXES
-- ============================================================================

-- Queue messages indexes
CREATE INDEX idx_queue_messages_queue_status
    ON queue_messages(queue_name, status);

CREATE INDEX idx_queue_messages_priority_scheduled
    ON queue_messages(priority DESC, scheduled_at ASC)
    WHERE status = 'pending';

CREATE INDEX idx_queue_messages_expires_at
    ON queue_messages(expires_at)
    WHERE expires_at IS NOT NULL AND status = 'pending';

CREATE INDEX idx_queue_messages_consumer
    ON queue_messages(consumer_id)
    WHERE consumer_id IS NOT NULL;

CREATE INDEX idx_queue_messages_created_at
    ON queue_messages(created_at DESC);

-- Consumer indexes
CREATE INDEX idx_queue_consumers_group
    ON queue_consumers(consumer_group);

CREATE INDEX idx_queue_consumers_heartbeat
    ON queue_consumers(last_heartbeat DESC);

-- Stats indexes
CREATE INDEX idx_queue_stats_updated
    ON queue_stats(updated_at DESC);

-- ============================================================================
-- FUNCTIONS: Queue operations
-- ============================================================================

CREATE OR REPLACE FUNCTION queue_publish(
    p_queue_name VARCHAR(255),
    p_task_data JSONB,
    p_priority SMALLINT DEFAULT 1,
    p_max_retries INTEGER DEFAULT 3,
    p_ttl_seconds INTEGER DEFAULT NULL,
    p_delay_seconds INTEGER DEFAULT 0,
    p_headers JSONB DEFAULT NULL
) RETURNS UUID AS $$
DECLARE
    v_message_id UUID;
    v_scheduled_at TIMESTAMP WITH TIME ZONE;
    v_expires_at TIMESTAMP WITH TIME ZONE;
BEGIN
    v_scheduled_at := NOW() + (p_delay_seconds || ' seconds')::INTERVAL;
    IF p_ttl_seconds IS NOT NULL THEN
        v_expires_at := NOW() + (p_ttl_seconds || ' seconds')::INTERVAL;
    END IF;

    INSERT INTO queue_messages (
        queue_name,
        task_data,
        priority,
        max_retries,
        scheduled_at,
        expires_at,
        headers
    ) VALUES (
        p_queue_name,
        p_task_data,
        p_priority,
        p_max_retries,
        v_scheduled_at,
        v_expires_at,
        p_headers
    ) RETURNING message_id INTO v_message_id;

    UPDATE queue_stats
    SET
        total_messages = total_messages + 1,
        published_total = published_total + 1,
        updated_at = NOW()
    WHERE queue_name = p_queue_name;

    RETURN v_message_id;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION queue_publish IS 'Publish a message to a queue';

CREATE OR REPLACE FUNCTION queue_consume(
    p_queue_name VARCHAR(255),
    p_consumer_id VARCHAR(255),
    p_timeout_seconds INTEGER DEFAULT 30
) RETURNS TABLE (
    message_id UUID,
    task_data JSONB,
    priority SMALLINT,
    retry_count INTEGER,
    max_retries INTEGER,
    headers JSONB,
    created_at TIMESTAMP WITH TIME ZONE
) AS $$
DECLARE
    v_message_id UUID;
BEGIN
    SELECT m.message_id INTO v_message_id
    FROM queue_messages m
    WHERE m.queue_name = p_queue_name
      AND m.status = 'pending'
      AND m.scheduled_at <= NOW()
      AND (m.expires_at IS NULL OR m.expires_at > NOW())
    ORDER BY m.priority DESC, m.scheduled_at ASC
    LIMIT 1
    FOR UPDATE SKIP LOCKED;

    IF v_message_id IS NULL THEN
        RETURN;
    END IF;

    UPDATE queue_messages m
    SET
        status = 'processing',
        consumer_id = p_consumer_id,
        consumed_at = NOW()
    WHERE m.message_id = v_message_id;

    UPDATE queue_stats
    SET
        total_messages = total_messages - 1,
        in_flight = in_flight + 1,
        consumed_total = consumed_total + 1,
        updated_at = NOW()
    WHERE queue_name = p_queue_name;

    RETURN QUERY
    SELECT
        m.message_id,
        m.task_data,
        m.priority,
        m.retry_count,
        m.max_retries,
        m.headers,
        m.created_at
    FROM queue_messages m
    WHERE m.message_id = v_message_id;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION queue_consume IS 'Consume next available message from a queue';

CREATE OR REPLACE FUNCTION queue_ack(
    p_message_id UUID
) RETURNS BOOLEAN AS $$
DECLARE
    v_queue_name VARCHAR(255);
BEGIN
    DELETE FROM queue_messages
    WHERE message_id = p_message_id
      AND status = 'processing'
    RETURNING queue_name INTO v_queue_name;

    IF NOT FOUND THEN
        RETURN FALSE;
    END IF;

    UPDATE queue_stats
    SET
        in_flight = in_flight - 1,
        updated_at = NOW()
    WHERE queue_name = v_queue_name;

    RETURN TRUE;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION queue_ack IS 'Acknowledge successful processing of a message';

CREATE OR REPLACE FUNCTION queue_nack(
    p_message_id UUID,
    p_error_message TEXT DEFAULT NULL,
    p_delay_seconds INTEGER DEFAULT 5
) RETURNS BOOLEAN AS $$
DECLARE
    v_queue_name VARCHAR(255);
    v_retry_count INTEGER;
    v_max_retries INTEGER;
    v_dead_letter_queue VARCHAR(255);
BEGIN
    SELECT
        queue_name,
        retry_count,
        max_retries
    INTO v_queue_name, v_retry_count, v_max_retries
    FROM queue_messages
    WHERE message_id = p_message_id
      AND status = 'processing'
    FOR UPDATE;

    IF NOT FOUND THEN
        RETURN FALSE;
    END IF;

    IF v_retry_count >= v_max_retries THEN
        SELECT dead_letter_queue INTO v_dead_letter_queue
        FROM queue_config
        WHERE queue_name = v_queue_name;

        IF v_dead_letter_queue IS NOT NULL THEN
            UPDATE queue_messages
            SET
                status = 'dead_letter',
                queue_name = v_dead_letter_queue,
                last_error = p_error_message,
                completed_at = NOW()
            WHERE message_id = p_message_id;
        ELSE
            UPDATE queue_messages
            SET
                status = 'failed',
                last_error = p_error_message,
                completed_at = NOW()
            WHERE message_id = p_message_id;
        END IF;
    ELSE
        UPDATE queue_messages
        SET
            status = 'pending',
            retry_count = retry_count + 1,
            scheduled_at = NOW() + (p_delay_seconds || ' seconds')::INTERVAL,
            consumer_id = NULL,
            last_error = p_error_message
        WHERE message_id = p_message_id;
    END IF;

    UPDATE queue_stats
    SET
        in_flight = in_flight - 1,
        total_messages = CASE
            WHEN v_retry_count >= v_max_retries THEN total_messages
            ELSE total_messages + 1
        END,
        failed_total = CASE
            WHEN v_retry_count >= v_max_retries THEN failed_total + 1
            ELSE failed_total
        END,
        updated_at = NOW()
    WHERE queue_name = v_queue_name;

    RETURN TRUE;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION queue_nack IS 'Negative acknowledge - requeue message or move to DLQ';

CREATE OR REPLACE FUNCTION queue_depth(
    p_queue_name VARCHAR(255)
) RETURNS BIGINT AS $$
BEGIN
    RETURN (
        SELECT COUNT(*)
        FROM queue_messages
        WHERE queue_name = p_queue_name
          AND status = 'pending'
          AND (expires_at IS NULL OR expires_at > NOW())
    );
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION queue_depth IS 'Get number of pending messages in a queue';

CREATE OR REPLACE FUNCTION queue_cleanup_expired() RETURNS INTEGER AS $$
DECLARE
    v_count INTEGER;
BEGIN
    DELETE FROM queue_messages
    WHERE expires_at IS NOT NULL
      AND expires_at <= NOW()
      AND status = 'pending'
    RETURNING COUNT(*) INTO v_count;

    RETURN COALESCE(v_count, 0);
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION queue_cleanup_expired IS 'Remove expired messages from queues';

-- ============================================================================
-- SEED DATA: Default queue configurations
-- ============================================================================

INSERT INTO queue_config (queue_name, max_size, durable) VALUES
    ('orcher:queue:default.critical', 10000, true),
    ('orcher:queue:default.high', 50000, true),
    ('orcher:queue:default.normal', 100000, true),
    ('orcher:queue:default.low', 100000, true)
ON CONFLICT (queue_name) DO NOTHING;

INSERT INTO queue_stats (queue_name) VALUES
    ('orcher:queue:default.critical'),
    ('orcher:queue:default.high'),
    ('orcher:queue:default.normal'),
    ('orcher:queue:default.low')
ON CONFLICT (queue_name) DO NOTHING;
