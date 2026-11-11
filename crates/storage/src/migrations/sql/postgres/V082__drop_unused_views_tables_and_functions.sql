-- V082: drop views, tables and functions nothing uses
--
-- Early migrations created reporting views, helper functions and a few tables
-- the engine never came to use: no code reads the views or tables, calls the
-- functions, or depends on them in the database, and the tables are empty.
-- Their migrations stay as they were (applied migrations are checked by
-- checksum); this removes what they made. Views and functions over the
-- account tables are left to the move of those tables to the cloud.
--
-- No CASCADE: if anything else has come to depend on these objects, this
-- fails rather than dropping it too.

-- Views: over runs (V003), the coordination queues (V014), actor state (V016)
-- and API key permissions (V005).
DROP VIEW IF EXISTS v_active_workflow_executions;
DROP VIEW IF EXISTS v_workflow_execution_summary;
DROP VIEW IF EXISTS v_failed_retryable_executions;
DROP VIEW IF EXISTS v_outbox_active_tasks;
DROP VIEW IF EXISTS v_timer_active_tasks;
DROP VIEW IF EXISTS v_dlq_summary;
DROP VIEW IF EXISTS actor_state_by_type;
DROP VIEW IF EXISTS actor_state_stats;
DROP VIEW IF EXISTS v_api_key_permissions;

-- Tables: system metrics and worker heartbeats (V001), cron validations
-- (V004), per-key permissions that scopes replaced (V005, V073), projection
-- snapshots (V012).
DROP TABLE IF EXISTS system_metrics;
DROP TABLE IF EXISTS worker_heartbeats;
DROP TABLE IF EXISTS cron_validations;
DROP TABLE IF EXISTS api_key_permissions;
DROP TABLE IF EXISTS projection_snapshots;

-- Functions nothing calls.
DROP FUNCTION IF EXISTS cleanup_expired_cron_validations();
DROP FUNCTION IF EXISTS cleanup_expired_idempotency();
DROP FUNCTION IF EXISTS cleanup_completed_queue_tasks(integer);
DROP FUNCTION IF EXISTS execution_log_mark_completed(uuid, text, jsonb);
DROP FUNCTION IF EXISTS execution_log_mark_failed(uuid, text, text, jsonb);
DROP FUNCTION IF EXISTS get_pending_child_workflows(character varying, character varying);
DROP FUNCTION IF EXISTS get_queue_metrics();
DROP FUNCTION IF EXISTS move_outbox_to_dlq(uuid);
DROP FUNCTION IF EXISTS move_timer_to_dlq(uuid);
DROP FUNCTION IF EXISTS get_next_stream_position(text);
DROP FUNCTION IF EXISTS get_current_global_position();
DROP FUNCTION IF EXISTS extract_aggregate_id(text);
DROP FUNCTION IF EXISTS extract_category(text);
