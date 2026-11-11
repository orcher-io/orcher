-- V071: drop the plugin tables
--
-- V007 created tables for a plugin system the engine never had: nothing ever
-- wrote to them or read them. Workers bring their own code and poll for work,
-- so there is nothing for the engine to load. V007 and V008 stay as they were
-- (applied migrations are checked by checksum); this removes what they made.
--
-- No CASCADE: if anything else has come to depend on these objects, this
-- fails rather than dropping it too.

-- V008's link from webhook configurations to a plugin; dropping the column
-- drops its foreign key and its index (idx_webhook_configurations_plugin).
ALTER TABLE webhook_configurations DROP COLUMN IF EXISTS processing_plugin_id;

DROP VIEW IF EXISTS v_plugin_resource_current;
DROP VIEW IF EXISTS v_plugin_execution_stats;
DROP VIEW IF EXISTS v_plugin_health_summary;
DROP VIEW IF EXISTS v_active_plugins;

-- Children before the table they reference. Their triggers and indexes go
-- with them; the shared updated_at trigger function stays.
DROP TABLE IF EXISTS plugin_api_endpoints;
DROP TABLE IF EXISTS plugin_schedules;
DROP TABLE IF EXISTS plugin_resource_usage;
DROP TABLE IF EXISTS plugin_events;
DROP TABLE IF EXISTS plugin_health_checks;
DROP TABLE IF EXISTS plugin_executions;
DROP TABLE IF EXISTS plugin_permissions;
DROP TABLE IF EXISTS plugin_dependencies;
DROP TABLE IF EXISTS plugin_configurations;
DROP TABLE IF EXISTS plugins;
