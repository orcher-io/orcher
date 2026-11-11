-- V079: drop the integration and webhook tables
--
-- V008 created tables, views and seed rows for an integrations and incoming
-- webhooks feature the engine never had: nothing writes to them or reads
-- them. V008 stays as it was (applied migrations are checked by checksum);
-- this removes what it made, as V071 did for V007's plugin tables.
--
-- No CASCADE: if anything else has come to depend on these objects, this
-- fails rather than dropping it too.

DROP VIEW IF EXISTS v_integration_operation_stats;
DROP VIEW IF EXISTS v_webhook_request_summary;
DROP VIEW IF EXISTS v_integration_health_summary;
DROP VIEW IF EXISTS v_active_integrations;

-- Children before the tables they reference. Their triggers and indexes go
-- with them; the shared updated_at trigger function stays.
DROP TABLE IF EXISTS integration_events;
DROP TABLE IF EXISTS integration_schedules;
DROP TABLE IF EXISTS webhook_requests;
DROP TABLE IF EXISTS webhook_configurations;
DROP TABLE IF EXISTS integration_operations;
DROP TABLE IF EXISTS integrations;
DROP TABLE IF EXISTS integration_providers;
