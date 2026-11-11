-- V080: drop the alert and notification tables
--
-- V006 created tables, views and seed rows for an alerting and user
-- notification system the engine never had: nothing writes to them or reads
-- them, and their only rows are the ones V006 seeded. V006 stays as it was
-- (applied migrations are checked by checksum); this removes what it made, as
-- V071 and V079 did for V007 and V008.
--
-- No CASCADE: if anything else has come to depend on these objects, this
-- fails rather than dropping it too.

DROP VIEW IF EXISTS v_pending_notifications;
DROP VIEW IF EXISTS v_active_alerts;
DROP VIEW IF EXISTS v_user_notification_settings;

-- Children before the tables they reference. Their triggers and indexes go
-- with them; the shared updated_at trigger function stays.
DROP TABLE IF EXISTS notification_deliveries;
DROP TABLE IF EXISTS notification_digests;
DROP TABLE IF EXISTS user_notification_category_preferences;
DROP TABLE IF EXISTS user_notification_preferences;
DROP TABLE IF EXISTS notifications;
DROP TABLE IF EXISTS notification_templates;
DROP TABLE IF EXISTS notification_channels;
DROP TABLE IF EXISTS notification_categories;
DROP TABLE IF EXISTS alert_rule_evaluations;
DROP TABLE IF EXISTS system_alerts;
DROP TABLE IF EXISTS alert_rules;
