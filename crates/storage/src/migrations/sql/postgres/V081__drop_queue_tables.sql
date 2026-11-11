-- V081: drop the database-backed message queue
--
-- V010 created a message queue in the database (tables, functions to publish,
-- consume, acknowledge and count, and seed rows) that the engine never used:
-- its queues are the outbox and the task and timer tables. Nothing calls the
-- functions or reads the tables, and their only rows are the seeded ones. V010
-- stays as it was (applied migrations are checked by checksum); this removes
-- what it made.
--
-- No CASCADE: if anything else has come to depend on these objects, this
-- fails rather than dropping it too.

DROP FUNCTION IF EXISTS queue_publish(character varying, jsonb, smallint, integer, integer, integer, jsonb);
DROP FUNCTION IF EXISTS queue_consume(character varying, character varying, integer);
DROP FUNCTION IF EXISTS queue_ack(uuid);
DROP FUNCTION IF EXISTS queue_nack(uuid, text, integer);
DROP FUNCTION IF EXISTS queue_depth(character varying);
DROP FUNCTION IF EXISTS queue_cleanup_expired();

-- Children before the table they reference. queue_config's trigger goes with
-- it, and then the function only that trigger called.
DROP TABLE IF EXISTS queue_messages;
DROP TABLE IF EXISTS queue_stats;
DROP TABLE IF EXISTS queue_consumers;
DROP TABLE IF EXISTS queue_config;
DROP FUNCTION IF EXISTS update_queue_config_timestamp();
