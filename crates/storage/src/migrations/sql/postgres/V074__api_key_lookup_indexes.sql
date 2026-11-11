-- V074: find an API key by its hash, and an organisation's keys, by index.
--
-- Every request whose key is not cached looks the key up by key_hash, which
-- had no index, so each lookup read the whole table. The index is unique: two
-- rows with one hash would be one key with two sets of grants, and which one
-- a lookup found would be arbitrary.
--
-- Listing an organisation's keys filters on organization_id.
CREATE UNIQUE INDEX IF NOT EXISTS idx_api_keys_key_hash ON api_keys (key_hash);
CREATE INDEX IF NOT EXISTS idx_api_keys_organization ON api_keys (organization_id, created_at DESC);
