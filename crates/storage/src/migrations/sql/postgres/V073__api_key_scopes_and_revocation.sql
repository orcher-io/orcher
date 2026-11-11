-- V073: API keys the engine itself issues: scopes, an owner that need not be
-- an account, and revocation that keeps the row.
--
-- user_id: optional. A key the engine issues (from its own API, or to its
-- first administrator) belongs to whoever issued it, recorded as created_by,
-- and need not be a row of the users table. Keys issued with an owner keep it.
--
-- scopes: what the key may do (orcher_core::auth::Scope names, or '*'). NULL is
-- a key issued before scopes existed, which may do everything it could before.
--
-- created_by: the principal that issued the key (a key id, a token subject,
-- "anonymous" on a deployment without authentication), for audit.
--
-- revoked_at / revoked_by: when, and by whom, the key was revoked. A revoked
-- key is refused like an inactive one, and its row stays as the record.
ALTER TABLE api_keys ALTER COLUMN user_id DROP NOT NULL;

ALTER TABLE api_keys
    ADD COLUMN IF NOT EXISTS scopes TEXT[] DEFAULT NULL,
    ADD COLUMN IF NOT EXISTS created_by TEXT DEFAULT NULL,
    ADD COLUMN IF NOT EXISTS revoked_at TIMESTAMPTZ DEFAULT NULL,
    ADD COLUMN IF NOT EXISTS revoked_by TEXT DEFAULT NULL;

COMMENT ON COLUMN api_keys.scopes IS
    'Scopes the key grants (e.g. workflows:read, workers:run, or *). NULL: issued before scopes, grants everything.';
COMMENT ON COLUMN api_keys.created_by IS
    'Principal that issued the key, for audit.';
COMMENT ON COLUMN api_keys.revoked_at IS
    'When the key was revoked; a revoked key is refused.';
