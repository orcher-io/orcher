-- V043: acting for any organisation is a capability a key is granted
--
-- An API key with no organisation could name any organisation in the request
-- header and be treated as that tenant. That is how the trusted edge works —
-- the gateway authenticates the user, checks their membership, and forwards
-- the organisation it resolved — but it made the power implicit in a NULL. Any
-- key stored without an owner, by hand or by a path that forgot to set one,
-- became a key to every tenant on the deployment, and nothing about the row
-- said so.
--
-- The capability is now a column, false unless granted.

ALTER TABLE api_keys
    ADD COLUMN IF NOT EXISTS can_delegate_organization BOOLEAN NOT NULL DEFAULT false;

-- Preserve exactly what works today. Every key that currently has no owner can
-- currently delegate, so granting it here changes nothing for them; what
-- changes is that a key created from now on without an owner cannot, unless
-- someone decides it should. On the deployments this was written against, the
-- only such key is the gateway's.
UPDATE api_keys
SET can_delegate_organization = true
WHERE organization_id IS NULL
  AND can_delegate_organization = false;

-- A key that belongs to an organisation and may also act for any other is a
-- contradiction, and the auth layer would refuse its cross-tenant claims
-- anyway. Refusing it at the schema means it cannot be created by mistake.
ALTER TABLE api_keys DROP CONSTRAINT IF EXISTS api_keys_delegation_is_ownerless;
ALTER TABLE api_keys
    ADD CONSTRAINT api_keys_delegation_is_ownerless
    CHECK (NOT (can_delegate_organization AND organization_id IS NOT NULL));

COMMENT ON COLUMN api_keys.can_delegate_organization IS
    'May act for whichever organisation the request names. For the trusted '
    'edge only: a key with this can act as every tenant on the deployment.';
