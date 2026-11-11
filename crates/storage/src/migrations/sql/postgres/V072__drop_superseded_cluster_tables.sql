-- V072: drop the first cluster model's tables
--
-- V009 created `clusters`, `nodes`, `node_heartbeats` and `cluster_events`
-- for a cluster model the engine replaced before it was ever used: the
-- engine's cluster is the members and shard leases V028 created
-- (`cluster_members`, `cluster_shards`). Nothing writes or reads the V009
-- tables. V009 stays as applied (applied migrations are checked by
-- checksum); this removes what it made.
--
-- No CASCADE: if anything else has come to depend on these tables, this
-- fails rather than dropping it too. Their triggers and indexes go with them;
-- the shared updated_at trigger function stays.

-- `clusters` and `nodes` reference each other (a cluster's leader node);
-- break the cycle so the tables can go in order.
ALTER TABLE IF EXISTS clusters DROP CONSTRAINT IF EXISTS clusters_leader_node_fk;

DROP TABLE IF EXISTS cluster_events;
DROP TABLE IF EXISTS node_heartbeats;
DROP TABLE IF EXISTS nodes;
DROP TABLE IF EXISTS clusters;
