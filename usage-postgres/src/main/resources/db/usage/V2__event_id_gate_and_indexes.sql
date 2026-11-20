-- V1 is frozen (UsageSchemaFrozenTest guards its checksum): every schema change from here on is a new V<n> file.

-- Dedup by event id alone. usage_event is partitioned by occurred_at, so its primary key (event_id, occurred_at) only
-- catches a redelivery that carries the same timestamp; a producer that re-stamps the time would be counted twice.
-- This table is not partitioned, so its key can be the id alone. The ledger inserts here first, in the same
-- transaction as the event, and treats a conflict as a duplicate. Purge it with JdbcRetention.
CREATE TABLE usage_event_id (
    event_id     UUID        PRIMARY KEY,
    recorded_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX ix_usage_event_id_recorded ON usage_event_id (recorded_at);

-- an installation that already holds events keeps deduplicating them
INSERT INTO usage_event_id (event_id, recorded_at)
SELECT event_id, min(recorded_at) FROM usage_event GROUP BY event_id;

-- the policy query asks for the subscription in force at an instant, so it needs (tenant, started_at); the partial
-- unique index on open subscriptions cannot serve "ended_at IS NULL OR ended_at > ?".
CREATE INDEX ix_tenant_subscription_tenant ON tenant_subscription (tenant_id, started_at DESC);

-- retention of the slots goes by the end of their period
CREATE INDEX ix_usage_event_slot_period_end ON usage_event_slot (period_end);
