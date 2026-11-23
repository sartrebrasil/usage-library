-- Who changed what, and when: written by usage-admin in the same transaction as every change it makes.
-- Append-only: the trigger below refuses UPDATE and DELETE, so the trail cannot be edited through SQL by accident.
CREATE TABLE usage_audit (
    id            UUID        PRIMARY KEY,
    occurred_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    actor         TEXT        NOT NULL,          -- the caller's name in the token (client id or user)
    action        VARCHAR(40) NOT NULL,          -- CREATE, UPDATE, DELETE, CHANGE_PLAN, SUBSCRIBE, END_SUBSCRIPTION, COMPENSATE...
    entity_type   VARCHAR(40) NOT NULL,          -- tenant, metric, plan, plan_limit, subscription, tenant_plan, override, compensation
    entity_id     TEXT        NOT NULL,
    tenant_id     TEXT,                          -- set when the change concerns one tenant
    before_state  JSONB,
    after_state   JSONB,
    reason        VARCHAR(300)
);
CREATE INDEX ix_usage_audit_entity ON usage_audit (entity_type, entity_id, occurred_at DESC);
CREATE INDEX ix_usage_audit_tenant ON usage_audit (tenant_id, occurred_at DESC) WHERE tenant_id IS NOT NULL;
CREATE INDEX ix_usage_audit_time ON usage_audit (occurred_at DESC);

CREATE FUNCTION usage_audit_immutable() RETURNS trigger AS $$
BEGIN
    RAISE EXCEPTION 'usage_audit is append-only';
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER usage_audit_no_change BEFORE UPDATE OR DELETE ON usage_audit
    FOR EACH ROW EXECUTE FUNCTION usage_audit_immutable();
