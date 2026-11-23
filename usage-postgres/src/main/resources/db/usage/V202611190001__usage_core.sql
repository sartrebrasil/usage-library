-- Schema of usage-postgres, in one migration while the project is in development (no installation to upgrade yet): when there is one,
-- freeze this file and put every change in a new migration. Tenant ids are TEXT so the host can use any id scheme.
-- Differences from the design doc: window_seconds / window_months instead of window_unit+size; no route_pattern,
-- soft limits or overage price yet (billing phase).

CREATE TABLE tenant (
    id          TEXT PRIMARY KEY,
    name        VARCHAR(200) NOT NULL,
    status      VARCHAR(20)  NOT NULL DEFAULT 'ACTIVE',
    created_by  TEXT,                                   -- who registered it (the caller's name in the token); nullable: a path without a caller has none
    created_at  TIMESTAMPTZ  NOT NULL DEFAULT now(),
    CONSTRAINT ck_tenant_status CHECK (status IN ('ACTIVE', 'SUSPENDED'))
);

CREATE TABLE metric (
    id             UUID PRIMARY KEY,
    code           VARCHAR(60) NOT NULL UNIQUE,
    description    VARCHAR(200),
    unit           VARCHAR(30) NOT NULL,
    -- ON_REQUEST: consumed by check (access metrics). ON_FINAL_STATUS: moved by ingest (result metrics).
    count_trigger  VARCHAR(20) NOT NULL DEFAULT 'ON_REQUEST',
    CONSTRAINT ck_metric_trigger CHECK (count_trigger IN ('ON_REQUEST', 'ON_FINAL_STATUS'))
);

CREATE TABLE plan (
    id          UUID PRIMARY KEY,
    code        VARCHAR(60)  NOT NULL,
    name        VARCHAR(200) NOT NULL,
    version     INT          NOT NULL,
    status      VARCHAR(20)  NOT NULL DEFAULT 'ACTIVE',
    created_at  TIMESTAMPTZ  NOT NULL DEFAULT now(),
    UNIQUE (code, version),
    CONSTRAINT ck_plan_status CHECK (status IN ('ACTIVE', 'DEPRECATED'))
);

CREATE TABLE plan_limit (
    id              UUID PRIMARY KEY,
    plan_id         UUID        NOT NULL REFERENCES plan(id),
    metric_id       UUID        NOT NULL REFERENCES metric(id),
    limit_type      VARCHAR(20) NOT NULL,
    limit_value     BIGINT      NOT NULL CHECK (limit_value >= 0),
    window_seconds  BIGINT      CHECK (window_seconds > 0),  -- fixed-length periods
    window_months   INT         CHECK (window_months > 0),   -- calendar periods starting on the anchor day
    enforcement     VARCHAR(20) NOT NULL DEFAULT 'REJECT',
    CONSTRAINT ck_limit_type CHECK (limit_type IN ('RATE', 'QUOTA', 'CONCURRENCY', 'METER')),
    CONSTRAINT ck_limit_enforcement CHECK (enforcement IN ('REJECT', 'ALLOW_OVERAGE')),
    -- CONCURRENCY has no window; every other limit has exactly one kind of window
    CONSTRAINT ck_limit_window CHECK (
        (limit_type = 'CONCURRENCY' AND window_seconds IS NULL AND window_months IS NULL)
        OR (limit_type <> 'CONCURRENCY' AND (window_seconds IS NULL) <> (window_months IS NULL))),
    -- window is part of the key: a daily and a monthly quota on the same metric can coexist
    CONSTRAINT uq_plan_limit UNIQUE NULLS NOT DISTINCT (plan_id, metric_id, limit_type, window_seconds, window_months)
);

-- The billing cycle belongs to the SUBSCRIPTION, not to the plan: changing plan in the middle of a cycle swaps
-- the limits but keeps the cycle, so counters and summaries of the running period carry over. A new
-- subscription (after a cancellation) starts a new cycle anchored at its start.
CREATE TABLE tenant_subscription (
    id            UUID PRIMARY KEY,
    tenant_id     TEXT        NOT NULL REFERENCES tenant(id),
    cycle_anchor  TIMESTAMPTZ NOT NULL,   -- periods start on this day and time (UTC), see PeriodCalculator
    started_at    TIMESTAMPTZ NOT NULL,
    ended_at      TIMESTAMPTZ,            -- NULL = in force
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT ck_subscription_range CHECK (ended_at IS NULL OR ended_at > started_at)
);
CREATE UNIQUE INDEX uq_tenant_open_subscription ON tenant_subscription (tenant_id) WHERE ended_at IS NULL;

-- Which plan the tenant is on, over time. Overrides hang here, so a plan change starts without them.
CREATE TABLE tenant_plan (
    id          UUID PRIMARY KEY,
    tenant_id   TEXT        NOT NULL REFERENCES tenant(id),
    plan_id     UUID        NOT NULL REFERENCES plan(id),
    valid_from  TIMESTAMPTZ NOT NULL,
    valid_to    TIMESTAMPTZ,              -- NULL = in force
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT ck_tenant_plan_range CHECK (valid_to IS NULL OR valid_to > valid_from)
);
CREATE INDEX ix_tenant_plan_tenant ON tenant_plan (tenant_id, valid_from DESC);
CREATE UNIQUE INDEX uq_tenant_open_plan ON tenant_plan (tenant_id) WHERE valid_to IS NULL;

CREATE TABLE tenant_limit_override (
    id              UUID PRIMARY KEY,
    tenant_plan_id  UUID   NOT NULL REFERENCES tenant_plan(id),
    plan_limit_id   UUID   NOT NULL REFERENCES plan_limit(id),
    limit_value     BIGINT NOT NULL CHECK (limit_value >= 0),
    reason          VARCHAR(300),
    UNIQUE (tenant_plan_id, plan_limit_id)
);

-- Source of truth of consumption. The key includes occurred_at because the table is partitioned by it, so on its own it only
-- catches a redelivery that carries the same timestamp: usage_event_id (below) is the dedup by event id alone.
CREATE TABLE usage_event (
    event_id     UUID        NOT NULL,
    occurred_at  TIMESTAMPTZ NOT NULL,
    tenant_id    TEXT        NOT NULL,
    metric_code  VARCHAR(60) NOT NULL,
    quantity     BIGINT      NOT NULL CHECK (quantity > 0),
    event_type   VARCHAR(15) NOT NULL DEFAULT 'USAGE',   -- direction comes from the type, never from the sign of quantity
    compensates_id UUID,                                  -- COMPENSATION only: the usage it reverses
    reason       VARCHAR(60),
    recorded_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (event_id, occurred_at),
    CONSTRAINT ck_usage_event_type CHECK (event_type IN ('USAGE', 'COMPENSATION')),
    CONSTRAINT ck_usage_event_compensates CHECK (event_type <> 'COMPENSATION' OR compensates_id IS NOT NULL)
) PARTITION BY RANGE (occurred_at);

-- Per-period totals that billing reads: one row per tenant, metric, quota window and period, the durable
-- twin of the quota counter. Written in the same transaction as usage_event.
--   total      usage while the period was OPEN (frozen once CLOSED)
--   late_total usage that arrived after the period was CLOSED (billing adds it as an adjustment)
--   credits    reversals of usage in a closed period (filled when compensations exist)
-- net consumption of a closed period = total + late_total - credits
CREATE TABLE usage_summary (
    tenant_id     TEXT        NOT NULL,
    metric_code   VARCHAR(60) NOT NULL,
    window_key    VARCHAR(20) NOT NULL,          -- width of the quota window: seconds ('86400') or months ('m1')
    period_start  TIMESTAMPTZ NOT NULL,
    period_end    TIMESTAMPTZ NOT NULL,
    total         BIGINT      NOT NULL DEFAULT 0 CHECK (total >= 0),
    late_total    BIGINT      NOT NULL DEFAULT 0 CHECK (late_total >= 0),
    credits       BIGINT      NOT NULL DEFAULT 0 CHECK (credits >= 0),
    -- the plan in force when the period last received usage while OPEN; frozen when it closes. Billing reads the
    -- limits and prices of exactly this (code, version).
    plan_code     VARCHAR(60) NOT NULL DEFAULT 'UNSPECIFIED',
    plan_version  INT         NOT NULL DEFAULT 1,
    status        VARCHAR(10) NOT NULL DEFAULT 'OPEN',
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, metric_code, window_key, period_start),
    CONSTRAINT ck_usage_summary_status CHECK (status IN ('OPEN', 'CLOSED')),
    CONSTRAINT ck_usage_summary_period CHECK (period_end > period_start)
);
CREATE INDEX ix_usage_summary_open ON usage_summary (period_end) WHERE status = 'OPEN';

-- The summary rows each usage event contributed to, so a compensation reverses exactly those and not what today's
-- policy would compute. Keep it as long as usage_event.
CREATE TABLE usage_event_slot (
    event_id      UUID        NOT NULL,
    window_key    VARCHAR(20) NOT NULL,
    period_start  TIMESTAMPTZ NOT NULL,
    period_end    TIMESTAMPTZ NOT NULL,
    plan_code     VARCHAR(60) NOT NULL,
    plan_version  INT         NOT NULL,
    PRIMARY KEY (event_id, window_key, period_start)
);

-- shortcut: everything lands in the default partition; add monthly partitions (pg_partman or a job) before volume grows.
CREATE TABLE usage_event_default PARTITION OF usage_event DEFAULT;
CREATE INDEX ix_usage_event_tenant_time ON usage_event (tenant_id, occurred_at);
-- a compensation finds its original by id alone, across partitions
CREATE INDEX ix_usage_event_id ON usage_event (event_id);

-- An original is reversed at most once. Not partitioned, so the primary key can be the original's id alone.
CREATE TABLE usage_compensation (
    compensates_id         UUID PRIMARY KEY,
    compensation_event_id  UUID        NOT NULL,
    created_at             TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Dedup by event id alone. usage_event is partitioned by occurred_at, so its primary key (event_id, occurred_at) only
-- catches a redelivery that carries the same timestamp; a producer that re-stamps the time would be counted twice.
-- This table is not partitioned, so its key can be the id alone. The ledger inserts here first, in the same
-- transaction as the event, and treats a conflict as a duplicate. Purge it with JdbcRetention.
CREATE TABLE usage_event_id (
    event_id     UUID        PRIMARY KEY,
    recorded_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX ix_usage_event_id_recorded ON usage_event_id (recorded_at);

-- the policy query asks for the subscription in force at an instant, so it needs (tenant, started_at); the partial
-- unique index on open subscriptions cannot serve "ended_at IS NULL OR ended_at > ?".
CREATE INDEX ix_tenant_subscription_tenant ON tenant_subscription (tenant_id, started_at DESC);

-- retention of the slots goes by the end of their period
CREATE INDEX ix_usage_event_slot_period_end ON usage_event_slot (period_end);

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
