package com.example.usage.admin.support;

import com.example.usage.core.model.TenantId;
import com.example.usage.postgres.Tables;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.transaction.support.TransactionTemplate;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Clock;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.function.Consumer;
import java.util.function.Supplier;

/** What every admin service shares: the database, the schema of the platform's tables, the transaction and the cache to invalidate. */
public record AdminContext(JdbcClient jdbc, Tables tables, TransactionTemplate tx, Consumer<TenantId> invalidate, Clock clock) {

    public String sql(String template) { return tables.sql(template); }

    public <T> T inTx(Supplier<T> body) { return tx.execute(status -> body.get()); }

    /** Runs {@code body} in a transaction and, once it committed, drops the tenant's cached policy. */
    public <T> T changingTenant(String tenantId, Supplier<T> body) {
        T result = inTx(body);
        invalidate.accept(new TenantId(tenantId));
        return result;
    }

    public static OffsetDateTime ts(Instant i) { return i.atOffset(ZoneOffset.UTC); }

    public static Instant instant(ResultSet rs, String column) throws SQLException {
        OffsetDateTime t = rs.getObject(column, OffsetDateTime.class);
        return t == null ? null : t.toInstant();
    }
}
