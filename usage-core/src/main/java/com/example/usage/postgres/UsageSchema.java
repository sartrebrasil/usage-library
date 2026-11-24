package com.example.usage.postgres;

import org.flywaydb.core.Flyway;

import javax.sql.DataSource;

public final class UsageSchema {
    private UsageSchema() {}

    /** Migrates into the connection's default schema (the tables then resolve through the search_path). */
    public static void migrate(DataSource dataSource) {
        migrate(dataSource, null);
    }

    /**
     * Migrates into a schema of its own, creating it if needed, with its own history table inside it. The platform's
     * generic table names (tenant, metric, plan...) then never collide with the host's, and a host schema that
     * already has tables is left alone. Pass the same schema to every Jdbc store.
     *
     * @param schema a lowercase identifier, or null/blank for the connection's default schema
     */
    public static void migrate(DataSource dataSource, String schema) {
        var flyway = Flyway.configure()
                .dataSource(dataSource)
                .locations("classpath:db/usage")
                .table("usage_schema_history");
        if (schema != null && !schema.isBlank()) {
            Tables.validate(schema);
            flyway.schemas(schema).defaultSchema(schema).createSchemas(true);
        }
        flyway.load().migrate();
    }
}
