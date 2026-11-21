package com.example.usage.postgres;

import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.security.MessageDigest;
import java.util.HexFormat;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * A migration that has run somewhere must never change: Flyway would refuse to start against that database (checksum
 * mismatch) and the ones that did not run it yet would diverge. Change the schema with a new V&lt;n&gt; file instead.
 */
class UsageSchemaFrozenTest {
    static final String V1_SHA256 = "5ddece0ecb000bdc2a15bfef9f3348d8b445ae2e235c01a86f9cb8525a1f85f2";

    @Test
    void v1IsFrozen() throws Exception {
        try (InputStream in = getClass().getResourceAsStream("/db/usage/V1__usage_core.sql")) {
            String actual = HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(in.readAllBytes()));
            assertEquals(V1_SHA256, actual, "V1__usage_core.sql was edited: put the change in a new migration");
        }
    }
}
