package com.example.usage.core.model;

import java.util.Objects;

/**
 * What is measured. {@link Mode} decides who owns the counter, one owner per metric:
 * <ul>
 *   <li>CONSUME_ON_CHECK: access metrics. {@code check} atomically consumes (gateway/servlet).</li>
 *   <li>REPORT_ONLY: result metrics. {@code check} only reads what was already consumed;
 *       the counter moves when {@code ingest} applies a reported event.</li>
 * </ul>
 */
public record Metric(String code, Mode mode) {
    public enum Mode { CONSUME_ON_CHECK, REPORT_ONLY }

    public Metric {
        Objects.requireNonNull(code, "code");
        Objects.requireNonNull(mode, "mode");
    }
}
