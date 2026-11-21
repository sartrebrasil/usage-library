/**
 * The decision engine and the jobs that keep it honest: {@link dev.usageplatform.core.UsageEngine} (check, acquire, report, ingest, compensate), the counter reconciler and the period maintenance.
 * Depends on {@code model} and {@code spi}; adapters (gateway, processor, consumer) call only the engine.
 */
package dev.usageplatform.core;
