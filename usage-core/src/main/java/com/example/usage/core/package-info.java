/**
 * The decision engine and the jobs that keep it honest: {@link com.example.usage.core.UsageEngine} (check, acquire, report, ingest, compensate), the counter reconciler and the period maintenance.
 * Depends on {@code model} and {@code spi}; adapters (gateway, processor, consumer) call only the engine.
 */
package com.example.usage.core;
