/**
 * The contracts a host or an adapter implements: the stores ({@code PolicyStore}, {@code CounterStore}, {@code Ledger}, {@code SummaryStore}), {@code JobLock}, {@code UsageSink} and {@code TenantResolver}.
 * Every operation is asynchronous ({@code CompletionStage}); {@code Stages} holds the small helpers for that. Depends only on {@code model}.
 */
package dev.usageplatform.core.spi;
