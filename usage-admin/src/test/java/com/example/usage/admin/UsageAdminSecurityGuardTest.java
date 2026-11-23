package com.example.usage.admin;

import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.WebApplicationContextRunner;
import org.springframework.security.web.SecurityFilterChain;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

/** The API refuses to start unprotected, and does not depend on which transaction managers the host has. */
class UsageAdminSecurityGuardTest {
    final WebApplicationContextRunner runner = new WebApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(UsageAdminAutoConfiguration.class))
            .withBean(DataSource.class, () -> mock(DataSource.class));

    @Test
    void withoutASecurityChainTheApplicationDoesNotStart() {
        runner.run(ctx -> assertThat(ctx).hasFailed().getFailure().rootCause()
                .hasMessageContaining("no SecurityFilterChain").hasMessageContaining("usage.admin.allow-unsecured"));
    }

    @Test
    void withAChainItStartsEvenWithOtherTransactionManagersAround() {
        runner.withBean(SecurityFilterChain.class, () -> mock(SecurityFilterChain.class))
                .withBean("kafkaTm", PlatformTransactionManager.class, () -> mock(PlatformTransactionManager.class))
                .withBean("jmsTm", PlatformTransactionManager.class, () -> mock(PlatformTransactionManager.class))
                .run(ctx -> assertThat(ctx).hasNotFailed().hasSingleBean(TenantAdmin.class));
    }

    @Test
    void protectingItElsewhereIsAnExplicitChoice() {
        runner.withPropertyValues("usage.admin.allow-unsecured=true").run(ctx -> assertThat(ctx).hasNotFailed());
    }

    @Test
    void itCanBeTurnedOff() {
        runner.withPropertyValues("usage.admin.enabled=false").run(ctx -> assertThat(ctx).hasNotFailed().doesNotHaveBean(TenantAdmin.class));
    }
}
