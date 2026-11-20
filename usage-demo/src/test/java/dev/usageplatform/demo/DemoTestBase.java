package dev.usageplatform.demo;

import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;

import java.net.ServerSocket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

/** The demo as shipped (its application.yml) against real Redis and PostgreSQL; the tests share one running application. */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.DEFINED_PORT)
abstract class DemoTestBase {
    // started once for every test class: the Spring context is cached and shared, so the containers must outlive each class
    static final GenericContainer<?> REDIS = new GenericContainer<>("redis:7-alpine").withExposedPorts(6379);
    static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:16-alpine");

    static {
        REDIS.start();
        POSTGRES.start();
    }

    static final int PORT = freePort();
    static final HttpClient HTTP = HttpClient.newBuilder().version(HttpClient.Version.HTTP_1_1).build();

    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry r) {
        r.add("demo.processing-time", () -> "2s"); // slow enough that a backlog builds up while the test submits
        r.add("server.port", () -> PORT); // the route points back at this port
        r.add("spring.datasource.url", POSTGRES::getJdbcUrl);
        r.add("spring.datasource.username", POSTGRES::getUsername);
        r.add("spring.datasource.password", POSTGRES::getPassword);
        r.add("usage.redis.uri", () -> "redis://" + REDIS.getHost() + ":" + REDIS.getMappedPort(6379));
    }

    static int freePort() {
        try (ServerSocket s = new ServerSocket(0)) {
            return s.getLocalPort();
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    static HttpResponse<String> call(String method, String path, String apiKey, String... headers) throws Exception {
        return send(method, path, apiKey, null, headers);
    }

    static HttpResponse<String> post(String path, String apiKey, String body) throws Exception {
        return send("POST", path, apiKey, body);
    }

    private static HttpResponse<String> send(String method, String path, String apiKey, String body, String... headers) throws Exception {
        var b = HttpRequest.newBuilder(URI.create("http://localhost:" + PORT + path)).timeout(java.time.Duration.ofSeconds(15))
                .method(method, body == null ? HttpRequest.BodyPublishers.noBody() : HttpRequest.BodyPublishers.ofString(body));
        if (apiKey != null) b.header("X-Api-Key", apiKey);
        for (int i = 0; i < headers.length; i += 2) b.header(headers[i], headers[i + 1]);
        return HTTP.send(b.build(), HttpResponse.BodyHandlers.ofString());
    }
}
