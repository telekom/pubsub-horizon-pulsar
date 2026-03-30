// Copyright 2024 Deutsche Telekom IT GmbH
//
// SPDX-License-Identifier: Apache-2.0

package de.telekom.horizon.pulsar.config;

import de.telekom.horizon.pulsar.testutils.HazelcastTestInstance;
import de.telekom.horizon.pulsar.utils.AbstractIntegrationTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.web.server.GracefulShutdownResult;
import org.springframework.boot.web.servlet.context.ServletWebServerApplicationContext;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration test that verifies graceful shutdown with actual HTTP connections.
 *
 * Tests that:
 * 1. In-flight requests complete successfully during shutdown
 * 2. The server drains connections before stopping
 */
@ExtendWith({HazelcastTestInstance.class})
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class GracefulShutdownIntegrationTest extends AbstractIntegrationTest {

    @Autowired
    private ServletWebServerApplicationContext applicationContext;

    @Autowired
    private TestRestTemplate restTemplate;

    @DynamicPropertySource
    static void dynamicProperties(DynamicPropertyRegistry registry) {
        registry.add("pulsar.security.oauth", () -> false);
    }

    @TestConfiguration
    static class SlowEndpointConfig {
        /**
         * Test-only controller that simulates a slow in-flight request.
         */
        @RestController
        static class SlowController {
            static final CountDownLatch requestStarted = new CountDownLatch(1);
            static final CountDownLatch allowComplete = new CountDownLatch(1);

            @GetMapping("/test/slow")
            public String slow() throws InterruptedException {
                requestStarted.countDown();
                // Block until the test signals completion (or timeout)
                allowComplete.await(10, TimeUnit.SECONDS);
                return "completed";
            }
        }
    }

    @Test
    void inFlightRequestCompletesAfterGracefulShutdown() throws Exception {
        var webServer = applicationContext.getWebServer();
        var responseRef = new AtomicReference<ResponseEntity<String>>();
        var shutdownResultRef = new AtomicReference<GracefulShutdownResult>();

        // 1. Start a slow request in a background thread
        var requestFuture = CompletableFuture.runAsync(() -> {
            responseRef.set(restTemplate.getForEntity("/test/slow", String.class));
        });

        // 2. Wait for the request to actually hit the server
        assertTrue(
                SlowEndpointConfig.SlowController.requestStarted.await(5, TimeUnit.SECONDS),
                "Slow request should have started"
        );

        // 3. Trigger graceful shutdown while request is in-flight
        webServer.shutDownGracefully(result -> shutdownResultRef.set(result));

        // 4. Let the in-flight request complete
        SlowEndpointConfig.SlowController.allowComplete.countDown();

        // 5. Wait for the request to finish
        requestFuture.get(10, TimeUnit.SECONDS);

        // 6. Verify the in-flight request completed successfully
        var response = responseRef.get();
        assertNotNull(response, "Response should not be null");
        assertEquals(HttpStatus.OK, response.getStatusCode(),
                "In-flight request should complete with 200 during graceful shutdown");
        assertEquals("completed", response.getBody());

        // 7. Verify shutdown completed successfully (all requests drained)
        // Give it a moment to finalize
        Thread.sleep(500);
        assertEquals(GracefulShutdownResult.IDLE, shutdownResultRef.get(),
                "Shutdown should report IDLE after all requests drained");
    }
}
