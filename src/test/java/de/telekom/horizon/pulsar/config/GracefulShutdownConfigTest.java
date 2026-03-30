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
import org.springframework.boot.web.embedded.tomcat.TomcatWebServer;
import org.springframework.boot.web.servlet.context.ServletWebServerApplicationContext;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith({HazelcastTestInstance.class})
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class GracefulShutdownConfigTest extends AbstractIntegrationTest {

    @Autowired
    private ServletWebServerApplicationContext applicationContext;

    @Test
    void webServerIsConfiguredForGracefulShutdown() {
        var webServer = applicationContext.getWebServer();
        assertTrue(webServer instanceof TomcatWebServer, "Expected TomcatWebServer");

        var shutdown = applicationContext.getEnvironment().getProperty("server.shutdown");
        assertEquals("graceful", shutdown);
    }

    @Test
    void shutdownTimeoutIsConfigured() {
        var timeout = applicationContext.getEnvironment()
                .getProperty("spring.lifecycle.timeout-per-shutdown-phase");
        assertTrue(timeout != null && timeout.contains("60000"),
                "Expected shutdown phase timeout of 60000ms, got: " + timeout);
    }
}
