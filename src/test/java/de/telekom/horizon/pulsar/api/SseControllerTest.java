// Copyright 2024 Deutsche Telekom IT GmbH
//
// SPDX-License-Identifier: Apache-2.0

package de.telekom.horizon.pulsar.api;

import de.telekom.horizon.pulsar.utils.AbstractIntegrationTest;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.servlet.MockMvc;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.head;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest
@AutoConfigureMockMvc
class SseControllerTest extends AbstractIntegrationTest {

    @Autowired
    public MockMvc mockMvc;

    @Test
    void headRequestReturns204() throws Exception {
        mockMvc.perform(head("/v1/integration/sse"))
                .andExpect(status().isNoContent())
                .andExpect(header().exists("X-Health-Check-Timestamp"));
    }

    @Test
    void getRequestsReturnsError() throws Exception {
        mockMvc.perform(get("/v1/integration/sse/subscriptionId"))
                .andExpect(status().isForbidden());
    }

    @DynamicPropertySource
    static void dynamicProperties(DynamicPropertyRegistry registry) {
        registry.add("pulsar.security.oauth", () -> false);
    }

}