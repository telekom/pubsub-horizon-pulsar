// Copyright 2024 Deutsche Telekom IT GmbH
//
// SPDX-License-Identifier: Apache-2.0

package de.telekom.horizon.pulsar.api;

import de.telekom.horizon.pulsar.cache.SubscriberCache;
import de.telekom.horizon.pulsar.exception.SubscriberDoesNotMatchSubscriptionException;
import de.telekom.horizon.pulsar.helper.StreamLimit;
import de.telekom.horizon.pulsar.service.SseService;
import de.telekom.horizon.pulsar.testutils.HazelcastTestInstance;
import de.telekom.horizon.pulsar.utils.AbstractIntegrationTest;
import de.telekom.horizon.pulsar.utils.MongoTestServerConfiguration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.context.annotation.Import;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.servlet.MockMvc;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;
import static org.springframework.security.test.web.servlet.request.SecurityMockMvcRequestPostProcessors.jwt;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.head;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@ExtendWith({HazelcastTestInstance.class})
@SpringBootTest
@AutoConfigureMockMvc
@Import({MongoTestServerConfiguration.class})
class SseControllerSecurityEnabledTest extends AbstractIntegrationTest {

    @Autowired
    public MockMvc mockMvc;

    @Value("${pulsar.security.issuerUrls}")
    private String issuerUrl;

    @SpyBean
    private SseService sseService;

    @SpyBean
    private SubscriberCache subscriberCache;

    @Test
    void headRequestReturns204() throws Exception {
        mockMvc.perform(head("/v1/integration/sse"))
                .andExpect(status().isNoContent())
                .andExpect(header().exists("X-Health-Check-Timestamp"));
    }

    @Test
    void getReturnsForbiddenIfSubscriberValidationFails() throws Exception {
        String env = "integration";
        String subscriptionId = "subscriptionId";
        String subscriberId = "subscriberId";

        var jwt = getJwt(subscriberId);

        // when / then
        mockMvc.perform(get("/v1/integration/sse/"+subscriptionId)
                        .with(jwt().jwt(jwt)))
                .andExpect(status().isForbidden())
                .andExpect(result -> assertInstanceOf(SubscriberDoesNotMatchSubscriptionException.class, result.getResolvedException()));

        verify(sseService, times(1)).validateSubscriberIdForSubscription(eq(env), eq(subscriptionId));
    }

    @Test
    void getEventsViaSSESuccessfullyWithOk() throws Exception {
        String env = "integration";
        String subscriptionId = "subscriptionId";
        String subscriberId = "subscriberId";

        when(subscriberCache.getSubscriberId(subscriptionId)).thenReturn(Optional.of(subscriberId));

        mockMvc.perform(get("/v1/integration/sse/"+subscriptionId).queryParam("includeHttpHeaders", "true")
                        .with(jwt().jwt(getJwt(subscriberId))))
                .andExpect(status().isOk());

        verify(sseService, times(1)).validateSubscriberIdForSubscription(eq(env), eq(subscriptionId));
        verify(sseService, times(1)).startEmittingEvents(eq(env), eq(subscriptionId), any(String.class), eq(true), any(), any(StreamLimit.class));
    }

    private Jwt getJwt(String subscriberId) {
        return Jwt.withTokenValue("token")
                .header("alg", "none")
                .claim("clientId", subscriberId)
                .claim("iss", issuerUrl)
                .build();
    }

    @DynamicPropertySource
    static void dynamicProperties(DynamicPropertyRegistry registry) {
        registry.add("pulsar.security.oauth", () -> true);
    }
}