// Copyright 2024 Deutsche Telekom IT GmbH
//
// SPDX-License-Identifier: Apache-2.0

package de.telekom.horizon.pulsar.service;

import de.telekom.eni.pandora.horizon.cache.service.JsonCacheService;
import de.telekom.eni.pandora.horizon.exception.JsonCacheException;
import de.telekom.eni.pandora.horizon.kubernetes.resource.Subscription;
import de.telekom.eni.pandora.horizon.kubernetes.resource.SubscriptionResource;
import de.telekom.eni.pandora.horizon.kubernetes.resource.SubscriptionResourceSpec;
import de.telekom.horizon.pulsar.helper.SseTaskStateContainer;
import de.telekom.horizon.pulsar.helper.StreamLimit;
import de.telekom.horizon.pulsar.testutils.MockHelper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.test.util.ReflectionTestUtils;

import java.time.Instant;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class SseTaskFactoryTest {

    @Mock
    JsonCacheService<SubscriptionResource> jsonCacheService;

    SseTaskFactory sseTaskFactorySpy;

    @BeforeEach
    void setupSseTaskFactoryTest() {
        MockHelper.init();

        sseTaskFactorySpy = spy(new SseTaskFactory(MockHelper.pulsarConfig, MockHelper.connectionCache, MockHelper.connectionGaugeCache, MockHelper.eventWriter, MockHelper.kafkaPicker, MockHelper.messageStateMongoRepo, MockHelper.deDuplicationService, MockHelper.metricsHelper, MockHelper.tracingHelper));
    }

    SubscriptionResource createSubscriptionResource() {
        var resource = new SubscriptionResource();

        var spec = new SubscriptionResourceSpec();
        var subscription = new Subscription();
        subscription.setSubscriptionId(MockHelper.TEST_SUBSCRIPTION_ID);
        spec.setSubscription(subscription);

        resource.setSpec(spec);

        return resource;
    }

    @Test
    void testCreateNew () throws JsonCacheException {
        ArgumentCaptor<SseTask> sseTaskCaptor = ArgumentCaptor.forClass(SseTask.class);

        var sseTaskStateContainer = new SseTaskStateContainer();

        when(MockHelper.connectionGaugeCache.getOrCreateGaugeForSubscription(MockHelper.TEST_ENVIRONMENT, MockHelper.TEST_SUBSCRIPTION_ID)).thenReturn(new AtomicInteger(1));

        // PUBLIC METHOD WE WANT TO TEST
        var task = sseTaskFactorySpy.createNew(MockHelper.TEST_ENVIRONMENT, MockHelper.TEST_SUBSCRIPTION_ID, MockHelper.TEST_CONTENT_TYPE, sseTaskStateContainer, false, new StreamLimit(), null, null);
        assertNotNull(task);

        // Let's verify that connectionGaugeCache.getOrCreateGaugeForSubscription(String environment, String subscriptionId) is called
        verify(MockHelper.connectionGaugeCache, times(1)).getOrCreateGaugeForSubscription(MockHelper.TEST_ENVIRONMENT, MockHelper.TEST_SUBSCRIPTION_ID);
        // Let's verify that connectionCache.addConnectionForSubscription(String environment, String subscriptionId, SseTask task) is called
        verify(MockHelper.connectionCache, times(1)).claimConnectionForSubscription(eq(MockHelper.TEST_SUBSCRIPTION_ID), sseTaskCaptor.capture());

        var sseTask = sseTaskCaptor.getValue();

        assertNotNull(sseTask);

        // Here we check that the sseTaskStateContainer has been passed successfully to the SseTask
        var container = (SseTaskStateContainer) ReflectionTestUtils.getField(sseTask, "sseTaskStateContainer");
        assertEquals(sseTaskStateContainer, container);

        // Here we check that the contentType has been passed successfully to the SseTask
        var contentType = (String) ReflectionTestUtils.getField(sseTask, "contentType");
        assertEquals(MockHelper.TEST_CONTENT_TYPE, contentType);
    }
}
