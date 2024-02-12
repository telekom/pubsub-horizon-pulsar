// Copyright 2024 Deutsche Telekom IT GmbH
//
// SPDX-License-Identifier: Apache-2.0

package de.telekom.horizon.pulsar.service;

import de.telekom.eni.pandora.horizon.kubernetes.resource.SubscriptionResource;
import de.telekom.eni.pandora.horizon.kubernetes.resource.SubscriptionResourceSpec;
import de.telekom.horizon.pulsar.helper.SseTaskStateContainer;
import de.telekom.horizon.pulsar.testutils.MockHelper;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.server.mock.EnableKubernetesMockClient;
import io.fabric8.kubernetes.client.server.mock.KubernetesMockServer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
@EnableKubernetesMockClient(crud = true)
class SseTaskFactoryTest {

    KubernetesClient kubernetesClient;
    KubernetesMockServer server;

    SseTaskFactory sseTaskFactorySpy;

    final String namespace = "default";
    final String podName = "foobar";

    @BeforeEach
    void setupSseTaskFactoryTest() {
        MockHelper.init();

        when(MockHelper.pulsarConfig.getNamespace()).thenReturn(namespace);
        when(MockHelper.pulsarConfig.getPodName()).thenReturn(podName);

        sseTaskFactorySpy = spy(new SseTaskFactory(MockHelper.pulsarConfig, kubernetesClient, MockHelper.connectionCache, MockHelper.connectionGaugeCache, MockHelper.eventWriter, MockHelper.kafkaPicker, MockHelper.messageStateMongoRepo, MockHelper.deDuplicationService, MockHelper.tracingHelper));
    }

    SubscriptionResource createSubscriptionResource() {
        var subscriptionResource = new SubscriptionResource();

        subscriptionResource.getMetadata().setName(MockHelper.TEST_SUBSCRIPTION_ID);
        subscriptionResource.setSpec(new SubscriptionResourceSpec());

        return subscriptionResource;
    }

    @Test
    void testCreateNew () {
        ArgumentCaptor<SseTask> sseTaskCaptor = ArgumentCaptor.forClass(SseTask.class);

        // First, we provision our KubernetesMockServer with a test Subscription
        kubernetesClient.resources(SubscriptionResource.class).inNamespace(namespace).create(createSubscriptionResource());

        var sseTaskStateContainer = new SseTaskStateContainer();

        when(MockHelper.connectionGaugeCache.getOrCreateGaugeForSubscription(MockHelper.TEST_ENVIRONMENT, MockHelper.TEST_SUBSCRIPTION_ID)).thenReturn(new AtomicInteger(1));

        // PUBLIC METHOD WE WANT TO TEST
        var task = sseTaskFactorySpy.createNew(MockHelper.TEST_ENVIRONMENT, MockHelper.TEST_SUBSCRIPTION_ID, MockHelper.TEST_CONTENT_TYPE, sseTaskStateContainer, false);
        assertNotNull(task);

        // Let's verify that connectionGaugeCache.getOrCreateGaugeForSubscription(String environment, String subscriptionId) is called
        verify(MockHelper.connectionGaugeCache, times(1)).getOrCreateGaugeForSubscription(MockHelper.TEST_ENVIRONMENT, MockHelper.TEST_SUBSCRIPTION_ID);
        // Let's verify that connectionCache.addConnectionForSubscription(String environment, String subscriptionId, SseTask task) is called
        verify(MockHelper.connectionCache, times(1)).addConnectionForSubscription(eq(MockHelper.TEST_ENVIRONMENT), eq(MockHelper.TEST_SUBSCRIPTION_ID), sseTaskCaptor.capture());

        // We expect that our SubscriptionResource has been modified
        var resource = kubernetesClient.resources(SubscriptionResource.class)
                .inNamespace(namespace)
                .withName(MockHelper.TEST_SUBSCRIPTION_ID).get();
        assertNotNull(resource);
        assertEquals("foobar", resource.getSpec().getSseActiveOnPod());

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
