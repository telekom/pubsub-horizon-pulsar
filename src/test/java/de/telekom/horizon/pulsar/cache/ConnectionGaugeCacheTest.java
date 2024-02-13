// Copyright 2024 Deutsche Telekom IT GmbH
//
// SPDX-License-Identifier: Apache-2.0

package de.telekom.horizon.pulsar.cache;

import de.telekom.eni.pandora.horizon.kubernetes.resource.Subscription;
import de.telekom.eni.pandora.horizon.kubernetes.resource.SubscriptionResource;
import de.telekom.eni.pandora.horizon.kubernetes.resource.SubscriptionResourceSpec;
import de.telekom.eni.pandora.horizon.metrics.HorizonMetricsHelper;
import de.telekom.eni.pandora.horizon.model.event.DeliveryType;
import de.telekom.horizon.pulsar.testutils.MockHelper;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.server.mock.EnableKubernetesMockClient;
import io.fabric8.kubernetes.client.server.mock.KubernetesMockServer;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.concurrent.atomic.AtomicInteger;

import static de.telekom.eni.pandora.horizon.metrics.HorizonMetricsConstants.METRIC_OPEN_SSE_CONNECTIONS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
@EnableKubernetesMockClient(crud = true)
@Slf4j
class ConnectionGaugeCacheTest {

    KubernetesClient kubernetesClient;
    KubernetesMockServer server;

    ConnectionGaugeCache cache;

    MeterRegistry meterRegistryMock;

    final String namespace = "default";

    @BeforeEach
    void setupSseServiceTest() {
        MockHelper.init();

        var metricsHelperMock = Mockito.mock(HorizonMetricsHelper.class);
        when(MockHelper.pulsarConfig.getNamespace()).thenReturn(namespace);
        meterRegistryMock = Mockito.mock(MeterRegistry.class);
        when(metricsHelperMock.getRegistry()).thenReturn(meterRegistryMock);

        this.cache = new ConnectionGaugeCache(MockHelper.pulsarConfig, kubernetesClient, metricsHelperMock);
    }

    @Test
    void getOrCreateGaugeForSubscription() {
        final var gauge = new AtomicInteger(1);

        // First, we provision our KubernetesMockServer with a test Subscription
        kubernetesClient.resources(SubscriptionResource.class).inNamespace(namespace).create(createSubscriptionResource());

        when(meterRegistryMock.gauge(anyString(), any(Iterable.class), any(Number.class))).thenReturn(gauge);

        {
            var result = cache.getOrCreateGaugeForSubscription(MockHelper.TEST_ENVIRONMENT, MockHelper.TEST_SUBSCRIPTION_ID);

            assertNotNull(result);
            assertEquals(gauge.get(), result.get());
        }
        // We do the same method call again, so that we can verify that the cache actually works
        {
            var result = cache.getOrCreateGaugeForSubscription(MockHelper.TEST_ENVIRONMENT, MockHelper.TEST_SUBSCRIPTION_ID);

            assertNotNull(result);
            assertEquals(gauge.get(), result.get());
        }

        ArgumentCaptor<Iterable<Tag>> tagsCaptor = ArgumentCaptor.forClass(Iterable.class);
        ArgumentCaptor<AtomicInteger> initialValueCaptor = ArgumentCaptor.forClass(AtomicInteger.class);

        // Should be called only 1x if cache works
        verify(meterRegistryMock, times(1)).gauge(eq(METRIC_OPEN_SSE_CONNECTIONS), tagsCaptor.capture(), initialValueCaptor.capture());

        assertNotNull(tagsCaptor.getValue());
        assertEquals(0, initialValueCaptor.getValue().get());
    }

    SubscriptionResource createSubscriptionResource() {
        var subscriptionResource = new SubscriptionResource();

        subscriptionResource.getMetadata().setName(MockHelper.TEST_SUBSCRIPTION_ID);
        var spec = new SubscriptionResourceSpec();
        var sub = new Subscription();
        sub.setSubscriptionId(MockHelper.TEST_SUBSCRIPTION_ID);
        sub.setDeliveryType(DeliveryType.SERVER_SENT_EVENT.getValue());
        sub.setType("foo.bar.v1");
        spec.setSubscription(sub);
        subscriptionResource.setSpec(spec);

        return subscriptionResource;
    }
}
