// Copyright 2024 Deutsche Telekom IT GmbH
//
// SPDX-License-Identifier: Apache-2.0

package de.telekom.horizon.pulsar.cache;

import de.telekom.eni.pandora.horizon.cache.service.JsonCacheService;
import de.telekom.eni.pandora.horizon.exception.JsonCacheException;
import de.telekom.eni.pandora.horizon.kubernetes.resource.Subscription;
import de.telekom.eni.pandora.horizon.kubernetes.resource.SubscriptionResource;
import de.telekom.eni.pandora.horizon.kubernetes.resource.SubscriptionResourceSpec;
import de.telekom.eni.pandora.horizon.metrics.HorizonMetricsHelper;
import de.telekom.eni.pandora.horizon.model.event.DeliveryType;
import de.telekom.horizon.pulsar.testutils.MockHelper;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import static de.telekom.eni.pandora.horizon.metrics.HorizonMetricsConstants.METRIC_OPEN_SSE_CONNECTIONS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
@Slf4j
class ConnectionGaugeCacheTest {

    @Mock
    JsonCacheService<SubscriptionResource> jsonCacheService;

    ConnectionGaugeCache cache;

    MeterRegistry meterRegistryMock;

    @BeforeEach
    void setupSseServiceTest() {
        MockHelper.init();

        var metricsHelperMock = Mockito.mock(HorizonMetricsHelper.class);
        meterRegistryMock = Mockito.mock(MeterRegistry.class);
        when(metricsHelperMock.getRegistry()).thenReturn(meterRegistryMock);

        this.cache = new ConnectionGaugeCache(MockHelper.pulsarConfig, jsonCacheService, metricsHelperMock);
    }

    @Test
    void getOrCreateGaugeForSubscription() throws JsonCacheException {
        final var gauge = new AtomicInteger(1);

        var subscription = createSubscriptionResource();
        when(jsonCacheService.getByKey(subscription.spec.getSubscription().getSubscriptionId())).thenReturn(Optional.of(subscription));
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

        var spec = new SubscriptionResourceSpec();
        spec.setEnvironment(MockHelper.TEST_ENVIRONMENT);
        var sub = new Subscription();
        sub.setSubscriptionId(MockHelper.TEST_SUBSCRIPTION_ID);
        sub.setDeliveryType(DeliveryType.SERVER_SENT_EVENT.getValue());
        sub.setType("foo.bar.v1");
        spec.setSubscription(sub);
        subscriptionResource.setSpec(spec);

        return subscriptionResource;
    }
}
