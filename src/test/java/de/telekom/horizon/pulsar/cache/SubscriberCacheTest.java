// Copyright 2024 Deutsche Telekom IT GmbH
//
// SPDX-License-Identifier: Apache-2.0

package de.telekom.horizon.pulsar.cache;

import de.telekom.eni.pandora.horizon.cache.service.JsonCacheService;
import de.telekom.eni.pandora.horizon.exception.JsonCacheException;
import de.telekom.eni.pandora.horizon.kubernetes.resource.Subscription;
import de.telekom.eni.pandora.horizon.kubernetes.resource.SubscriptionResource;
import de.telekom.eni.pandora.horizon.kubernetes.resource.SubscriptionResourceSpec;
import de.telekom.horizon.pulsar.config.PulsarConfig;
import de.telekom.horizon.pulsar.testutils.MockHelper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class SubscriberCacheTest {

    @Mock
    JsonCacheService<SubscriptionResource> jsonCacheService;

    @Mock
    PulsarConfig pulsarConfig;
    
    SubscriberCache cache;

    @BeforeEach
    void setupSseServiceTest() {
        MockHelper.init();

        this.cache = new SubscriberCache(pulsarConfig, jsonCacheService);
        
    }

    @Test
    void testGet() throws JsonCacheException {
        {
            var result = cache.getSubscriberId(MockHelper.TEST_SUBSCRIPTION_ID);
            assertThat(result).isEmpty();
        }

        var resource = new SubscriptionResource();
        var spec = new SubscriptionResourceSpec();
        var subscription = new Subscription();
        subscription.setSubscriberId(MockHelper.TEST_SUBSCRIBER_ID);
        spec.setSubscription(subscription);
        resource.setSpec(spec);

        when(jsonCacheService.getByKey(MockHelper.TEST_SUBSCRIPTION_ID)).thenReturn(Optional.of(resource));

        // We get the entry that we added before
        {
            var result = cache.getSubscriberId(MockHelper.TEST_SUBSCRIPTION_ID);
            assertThat(result.orElse(null)).isEqualTo(MockHelper.TEST_SUBSCRIBER_ID);
        }
    }
}
