// Copyright 2024 Deutsche Telekom IT GmbH
//
// SPDX-License-Identifier: Apache-2.0

package de.telekom.horizon.pulsar.cache;

import de.telekom.horizon.pulsar.config.PulsarConfig;
import de.telekom.horizon.pulsar.testutils.MockHelper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.concurrent.ConcurrentHashMap;

import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(MockitoExtension.class)
class SubscriberCacheTest {

    @Mock
   PulsarConfig pulsarConfig;
    
    SubscriberCache cache;

    @BeforeEach
    void setupSseServiceTest() {
        MockHelper.init();

        this.cache = new SubscriberCache(pulsarConfig);
        
    }

    @Test
    void testAdd() {
        // Let's access the private map, create a spy from it and inject it again
        var map = (ConcurrentHashMap<String, String>) ReflectionTestUtils.getField(cache, "map");
        var mapSpy = Mockito.spy(map);
        ReflectionTestUtils.setField(cache, "map", mapSpy);

        // We add a subscriberId entry for an environment/subscriptionId combination
        cache.add(MockHelper.TEST_ENVIRONMENT, MockHelper.TEST_SUBSCRIPTION_ID, MockHelper.TEST_SUBSCRIBER_ID);
        // Check that map contains only one entry with the mocked task
        assertThat(mapSpy).hasSize(1);
        assertThat(mapSpy.values().stream().findFirst().orElse(null)).isEqualTo(MockHelper.TEST_SUBSCRIBER_ID);
    }

    @Test
    void testRemove() {
        // Let's access the private map, create a spy from it and inject it again
        var map = (ConcurrentHashMap<String, String>) ReflectionTestUtils.getField(cache, "map");
        var mapSpy = Mockito.spy(map);
        ReflectionTestUtils.setField(cache, "map", mapSpy);

        // We add a subscriberId entry for an environment/subscriptionId combination
        cache.add(MockHelper.TEST_ENVIRONMENT, MockHelper.TEST_SUBSCRIPTION_ID, MockHelper.TEST_SUBSCRIBER_ID);
        // Check that map contains only one entry
        assertThat(mapSpy).hasSize(1);
        // Let's remove it again
        cache.remove(MockHelper.TEST_ENVIRONMENT, MockHelper.TEST_SUBSCRIPTION_ID);
        // Check that map contains only one entry
        assertThat(mapSpy).isEmpty();
    }

    @Test
    void testGet() {
        // We try to access an entry from an empty cache
        {
            var result = cache.get(MockHelper.TEST_ENVIRONMENT, MockHelper.TEST_SUBSCRIPTION_ID);
            assertThat(result).isNull();
        }
        // We add a subscriberId entry for an environment/subscriptionId combination
        cache.add(MockHelper.TEST_ENVIRONMENT, MockHelper.TEST_SUBSCRIPTION_ID, MockHelper.TEST_SUBSCRIBER_ID);
        // We try to access an entry with a key that does not exist
        {
            var result = cache.get(MockHelper.TEST_ENVIRONMENT, MockHelper.TEST_SUBSCRIPTION_ID + "foobar");
            assertThat(result).isNull();
        }
        // We get the entry that we added before
        {
            var result = cache.get(MockHelper.TEST_ENVIRONMENT, MockHelper.TEST_SUBSCRIPTION_ID);
            assertThat(result).isEqualTo(MockHelper.TEST_SUBSCRIBER_ID);
        }
    }
}
