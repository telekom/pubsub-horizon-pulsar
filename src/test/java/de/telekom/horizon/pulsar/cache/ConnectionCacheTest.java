// Copyright 2024 Deutsche Telekom IT GmbH
//
// SPDX-License-Identifier: Apache-2.0

package de.telekom.horizon.pulsar.cache;

import com.hazelcast.cluster.Cluster;
import com.hazelcast.cluster.Member;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.topic.ITopic;
import de.telekom.horizon.pulsar.service.SseTask;
import de.telekom.horizon.pulsar.testutils.MockHelper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class ConnectionCacheTest {

    @Mock
    HazelcastInstance hazelcastInstance;

    ConnectionCache cache;

    @BeforeEach
    void setupSseServiceTest() {
        MockHelper.init();

        var cluster = Mockito.mock(Cluster.class);
        var member = Mockito.mock(Member.class);

        when(hazelcastInstance.getCluster()).thenReturn(cluster);
        when(cluster.getLocalMember()).thenReturn(member);
        when(member.getUuid()).thenReturn(UUID.fromString("477bf3c9-ef1f-41de-9574-419a2ab61131"));

        var workers = Mockito.mock(ITopic.class);
        when(hazelcastInstance.getTopic("workers")).thenReturn(workers);

        this.cache = new ConnectionCache(MockHelper.pulsarConfig, hazelcastInstance);
    }

    @Test
    void testAddConnectionForSubscription() {
        var sseTaskMock = Mockito.mock(SseTask.class);

        // Let's access the private map, create a spy from it and inject it again
        var map = (ConcurrentHashMap<String, SseTask>) ReflectionTestUtils.getField(cache, "map");
        var mapSpy = Mockito.spy(map);
        ReflectionTestUtils.setField(cache, "map", mapSpy);

        // We add a new connection for a subscriptionId
        cache.claimConnectionForSubscription(MockHelper.TEST_SUBSCRIPTION_ID, sseTaskMock);
        // Check that map contains only one entry with the mocked task
        assertThat(mapSpy).hasSize(1);
        assertThat(mapSpy.values().stream().findFirst().orElse(null)).isEqualTo(sseTaskMock);
        // Since we start with an empty cache we can assume there was no existing connection before
        // and therefore no connection will be terminated
        verify(sseTaskMock, never()).terminate();

    }

    @Test
    void testAddConnectionForSubscriptionMultipleTimes() {
        var sseTaskMock = Mockito.mock(SseTask.class);

        // Let's access the private map, create a spy from it and inject it again
        var map = (ConcurrentHashMap<String, SseTask>) ReflectionTestUtils.getField(cache, "map");
        var mapSpy = Mockito.spy(map);
        ReflectionTestUtils.setField(cache, "map", mapSpy);

        // We add a new connection for a subscriptionId
        cache.claimConnectionForSubscription(MockHelper.TEST_SUBSCRIPTION_ID, sseTaskMock);
        // We do that a second time
        cache.claimConnectionForSubscription(MockHelper.TEST_SUBSCRIPTION_ID, sseTaskMock);
        // Check that map contains only one entry
        assertThat(mapSpy).hasSize(1);
        // And another one, but this time with a different subscriptionId
        cache.claimConnectionForSubscription(MockHelper.TEST_SUBSCRIPTION_ID + "foobar", sseTaskMock);
        // Check that map contains two entries
        assertThat(mapSpy).hasSize(2);
        // We check that one connection has been terminated
        verify(sseTaskMock, times(1)).terminate();
    }

    @Test
    void testRemoveConnectionForSubscription() {
        var sseTaskMock = Mockito.mock(SseTask.class);

        // Let's access the private map, create a spy from it and inject it again
        var map = (ConcurrentHashMap<String, SseTask>) ReflectionTestUtils.getField(cache, "map");
        var mapSpy = Mockito.spy(map);
        ReflectionTestUtils.setField(cache, "map", mapSpy);

        // We add a new connection for a subscriptionId
        cache.claimConnectionForSubscription(MockHelper.TEST_SUBSCRIPTION_ID, sseTaskMock);
        // Check that map contains only one entry
        assertThat(mapSpy).hasSize(1);
        // Let's remove it again
        cache.removeConnectionForSubscription(MockHelper.TEST_SUBSCRIPTION_ID);
        // Check that map contains only one entry
        assertThat(mapSpy).isEmpty();
        // We check that the connection has been terminated
        verify(sseTaskMock, times(1)).terminate();
    }
}
