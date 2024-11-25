// Copyright 2024 Deutsche Telekom IT GmbH
//
// SPDX-License-Identifier: Apache-2.0

package de.telekom.horizon.pulsar.service;

import com.hazelcast.cluster.Cluster;
import com.hazelcast.cluster.Member;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.topic.ITopic;
import de.telekom.horizon.pulsar.cache.ConnectionCache;
import de.telekom.horizon.pulsar.exception.SubscriberDoesNotMatchSubscriptionException;
import de.telekom.horizon.pulsar.helper.SseTaskStateContainer;
import de.telekom.horizon.pulsar.helper.StreamLimit;
import de.telekom.horizon.pulsar.testutils.MockHelper;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.*;


@ExtendWith(MockitoExtension.class)
class SseServiceTest {

    SseService sseService;

    @Mock
    SseTaskFactory sseTaskFactoryMock;

    @BeforeEach
    void setupSseServiceTest() {
        MockHelper.init();

        sseService = spy(new SseService(MockHelper.tokenService, sseTaskFactoryMock, MockHelper.subscriberCache, MockHelper.pulsarConfig));
    }

    @Test
    void testValidateSubscriberIdForSubscriptionIfSubscriberIdIsBlank() {
        when(MockHelper.pulsarConfig.isEnableSubscriberCheck()).thenReturn(true);
        when(MockHelper.tokenService.getSubscriberId()).thenReturn("");

        Assertions.assertThrows(SubscriberDoesNotMatchSubscriptionException.class, () ->
            sseService.validateSubscriberIdForSubscription(MockHelper.TEST_ENVIRONMENT, MockHelper.TEST_SUBSCRIPTION_ID)
        );
    }

    @Test
    void testValidateSubscriberIdForSubscriberIdIsEqualToSubscriberCache() {
        when(MockHelper.pulsarConfig.isEnableSubscriberCheck()).thenReturn(true);
        when(MockHelper.tokenService.getSubscriberId()).thenReturn(MockHelper.TEST_SUBSCRIBER_ID);

        SubscriberDoesNotMatchSubscriptionException exception = Assertions.assertThrows(SubscriberDoesNotMatchSubscriptionException.class, () ->
            sseService.validateSubscriberIdForSubscription(MockHelper.TEST_ENVIRONMENT, MockHelper.TEST_SUBSCRIPTION_ID)
        );
        Assertions.assertEquals(String.format("The subscription does not belong to subscriber with id '%s'", MockHelper.TEST_SUBSCRIBER_ID), exception.getMessage());

        Assertions.assertNotEquals(MockHelper.TEST_SUBSCRIBER_ID, MockHelper.subscriberCache.getSubscriberId(MockHelper.TEST_SUBSCRIPTION_ID).orElse(null));
    }

    @Test
    void testStartEmittingEvents() {
        var sseTaskStateContainerCaptor = ArgumentCaptor.forClass(SseTaskStateContainer.class);

        // Get ThreadPoolTaskExecutor via Reflections since it's private
        var taskExecutor = (ThreadPoolTaskExecutor) ReflectionTestUtils.getField(sseService,"taskExecutor");

        assertNotNull(taskExecutor);

        // Crate a spy of ThreadPoolTaskExecutor to verify invocations
        var taskExecutorSpy = Mockito.spy(taskExecutor);

        // Inject spy of ThreadPoolTaskExecutor again into SseService
        ReflectionTestUtils.setField(sseService, "taskExecutor", taskExecutorSpy, ThreadPoolTaskExecutor.class);

        // Invoke private init() method that usually gets called in the constructor
        ReflectionTestUtils.invokeMethod(sseService, "init");

        // Verify that ThreadPoolTaskExecutor has been set up correctly
        verify(taskExecutorSpy).setMaxPoolSize(MockHelper.pulsarConfig.getThreadPoolSize());
        verify(taskExecutorSpy).setCorePoolSize(MockHelper.pulsarConfig.getThreadPoolSize());
        verify(taskExecutorSpy).setQueueCapacity(MockHelper.pulsarConfig.getQueueCapacity());
        verify(taskExecutorSpy).afterPropertiesSet();

        var sseTask = new SseTask(Mockito.mock(SseTaskStateContainer.class), Mockito.mock(EventMessageSupplier.class), MockHelper.openConnectionGaugeValue, sseTaskFactoryMock);

        // We are spying on the actual task here
        var sseTaskSpy= Mockito.spy(sseTask);

        var hazelcastInstanceMock = Mockito.mock(HazelcastInstance.class);

        var cacheClusterMock = Mockito.mock(Cluster.class);
        var cacheMemberMock = Mockito.mock(Member.class);

        when(hazelcastInstanceMock.getCluster()).thenReturn(cacheClusterMock);
        when(cacheClusterMock.getLocalMember()).thenReturn(cacheMemberMock);
        when(cacheMemberMock.getUuid()).thenReturn(UUID.fromString("477bf3c9-ef1f-41de-9574-419a2ab61131"));

        var cacheWorkers = Mockito.mock(ITopic.class);
        when(hazelcastInstanceMock.getTopic("workers")).thenReturn(cacheWorkers);

        var connectionCache = new ConnectionCache(hazelcastInstanceMock);

        var map = new ConcurrentHashMap<>();
        map.put(MockHelper.TEST_SUBSCRIPTION_ID, sseTaskSpy);
        ReflectionTestUtils.setField(connectionCache, "map", map, ConcurrentHashMap.class);

        var connectionCacheSpy = Mockito.spy(connectionCache);

        when(sseTaskFactoryMock.getConnectionCache()).thenReturn(connectionCacheSpy);
        when(sseTaskFactoryMock.createNew(eq(MockHelper.TEST_ENVIRONMENT), eq(MockHelper.TEST_SUBSCRIPTION_ID), eq(MockHelper.TEST_CONTENT_TYPE), sseTaskStateContainerCaptor.capture(), eq(false), anyString(), any(StreamLimit.class))).thenReturn(sseTaskSpy);

        // The mocked task should trigger the termination condition of SseTaskStateContainer.setReady(long timeout) immediately
        // otherwise startEmittingEvents() would run until the timeout is reached, since setReady() is not called asynchronously
        // in this test
        /*doAnswer(x -> {
            sseTaskStateContainerCaptor.getValue().getRunning().compareAndExchange(false, true);
            return null;
        }).when(sseTaskMock).run();*/

        var latch = new CountDownLatch(1);

        // We create another thread here that will cause the test
        // to fail if the termination condition of SseTaskStateContainer.setReady(long timeout)
        // would not work correctly
        new Thread(() -> {
            try {
                var reachedZero = latch.await(10, TimeUnit.SECONDS);
                assertTrue(reachedZero);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }).start();

        // PUBLIC METHOD WE WANT TO TEST
        var responseContainer = sseService.startEmittingEvents(MockHelper.TEST_ENVIRONMENT, MockHelper.TEST_SUBSCRIPTION_ID, MockHelper.TEST_CONTENT_TYPE, false, anyString(), new StreamLimit());

        latch.countDown();

        assertNotNull(responseContainer);
        verify(taskExecutorSpy).submit(sseTaskSpy);

        sseService.stopEmittingEvents(MockHelper.TEST_SUBSCRIPTION_ID);

        verify(sseTaskSpy).terminate();
        verify(connectionCacheSpy).removeConnectionForSubscription(MockHelper.TEST_SUBSCRIPTION_ID);
    }
}


