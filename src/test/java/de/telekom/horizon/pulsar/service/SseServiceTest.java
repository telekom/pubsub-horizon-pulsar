package de.telekom.horizon.pulsar.service;

import de.telekom.horizon.pulsar.exception.SubscriberDoesNotMatchSubscriptionException;
import de.telekom.horizon.pulsar.helper.SseTaskStateContainer;
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

        sseService = spy(new SseService(MockHelper.tokenService, sseTaskFactoryMock, MockHelper.subscriptionResourceListener, MockHelper.subscriberCache, MockHelper.pulsarConfig, MockHelper.deDuplicationService));
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

        Assertions.assertNotEquals(MockHelper.TEST_SUBSCRIBER_ID, MockHelper.subscriberCache.get(MockHelper.TEST_ENVIRONMENT, MockHelper.TEST_SUBSCRIPTION_ID));
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

        // We are mocking the actual task here
        var sseTaskMock = Mockito.mock(SseTask.class);

        when(sseTaskFactoryMock.createNew(eq(MockHelper.TEST_ENVIRONMENT), eq(MockHelper.TEST_SUBSCRIPTION_ID), eq(MockHelper.TEST_CONTENT_TYPE), sseTaskStateContainerCaptor.capture(), eq(false))).thenReturn(sseTaskMock);

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
        var responseContainer = sseService.startEmittingEvents(MockHelper.TEST_ENVIRONMENT, MockHelper.TEST_SUBSCRIPTION_ID, MockHelper.TEST_CONTENT_TYPE, false);

        latch.countDown();

        assertNotNull(responseContainer);

        verify(taskExecutorSpy).submit(sseTaskMock);
    }
}


