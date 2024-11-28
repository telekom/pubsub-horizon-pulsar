// Copyright 2024 Deutsche Telekom IT GmbH
//
// SPDX-License-Identifier: Apache-2.0

package de.telekom.horizon.pulsar.service;

import brave.Span;
import brave.Tracer;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import de.telekom.eni.pandora.horizon.kafka.event.EventWriter;
import de.telekom.eni.pandora.horizon.model.event.*;
import de.telekom.eni.pandora.horizon.tracing.HorizonTracer;
import de.telekom.horizon.pulsar.exception.ConnectionCutOutException;
import de.telekom.horizon.pulsar.exception.ConnectionTimeoutException;
import de.telekom.horizon.pulsar.exception.StreamLimitExceededException;
import de.telekom.horizon.pulsar.helper.EventMessageContext;
import de.telekom.horizon.pulsar.helper.SseTaskStateContainer;
import de.telekom.horizon.pulsar.helper.StreamLimit;
import de.telekom.horizon.pulsar.testutils.MockHelper;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.support.SendResult;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.web.servlet.mvc.method.annotation.ResponseBodyEmitter;

import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static de.telekom.horizon.pulsar.testutils.MockHelper.metricsHelper;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
@Slf4j
class SseTaskTest {
    SseTask sseTaskSpy;

    @Mock
    SseTaskStateContainer sseTaskStateContainerMock;

    @Mock
    EventMessageSupplier eventMessageSupplierMock;

    @BeforeEach
    void setupSseServiceTest() {
        MockHelper.init();

        var sseTask = new SseTask(sseTaskStateContainerMock, eventMessageSupplierMock, MockHelper.openConnectionGaugeValue, MockHelper.sseTaskFactory);
        sseTaskSpy = spy(sseTask);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testRun(boolean ignoreDeduplication) throws IOException {
        final var o = new ObjectMapper();
        // For this test we set the timeout to 10s to simulate the automatic timeout if there are no new events flowing
        final var timeout = 10000L;
        when(MockHelper.pulsarConfig.getSseTimeout()).thenReturn(timeout);

        // We create a new SubscriptionEventMessage queue and add some test messages
        var itemQueue = new ConcurrentLinkedQueue<SubscriptionEventMessage>();
        itemQueue.add(MockHelper.createSubscriptionEventMessageForTesting(DeliveryType.SERVER_SENT_EVENT));
        var item = MockHelper.createSubscriptionEventMessageForTesting(DeliveryType.SERVER_SENT_EVENT);
        itemQueue.add(item);

        // add duplicates
        var duplicatesNumber = 2;
        for (var i = 0; i < duplicatesNumber; i++) {
            itemQueue.add(item);
        }

        final var itemQueueInitialSize = itemQueue.size();

        // We check whether an EventWriter has been created successfully
        // since it is not accessible, we use ReflectionTestUtils.
        // We then overwrite it with a mock, so that we can do checks on it
        var eventWriter = (EventWriter) ReflectionTestUtils.getField(sseTaskSpy,"eventWriter");
        assertNotNull(eventWriter);
        var eventWriterMock = mock(EventWriter.class);
        ReflectionTestUtils.setField(sseTaskSpy,"eventWriter", eventWriterMock);

        // We assume the task has not been canceled
        when(sseTaskStateContainerMock.getCanceled()).thenReturn(new AtomicBoolean(false));
        // We assume the task is not running in the beginning
        var taskRunningSpy = spy(new AtomicBoolean(false));
        when(sseTaskStateContainerMock.getRunning()).thenReturn(taskRunningSpy);
        // Mock the ResponseBodyEmitter so that we can verify data is actually sent
        var emitterMock = mock(ResponseBodyEmitter.class);
        when(sseTaskStateContainerMock.getEmitter()).thenReturn(emitterMock);
        // We mock the EventMessageSupplier since it's tested in a separate test
        when(eventMessageSupplierMock.get()).thenAnswer(i -> {
            await().pollDelay(10, TimeUnit.MILLISECONDS).untilTrue(new AtomicBoolean(true));
            return new EventMessageContext(itemQueue.poll(), false, new StreamLimit(), ignoreDeduplication, Mockito.mock(Span.class), Mockito.mock(Tracer.SpanInScope.class));
        });
        when(eventMessageSupplierMock.getSubscriptionId()).thenReturn(MockHelper.TEST_SUBSCRIPTION_ID);
        // The following checks that we track a DELIVERED event with the de-duplication service
        // which is done in the callback of the eventwriter
        SendResult<String, String> sendResult = mock(SendResult.class);
        var succeededFuture = CompletableFuture.completedFuture(sendResult);
        when(eventWriterMock.send(anyString(), notNull(), any())).thenReturn(succeededFuture);

        // Used for verifying the timout worked
        var started = Instant.now();

        var counterMock = Mockito.mock(Counter.class);
        var registryMock = Mockito.mock(MeterRegistry.class);
        when(registryMock.counter(any(), any(Tags.class))).thenReturn(counterMock);
        when(metricsHelper.buildTagsFromSubscriptionEventMessage(any())).thenReturn(Tags.empty());
        when(metricsHelper.getRegistry()).thenReturn(registryMock);

        var mockedDeduplicationCache = new HashMap<String, String>();
        when(MockHelper.deDuplicationService.track(any(SubscriptionEventMessage.class))).thenAnswer(invocation -> {
            SubscriptionEventMessage msg = invocation.getArgument(0);
            return mockedDeduplicationCache.put(msg.getUuid(), msg.getUuid());
        });

        if (!ignoreDeduplication) {
            when(MockHelper.deDuplicationService.get(any(SubscriptionEventMessage.class))).thenAnswer(invocation -> {
                SubscriptionEventMessage msg = invocation.getArgument(0);
                return mockedDeduplicationCache.get(msg.getUuid());
            });
        }

        // PUBLIC METHOD WE WANT TO TEST
        sseTaskSpy.run();

        // Used for verifying the timout worked
        var stopped = Instant.now();

        // Verify that metrics for a new open connection are handled
        verify(MockHelper.openConnectionGaugeValue, times(1)).getAndSet(1);
        // Verify that task status switched from not running to running
        verify(taskRunningSpy, times(1)).compareAndExchange(false, true);
        // Verify that a timeout happened
        verify(emitterMock, times(1)).completeWithError(any(ConnectionTimeoutException.class));
        // Verify that never a connection cut out happened
        verify(emitterMock, never()).completeWithError(any(ConnectionCutOutException.class));
        // Verify that ResponseBodyEmitter emits data
        ArgumentCaptor<String> jsonStringCaptor = ArgumentCaptor.forClass(String.class);
        // Verify that for each emitted event a DELIVERED status will be written
        ArgumentCaptor<StatusMessage> statusMessageCaptor = ArgumentCaptor.forClass(StatusMessage.class);

        if (ignoreDeduplication) {
            verify(emitterMock, times(itemQueueInitialSize)).send(jsonStringCaptor.capture());
            verify(eventWriterMock, times(itemQueueInitialSize)).send(anyString(), statusMessageCaptor.capture(), any(HorizonTracer.class));
            // Verify that de-duplication logic is bypassed
            verify(MockHelper.deDuplicationService, times(itemQueueInitialSize)).track(any());
        } else {
            verify(emitterMock, times(itemQueueInitialSize - duplicatesNumber)).send(jsonStringCaptor.capture());
            verify(eventWriterMock, times(itemQueueInitialSize - duplicatesNumber)).send(anyString(), statusMessageCaptor.capture(), any(HorizonTracer.class));
            // Verify that for each emitted event a de-duplication entry will be written
            verify(MockHelper.deDuplicationService, times(itemQueueInitialSize - duplicatesNumber)).track(any());
        }

        assertNotNull(jsonStringCaptor.getValue());
        var event = o.readValue(jsonStringCaptor.getValue(), Event.class);
        assertNotNull(event);

        assertNotNull(statusMessageCaptor.getValue());
        assertEquals(Status.DELIVERED, statusMessageCaptor.getValue().getStatus());

        // Verify that ResponseBodyEmitter finishes with a ConnectionTimeoutException
        verify(emitterMock, times(1)).completeWithError(any(ConnectionTimeoutException.class));
        // Verify that emitter has completed
        verify(emitterMock, times(1)).onCompletion(any());
        // Verify that metrics for a closed connection are handled
        verify(MockHelper.openConnectionGaugeValue, times(1)).getAndSet(0);
        // Verify that the timout worked
        assertEquals(timeout, stopped.minusMillis(started.toEpochMilli()).getEpochSecond() * 1000);
    }

    @Test
    void testTerminateConnection() throws InterruptedException, IOException {
        // We check whether an EventWriter has been created successfully
        // since it is not accessible, we use ReflectionTestUtils.
        // We then overwrite it with a mock, so that we can do checks on it
        var eventWriter = (EventWriter) ReflectionTestUtils.getField(sseTaskSpy,"eventWriter");
        assertNotNull(eventWriter);
        var eventWriterMock = mock(EventWriter.class);
        ReflectionTestUtils.setField(sseTaskSpy,"eventWriter", eventWriterMock);

        // We assume the task has not been canceled
        when(sseTaskStateContainerMock.getCanceled()).thenReturn(new AtomicBoolean(false));
        // We assume the task is not running in the beginning
        var taskRunningSpy = spy(new AtomicBoolean(false));
        when(sseTaskStateContainerMock.getRunning()).thenReturn(taskRunningSpy);

        // Mock the ResponseBodyEmitter so that we can verify data is actually sent
        var emitterMock = mock(ResponseBodyEmitter.class);
        when(sseTaskStateContainerMock.getEmitter()).thenReturn(emitterMock);

        var latch = new CountDownLatch(1);

        new Thread(() -> {
            // Connection cut out after 3 seconds
            await().pollDelay(3, TimeUnit.SECONDS).untilTrue(new AtomicBoolean(true));

            // PUBLIC METHOD WE WANT TO TEST
            sseTaskSpy.terminate();
            latch.countDown();
        }).start();

        var streamLimit = new StreamLimit(); // default values

        // We mock the EventMessageSupplier and let the mock always answer with a new event message to simulate
        // an endless stream
        when(eventMessageSupplierMock.get()).thenAnswer(i -> {
            await().pollDelay(10, TimeUnit.MILLISECONDS).untilTrue(new AtomicBoolean(true));
            return new EventMessageContext(MockHelper.createSubscriptionEventMessageForTesting(DeliveryType.SERVER_SENT_EVENT), false, streamLimit, false, Mockito.mock(Span.class), Mockito.mock(Tracer.SpanInScope.class));
        });
        when(eventMessageSupplierMock.getSubscriptionId()).thenReturn(MockHelper.TEST_SUBSCRIPTION_ID);
        // The following checks that we track a DELIVERED event with the de-duplication service
        // which is done in the callback of the eventwriter
        SendResult<String, String> sendResult = mock(SendResult.class);
        var succeededFuture = CompletableFuture.completedFuture(sendResult);
        when(eventWriterMock.send(anyString(), notNull(), any())).thenReturn(succeededFuture);

        var counterMock = Mockito.mock(Counter.class);
        var registryMock = Mockito.mock(MeterRegistry.class);
        when(registryMock.counter(any(), any(Tags.class))).thenReturn(counterMock);
        when(metricsHelper.buildTagsFromSubscriptionEventMessage(any())).thenReturn(Tags.empty());
        when(metricsHelper.getRegistry()).thenReturn(registryMock);

        sseTaskSpy.run();

        var reachedZero = latch.await(10000, TimeUnit.MILLISECONDS);
        assertTrue(reachedZero);

        verify(emitterMock, atLeast(1)).send(anyString());

        // Verify that ResponseBodyEmitter finishes with a ConnectionCutOutException
        verify(emitterMock, times(1)).completeWithError(any(ConnectionCutOutException.class));
        // Verify that emitter has completed
        verify(emitterMock, times(1)).onCompletion(any());
        // Verify that metrics for a closed connection are handled
        verify(MockHelper.openConnectionGaugeValue, times(1)).getAndSet(0);
    }

    @Test
    void testTerminateConnectionThroughMaxNumberStreamLimit() throws IOException {
        // We check whether an EventWriter has been created successfully
        // since it is not accessible, we use ReflectionTestUtils.
        // We then overwrite it with a mock, so that we can do checks on it
        var eventWriter = (EventWriter) ReflectionTestUtils.getField(sseTaskSpy,"eventWriter");
        assertNotNull(eventWriter);
        var eventWriterMock = mock(EventWriter.class);
        ReflectionTestUtils.setField(sseTaskSpy,"eventWriter", eventWriterMock);

        // We assume the task has not been canceled
        when(sseTaskStateContainerMock.getCanceled()).thenReturn(new AtomicBoolean(false));
        // We assume the task is not running in the beginning
        var taskRunningSpy = spy(new AtomicBoolean(false));
        when(sseTaskStateContainerMock.getRunning()).thenReturn(taskRunningSpy);

        // Mock the ResponseBodyEmitter so that we can verify data is actually sent
        var emitterMock = mock(ResponseBodyEmitter.class);
        when(sseTaskStateContainerMock.getEmitter()).thenReturn(emitterMock);

        var maxNumberLimit = 5;

        var streamLimit = StreamLimit.of(maxNumberLimit, 0, 0);

        // We mock the EventMessageSupplier and let the mock always answer with a new event message to simulate
        // an endless stream
        when(eventMessageSupplierMock.get()).thenAnswer(i -> {
            await().pollDelay(10, TimeUnit.MILLISECONDS).untilTrue(new AtomicBoolean(true));
            return new EventMessageContext(MockHelper.createSubscriptionEventMessageForTesting(DeliveryType.SERVER_SENT_EVENT), false, streamLimit, false, Mockito.mock(Span.class), Mockito.mock(Tracer.SpanInScope.class));
        });
        when(eventMessageSupplierMock.getSubscriptionId()).thenReturn(MockHelper.TEST_SUBSCRIPTION_ID);
        // The following checks that we track a DELIVERED event with the de-duplication service
        // which is done in the callback of the eventwriter
        SendResult<String, String> sendResult = mock(SendResult.class);
        var succeededFuture = CompletableFuture.completedFuture(sendResult);
        when(eventWriterMock.send(anyString(), notNull(), any())).thenReturn(succeededFuture);

        var counterMock = Mockito.mock(Counter.class);
        var registryMock = Mockito.mock(MeterRegistry.class);
        when(registryMock.counter(any(), any(Tags.class))).thenReturn(counterMock);
        when(metricsHelper.buildTagsFromSubscriptionEventMessage(any())).thenReturn(Tags.empty());
        when(metricsHelper.getRegistry()).thenReturn(registryMock);

        sseTaskSpy.run();

        verify(emitterMock, times(maxNumberLimit)).send(anyString());

        // Verify that ResponseBodyEmitter finishes with a ConnectionCutOutException
        verify(emitterMock, times(1)).completeWithError(any(StreamLimitExceededException.class));
        // Verify that emitter has completed
        verify(emitterMock, times(1)).onCompletion(any());
        // Verify that metrics for a closed connection are handled
        verify(MockHelper.openConnectionGaugeValue, times(1)).getAndSet(0);
    }

    @Test
    void testTerminateConnectionThroughMaxByteStreamLimit() throws IOException {
        // We check whether an EventWriter has been created successfully
        // since it is not accessible, we use ReflectionTestUtils.
        // We then overwrite it with a mock, so that we can do checks on it
        var eventWriter = (EventWriter) ReflectionTestUtils.getField(sseTaskSpy,"eventWriter");
        assertNotNull(eventWriter);
        var eventWriterMock = mock(EventWriter.class);
        ReflectionTestUtils.setField(sseTaskSpy,"eventWriter", eventWriterMock);

        // We assume the task has not been canceled
        when(sseTaskStateContainerMock.getCanceled()).thenReturn(new AtomicBoolean(false));
        // We assume the task is not running in the beginning
        var taskRunningSpy = spy(new AtomicBoolean(false));
        when(sseTaskStateContainerMock.getRunning()).thenReturn(taskRunningSpy);

        // Mock the ResponseBodyEmitter so that we can verify data is actually sent
        var emitterMock = mock(ResponseBodyEmitter.class);
        when(sseTaskStateContainerMock.getEmitter()).thenReturn(emitterMock);

        var maxBytesLimit = 2000;

        var streamLimit = StreamLimit.of(0, 0, maxBytesLimit);

        // We mock the EventMessageSupplier and let the mock always answer with a new event message to simulate
        // an endless stream
        when(eventMessageSupplierMock.get()).thenAnswer(i -> {
            await().pollDelay(10, TimeUnit.MILLISECONDS).untilTrue(new AtomicBoolean(true));
            return new EventMessageContext(MockHelper.createSubscriptionEventMessageForTesting(DeliveryType.SERVER_SENT_EVENT), false, streamLimit, false, Mockito.mock(Span.class), Mockito.mock(Tracer.SpanInScope.class));
        });
        when(eventMessageSupplierMock.getSubscriptionId()).thenReturn(MockHelper.TEST_SUBSCRIPTION_ID);
        // The following checks that we track a DELIVERED event with the de-duplication service
        // which is done in the callback of the eventwriter
        SendResult<String, String> sendResult = mock(SendResult.class);
        var succeededFuture = CompletableFuture.completedFuture(sendResult);
        when(eventWriterMock.send(anyString(), notNull(), any())).thenReturn(succeededFuture);

        var counterMock = Mockito.mock(Counter.class);
        var registryMock = Mockito.mock(MeterRegistry.class);
        when(registryMock.counter(any(), any(Tags.class))).thenReturn(counterMock);
        when(metricsHelper.buildTagsFromSubscriptionEventMessage(any())).thenReturn(Tags.empty());
        when(metricsHelper.getRegistry()).thenReturn(registryMock);

        sseTaskSpy.run();

        ArgumentCaptor<String> jsonCaptor = ArgumentCaptor.forClass(String.class);

        verify(emitterMock, atLeast(1)).send(jsonCaptor.capture());

        var bytesSum = jsonCaptor.getAllValues().stream().mapToInt(x -> x.getBytes().length).sum();

        assertTrue(bytesSum >= maxBytesLimit);

        // Verify that ResponseBodyEmitter finishes with a ConnectionCutOutException
        verify(emitterMock, times(1)).completeWithError(any(StreamLimitExceededException.class));
        // Verify that emitter has completed
        verify(emitterMock, times(1)).onCompletion(any());
        // Verify that metrics for a closed connection are handled
        verify(MockHelper.openConnectionGaugeValue, times(1)).getAndSet(0);
    }

    @Test
    void testTerminateConnectionThroughMaxMinutesStreamLimit() throws IOException {
        // We check whether an EventWriter has been created successfully
        // since it is not accessible, we use ReflectionTestUtils.
        // We then overwrite it with a mock, so that we can do checks on it
        var eventWriter = (EventWriter) ReflectionTestUtils.getField(sseTaskSpy,"eventWriter");
        assertNotNull(eventWriter);
        var eventWriterMock = mock(EventWriter.class);
        ReflectionTestUtils.setField(sseTaskSpy,"eventWriter", eventWriterMock);

        // We assume the task has not been canceled
        when(sseTaskStateContainerMock.getCanceled()).thenReturn(new AtomicBoolean(false));
        // We assume the task is not running in the beginning
        var taskRunningSpy = spy(new AtomicBoolean(false));
        when(sseTaskStateContainerMock.getRunning()).thenReturn(taskRunningSpy);

        // Mock the ResponseBodyEmitter so that we can verify data is actually sent
        var emitterMock = mock(ResponseBodyEmitter.class);
        when(sseTaskStateContainerMock.getEmitter()).thenReturn(emitterMock);

        var maxMinutes = 1;

        var streamLimit = StreamLimit.of(0, maxMinutes, 0);

        // We mock the EventMessageSupplier and let the mock always answer with a new event message to simulate
        // an endless stream
        when(eventMessageSupplierMock.get()).thenAnswer(i -> {
            await().pollDelay(10, TimeUnit.MILLISECONDS).untilTrue(new AtomicBoolean(true));

            return new EventMessageContext(MockHelper.createSubscriptionEventMessageForTesting(DeliveryType.SERVER_SENT_EVENT), false, streamLimit, false, Mockito.mock(Span.class), Mockito.mock(Tracer.SpanInScope.class));
        });
        when(eventMessageSupplierMock.getSubscriptionId()).thenReturn(MockHelper.TEST_SUBSCRIPTION_ID);
        // The following checks that we track a DELIVERED event with the de-duplication service
        // which is done in the callback of the eventwriter
        SendResult<String, String> sendResult = mock(SendResult.class);
        var succeededFuture = CompletableFuture.completedFuture(sendResult);
        when(eventWriterMock.send(anyString(), notNull(), any())).thenReturn(succeededFuture);

        var counterMock = Mockito.mock(Counter.class);
        var registryMock = Mockito.mock(MeterRegistry.class);
        when(registryMock.counter(any(), any(Tags.class))).thenReturn(counterMock);
        when(metricsHelper.buildTagsFromSubscriptionEventMessage(any())).thenReturn(Tags.empty());
        when(metricsHelper.getRegistry()).thenReturn(registryMock);

        sseTaskSpy.run();

        assertEquals(maxMinutes, ChronoUnit.MINUTES.between(sseTaskSpy.getStartTime(), sseTaskSpy.getStopTime()));

        // Verify that ResponseBodyEmitter finishes with a ConnectionCutOutException
        verify(emitterMock, times(1)).completeWithError(any(StreamLimitExceededException.class));
        // Verify that emitter has completed
        verify(emitterMock, times(1)).onCompletion(any());
        // Verify that metrics for a closed connection are handled
        verify(MockHelper.openConnectionGaugeValue, times(1)).getAndSet(0);
    }

    @Test
    void testSendEventFailed() throws IOException {
        // We create a new SubscriptionEventMessage queue and add some test messages
        var itemQueue = new ConcurrentLinkedQueue<SubscriptionEventMessage>();
        itemQueue.add(MockHelper.createSubscriptionEventMessageForTesting(DeliveryType.SERVER_SENT_EVENT));
        itemQueue.add(MockHelper.createSubscriptionEventMessageForTesting(DeliveryType.SERVER_SENT_EVENT));

        final var itemQueueInitialSize = itemQueue.size();

        var objectMapperMock = mock(ObjectMapper.class);
        ReflectionTestUtils.setField(sseTaskSpy,"objectMapper", objectMapperMock);
        when(objectMapperMock.writeValueAsString(any(Event.class))).thenThrow(JsonProcessingException.class);

        // We check whether an EventWriter has been created successfully
        // since it is not accessible, we use ReflectionTestUtils.
        // We also create a spy from it and inject it again
        var eventWriter = (EventWriter) ReflectionTestUtils.getField(sseTaskSpy,"eventWriter");
        assertNotNull(eventWriter);
        var eventWriterMock = mock(EventWriter.class);
        ReflectionTestUtils.setField(sseTaskSpy,"eventWriter", eventWriterMock);

        // We assume the task has not been canceled
        when(sseTaskStateContainerMock.getCanceled()).thenReturn(new AtomicBoolean(false));
        // We assume the task is not running in the beginning
        var taskRunningSpy = spy(new AtomicBoolean(false));
        when(sseTaskStateContainerMock.getRunning()).thenReturn(taskRunningSpy);
        // Mock the ResponseBodyEmitter so that we can verify data is actually sent
        var emitterMock = mock(ResponseBodyEmitter.class);
        when(sseTaskStateContainerMock.getEmitter()).thenReturn(emitterMock);
        // We mock the EventMessageSupplier since it's tested in a separate test
        when(eventMessageSupplierMock.get()).thenAnswer(i -> {
            await().pollDelay(10, TimeUnit.MILLISECONDS).untilTrue(new AtomicBoolean(true));
            return new EventMessageContext(itemQueue.poll(), false, new StreamLimit(), false, Mockito.mock(Span.class), Mockito.mock(Tracer.SpanInScope.class));
        });
        when(eventMessageSupplierMock.getSubscriptionId()).thenReturn(MockHelper.TEST_SUBSCRIPTION_ID);
        // The following checks that we track a DELIVERED event with the de-duplication service
        // which is done in the callback of the eventwriter
        SendResult<String, String> sendResult = mock(SendResult.class);
        var succeededFuture = CompletableFuture.completedFuture(sendResult);
        when(eventWriterMock.send(anyString(), notNull(), any())).thenReturn(succeededFuture);

        sseTaskSpy.run();

        // Verify that ResponseBodyEmitter NEVER emitted any data
        verify(emitterMock, never()).send(anyString());
        // Verify that for each emitted event a FAILED status will be written
        ArgumentCaptor<StatusMessage> statusMessageCaptor = ArgumentCaptor.forClass(StatusMessage.class);
        try {
            verify(eventWriterMock, times(itemQueueInitialSize)).send(anyString(), statusMessageCaptor.capture(), any(HorizonTracer.class));
        } catch (JsonProcessingException e) {
            fail(e);
        }
        assertNotNull(statusMessageCaptor.getValue());
        assertEquals(Status.FAILED, statusMessageCaptor.getValue().getStatus());
        // Verify that emitter has completed
        verify(emitterMock, times(1)).onCompletion(any());
        // Verify that metrics for a closed connection are handled
        verify(MockHelper.openConnectionGaugeValue, times(1)).getAndSet(0);
        // Verify that for each emitted event a de-duplication entry will be written
        verify(MockHelper.deDuplicationService, times(itemQueueInitialSize)).track(any());
    }
}
