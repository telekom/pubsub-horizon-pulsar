// Copyright 2024 Deutsche Telekom IT GmbH
//
// SPDX-License-Identifier: Apache-2.0

package de.telekom.horizon.pulsar.testutils;

import brave.ScopedSpan;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import de.telekom.eni.pandora.horizon.cache.service.DeDuplicationService;
import de.telekom.eni.pandora.horizon.kafka.config.KafkaProperties;
import de.telekom.eni.pandora.horizon.kafka.event.EventWriter;
import de.telekom.eni.pandora.horizon.metrics.HorizonMetricsHelper;
import de.telekom.eni.pandora.horizon.model.db.Coordinates;
import de.telekom.eni.pandora.horizon.model.db.PartialEvent;
import de.telekom.eni.pandora.horizon.model.db.State;
import de.telekom.eni.pandora.horizon.model.event.DeliveryType;
import de.telekom.eni.pandora.horizon.model.event.Event;
import de.telekom.eni.pandora.horizon.model.event.Status;
import de.telekom.eni.pandora.horizon.model.event.SubscriptionEventMessage;
import de.telekom.eni.pandora.horizon.mongo.model.MessageStateMongoDocument;
import de.telekom.eni.pandora.horizon.mongo.repository.MessageStateMongoRepo;
import de.telekom.eni.pandora.horizon.tracing.HorizonTracer;
import de.telekom.eni.pandora.horizon.tracing.ScopedDebugSpanWrapper;
import de.telekom.horizon.pulsar.cache.ConnectionCache;
import de.telekom.horizon.pulsar.cache.ConnectionGaugeCache;
import de.telekom.horizon.pulsar.cache.SubscriberCache;
import de.telekom.horizon.pulsar.config.PulsarConfig;
import de.telekom.horizon.pulsar.service.SseTaskFactory;
import de.telekom.horizon.pulsar.service.TokenService;
import de.telekom.horizon.pulsar.utils.KafkaPicker;
import lombok.Getter;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.core.env.Environment;
import org.springframework.http.MediaType;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.servlet.mvc.method.annotation.ResponseBodyEmitter;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;

import static org.mockito.Mockito.*;

public class MockHelper {
    public static KafkaTemplate kafkaTemplate;
    public static EventWriter eventWriter;
    public static KafkaProperties kafkaProperties;
    public static KafkaPicker kafkaPicker;
    public static ConnectionCache connectionCache;
    public static ConnectionGaugeCache connectionGaugeCache;
    @Getter
    public static AtomicInteger openConnectionGaugeValue;
    public static SubscriberCache subscriberCache;
    public static PulsarConfig pulsarConfig;
    public static MessageStateMongoRepo messageStateMongoRepo;
    public static HorizonTracer tracingHelper;
    public static SseTaskFactory sseTaskFactory;
    public static Environment environment;
    public static ResponseBodyEmitter emitter;
    public static TokenService tokenService;
    public static HorizonMetricsHelper metricsHelper;

    public static DeDuplicationService deDuplicationService;



    public static String TEST_EVENT_ID = "abc123-def456-ghi789";
    public static String TEST_ENVIRONMENT = "bond";
    public static String TEST_SUBSCRIPTION_ID = "1-2-3";
    public static String TEST_SUBSCRIBER_ID = "eni-pan-dora";
    public static String TEST_CONTENT_TYPE = MediaType.TEXT_EVENT_STREAM_VALUE;
    public static String TEST_TOPIC = "subscribed";


    public static void init() {
        kafkaTemplate = mock(KafkaTemplate.class);
        eventWriter = mock(EventWriter.class);
        kafkaProperties = mock(KafkaProperties.class);
        connectionCache = mock(ConnectionCache.class);
        connectionGaugeCache = mock(ConnectionGaugeCache.class);
        openConnectionGaugeValue = mock(AtomicInteger.class);
        subscriberCache = mock(SubscriberCache.class);
        pulsarConfig = mock(PulsarConfig.class);
        messageStateMongoRepo = mock(MessageStateMongoRepo.class);
        tracingHelper = mock(HorizonTracer.class);
        environment = mock(Environment.class);
        emitter = mock(ResponseBodyEmitter.class);
        tokenService = mock(TokenService.class);
        deDuplicationService = mock(DeDuplicationService.class);

        lenient().when(tracingHelper.startScopedDebugSpan(any())).thenReturn(mock(ScopedDebugSpanWrapper.class));
        lenient().when(tracingHelper.startScopedSpan(any())).thenReturn(mock(ScopedSpan.class));
        lenient().when(pulsarConfig.getSseBatchSize()).thenReturn(10);
        lenient().when(pulsarConfig.getSsePollDelay()).thenReturn(1000L);
        lenient().when(pulsarConfig.getThreadPoolSize()).thenReturn(100);
        lenient().when(pulsarConfig.getQueueCapacity()).thenReturn(100);

        kafkaPicker = new KafkaPicker(kafkaTemplate);
        sseTaskFactory = new SseTaskFactory(pulsarConfig, connectionCache, connectionGaugeCache, eventWriter, kafkaPicker, messageStateMongoRepo, deDuplicationService, metricsHelper, tracingHelper);
    }

    public static SubscriptionEventMessage createSubscriptionEventMessageForTesting(DeliveryType deliveryType, boolean withAdditionalFields) {
        var subscriptionEventMessageForTesting = new SubscriptionEventMessage();

        var event = new Event();
        event.setId(RandomStringUtils.random(12, true, true));
        event.setData(Map.of("message", "foobar"));

        subscriptionEventMessageForTesting.setUuid(TEST_EVENT_ID);
        subscriptionEventMessageForTesting.setEvent(event);
        subscriptionEventMessageForTesting.setEnvironment(TEST_ENVIRONMENT);
        subscriptionEventMessageForTesting.setSubscriptionId(TEST_SUBSCRIPTION_ID);
        subscriptionEventMessageForTesting.setDeliveryType(deliveryType);

        return subscriptionEventMessageForTesting;
    }

    public static MessageStateMongoDocument createMessageStateDocumentForTesting (State state) {
        return new MessageStateMongoDocument(
                state.getUuid(),
                state.getCoordinates(),
                state.getStatus(),
                state.getEnvironment(),
                state.getDeliveryType(),
                state.getSubscriptionId(),
                state.getEvent(),
                state.getProperties(),
                state.getMultiplexedFrom(),
                state.getEventRetentionTime(),
                state.getTimestamp(),
                state.getModified(),
                state.getError(),
                state.getAppliedScopes(),
                state.getScopeEvaluationResult(),
                state.getConsumerEvaluationResult()
        );
    }

    private static String createSubscriptionIdForTesting(boolean randomSubscriptionId) {
        var testSubscriptionId = RandomStringUtils.random(10, true, true);
        if (randomSubscriptionId) {
            return testSubscriptionId;
        }
        return TEST_SUBSCRIPTION_ID;
    }

    private static State createStateForTesting(String environment, Status status, boolean randomSubscriptionId) {
        var event = new Event();
        event.setId(RandomStringUtils.random(12, true, true));

        var testSubscriptionId = createSubscriptionIdForTesting(randomSubscriptionId);
        var eventMessage = new SubscriptionEventMessage(event, environment, DeliveryType.SERVER_SENT_EVENT, testSubscriptionId, RandomStringUtils.random(12, true, true));
        eventMessage.setAdditionalFields(new HashMap<>());

        int randomPartition = ThreadLocalRandom.current().nextInt(0, Integer.MAX_VALUE);
        long randomOffset = ThreadLocalRandom.current().nextInt(0, Integer.MAX_VALUE);

        return State
                .builder(status, eventMessage, null, null)
                .uuid(eventMessage.getUuid())
                .subscriptionId(testSubscriptionId)
                .coordinates(new Coordinates(randomPartition,randomOffset))
                .deliveryType(DeliveryType.SERVER_SENT_EVENT)
                .event(new PartialEvent(event.getId(), event.getType()))
                .build();
    }
    public static List<State> createStatesForTesting (int count, String environment, Status status, boolean randomSubscriptionId) {
        var states = new ArrayList<State>();
        for (int i = 0; i < count; i++) {
            states.add(createStateForTesting(environment, status, randomSubscriptionId));
        }
        return states;
    }

    public static List<MessageStateMongoDocument> createMessageStateMongoDocumentsForTesting (int count, String environment, Status status, boolean randomSubscriptionId) {
        var states = createStatesForTesting(count, environment, status, randomSubscriptionId);
        return states.stream().map(MockHelper::createMessageStateDocumentForTesting).toList();
    }

    public static ConsumerRecord<String, String> createConsumerRecordForTesting(SubscriptionEventMessage subscriptionEventMessage) throws JsonProcessingException {
        var objectmapper = new ObjectMapper();
        String json = objectmapper.writeValueAsString(subscriptionEventMessage);
        return new ConsumerRecord<>(TEST_TOPIC, 0, 0L, subscriptionEventMessage.getUuid(), json);
    }
}
