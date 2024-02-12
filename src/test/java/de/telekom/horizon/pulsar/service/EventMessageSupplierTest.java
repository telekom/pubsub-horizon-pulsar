package de.telekom.horizon.pulsar.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import de.telekom.eni.pandora.horizon.kafka.event.EventWriter;
import de.telekom.eni.pandora.horizon.model.event.DeliveryType;
import de.telekom.eni.pandora.horizon.model.event.Status;
import de.telekom.eni.pandora.horizon.model.event.StatusMessage;
import de.telekom.horizon.pulsar.testutils.MockHelper;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.SliceImpl;
import org.springframework.data.domain.Sort;
import org.springframework.test.util.ReflectionTestUtils;

import java.time.Duration;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
@Slf4j
class EventMessageSupplierTest {

    EventMessageSupplier eventMessageSupplier;

    @BeforeEach
    void setupEventMessageSupplierTest() {
        MockHelper.init();

        eventMessageSupplier = new EventMessageSupplier(MockHelper.TEST_SUBSCRIPTION_ID, MockHelper.sseTaskFactory, false);
    }

    @ParameterizedTest
    @ValueSource(ints = {10})
    void testGetEventMessageContext(int polls) {
        final var objectMapper = new ObjectMapper();

        var pageableCaptor = ArgumentCaptor.forClass(Pageable.class);

        // We create a list of some test state documents similar as we would get from MongoDB
        var states = MockHelper.createMessageStateMongoDocumentsForTesting(MockHelper.pulsarConfig.getSseBatchSize(), MockHelper.TEST_ENVIRONMENT, Status.PROCESSED, false);

        // We mock the request to MongoDB and return our dummy state documents instead
        // We also capture the pageable argument to check whether is has been used correctly, later
        when(MockHelper.messageStateMongoRepo.findByStatusInAndDeliveryTypeAndSubscriptionIdAsc(eq(List.of(Status.PROCESSED)),
                eq(DeliveryType.SERVER_SENT_EVENT),
                eq(MockHelper.TEST_SUBSCRIPTION_ID),
                pageableCaptor.capture())).thenReturn(new SliceImpl<>(states));

        // We create a new SubscriptionEventMessage for testing
        var subscriptionEventMessage = MockHelper.createSubscriptionEventMessageForTesting(DeliveryType.CALLBACK, false);

        // We mock the picked message from Kafka
        ConsumerRecord<String, String> record = Mockito.mock(ConsumerRecord.class);

        try {
            // We mock getting the actual record value here and respond with a json string representation of the
            // previously created SubscriptionEventMessage
            when(record.value()).thenReturn(objectMapper.writeValueAsString(subscriptionEventMessage));
        } catch (JsonProcessingException e) {
            fail(e);
        }

        // EventWriter ha private access and will be created in the constructor of the SseTaskFactory
        // Since we want to check invocations with it, we will overwrite the EventWriter in EventMessageSupplier via reflections
        var eventWriterMock = mock(EventWriter.class);
        ReflectionTestUtils.setField(eventMessageSupplier, "eventWriter", eventWriterMock, EventWriter.class);


        // We do multiple calls to EventMessageSupplier.get() in order to test
        // that each call will fetch the next event message in the queue
        for (int i = 0; i < polls; i++) {
            // We mock the actual picking of a message from Kafka here
            when(MockHelper.kafkaTemplate.receive(eq(MockHelper.TEST_TOPIC), eq(states.get(i).getCoordinates().partition()), eq(states.get(i).getCoordinates().offset()), eq(Duration.ofMillis(30000)))).thenReturn(record);

            // PUBLIC METHOD WE WANT TO TEST
            var result = eventMessageSupplier.get();
            assertNotNull(result);

            // Check that we fetch batches from MongoDB correctly
            var pageable = pageableCaptor.getValue();
            assertEquals(0, pageable.getOffset());
            assertEquals(MockHelper.pulsarConfig.getSseBatchSize(), pageable.getPageSize());
            assertTrue(Optional.ofNullable(pageable.getSort().getOrderFor("timestamp")).map(Sort.Order::isAscending).orElse(false));

            var m = result.getSubscriptionEventMessage();
            assertNotNull(m);

            // Check that the state message and event message match
            assertEquals(subscriptionEventMessage.getUuid(), m.getUuid());
        }
    }

    @Test
    void testGetEventMessageContextWithSubscriberDoesNotMatchSubscriptionException() {
        final var objectMapper = new ObjectMapper();

        var statusMessageCaptor = ArgumentCaptor.forClass(StatusMessage.class);

        // We create a list of some test state documents similar as we would get from MongoDB
        var states = MockHelper.createMessageStateMongoDocumentsForTesting(MockHelper.pulsarConfig.getSseBatchSize(), MockHelper.TEST_ENVIRONMENT, Status.PROCESSED, false);

        // We mock the request to MongoDB and return our dummy state documents instead
        // We also capture the pageable argument to check whether is has been used correctly, later
        when(MockHelper.messageStateMongoRepo.findByStatusInAndDeliveryTypeAndSubscriptionIdAsc(eq(List.of(Status.PROCESSED)),
                eq(DeliveryType.SERVER_SENT_EVENT),
                eq(MockHelper.TEST_SUBSCRIPTION_ID),
                any(Pageable.class))).thenReturn(new SliceImpl<>(states));

        // We create a new SubscriptionEventMessage for testing,
        // and we overwrite the subscriptionId so that it doesn't match the standard testing subscriptionId
        // in the state messages anymore
        var subscriptionEventMessage = MockHelper.createSubscriptionEventMessageForTesting(DeliveryType.CALLBACK, false);
        subscriptionEventMessage.setSubscriptionId("something different");

        // We mock the picked message from Kafka
        ConsumerRecord<String, String> record = Mockito.mock(ConsumerRecord.class);

        try {
            // We mock getting the actual record value here and respond with a json string representation of the
            // previously created SubscriptionEventMessage
            when(record.value()).thenReturn(objectMapper.writeValueAsString(subscriptionEventMessage));
        } catch (JsonProcessingException e) {
            fail(e);
        }

        // We mock the actual picking of a message from Kafka here by using the coordinates of the first state message
        when(MockHelper.kafkaTemplate.receive(eq(MockHelper.TEST_TOPIC), eq(states.getFirst().getCoordinates().partition()), eq(states.getFirst().getCoordinates().offset()), eq(Duration.ofMillis(30000)))).thenReturn(record);

        // EventWriter ha private access and will be created in the constructor of the SseTaskFactory
        // Since we want to check invocations with it, we will overwrite the EventWriter in EventMessageSupplier via reflections
        var eventWriterMock = mock(EventWriter.class);
        ReflectionTestUtils.setField(eventMessageSupplier, "eventWriter", eventWriterMock, EventWriter.class);

        // PUBLIC METHOD WE WANT TO TEST
        var result = eventMessageSupplier.get();
        assertNotNull(result);

        try {
            // EventMessageSupplier writes a FAILED status in case there was a problem (e.g. subscriptionIds of status und event message do not match)
            // Let's verify that a new status has been written
            verify(eventWriterMock).send(any(), statusMessageCaptor.capture(), eq(MockHelper.tracingHelper));
        } catch (JsonProcessingException e) {
            fail(e);
        }

        var m = result.getSubscriptionEventMessage();
        assertNull(m);

        // Let's check that a FAILED status has been written since the subscriptionIds did not match
        var statusMessage = statusMessageCaptor.getValue();
        assertEquals(states.getFirst().getUuid(), statusMessage.getUuid());
        assertEquals(states.getFirst().getEvent().getId(), statusMessage.getEvent().getId());
        assertEquals(Status.FAILED, statusMessage.getStatus());
    }

    @Test
    void testGetEventMessageContextWithCouldNotFindEventMessageException() {
        var statusMessageCaptor = ArgumentCaptor.forClass(StatusMessage.class);

        // We create a list of some test state documents similar as we would get from MongoDB
        var states = MockHelper.createMessageStateMongoDocumentsForTesting(MockHelper.pulsarConfig.getSseBatchSize(), MockHelper.TEST_ENVIRONMENT, Status.PROCESSED, false);

        // We mock the request to MongoDB and return our dummy state documents instead
        // We also capture the pageable argument to check whether is has been used correctly, later
        when(MockHelper.messageStateMongoRepo.findByStatusInAndDeliveryTypeAndSubscriptionIdAsc(eq(List.of(Status.PROCESSED)),
                eq(DeliveryType.SERVER_SENT_EVENT),
                eq(MockHelper.TEST_SUBSCRIPTION_ID),
                any(Pageable.class))).thenReturn(new SliceImpl<>(states));

        // We mock the actual picking of a message from Kafka here by using the coordinates of the first state message
        // But we return null here in order to simulate that a message could not be found
        when(MockHelper.kafkaTemplate.receive(eq(MockHelper.TEST_TOPIC), eq(states.getFirst().getCoordinates().partition()), eq(states.getFirst().getCoordinates().offset()), eq(Duration.ofMillis(30000)))).thenReturn(null);

        // EventWriter ha private access and will be created in the constructor of the SseTaskFactory
        // Since we want to check invocations with it, we will overwrite the EventWriter in EventMessageSupplier via reflections
        var eventWriterMock = mock(EventWriter.class);
        ReflectionTestUtils.setField(eventMessageSupplier, "eventWriter", eventWriterMock, EventWriter.class);

        // PUBLIC METHOD WE WANT TO TEST
        var result = eventMessageSupplier.get();
        assertNotNull(result);

        try {
            // EventMessageSupplier writes a FAILED status in case there was a problem (e.g. subscriptionIds of status und event message do not match)
            // Let's verify that a new status has been written
            verify(eventWriterMock).send(any(), statusMessageCaptor.capture(), eq(MockHelper.tracingHelper));
        } catch (JsonProcessingException e) {
            fail(e);
        }

        // Let's check that a FAILED status has been written since the message could not be fund
        var statusMessage = statusMessageCaptor.getValue();
        assertEquals(states.getFirst().getUuid(), statusMessage.getUuid());
        assertEquals(states.getFirst().getEvent().getId(), statusMessage.getEvent().getId());
        assertEquals(Status.FAILED, statusMessage.getStatus());

        var m = result.getSubscriptionEventMessage();
        assertNull(m);
    }
}

