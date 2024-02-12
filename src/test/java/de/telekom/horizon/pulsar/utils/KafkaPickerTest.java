package de.telekom.horizon.pulsar.utils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import de.telekom.eni.pandora.horizon.kafka.event.EventWriter;
import de.telekom.eni.pandora.horizon.model.event.DeliveryType;
import de.telekom.eni.pandora.horizon.model.event.Event;
import de.telekom.eni.pandora.horizon.model.event.Status;
import de.telekom.eni.pandora.horizon.model.event.SubscriptionEventMessage;
import de.telekom.eni.pandora.horizon.model.meta.EventRetentionTime;
import de.telekom.eni.pandora.horizon.mongo.model.MessageStateMongoDocument;
import de.telekom.eni.pandora.horizon.mongo.repository.MessageStateMongoRepo;
import de.telekom.horizon.pulsar.exception.CouldNotPickMessageException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;

import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest
@Import({MongoTestServerConfiguration.class})
class KafkaPickerTest extends AbstractIntegrationTest {

    @Autowired
    private MessageStateMongoRepo mongoRepo;

    @Autowired
    private EventWriter eventWriter;

    private KafkaPicker kafkaPicker;

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private KafkaPicker kafkaPicker(KafkaTemplate<String, String> kafkaTemplate) {
        kafkaPicker = new KafkaPicker(kafkaTemplate);
        return kafkaPicker;
    }

    @Test
    void pickEventsFromKafkaSuccessfully() throws ExecutionException, JsonProcessingException, InterruptedException, TimeoutException {
        String subscriptionId = "kafkaPickerTest";
        String eventId = UUID.randomUUID().toString();

        MessageStateMongoDocument messageStateMongoDocument = prepareKafkaAndMongoForSseTest(subscriptionId, eventId);

        assertDoesNotThrow(() -> {
            ConsumerRecord<String, String> stringStringConsumerRecord = kafkaPicker.pickEvent(messageStateMongoDocument);
            SubscriptionEventMessage subscriptionEventMessage = objectMapper.readValue(stringStringConsumerRecord.value(), SubscriptionEventMessage.class);
            assertEquals(subscriptionId, subscriptionEventMessage.getSubscriptionId());
            assertEquals(eventId, subscriptionEventMessage.getEvent().getId());
        });
    }

    @Test
    void pickEventsFromKafkaWithWrongPartition() throws ExecutionException, JsonProcessingException, InterruptedException, TimeoutException {
        String subscriptionId = "kafkaPickerTest";
        String eventId = UUID.randomUUID().toString();

        MessageStateMongoDocument messageStateMongoDocument = prepareKafkaAndMongoForSseTest(subscriptionId, eventId);
        messageStateMongoDocument.setCoordinates(messageStateMongoDocument.getCoordinates().partition(), messageStateMongoDocument.getCoordinates().offset() + 1);

        assertThrows(CouldNotPickMessageException.class, () -> kafkaPicker.pickEvent(messageStateMongoDocument));
    }

    private MessageStateMongoDocument prepareKafkaAndMongoForSseTest(String subscriptionId, String eventId) throws JsonProcessingException, ExecutionException, InterruptedException, TimeoutException {
        // create kafka subscribedmessage
        var message = new SubscriptionEventMessage();
        message.setSubscriptionId(subscriptionId);
        message.setDeliveryType(DeliveryType.SERVER_SENT_EVENT);
        message.setStatus(Status.PROCESSED);
        Event e = new Event();
        e.setType("testevent");
        e.setId(eventId);
        e.setData("test".getBytes());

        message.setEvent(e);

        SendResult<String, String> subscribed = eventWriter.send(EventRetentionTime.TTL_1_HOUR.getTopic(), message).get(5, TimeUnit.SECONDS);

        // create mongo state and store to mongo
        MessageStateMongoDocument state = new MessageStateMongoDocument();
        state.setDeliveryType(DeliveryType.SERVER_SENT_EVENT);
        state.setProperties(Collections.emptyMap());
        state.setStatus(Status.PROCESSED);
        state.setEventRetentionTime(EventRetentionTime.TTL_1_HOUR);
        state.setSubscriptionId(subscriptionId);
        state.setCoordinates(subscribed.getRecordMetadata().partition(), subscribed.getRecordMetadata().offset());

        return mongoRepo.save(state);
    }
}
