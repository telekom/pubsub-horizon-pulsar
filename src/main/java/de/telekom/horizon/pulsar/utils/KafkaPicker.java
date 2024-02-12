package de.telekom.horizon.pulsar.utils;

import de.telekom.eni.pandora.horizon.model.db.State;
import de.telekom.eni.pandora.horizon.model.meta.EventRetentionTime;
import de.telekom.horizon.pulsar.exception.CouldNotFindEventMessageException;
import de.telekom.horizon.pulsar.exception.CouldNotPickMessageException;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.Objects;

/**
 * Component for picking events from Kafka based on the state information.
 *
 * This class is responsible for retrieving event messages from Kafka based on the provided
 * state information. It uses a KafkaTemplate to interact with Kafka topics and fetches the
 * desired event based on partition and offset from the state object.
 */
@Slf4j
@Component
public class KafkaPicker {

    private final KafkaTemplate<String, String> kafkaTemplate;

    /**
     * Constructs a new instance of the KafkaPicker.
     *
     * @param kafkaTemplate The KafkaTemplate used for interacting with Kafka.
     */
    public KafkaPicker(
            KafkaTemplate<String, String> kafkaTemplate) {

        this.kafkaTemplate = kafkaTemplate;
    }

    /**
     * Picks an event message from Kafka based on the provided state information.
     *
     * @param state The state information containing details about the desired event.
     * @return The ConsumerRecord representing the picked event message.
     * @throws CouldNotPickMessageException If an error occurs while picking the event message.
     */
    public ConsumerRecord<String, String> pickEvent(State state) throws CouldNotPickMessageException {
        try {
            var topicName = Objects.requireNonNullElse(state.getEventRetentionTime(), EventRetentionTime.DEFAULT).getTopic();
            var partition = state.getCoordinates().partition();
            var offset = state.getCoordinates().offset();

            log.debug("Picking event message at partition {} and offset {}", partition, offset);

            ConsumerRecord<String, String> rec = kafkaTemplate.receive(topicName, partition, offset, Duration.ofMillis(30000));

            if (rec == null || rec.value() == null) {
                var errorMessage = String.format("Could not find a valid event message at partition %s and offset %s", partition, offset);

                throw new CouldNotFindEventMessageException(errorMessage);
            }

            return rec;
        } catch (Exception e) {
            throw new CouldNotPickMessageException(e);
        }
    }
}
