// Copyright 2024 Deutsche Telekom IT GmbH
//
// SPDX-License-Identifier: Apache-2.0

package de.telekom.horizon.pulsar.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import de.telekom.eni.pandora.horizon.kafka.event.EventWriter;
import de.telekom.eni.pandora.horizon.model.db.State;
import de.telekom.eni.pandora.horizon.model.event.DeliveryType;
import de.telekom.eni.pandora.horizon.model.event.Status;
import de.telekom.eni.pandora.horizon.model.event.StatusMessage;
import de.telekom.eni.pandora.horizon.model.event.SubscriptionEventMessage;
import de.telekom.eni.pandora.horizon.model.meta.EventRetentionTime;
import de.telekom.eni.pandora.horizon.mongo.repository.MessageStateMongoRepo;
import de.telekom.eni.pandora.horizon.tracing.HorizonTracer;
import de.telekom.horizon.pulsar.config.PulsarConfig;
import de.telekom.horizon.pulsar.exception.CouldNotFindEventMessageException;
import de.telekom.horizon.pulsar.exception.CouldNotPickMessageException;
import de.telekom.horizon.pulsar.exception.SubscriberDoesNotMatchSubscriptionException;
import de.telekom.horizon.pulsar.helper.EventMessageContext;
import de.telekom.horizon.pulsar.helper.StreamLimit;
import de.telekom.horizon.pulsar.utils.KafkaPicker;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.bson.types.ObjectId;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.Supplier;

/**
 * Supplier for providing {@link EventMessageContext} instances.
 *
 * This class, annotated with {@code @Slf4j}, serves as a Supplier for generating instances
 * of {@link EventMessageContext}. It is designed to work in conjunction with a Pulsar
 * messaging system, KafkaPicker, and other components to fetch and process messages for a
 * specified subscription. The primary method, {@code get()}, polls for message states,
 * picks subscribed messages, and handles exceptions accordingly. It also involves
 * tracing spans and maintaining a queue of message states for efficient processing.
 */
@Slf4j
public class EventMessageSupplier implements Supplier<EventMessageContext> {

    private final PulsarConfig pulsarConfig;
    @Getter
    private final String subscriptionId;
    private final Boolean includeHttpHeaders;
    private final StreamLimit streamLimit;
    private final KafkaPicker kafkaPicker;
    private final EventWriter eventWriter;
    private final MessageStateMongoRepo messageStateMongoRepo;
    private final HorizonTracer tracingHelper;
    private final ConcurrentLinkedQueue<State> messageStates = new ConcurrentLinkedQueue<>();
    private Instant lastPoll;
    private String currentOffset;
    private final ObjectMapper objectMapper = new ObjectMapper();

    /**
     * Constructs an instance of {@code EventMessageSupplier}.
     *
     * @param subscriptionId     The subscriptionId for which messages are fetched.
     * @param factory            The {@link SseTaskFactory} used for obtaining related components.
     * @param includeHttpHeaders Boolean flag indicating whether to include HTTP headers in the generated {@code EventMessageContext}.
     * @param startingOffset             Enables offset based streaming. Specifies the offset (message id) of the last received event message.
     * @param streamLimit        The {@link StreamLimit} represents any customer specific conditions for terminating the stream early.
     */
    public EventMessageSupplier(String subscriptionId, SseTaskFactory factory, boolean includeHttpHeaders, String startingOffset, StreamLimit streamLimit) {
        this.subscriptionId = subscriptionId;

        this.pulsarConfig = factory.getPulsarConfig();
        this.messageStateMongoRepo = factory.getMessageStateMongoRepo();
        this.kafkaPicker = factory.getKafkaPicker();
        this.eventWriter = factory.getEventWriter();
        this.tracingHelper = factory.getTracingHelper();
        this.includeHttpHeaders = includeHttpHeaders;
        this.currentOffset = startingOffset;
        this.streamLimit = streamLimit;
    }

    /**
     * Gets the next available {@code EventMessageContext} from the supplier.
     *
     * This method polls for message states, picks subscribed messages, and handles exceptions
     * accordingly. It also involves tracing spans and maintains a queue of message states for
     * efficient processing.
     *
     * @return The next available {@code EventMessageContext}.
     */
    @Override
    public EventMessageContext get() {
        pollMessageStates();

        if (!messageStates.isEmpty()) {
            var state = messageStates.poll();
            var ignoreDeduplication = StringUtils.isNotEmpty(currentOffset);

            // TODO: these spans get duplicated cause of the vortex latency - will be resolved DHEI-13764

            var span = tracingHelper.startSpanFromState("work on state message", state);
            var spanInScope = tracingHelper.withSpanInScope(span);

            var pickSpan = tracingHelper.startScopedDebugSpan("pick subscribed message");
            tracingHelper.addTagsToSpanFromState(pickSpan, state);

            try {
                var rec = kafkaPicker.pickEvent(state);

                var message = deserializeSubscriptionEventMessage(rec.value(), state);

                if (message != null) {
                    tracingHelper.addTagsToSpanFromSubscriptionEventMessage(pickSpan, message);

                    // we do a sanity check here that checks whether metadata and event message fit to each other
                    if (!subscriptionId.equals(message.getSubscriptionId())) {
                        var errorMessage = String.format("Event message %s did not match subscriptionId %s", state.getUuid(), state.getSubscriptionId());
                        throw new SubscriberDoesNotMatchSubscriptionException(errorMessage);
                    }

                    Optional.ofNullable(message.getHttpHeaders()).ifPresent(headers -> headers.put("x-pubsub-offset-id", new ArrayList<>(List.of(state.getUuid()))));
                }
                return new EventMessageContext(message, includeHttpHeaders, streamLimit, ignoreDeduplication, span, spanInScope);
            } catch (CouldNotPickMessageException | SubscriberDoesNotMatchSubscriptionException e) {
                handleException(state, e);
                return new EventMessageContext(null, includeHttpHeaders, streamLimit, ignoreDeduplication, span, spanInScope);
            } finally {
                pickSpan.finish();
            }
        }

        return new EventMessageContext();
    }

    /**
     * Handles exceptions by logging errors, updating event status, and adding tags to the current span.
     *
     * @param state The current state of the message.
     * @param e     The exception to handle.
     */
    private void handleException(State state, Exception e) {
        var currentSpan = Optional.ofNullable(tracingHelper.getCurrentSpan());
        currentSpan.ifPresent(s -> s.error(e));

        if (e.getCause() instanceof CouldNotFindEventMessageException || e instanceof SubscriberDoesNotMatchSubscriptionException) {
            try {
                var status = Status.FAILED;
                var statusMessage = new StatusMessage(state.getUuid(), state.getEvent().getId(), status, state.getDeliveryType());
                eventWriter.send(Objects.requireNonNullElse(state.getEventRetentionTime(), EventRetentionTime.DEFAULT).getTopic(),statusMessage, tracingHelper);
                currentSpan.ifPresent(s -> tracingHelper.addTagsToSpan(s, List.of(Pair.of("status", status.name()))));
            } catch (Exception e1) {
                var err = String.format("Error occurred while updating the event status: %s", e1.getMessage());
                log.error(err, e1);
            }
        }

        log.error("Could not pick event, error: {}", e.getMessage());
    }

    /**
     * Polls for message states and adds them to the queue.
     *
     * This method polls for message states and adds them to the queue. It also involves
     * tracing spans and maintains a queue of message states for efficient processing.
     */
    private void pollMessageStates() {
        if (messageStates.isEmpty()) {
            delay();

            Pageable pageable = PageRequest.of(0, pulsarConfig.getSseBatchSize(), Sort.by(Sort.Direction.ASC, "timestamp"));

            if (StringUtils.isNoneEmpty(currentOffset)) {
                var offsetMsg = messageStateMongoRepo.findById(currentOffset);
                if (offsetMsg.isPresent()) {
                    var offsetTimestamp = offsetMsg.get().getTimestamp();

                    var list = messageStateMongoRepo.findByDeliveryTypeAndSubscriptionIdAndTimestampGreaterThanAsc(
                                    DeliveryType.SERVER_SENT_EVENT,
                                    subscriptionId,
                                    offsetTimestamp,
                                    pageable
                            ).stream()
                            .filter(m -> m.getCoordinates() != null) // we skip messages that refer to -1 partitions and offsets
                            .toList();

                    messageStates.addAll(list);

                    if (!list.isEmpty()) {
                        currentOffset = list.getLast().getUuid();
                    }
                }
            } else {
                var list = messageStateMongoRepo.findByStatusInAndDeliveryTypeAndSubscriptionIdAsc(
                                List.of(Status.PROCESSED),
                                DeliveryType.SERVER_SENT_EVENT,
                                subscriptionId,
                                pageable
                        ).stream()
                        .filter(m -> m.getCoordinates() != null) // we skip messages that refer to -1 partitions and offsets
                        .toList();

                messageStates.addAll(list);
            }

            lastPoll = Instant.now();
        }
    }

    /**
     * Delays the current thread for the configured amount of time.
     *
     * This method delays the current thread for the configured amount of time.
     */
    private void delay () {
        var pollDelay = pulsarConfig.getSsePollDelay();

        if (lastPoll == null || pollDelay <= 0L) {
            return;
        }

        var sleepTime = Math.max(0, pulsarConfig.getSsePollDelay() - ChronoUnit.MILLIS.between(lastPoll, Instant.now()));
        try {
            Thread.sleep(sleepTime);
        } catch (InterruptedException e) {
            log.error(e.getMessage(), e);
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Deserializes a JSON string into a {@link SubscriptionEventMessage} object.
     *
     * @param json  The JSON string to deserialize.
     * @param state The state associated with the message.
     * @return The deserialized {@code SubscriptionEventMessage} or null if deserialization fails.
     */
    private SubscriptionEventMessage deserializeSubscriptionEventMessage(String json, State state) {
        try {
            return objectMapper.readValue(json, SubscriptionEventMessage.class);
        } catch (JsonMappingException e) {
            log.error("Could not deserialize (map) json for state {}", state);
        } catch (JsonProcessingException e) {
            log.error("Could not deserialize (process) json for state {}", state);
        }
        return null;
    }
}

