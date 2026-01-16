// Copyright 2024 Deutsche Telekom IT GmbH
//
// SPDX-License-Identifier: Apache-2.0

package de.telekom.horizon.pulsar.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.MongoException;
import com.mongodb.client.MongoCollection;
import de.telekom.eni.pandora.horizon.cache.service.DeDuplicationService;
import de.telekom.eni.pandora.horizon.kafka.event.EventWriter;
import de.telekom.eni.pandora.horizon.metrics.HorizonMetricsHelper;
import de.telekom.eni.pandora.horizon.model.event.Status;
import de.telekom.eni.pandora.horizon.model.event.StatusMessage;
import de.telekom.eni.pandora.horizon.model.event.SubscriptionEventMessage;
import de.telekom.eni.pandora.horizon.model.meta.EventRetentionTime;
import de.telekom.eni.pandora.horizon.tracing.HorizonTracer;
import de.telekom.horizon.pulsar.config.PulsarConfig;
import de.telekom.horizon.pulsar.exception.ConnectionCutOutException;
import de.telekom.horizon.pulsar.exception.ConnectionTimeoutException;
import de.telekom.horizon.pulsar.exception.StreamLimitExceededException;
import de.telekom.horizon.pulsar.helper.EventMessageContext;
import de.telekom.horizon.pulsar.helper.SseResponseWrapper;
import de.telekom.horizon.pulsar.helper.SseTaskStateContainer;
import de.telekom.horizon.pulsar.helper.StreamLimit;
import de.telekom.horizon.pulsar.mongo.MongoUpdateBatch;
import io.micrometer.core.instrument.Tags;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;
import org.bson.Document;
import org.springframework.http.MediaType;
import org.springframework.lang.NonNull;
import org.springframework.lang.Nullable;
import org.springframework.web.server.UnsupportedMediaTypeStatusException;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

import static de.telekom.eni.pandora.horizon.metrics.HorizonMetricsConstants.METRIC_SENT_SSE_EVENTS;

/**
 * Represents a Server-Sent Events (SSE) task responsible for emitting events to clients.
 *
 * This task runs in a separate thread and emits Server-Sent Events to clients through an SseEmitter.
 * It manages the emission of events, handles timeouts, and ensures proper clean-up.
 */
@Slf4j
public class SseTask implements Runnable {
    private static final String APPLICATION_STREAM_JSON_VALUE = "application/stream+json"; //Sonar code smell
    private static final String TIMEOUT_MESSAGE = "No events received for more than %s ms";

    private final SseTaskStateContainer sseTaskStateContainer;

    private final EventMessageSupplier eventMessageSupplier;

    @Getter
    private final AtomicInteger openConnectionGaugeValue;
    private final PulsarConfig pulsarConfig;
    private final EventWriter eventWriter;
    private final DeDuplicationService deDuplicationService;
    private final HorizonTracer tracingHelper;
    private final HorizonMetricsHelper metricsHelper;

    @Setter
    private String contentType = APPLICATION_STREAM_JSON_VALUE;

    @Getter
    private Instant startTime;
    @Getter
    private Instant stopTime;
    private Instant lastEventMessage = Instant.now();
    private AtomicLong bytesConsumed = new AtomicLong(0);
    private AtomicLong numberConsumed = new AtomicLong(0);

    private final AtomicBoolean isCutOut = new AtomicBoolean(false);
    private final AtomicBoolean isEmitterCompleted = new AtomicBoolean(false);

    private final MongoUpdateBatch mongoUpdateBatch;
    private final ObjectMapper objectMapper = new ObjectMapper();

    /**
     * Constructs a new instance of the SSE task.
     *
     * @param sseTaskStateContainer The state container for managing the SSE task's state.
     * @param eventMessageSupplier  The supplier for generating EventMessageContext objects.
     * @param openConnectionGaugeValue The gauge value representing the number of open connections.
     * @param factory The factory providing dependencies for the SSE task.
     */
    public SseTask(SseTaskStateContainer sseTaskStateContainer,
                   EventMessageSupplier eventMessageSupplier,
                   AtomicInteger openConnectionGaugeValue,
                   SseTaskFactory factory,
                   MongoCollection<Document> collection) {

        this.sseTaskStateContainer = sseTaskStateContainer;
        this.eventMessageSupplier = eventMessageSupplier;
        this.openConnectionGaugeValue = openConnectionGaugeValue;

        this.pulsarConfig = factory.getPulsarConfig();
        this.eventWriter = factory.getEventWriter();
        this.deDuplicationService = factory.getDeDuplicationService();
        this.tracingHelper = factory.getTracingHelper();
        this.metricsHelper = factory.getMetricsHelper();
        this.mongoUpdateBatch = new MongoUpdateBatch(eventMessageSupplier.getSubscriptionId(), collection);
    }

    /**
    * Creates a stream topology that generates EventMessageContext objects using an EventMessageSupplier.
    *
    * The stream is generated by producing EventMessageContext objects from the provided EventMessageSupplier.
    * Generation stops when the condition specified by applyStreamEndFilter or applyConnectionTimeoutFilter is no longer met.
    * Additionally, a debug message with the associated subscriptionId is logged when the stream is closed.
    *
    * @param eventMessageSupplier The supplier for EventMessageContext objects.
    * @return A stream of EventMessageContext objects according to the defined topology.
    *
    */
    private Stream<EventMessageContext> createStreamTopology(EventMessageSupplier eventMessageSupplier) {
        return Stream.generate(eventMessageSupplier)
                .takeWhile(this::applyStreamEndFilter)
                .takeWhile(this::applyConnectionTimeoutFilter)
                .onClose(() -> log.debug("Closing stream for subscriptionId {}", eventMessageSupplier.getSubscriptionId()));
    }

    /**
     * Runs the SSE task, emitting events to clients.
     *
     * This method is responsible for executing the SSE task, which involves emitting Server-Sent Events (SSE)
     * to connected clients. It checks if the task has been canceled and monitors the completion status of the SSE emitter.
     * If the emitter has successfully sent all SSE messages, the completion status is monitored, and the necessary cleanup
     * and error handling procedures are performed. The method also updates the open connection gauge value.
     */
    @Override
    public void run() {
        var stream = createStreamTopology(eventMessageSupplier);

        if (sseTaskStateContainer.getCanceled().get()) {
            return;
        }

        // Monitor the status of the emitter completion.
        sseTaskStateContainer.getEmitter().onCompletion(() -> isEmitterCompleted.compareAndExchange(false, true));
        sseTaskStateContainer.getEmitter().onError(e -> isEmitterCompleted.compareAndExchange(false, true));

        startTime = Instant.now();

        // Mark the task as running and increment the open connection gauge value.
        sseTaskStateContainer.getRunning().compareAndExchange(false, true);
        openConnectionGaugeValue.getAndSet(1);

        try (stream){
            stream.forEach(this::emitEventMessage);
        } catch (Exception e) {
            log.error(String.format("Error occurred: %s", e.getMessage()), e);

            sseTaskStateContainer.getEmitter().completeWithError(e);
        } finally {
            openConnectionGaugeValue.getAndSet(0);
            stopTime = Instant.now();
        }
    }

    /**
     * Terminates the SSE task.
     *
     * This method sets the {@code isCutOut} flag to indicate that the SSE task should be terminated.
     */
    public void terminate() {
        isCutOut.compareAndExchange(false, true);
        flushStatusUpdates(true);
    }

    /**
     * Applies the stream end filter to determine if the SSE stream should continue processing events.
     *
     * This method checks whether the SSE emitter has completed successfully or if a connection cut-out has been
     * requested. If either condition is met, it completes the emitter with an error (if necessary) and finishes
     * the associated span.
     *
     * @param context The context containing information about the event message.
     * @return {@code true} if the stream should continue processing events; otherwise, {@code false}.
     */
    private boolean applyStreamEndFilter(EventMessageContext context) {
        if (isEmitterCompleted.get()) {
            context.finishSpan();

            return false;
        }

        if (isCutOut.get()) {
            sseTaskStateContainer.getEmitter().completeWithError(new ConnectionCutOutException());
            context.finishSpan();

            return false;
        }

        final StreamLimit streamLimit = context.getStreamLimit();
        if (streamLimit != null) {
            final boolean maxNumberExceeded = streamLimit.getMaxNumber() > 0 && numberConsumed.get() >= streamLimit.getMaxNumber();
            final boolean maxMinutesExceeded = streamLimit.getMaxMinutes() > 0 && ChronoUnit.MINUTES.between(startTime, Instant.now()) >= streamLimit.getMaxMinutes();
            final boolean maxBytesExceeded = streamLimit.getMaxBytes() > 0 && bytesConsumed.get() >= streamLimit.getMaxBytes();

            if (maxNumberExceeded || maxMinutesExceeded || maxBytesExceeded) {
                sseTaskStateContainer.getEmitter().completeWithError(new StreamLimitExceededException());
                context.finishSpan();
                return false;
            }
        }

        return true;
    }

    /**
     * Applies the connection timeout filter to determine if the SSE connection has timed out.
     *
     * This method checks if the connection timeout has been reached by comparing the last event message time
     * with the current time. If the timeout is exceeded, it completes the emitter with a connection timeout error
     * and finishes the associated span.
     *
     * @param context The context containing information about the event message.
     * @return {@code true} if the connection timeout has not been reached; otherwise, {@code false}.
     */
    private boolean applyConnectionTimeoutFilter(EventMessageContext context) {
        var now = Instant.now();

        if (context.getSubscriptionEventMessage() == null) {
            if (lastEventMessage.plusMillis(pulsarConfig.getSseTimeout()).isBefore(now)) {
                sseTaskStateContainer.getEmitter().completeWithError(new ConnectionTimeoutException(String.format(TIMEOUT_MESSAGE, pulsarConfig.getSseTimeout())));
                context.finishSpan();

                return false;
            }
        } else {
            lastEventMessage = now;
        }

        return true;
    }

    /**
     * Serializes the SSE event into the appropriate format based on the content type.
     *
     * This method takes an SSE event, serializes it into JSON format, and formats the result according to the specified
     * content type. It supports the "application/stream+json" and "text/event-stream" content types.
     *
     * @param event The SSE event to be serialized.
     * @return The serialized SSE event in the specified content type format.
     * @throws JsonProcessingException If an error occurs during JSON serialization.
     * @throws UnsupportedMediaTypeStatusException If the specified content type is not a valid SSE format.
     */
    private String serializeEvent(@NonNull SseResponseWrapper event) throws JsonProcessingException, UnsupportedMediaTypeStatusException {
        var eventJson = objectMapper.writeValueAsString(event);

        return switch (contentType) {
            case APPLICATION_STREAM_JSON_VALUE -> String.format("%s%n", eventJson);
            case MediaType.TEXT_EVENT_STREAM_VALUE -> String.format("data: %s%n%n", eventJson);
            default ->
                    throw new UnsupportedMediaTypeStatusException(String.format("Content type %s is no valid SSE format.", contentType));
        };
    }

    /**
     * Emits an event message, handling duplicates and pushing metadata.
     *
     * This method processes the given subscription event message. It checks for duplicates in the deduplication cache
     * and decides whether to ignore, mark as duplicate, or send the event. Finally, it finishes the associated span.
     *
     * @param context The context containing information about the event message.
     */
    //ToDo: Removed synchronized modifier. Was it really needed?
    private void emitEventMessage(EventMessageContext context) {
        var msg = context.getSubscriptionEventMessage();

        if (msg == null) {
            context.finishSpan();
            return;
        }

        if (!context.isIgnoreDeduplication()) {
            String msgUuidOrNull = deDuplicationService.get(msg);
            boolean isDuplicate = Objects.nonNull(msgUuidOrNull);
            if (isDuplicate) {
                metricsHelper.getRegistry().counter(
                        "deduplication_hits",
                        Tags.of(
                                "subscriptionId", msg.getSubscriptionId()
                        )
                ).increment();

                if(Objects.equals(msg.getUuid(), msgUuidOrNull)) {
                    log.debug("Message with id {} was found in the deduplication cache with the same UUID. Message will be ignored, because status will probably set to DELIVERED in the next minutes.", msg.getUuid());
                } else {
                    log.debug("Message with id {} was found in the deduplication cache with another UUID. Message will be set to DUPLICATE to prevent event being stuck at PROCESSED.", msg.getUuid());
                    pushMetadata(msg, Status.DUPLICATE, null);
                }

                context.finishSpan();
                return;
            }
        }

        try {
            sendEvent(msg, context.getIncludeHttpHeaders());
        } finally {

            context.finishSpan();
        }
    }

    /**
     * Sends an event to the SSE emitter, handling serialization and metadata pushing.
     *
     * This method takes a subscription event message, serializes it into JSON format, sends it to the SSE emitter, and
     * pushes metadata such as status and exceptions. It finishes the associated span.
     *
     * @param msg The subscription event message to be sent.
     * @param includeHttpHeaders Flag indicating whether to include HTTP headers in the event.
     */
    private void sendEvent(SubscriptionEventMessage msg, boolean includeHttpHeaders) {
        var sendSpan = tracingHelper.startScopedSpan("send");
        tracingHelper.addTagsToSpanFromSubscriptionEventMessage(sendSpan, msg);

        try {
            var eventJson = serializeEvent(new SseResponseWrapper(msg, includeHttpHeaders));

            sseTaskStateContainer.getEmitter().send(eventJson);

            //pushMetadata(msg, Status.DELIVERED, null);
            mongoUpdateBatch.updateStatus(msg, Status.DELIVERED);

            bytesConsumed.addAndGet(eventJson.getBytes(StandardCharsets.UTF_8).length);
            numberConsumed.incrementAndGet();

            metricsHelper.getRegistry().counter(METRIC_SENT_SSE_EVENTS, metricsHelper.buildTagsFromSubscriptionEventMessage(msg)).increment();
        } catch (JsonProcessingException e) {
            var err = String.format("Error occurred while emitting the event: %s", e.getMessage());
            log.info(err, e);
            sendSpan.error(e);

            pushMetadata(msg, Status.FAILED, e);
            mongoUpdateBatch.updateStatus(msg, Status.FAILED);
        } catch (Exception e) {
            var err = String.format("Error occurred while emitting the event: %s", e.getMessage());
            log.info(err, e);
            sendSpan.error(e);

            terminate();
        } finally {
            flushStatusUpdates(false);

            sendSpan.finish();
        }
    }

    private void flushStatusUpdates(boolean force) {
        var thresholdReached = mongoUpdateBatch.getSize() >= pulsarConfig.getSseBatchSize();
        if (thresholdReached || force) {
            var flushSpan = tracingHelper.startScopedSpan("flush event updates");
            try {
                mongoUpdateBatch.flush();
            } catch (MongoException e) {
                log.error("Error occurred while updating the event status of events for {}: {}", eventMessageSupplier.getSubscriptionId(), e.getMessage(), e);
                throw e;
            } finally {
                flushSpan.finish();
            }
        }
    }

    /**
     * Pushes metadata related to the subscription event message, such as status and exceptions.
     *
     * This method starts a scoped debug span for pushing metadata, adds tags to the span from the subscription event
     * message, adds a "status" tag, creates a status message, sends it to the event writer, and tracks the message
     * in the deduplication service. It finishes the associated span.
     *
     * @param msg The subscription event message.
     * @param status The status of the event message.
     * @param exception The exception associated with the event message, if any.
     */
    private void pushMetadata(SubscriptionEventMessage msg, Status status, @Nullable Exception exception) {
        var trackSpan = tracingHelper.startScopedDebugSpan("push metadata");
        tracingHelper.addTagsToSpanFromSubscriptionEventMessage(trackSpan, msg);
        tracingHelper.addTagsToSpan(trackSpan, List.of(
                Pair.of("status", status.name())
        ));

        StatusMessage statusMessage = new StatusMessage(msg.getUuid(), msg.getEvent().getId(), status, msg.getDeliveryType());

        if (!Objects.isNull(exception)) {
            statusMessage.withThrowable(exception);
        }

        try {
            var afterSendFuture = eventWriter.send(Objects.requireNonNullElse(msg.getEventRetentionTime(), EventRetentionTime.DEFAULT).getTopic(), statusMessage, tracingHelper);

            if (status.equals(Status.DELIVERED) || status.equals(Status.FAILED)) {
                afterSendFuture.thenAccept(result -> deDuplicationService.track(msg));
            }
        } catch (Exception e) {
            var err = String.format("Error occurred while updating the event status: %s", e.getMessage());
            log.error(err, e);
            trackSpan.error(e);
        } finally {
            trackSpan.finish();
        }
    }
}
