// Copyright 2024 Deutsche Telekom IT GmbH
//
// SPDX-License-Identifier: Apache-2.0

package de.telekom.horizon.pulsar.api;

import de.telekom.horizon.pulsar.exception.SubscriberDoesNotMatchSubscriptionException;
import de.telekom.horizon.pulsar.helper.StreamLimit;
import de.telekom.horizon.pulsar.service.SseService;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.servlet.mvc.method.annotation.ResponseBodyEmitter;

import java.time.Instant;
import java.util.Objects;

/**
 * REST Controller for managing Server-Sent Events (SSE) subscriptions.
 *
 * This controller provides endpoints for handling SSE subscriptions, including
 * a health check endpoint and a streaming endpoint for SSE events.
 *
 */
@RestController
@Slf4j
@RequestMapping("/v1/{environment}")
public class SseController {

    private static final String APPLICATION_STREAM_JSON_VALUE = "application/stream+json";

    private final SseService sseService;

    /**
     * Constructs an instance of {@code SseController} with the specified {@code SseService}.
     *
     * @param sseService The service for managing SSE subscriptions.
     */
    public SseController(SseService sseService) {
        this.sseService = sseService;
    }

    /**
     * Handles HEAD requests for health checking.
     *
     * @param environment The environment path variable.
     * @return A response indicating the health status without a message body.
     */
    @RequestMapping(value = "/sse", method = RequestMethod.HEAD)
    public ResponseEntity<Void> headRequest(@PathVariable String environment) {
        return ResponseEntity.status(HttpStatus.NO_CONTENT).header("X-Health-Check-Timestamp", Instant.now().toString()).build();
    }

    /**
     * Retrieves SSE stream for the specified subscriptionId.
     *
     * @param environment        The environment path variable.
     * @param subscriptionId     The subscriptionId path variable.
     * @param includeHttpHeaders Whether to include HTTP headers in the response.
     * @param maxNumber          Whether to terminate after a certain number of consumed events.
     * @param maxMinutes         Whether to terminate after a certain time (in minutes).
     * @param maxBytes           Whether to terminate after a certain number of bytes consumed.
     * @param accept             The value of the "Accept" header in the request.
     * @return A response containing a {@code ResponseBodyEmitter} for SSE streaming.
     * @throws SubscriberDoesNotMatchSubscriptionException If the subscriber does not match the specified subscription.
     */
    @GetMapping(value = "/sse/{subscriptionId}", produces = {MediaType.ALL_VALUE, APPLICATION_STREAM_JSON_VALUE, MediaType.TEXT_EVENT_STREAM_VALUE})
    public ResponseEntity<ResponseBodyEmitter> getSseStream(@PathVariable String environment,
                                                            @PathVariable String subscriptionId,
                                                            @RequestParam(defaultValue = "false") boolean includeHttpHeaders,
                                                            @RequestParam(defaultValue = "0") int maxNumber,
                                                            @RequestParam(defaultValue = "0") int maxMinutes,
                                                            @RequestParam(defaultValue = "0") int maxBytes,
                                                            @RequestHeader(value = "Last-Event-ID", required = false) String offset,
                                                            @RequestHeader(value = HttpHeaders.ACCEPT, required = false) String accept) throws SubscriberDoesNotMatchSubscriptionException {

        sseService.validateSubscriberIdForSubscription(environment, subscriptionId);

        if (!Objects.equals(APPLICATION_STREAM_JSON_VALUE, accept) && !Objects.equals(MediaType.TEXT_EVENT_STREAM_VALUE, accept)) {
            log.debug("Unsupported media type '{}', defaulting to '{}'", accept, APPLICATION_STREAM_JSON_VALUE);
            accept = APPLICATION_STREAM_JSON_VALUE;
        }

        var responseContainer = sseService.startEmittingEvents(environment, subscriptionId, accept, StringUtils.isNotEmpty(offset) || includeHttpHeaders, offset, StreamLimit.of(maxNumber, maxMinutes, maxBytes));

        var responseHeaders = new HttpHeaders();
        responseHeaders.add(HttpHeaders.CONTENT_TYPE, accept);
        responseHeaders.add(HttpHeaders.CACHE_CONTROL, "no-cache");
        responseHeaders.add("X-Accel-Buffering", "no");

        return new ResponseEntity<>(responseContainer.getEmitter(), responseHeaders, HttpStatus.OK);
    }

    /**
     * Stops an active SSE stream for the specified subscriptionId.
     *
     * @param environment       The environment path variable.
     * @param subscriptionId    The subscriptionId path variable.
     * @throws SubscriberDoesNotMatchSubscriptionException If the subscriber does not match the specified subscription.
     */
    @PostMapping(value = "/sse/{subscriptionId}/terminate", produces = {MediaType.ALL_VALUE, APPLICATION_STREAM_JSON_VALUE, MediaType.TEXT_EVENT_STREAM_VALUE})
    public ResponseEntity<Void> terminateSseStream(@PathVariable String environment, @PathVariable String subscriptionId) throws SubscriberDoesNotMatchSubscriptionException {

        sseService.validateSubscriberIdForSubscription(environment, subscriptionId);
        sseService.stopEmittingEvents(subscriptionId);

        return ResponseEntity.status(HttpStatus.OK).build();
    }
}
