// Copyright 2024 Deutsche Telekom IT GmbH
//
// SPDX-License-Identifier: Apache-2.0

package de.telekom.horizon.pulsar.service;

import de.telekom.eni.pandora.horizon.cache.service.DeDuplicationService;
import de.telekom.horizon.pulsar.cache.ConnectionCache;
import de.telekom.horizon.pulsar.cache.SubscriberCache;
import de.telekom.horizon.pulsar.config.PulsarConfig;
import de.telekom.horizon.pulsar.exception.SubscriberDoesNotMatchSubscriptionException;
import de.telekom.horizon.pulsar.helper.SseTaskStateContainer;
import lombok.extern.slf4j.Slf4j;
import org.apache.logging.log4j.util.Strings;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.context.event.ContextClosedEvent;
import org.springframework.context.event.EventListener;
import org.springframework.kafka.event.ContainerStoppedEvent;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.stereotype.Service;


/**
 * Service class for handling Server-Sent Events (SSE).
 * This class, annotated with {@code @Slf4j} and {@code @Service}, provides functionality
 * for managing Server-Sent Events. It includes methods for initializing a thread pool, starting
 * a subscription resource listener, validating subscriberIds for subscriptions, and initiating
 * the emission of events. The class collaborates with other components such as {@code TokenService},
 * {@code SseTaskFactory}, {@code SubscriptionResourceListener}, {@code SubscriberCache}, and
 * {@code PulsarConfig}.
 */
@Slf4j
@Service
public class SseService {

    private final TokenService tokenService;
    private final SseTaskFactory sseTaskFactory;
    private final SubscriberCache subscriberCache;
    private final PulsarConfig pulsarConfig;

    private final ThreadPoolTaskExecutor taskExecutor = new ThreadPoolTaskExecutor();

    /**
     * Constructs an instance of {@code SseService}.
     *
     * @param tokenService                  The {@link TokenService} for handling token-related operations.
     * @param sseTaskFactory                The {@link SseTaskFactory} for creating Server-Sent Event tasks.
     * @param subscriberCache               The {@link SubscriberCache} for caching subscriber information.
     * @param pulsarConfig                  The {@link PulsarConfig} for Pulsar-related configuration.
     */
    @Autowired
    public SseService(TokenService tokenService,
                      SseTaskFactory sseTaskFactory,
                      SubscriberCache subscriberCache,
                      PulsarConfig pulsarConfig) {

        this.tokenService = tokenService;
        this.sseTaskFactory = sseTaskFactory;
        this.subscriberCache = subscriberCache;
        this.pulsarConfig = pulsarConfig;

        init();
    }

    /**
     * Initializes the SseService, setting up the thread pool and starting the subscription resource listeners.
     */
    private void init() {
        this.taskExecutor.setMaxPoolSize(pulsarConfig.getThreadPoolSize());
        this.taskExecutor.setCorePoolSize(pulsarConfig.getThreadPoolSize());
        this.taskExecutor.setQueueCapacity(pulsarConfig.getQueueCapacity());
        this.taskExecutor.afterPropertiesSet();

    }


    /**
     * Validates that the subscriberID matches the subscription, throwing an exception if not.
     *
     * @param environment    The environment associated with the subscription.
     * @param subscriptionId The ID of the subscription to validate.
     * @throws SubscriberDoesNotMatchSubscriptionException If the subscriberId does not match the subscription.
     */
    public void validateSubscriberIdForSubscription(String environment, String subscriptionId) throws SubscriberDoesNotMatchSubscriptionException {
        if (pulsarConfig.isEnableSubscriberCheck()) {
            var subscriberId = tokenService.getSubscriberId();

            var oSubscriberId = subscriberCache.getSubscriberId(subscriptionId);
            if (Strings.isBlank(subscriberId) || oSubscriberId.isEmpty() || !subscriberId.equals(oSubscriberId.get())) {
                throw new SubscriberDoesNotMatchSubscriptionException(String.format("The subscription does not belong to subscriber with id '%s'", subscriberId));
            }
        }
    }

    /**
     * Starts emitting events for the specified subscription.
     *
     * @param environment      The environment associated with the subscription.
     * @param subscriptionId   The ID of the subscription for which events should be emitted.
     * @param contentType      The content type for the events.
     * @param includeHttpHeaders A boolean flag indicating whether to include HTTP headers in the emitted events.
     * @return The {@link SseTaskStateContainer} representing the state of the emitted events.
     */
    public SseTaskStateContainer startEmittingEvents(String environment, String subscriptionId, String contentType, boolean includeHttpHeaders) {
        var responseContainer = new SseTaskStateContainer();

        taskExecutor.submit(sseTaskFactory.createNew(environment, subscriptionId, contentType, responseContainer, includeHttpHeaders));

        responseContainer.setReady(pulsarConfig.getSseTimeout());

        return responseContainer;
    }
}
