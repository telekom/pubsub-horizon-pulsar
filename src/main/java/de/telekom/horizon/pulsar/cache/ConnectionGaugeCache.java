// Copyright 2024 Deutsche Telekom IT GmbH
//
// SPDX-License-Identifier: Apache-2.0

package de.telekom.horizon.pulsar.cache;

import de.telekom.eni.pandora.horizon.cache.service.JsonCacheService;
import de.telekom.eni.pandora.horizon.cache.util.Query;
import de.telekom.eni.pandora.horizon.exception.JsonCacheException;
import de.telekom.eni.pandora.horizon.kubernetes.resource.SubscriptionResource;
import de.telekom.eni.pandora.horizon.metrics.HorizonMetricsHelper;
import de.telekom.horizon.pulsar.config.PulsarConfig;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.micrometer.core.instrument.Tags;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static de.telekom.eni.pandora.horizon.metrics.HorizonMetricsConstants.*;

/**
 * Component for managing and caching gauges representing open Server-Sent Events (SSE) connections.
 *
 * This component is responsible for creating and caching gauges that measure the number of open SSE connections
 * for a specific subscription. It utilizes metrics provided by a metrics registry, and connections are
 * associated with a unique key generated from the environment and subscriptionId.
 */
@Component
@Slf4j
public class ConnectionGaugeCache {

    private final PulsarConfig pulsarConfig;

    private final JsonCacheService<SubscriptionResource> cache;

    private final ConcurrentHashMap<String, AtomicInteger> metricsCache = new ConcurrentHashMap<>();

    private final HorizonMetricsHelper metricsHelper;

    /**
     * Constructs an instance of {@code ConnectionGaugeCache} with the specified dependencies.
     *
     * @param pulsarConfig   Configuration for Pulsar.
     * @param cache          Cache for storing subscription resources.
     * @param metricsHelper  Helper for managing Horizon metrics.
     */
    public ConnectionGaugeCache(PulsarConfig pulsarConfig, JsonCacheService<SubscriptionResource> cache, HorizonMetricsHelper metricsHelper) {
        this.pulsarConfig = pulsarConfig;
        this.cache = cache;
        this.metricsHelper = metricsHelper;
    }

    /**
     * Gets or creates a gauge representing the number of open SSE connections for the specified subscription.
     *
     * @param environment   The environment associated with the subscription.
     * @param subscriptionId The unique identifier for the subscription.
     * @return A gauge representing the number of open SSE connections for the specified subscription.
     */
    public AtomicInteger getOrCreateGaugeForSubscription(String environment, String subscriptionId) {
        return metricsCache.computeIfAbsent(keyOf(environment, subscriptionId), p -> createGaugeForSubscription(environment, subscriptionId));
    }

    /**
     * Builds tags for identifying SSE subscriptions in metrics.
     *
     * @param environment The environment associated with the subscription.
     * @param resource    The Kubernetes Subscription resource.
     * @return Tags for identifying SSE subscriptions in metrics.
     */
    private Tags buildTagsForSseSubscription(String environment, SubscriptionResource resource) {
        return Tags.of(
                TAG_ENVIRONMENT, environment,
                TAG_EVENT_TYPE, resource.getSpec().getSubscription().getType(),
                TAG_DELIVERY_TYPE, resource.getSpec().getSubscription().getDeliveryType(),
                TAG_SUBSCRIPTION_ID, resource.getSpec().getSubscription().getSubscriptionId()
        );
    }

    /**
     * Creates a gauge for measuring the number of open SSE connections for the specified subscription.
     *
     * @param environment   The environment associated with the subscription.
     * @param subscriptionId The unique identifier for the subscription.
     * @return A gauge measuring the number of open SSE connections for the specified subscription.
     */
    private AtomicInteger createGaugeForSubscription(String environment, String subscriptionId) {

        var env = environment;
        if (Objects.equals(pulsarConfig.getDefaultEnvironment(), environment)) {
            env = "default";
        }

        var builder = Query.builder(SubscriptionResource.class)
                .addMatcher("spec.environment", env)
                .addMatcher("spec.subscription.subscriptionId", subscriptionId);

        List<SubscriptionResource> list = new ArrayList<>();
        try {
            list = cache.getQuery(builder.build());

        } catch (JsonCacheException e) {
            log.error("Error occurred while executing query on JsonCacheService", e);
        }

        var resource = list.stream()
                .findFirst()
                .orElse(null);

        Tags tags = Tags.empty();

        if (resource != null) {
                tags = buildTagsForSseSubscription(environment, resource);
        }

        return metricsHelper.getRegistry().gauge(METRIC_OPEN_SSE_CONNECTIONS, tags, new AtomicInteger(0));
    }

    /**
     * Generates a unique key for the cache using the environment and subscriptionId.
     *
     * @param environment   The environment associated with the subscription.
     * @param subscriptionId The unique identifier for the subscription.
     * @return A unique key for the cache based on the provided environment and subscriptionId.
     */
    private String keyOf(String environment, String subscriptionId) {
        return String.format("%s--%s", environment, subscriptionId);
    }
}
