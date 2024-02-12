package de.telekom.horizon.pulsar.cache;

import de.telekom.eni.pandora.horizon.kubernetes.resource.SubscriptionResource;
import de.telekom.eni.pandora.horizon.metrics.HorizonMetricsHelper;
import de.telekom.horizon.pulsar.config.PulsarConfig;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.micrometer.core.instrument.Tags;
import org.springframework.stereotype.Component;

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
public class ConnectionGaugeCache {

    private final PulsarConfig pulsarConfig;

    private final KubernetesClient kubernetesClient;

    private final HorizonMetricsHelper metricsHelper;

    private final ConcurrentHashMap<String, AtomicInteger> cache = new ConcurrentHashMap<>();

    /**
     * Constructs an instance of {@code ConnectionGaugeCache} with the specified dependencies.
     *
     * @param pulsarConfig   Configuration for Pulsar.
     * @param kubernetesClient   Kubernetes client for interacting with resources.
     * @param metricsHelper  Helper for managing Horizon metrics.
     */
    public ConnectionGaugeCache(PulsarConfig pulsarConfig, KubernetesClient kubernetesClient, HorizonMetricsHelper metricsHelper) {
        this.pulsarConfig = pulsarConfig;
        this.kubernetesClient = kubernetesClient;
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
        return cache.computeIfAbsent(keyOf(environment, subscriptionId), p -> createGaugeForSubscription(environment, subscriptionId));
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
        var resource = kubernetesClient.resources(SubscriptionResource.class).inNamespace(pulsarConfig.getNamespace()).withName(subscriptionId).get();
        var tags = buildTagsForSseSubscription(environment, resource);

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
