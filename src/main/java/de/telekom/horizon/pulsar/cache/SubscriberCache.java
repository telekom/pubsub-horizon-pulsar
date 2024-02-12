package de.telekom.horizon.pulsar.cache;


import org.springframework.stereotype.Component;

import java.util.concurrent.ConcurrentHashMap;

@Component
public class SubscriberCache {

    //environment--subscriptionId -> subscriberId]
    private final ConcurrentHashMap<String, String> map = new ConcurrentHashMap<>();

    /**
     * Adds a subscriberId to the cache for the specified subscription.
     *
     * @param environment     The environment associated with the subscription.
     * @param subscriptionId  The unique identifier for the subscription.
     * @param subscriberId    The identifier for the subscriber to be added.
     */
    public void add(String environment, String subscriptionId, String subscriberId) {
        map.put(keyOf(environment, subscriptionId), subscriberId);
    }

    /**
     * Removes the subscriberId from the cache for the specified subscription.
     *
     * @param environment     The environment associated with the subscription.
     * @param subscriptionId  The unique identifier for the subscription.
     */
    public void remove(String environment, String subscriptionId) {
        map.remove(keyOf(environment, subscriptionId));
    }

    /**
     * Retrieves the subscriberId for the specified subscription.
     *
     * @param environment     The environment associated with the subscription.
     * @param subscriptionId  The unique identifier for the subscription.
     * @return The subscriberId associated with the specified subscription, or {@code null} if not found.
     */
    public String get(String environment, String subscriptionId) {
        return map.get(keyOf(environment, subscriptionId));
    }

    /**
     * Generates a unique key for the cache using the environment and subscriptionId.
     *
     * @param environment     The environment associated with the subscription.
     * @param subscriptionId  The unique identifier for the subscription.
     * @return A unique key for the cache based on the provided environment and subscriptionId.
     */
    private String keyOf(String environment, String subscriptionId) {
        return String.format("%s--%s", environment, subscriptionId);
    }
}
