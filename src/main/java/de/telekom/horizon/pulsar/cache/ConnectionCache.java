package de.telekom.horizon.pulsar.cache;

import de.telekom.horizon.pulsar.service.SseTask;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.concurrent.ConcurrentHashMap;

/**
 * Component for managing and caching Server-Sent Events (SSE) connections.
 *
 * This component maintains a cache of SSE connections, associating them with a unique key
 * generated from the environment and subscriptionId. It provides methods to add, remove, and
 * terminate connections associated with a specific subscription.
 */
@Slf4j
@Component
public class ConnectionCache {

    // environment--subscriptionId -> SseTask
    private final ConcurrentHashMap<String, SseTask> map = new ConcurrentHashMap<>();

    /**
     * Adds an SSE connection to the cache for the specified subscription.
     *
     * @param environment The environment associated with the subscription.
     * @param subscriptionId The unique identifier for the subscription.
     * @param pollTask The SSE task representing the connection to be added.
     */
    public void addConnectionForSubscription(String environment, String subscriptionId, SseTask pollTask) {
        terminateConnection(map.put(keyOf(environment, subscriptionId), pollTask));
    }

    /**
     * Removes the SSE connection from the cache for the specified subscription.
     *
     * @param environment The environment associated with the subscription.
     * @param subscriptionId The unique identifier for the subscription.
     */
    public void removeConnectionForSubscription(String environment, String subscriptionId) {
        terminateConnection(map.remove(keyOf(environment, subscriptionId)));
    }

    /**
     * Terminates the specified SSE task, if not null.
     *
     * @param task The SSE task to be terminated.
     */
    private void terminateConnection(SseTask task) {
        if (task != null) {
            task.terminate();
        }
    }

    /**
     * Generates a unique key for the cache using the environment and subscriptionId.
     *
     * @param environment The environment associated with the subscription.
     * @param subscriptionId The unique identifier for the subscription.
     * @return A unique key for the cache based on the provided environment and subscriptionId.
     */
    private String keyOf(String environment, String subscriptionId) {
        return String.format("%s--%s", environment, subscriptionId);
    }
}
