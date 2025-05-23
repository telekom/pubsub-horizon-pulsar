// Copyright 2024 Deutsche Telekom IT GmbH
//
// SPDX-License-Identifier: Apache-2.0

package de.telekom.horizon.pulsar.cache;

import com.hazelcast.client.HazelcastClientOfflineException;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.topic.ITopic;
import com.hazelcast.topic.Message;
import com.hazelcast.topic.MessageListener;
import de.telekom.horizon.pulsar.exception.CacheInitializationException;
import de.telekom.horizon.pulsar.helper.WorkerClaim;
import de.telekom.horizon.pulsar.service.SseTask;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.UUID;
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
public class ConnectionCache implements MessageListener<WorkerClaim> {

    // subscriptionId -> SseTask
    private final ConcurrentHashMap<String, SseTask> map = new ConcurrentHashMap<>();

    private UUID workerId;

    private ITopic<WorkerClaim> workers;

    private final HazelcastInstance hazelcastInstance;

    public ConnectionCache(HazelcastInstance hazelcastInstance) {
        try {
            initConnection(hazelcastInstance);
        } catch (CacheInitializationException e) {
            log.error("Failed to initialize connection cache: {}", e.getMessage());
        }
        this.hazelcastInstance = hazelcastInstance;
    }

    @Override
    public void onMessage(Message<WorkerClaim> workerClaim) {
        var isLocalMember = workerClaim.getMessageObject().getWorkerId().compareTo(workerId) == 0;
        if (!isLocalMember) {
            var subscriptionId = workerClaim.getMessageObject().getSubscriptionId();
            removeConnectionForSubscription(subscriptionId);
        }
    }

    /**
     * Removes the SSE connection from the cache for the specified subscription.
     *
     * @param subscriptionId The unique identifier for the subscription.
     */
    public void removeConnectionForSubscription(String subscriptionId) {
        terminateConnection(map.remove(subscriptionId));
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
     * Claims the connection for the specified subscription and updates the local connection cache.
     *
     * @param subscriptionId   The ID of the subscription for which the connection is claimed.
     * @param connection       The {@link SseTask} representing the connection.
     */
    public void claimConnectionForSubscription(String subscriptionId, SseTask connection) {
        if (workerId == null || workers == null) {
            initConnection(hazelcastInstance);
            return;
        }
        try {
            workers.publish(new WorkerClaim(subscriptionId, workerId));
            terminateConnection(map.put(subscriptionId, connection));
        } catch (HazelcastClientOfflineException e) {
            log.warn("Failed to claim connection for subscriptionId: {}, errorMessage: {}", subscriptionId, e.getMessage());
            throw new CacheInitializationException(e);
        }
    }

    private void initConnection(HazelcastInstance hazelcastInstance) throws CacheInitializationException {
        try {
            workerId = hazelcastInstance.getLocalEndpoint().getUuid();
            workers = hazelcastInstance.getTopic("workers");
            workers.addMessageListener(this);
        } catch (Exception e) {
            log.warn("Failed to instantiate Hazelcast: {}", e.getMessage());
            throw new CacheInitializationException(e);
        }
    }
}
