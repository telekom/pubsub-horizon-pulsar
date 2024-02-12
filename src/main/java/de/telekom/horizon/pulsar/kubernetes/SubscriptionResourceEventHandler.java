// Copyright 2024 Deutsche Telekom IT GmbH
//
// SPDX-License-Identifier: Apache-2.0

package de.telekom.horizon.pulsar.kubernetes;


import de.telekom.eni.pandora.horizon.kubernetes.InformerStoreInitSupport;
import de.telekom.eni.pandora.horizon.kubernetes.resource.SubscriptionResource;
import de.telekom.horizon.pulsar.cache.ConnectionCache;
import de.telekom.horizon.pulsar.cache.SubscriberCache;
import de.telekom.horizon.pulsar.config.PulsarConfig;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.client.informers.ResourceEventHandler;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Service class handling events related to Subscription resources and supporting initialization.
 *
 * This class implements the {@link ResourceEventHandler} interface for Subscription resources
 * and extends the {@link InformerStoreInitSupport} interface to support initialization.
 * It manages events such as addition, update, and deletion of Subscription resources and interacts
 * with the {@link SubscriberCache}, {@link ConnectionCache}, and {@link PulsarConfig} to maintain state.
 * The class is annotated with Lombok's {@code @Slf4j} for generating a logger and {@code @Service} for
 * indicating that it is a Spring service.
 */
@Service
@Slf4j
public class SubscriptionResourceEventHandler implements ResourceEventHandler<SubscriptionResource>, InformerStoreInitSupport {

    private final SubscriberCache subscriberCache;
    private final ConnectionCache connectionCache;
    private final PulsarConfig pulsarConfig;

    /**
     * Constructs an instance of {@code SubscriptionResourceEventHandler} with the specified dependencies.
     *
     * @param subscriberCache Cache for managing subscriberIds associated with SSE subscriptions.
     * @param connectionCache Cache for managing connections associated with SSE subscriptions.
     * @param pulsarConfig    Configuration for Pulsar.
     */
    @Autowired
    public SubscriptionResourceEventHandler(SubscriberCache subscriberCache,
                                            ConnectionCache connectionCache,
                                            PulsarConfig pulsarConfig) {
        this.subscriberCache = subscriberCache;
        this.connectionCache = connectionCache;
        this.pulsarConfig = pulsarConfig;
    }

    /**
     * Handles the addition of a Subscription resource.
     *
     * @param resource The added Subscription resource.
     */
    @Override
    public void onAdd(SubscriptionResource resource) {
        log.debug("Add SubscriptionResource: {}", resource);

        var environment = determineEnvironment(resource).orElse(pulsarConfig.getDefaultEnvironment());
        var subscription = resource.getSpec().getSubscription();

        subscriberCache.add(environment, subscription.getSubscriptionId(), subscription.getSubscriberId());
    }

    /**
     * Handles the update of a Subscription resource.
     *
     * @param oldResource The old state of the Subscription resource.
     * @param newResource The updated state of the Subscription resource.
     */
    @Override
    public void onUpdate(SubscriptionResource oldResource, SubscriptionResource newResource) {
        log.debug("Update SubscriptionResource: {}", newResource);

        var subscription = newResource.getSpec().getSubscription();
        var environment = determineEnvironment(newResource).orElse(pulsarConfig.getDefaultEnvironment());
        var subscriptionId = subscription.getSubscriptionId();

        subscriberCache.add(environment, subscriptionId, subscription.getSubscriberId());

        if (!Objects.equals(newResource.getSpec().getSseActiveOnPod(), pulsarConfig.getPodName())) {
            connectionCache.removeConnectionForSubscription(environment, subscriptionId);
        }
    }

    /**
     * Handles the deletion of a Subscription resource.
     *
     * @param resource               The deleted Subscription resource.
     * @param deletedFinalStateUnknown Indicates whether the final state of the resource is unknown.
     */
    @Override
    public void onDelete(SubscriptionResource resource, boolean deletedFinalStateUnknown) {
        log.debug("Delete SubscriptionsRessource: {}", resource);

        var subscription = resource.getSpec().getSubscription();
        var environment = determineEnvironment(resource).orElse(pulsarConfig.getDefaultEnvironment());
        var subscriptionId = subscription.getSubscriptionId();

        subscriberCache.remove(environment, subscriptionId);
        connectionCache.removeConnectionForSubscription(environment, subscriptionId);
    }

    /**
     * Determines the environment associated with a Subscription resource.
     *
     * @param resource The Subscription resource.
     * @return An optional containing the environment if present, otherwise empty.
     */
    private Optional<String> determineEnvironment(SubscriptionResource resource) {
        return Optional.ofNullable(resource.getSpec().getEnvironment());
    }

    /**
     * Adds all Subscription resources in the provided list.
     *
     * @param list The list of Subscription resources to be added.
     * @param <T>  Type parameter representing a resource that extends {@link HasMetadata}.
     */
    @Override
    public <T extends HasMetadata> void addAll(List<T> list) {
        list.forEach(l -> onAdd((SubscriptionResource) l));
    }
}
