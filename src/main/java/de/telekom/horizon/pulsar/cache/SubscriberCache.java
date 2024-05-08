// Copyright 2024 Deutsche Telekom IT GmbH
//
// SPDX-License-Identifier: Apache-2.0

package de.telekom.horizon.pulsar.cache;


import de.telekom.eni.pandora.horizon.cache.service.JsonCacheService;
import de.telekom.eni.pandora.horizon.exception.JsonCacheException;
import de.telekom.eni.pandora.horizon.kubernetes.resource.SubscriptionResource;
import de.telekom.horizon.pulsar.config.PulsarConfig;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Optional;

@Component
@Slf4j
public class SubscriberCache {

    private final PulsarConfig pulsarConfig;

    private final JsonCacheService<SubscriptionResource> cache;
    @Autowired
    public SubscriberCache(PulsarConfig pulsarConfig, JsonCacheService<SubscriptionResource> cache) {
        this.pulsarConfig = pulsarConfig;
        this.cache = cache;
    }

    public Optional<String> getSubscriberId(String subscriptionId) {
        Optional<SubscriptionResource> subscription = Optional.empty();
        try {
            subscription = cache.getByKey(subscriptionId);
        } catch (JsonCacheException e) {
            log.error("Error occurred while executing query on JsonCacheService", e);
        }

        return subscription.map(subscriptionResource -> subscriptionResource.getSpec().getSubscription().getSubscriberId());
    }
}
