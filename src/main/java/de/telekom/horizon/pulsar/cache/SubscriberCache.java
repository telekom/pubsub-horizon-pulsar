// Copyright 2024 Deutsche Telekom IT GmbH
//
// SPDX-License-Identifier: Apache-2.0

package de.telekom.horizon.pulsar.cache;


import de.telekom.eni.pandora.horizon.cache.service.SubscriptionCacheReader;
import de.telekom.eni.pandora.horizon.exception.SubscriptionCacheReadException;
import de.telekom.eni.pandora.horizon.kubernetes.resource.SubscriptionResource;
import de.telekom.horizon.pulsar.config.PulsarConfig;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Optional;

@Component
public class SubscriberCache {

    private final PulsarConfig pulsarConfig;

    private final SubscriptionCacheReader cache;
    @Autowired
    public SubscriberCache(PulsarConfig pulsarConfig, SubscriptionCacheReader cache) {
        this.pulsarConfig = pulsarConfig;
        this.cache = cache;
    }

    public Optional<String> getSubscriberId(String subscriptionId) throws SubscriptionCacheReadException {
        return cache.getById(subscriptionId)
                .map(subscriptionResource -> subscriptionResource.getSpec().getSubscription().getSubscriberId());
    }
}
