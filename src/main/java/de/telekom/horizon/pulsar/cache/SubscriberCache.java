// Copyright 2024 Deutsche Telekom IT GmbH
//
// SPDX-License-Identifier: Apache-2.0

package de.telekom.horizon.pulsar.cache;


import de.telekom.eni.pandora.horizon.cache.service.JsonCacheService;
import de.telekom.eni.pandora.horizon.cache.util.Query;
import de.telekom.eni.pandora.horizon.exception.JsonCacheException;
import de.telekom.eni.pandora.horizon.kubernetes.resource.SubscriptionResource;
import de.telekom.horizon.pulsar.config.PulsarConfig;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

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

    public String getSubscriberId(String environment, String subscriptionId) {

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

        return list.stream()
                .map(sr -> sr.getSpec().getSubscription().getSubscriberId())
                .findFirst()
                .orElse(null);
    }
}
