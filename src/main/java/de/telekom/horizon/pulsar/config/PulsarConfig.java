// Copyright 2024 Deutsche Telekom IT GmbH
//
// SPDX-License-Identifier: Apache-2.0

package de.telekom.horizon.pulsar.config;

import lombok.Getter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

@Getter
@Configuration
public class PulsarConfig {

    @Value("${pulsar.features.subscriberCheck:true}")
    private boolean enableSubscriberCheck;

    @Value("${pulsar.ssePollDelay:1000}")
    private long ssePollDelay;

    @Value("${pulsar.sseTimeout:60000}")
    private long sseTimeout;

    @Value("${pulsar.sseBatchSize:20}")
    private int sseBatchSize;

    @Value("${pulsar.threadpool-size}")
    private int threadPoolSize;

    @Value("${pulsar.queue-capacity}")
    private int queueCapacity;

    @Value("${pulsar.defaultEnvironment}")
    private String defaultEnvironment;
}
