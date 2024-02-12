// Copyright 2024 Deutsche Telekom IT GmbH
//
// SPDX-License-Identifier: Apache-2.0

package de.telekom.horizon.pulsar.utils;

import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.server.mock.KubernetesServer;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;

@TestConfiguration
public class MockedKubernetesConfiguration {

    public static KubernetesServer server = new KubernetesServer(false, true);

    @Bean
    @Primary
    KubernetesClient mockedKubernetesClient() {
        return server.getClient();
    }

}
