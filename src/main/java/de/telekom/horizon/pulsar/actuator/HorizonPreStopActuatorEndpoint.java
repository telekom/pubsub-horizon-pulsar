// Copyright 2024 Deutsche Telekom IT GmbH
//
// SPDX-License-Identifier: Apache-2.0

package de.telekom.horizon.pulsar.actuator;

import de.telekom.horizon.pulsar.config.PodConfig;
import io.fabric8.kubernetes.api.model.EndpointAddress;
import io.fabric8.kubernetes.client.KubernetesClient;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.actuate.endpoint.annotation.Endpoint;
import org.springframework.boot.actuate.endpoint.annotation.WriteOperation;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.concurrent.TimeUnit;

@Slf4j
@Component
@ConditionalOnBean(value = KubernetesClient.class)
@Endpoint(id = "horizon-prestop")
public class HorizonPreStopActuatorEndpoint {

    private final PodConfig podConfig;

    private final KubernetesClient kubernetesClient;

    public HorizonPreStopActuatorEndpoint(PodConfig podConfig, KubernetesClient kubernetesClient) {
        this.podConfig = podConfig;
        this.kubernetesClient = kubernetesClient;
    }

    @WriteOperation
    public void handlePreStop() {
        waitUntil(() -> !isEndpointRegistered(), 10, TimeUnit.SECONDS, 1000);
    }

    @FunctionalInterface
    public interface CheckCondition {
        boolean check();
    }

    public static void waitUntil(CheckCondition condition, long timeout, TimeUnit unit, long delayMillis) {
        long timeoutNanos = unit.toNanos(timeout);
        long startTime = System.nanoTime();

        while (System.nanoTime() - startTime < timeoutNanos) {
            try {
                if (condition.check()) {
                    return; // Exit as soon as the condition is true
                }
                Thread.sleep(delayMillis); // Avoid busy-waiting
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt(); // Restore the interrupted status
                throw new RuntimeException("Thread was interrupted", e);
            }
        }
    }

    public boolean isEndpointRegistered() {
        try {
            var endpoints = kubernetesClient.endpoints().inNamespace(podConfig.getPodNamespace()).withName(podConfig.getServiceName()).get();

            if (endpoints.getSubsets() != null) {
                for (var subset : endpoints.getSubsets()) {
                    List<String> addresses = subset.getAddresses()
                            .stream()
                            .map(EndpointAddress::getIp)
                            .toList();

                    if (addresses.contains(podConfig.getPodIp())) {
                        return true; // Pod's IP exists in the endpoints list
                    }
                }
            }
        } catch (Exception e) {
            log.error("Could not check endpoint status", e);
        }

        return false;
    }
}