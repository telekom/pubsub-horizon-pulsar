package de.telekom.horizon.pulsar.config;

import lombok.Getter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

@Getter
@Configuration
public class PodConfig {

    @Value("${pod.ip}")
    private String podIp;

    @Value("${pod.namespace}")
    private String podNamespace;

    @Value("${pod.service-name}")
    private String serviceName;
}
