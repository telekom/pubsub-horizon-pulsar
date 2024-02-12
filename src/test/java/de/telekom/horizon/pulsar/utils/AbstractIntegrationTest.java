package de.telekom.horizon.pulsar.utils;

import de.telekom.horizon.pulsar.testutils.EmbeddedKafkaHolder;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

public abstract class AbstractIntegrationTest {

    static {
        EmbeddedKafkaHolder.getEmbeddedKafka();
    }

    public static final EmbeddedKafkaBroker broker = EmbeddedKafkaHolder.getEmbeddedKafka();

    @DynamicPropertySource
    static void dynamicProperties(DynamicPropertyRegistry registry) {
        registry.add("management.tracing.enabled", () -> false);
        registry.add("management.tracing.endpoint", () -> "http://localhost:9411");
        registry.add("horizon.kafka.bootstrapServers", broker::getBrokersAsString);
        registry.add("horizon.victorialog.enabled", () -> false);
    }

}
