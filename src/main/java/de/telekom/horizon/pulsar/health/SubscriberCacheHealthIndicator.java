package de.telekom.horizon.pulsar.health;

import de.telekom.eni.pandora.horizon.kubernetes.InformerStoreInitHandler;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.stereotype.Component;

/**
 * Health indicator for monitoring the status of the Subscriber Cache's initialization.
 *
 * This component implements the {@link HealthIndicator} interface to provide health information
 * regarding the initialization status of the Subscriber Cache. It relies on an {@link InformerStoreInitHandler}
 * to determine if the cache is fully synchronized, and exposes additional details about the initialization process.
 */
@Component
public class SubscriberCacheHealthIndicator implements HealthIndicator {

    private final InformerStoreInitHandler informerStoreInitHandler;

    /**
     * Constructs an instance of {@code SubscriberCacheHealthIndicator} with the specified {@code InformerStoreInitHandler}.
     *
     * @param informerStoreInitHandler The handler for monitoring Subscriber Cache initialization.
     */
    public SubscriberCacheHealthIndicator(InformerStoreInitHandler informerStoreInitHandler) {
        this.informerStoreInitHandler = informerStoreInitHandler;
    }

    /**
     * Checks the health of the Subscriber Cache based on its initialization status.
     *
     * @return A {@link Health} object indicating the health status and additional details.
     */
    @Override
    public Health health() {
        Health.Builder status = Health.up();

        // Check if the Subscriber Cache is fully synchronized
        if (!informerStoreInitHandler.isFullySynced()) {
            status = Health.down();
        }

        // Include details about the initialization process in the health status
        return status.withDetails(informerStoreInitHandler.getInitalSyncedStats()).build();
    }
}