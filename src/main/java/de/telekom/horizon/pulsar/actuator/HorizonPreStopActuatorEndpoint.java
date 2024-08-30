package de.telekom.horizon.pulsar.actuator;

import de.telekom.horizon.pulsar.cache.ConnectionCache;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.actuate.endpoint.annotation.Endpoint;
import org.springframework.boot.actuate.endpoint.annotation.WriteOperation;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@Endpoint(id = "horizon-prestop")
public class HorizonPreStopActuatorEndpoint {

    private final ConnectionCache connectionCache;

    public HorizonPreStopActuatorEndpoint(ConnectionCache connectionCache) {
        this.connectionCache = connectionCache;
    }

    @WriteOperation
    public void handlePreStop() {
        log.info("Got PreStop request. Terminating all connections...");
        connectionCache.terminateAllConnections();
    }
}