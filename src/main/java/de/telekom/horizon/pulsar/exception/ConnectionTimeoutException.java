package de.telekom.horizon.pulsar.exception;

public class ConnectionTimeoutException extends HorizonPulsarException {
    public ConnectionTimeoutException(String message) {
        super(message);
    }
}
