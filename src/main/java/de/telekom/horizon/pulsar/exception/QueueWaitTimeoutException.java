package de.telekom.horizon.pulsar.exception;

public class QueueWaitTimeoutException extends HorizonPulsarException {
    public QueueWaitTimeoutException(String message) {
        super(message);
    }
}
