package de.telekom.horizon.pulsar.exception;

public class SubscriberDoesNotMatchSubscriptionException extends HorizonPulsarException {
    public SubscriberDoesNotMatchSubscriptionException(String message) {
        super(message);
    }
}
