package de.telekom.horizon.pulsar.exception;

public class CouldNotPickMessageException extends HorizonPulsarException {

    public CouldNotPickMessageException(Throwable t) {
        super(t);
    }

    public CouldNotPickMessageException(String message, Throwable t) {
        super(message, t);
    }
}
