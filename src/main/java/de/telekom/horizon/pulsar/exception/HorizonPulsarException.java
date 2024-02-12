package de.telekom.horizon.pulsar.exception;

public class HorizonPulsarException extends Exception {

    public HorizonPulsarException() {
        super();
    }

    public HorizonPulsarException(Throwable t) {
        super(t);
    }

    public HorizonPulsarException(String message) {
        super(message);
    }

    public HorizonPulsarException(String message, Throwable t) {
        super(message, t);
    }

}
