// Copyright 2024 Deutsche Telekom IT GmbH
//
// SPDX-License-Identifier: Apache-2.0

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
