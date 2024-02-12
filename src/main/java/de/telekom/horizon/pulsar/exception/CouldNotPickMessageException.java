// Copyright 2024 Deutsche Telekom IT GmbH
//
// SPDX-License-Identifier: Apache-2.0

package de.telekom.horizon.pulsar.exception;

public class CouldNotPickMessageException extends HorizonPulsarException {

    public CouldNotPickMessageException(Throwable t) {
        super(t);
    }

    public CouldNotPickMessageException(String message, Throwable t) {
        super(message, t);
    }
}
