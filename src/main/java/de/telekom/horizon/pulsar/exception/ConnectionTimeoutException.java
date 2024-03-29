// Copyright 2024 Deutsche Telekom IT GmbH
//
// SPDX-License-Identifier: Apache-2.0

package de.telekom.horizon.pulsar.exception;

public class ConnectionTimeoutException extends HorizonPulsarException {
    public ConnectionTimeoutException(String message) {
        super(message);
    }
}
