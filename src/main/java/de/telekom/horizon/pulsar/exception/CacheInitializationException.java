// Copyright 2024 Deutsche Telekom IT GmbH
//
// SPDX-License-Identifier: Apache-2.0

package de.telekom.horizon.pulsar.exception;

public class CacheInitializationException extends RuntimeException {
    public CacheInitializationException() {
        super();
    }

    public CacheInitializationException(Throwable t) {
        super(t);
    }
}


