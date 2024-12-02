// Copyright 2024 Deutsche Telekom IT GmbH
//
// SPDX-License-Identifier: Apache-2.0

package de.telekom.horizon.pulsar.actuator;

import org.springframework.context.ApplicationEvent;

public class HorizonPreStopEvent extends ApplicationEvent {
    private String message;

    public HorizonPreStopEvent(Object source, String message) {
        super(source);
        this.message = message;
    }
    public String getMessage() {
        return message;
    }
}