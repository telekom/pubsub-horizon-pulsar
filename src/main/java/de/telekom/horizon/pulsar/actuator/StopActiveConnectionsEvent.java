// Copyright 2024 Deutsche Telekom IT GmbH
//
// SPDX-License-Identifier: Apache-2.0

package de.telekom.horizon.pulsar.actuator;

import org.springframework.context.ApplicationEvent;

public class StopActiveConnectionsEvent extends ApplicationEvent {
    private String message;

    public StopActiveConnectionsEvent(Object source, String message) {
        super(source);
        this.message = message;
    }
    public String getMessage() {
        return message;
    }
}