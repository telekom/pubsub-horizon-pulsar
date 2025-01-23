// Copyright 2024 Deutsche Telekom IT GmbH
//
// SPDX-License-Identifier: Apache-2.0

package de.telekom.horizon.pulsar;

import lombok.Getter;
import org.springframework.context.ApplicationEvent;

@Getter
public class StopPulsarEvent extends ApplicationEvent {
    private final String message;

    public StopPulsarEvent(Object source, String message) {
        super(source);
        this.message = message;
    }
}