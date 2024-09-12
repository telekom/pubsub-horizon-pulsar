// Copyright 2024 Deutsche Telekom IT GmbH
//
// SPDX-License-Identifier: Apache-2.0

package de.telekom.horizon.pulsar.helper;

import brave.Span;
import brave.Tracer;
import de.telekom.eni.pandora.horizon.model.event.SubscriptionEventMessage;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.util.Optional;

/**
 * Context class representing the context of an event message in a subscription.
 *
 * This class encapsulates information related to a subscription event message, including
 * whether to include HTTP headers or stream limits, and optional tracing components such as a span and span in scope.
 * It provides a method to finish the span and span in scope if they are present.
 */

@NoArgsConstructor
@AllArgsConstructor
public class EventMessageContext {
    @Getter
    private SubscriptionEventMessage subscriptionEventMessage;
    @Getter
    private Boolean includeHttpHeaders;
    @Getter
    private StreamLimit streamLimit;

    private Span span;
    private Tracer.SpanInScope spanInScope;
    public void finishSpan() {
        Optional.ofNullable(spanInScope).ifPresent(Tracer.SpanInScope::close);
        Optional.ofNullable(span).ifPresent(Span::finish);
    }
}
