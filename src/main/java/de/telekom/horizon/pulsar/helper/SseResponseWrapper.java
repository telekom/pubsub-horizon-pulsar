// Copyright 2024 Deutsche Telekom IT GmbH
//
// SPDX-License-Identifier: Apache-2.0

package de.telekom.horizon.pulsar.helper;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import de.telekom.eni.pandora.horizon.model.event.Event;
import de.telekom.eni.pandora.horizon.model.event.SubscriptionEventMessage;
import lombok.Getter;

import java.util.List;
import java.util.Map;

/**
 * Wrapper class for Server-Sent Event (SSE) responses.
 *
 * This class extends the base class {@link Event} and includes additional fields
 * for representing SSE-specific information, such as HTTP headers. The class is annotated
 * with Lombok's {@code @Getter} and Jackson's {@code @JsonInclude} annotations to generate
 * getters and control JSON serialization behavior, respectively.
 */
@Getter
@JsonInclude(JsonInclude.Include.NON_NULL)
public class SseResponseWrapper extends Event {

    @JsonProperty("httpHeaders")
    private Map<String, List<String>> httpHeaders;

    /**
     * Constructs an instance of {@code SseResponseWrapper} based on the provided {@code SubscriptionEventMessage}.
     *
     * @param msg               The subscription event message containing information to be wrapped.
     * @param includeHttpHeaders Whether to include HTTP headers in the response.
     */
    public SseResponseWrapper(SubscriptionEventMessage msg, boolean includeHttpHeaders) {
        this.setId(msg.getEvent().getId());
        this.setType(msg.getEvent().getType());
        this.setSource(msg.getEvent().getSource());
        this.setSpecVersion(msg.getEvent().getSpecVersion());
        this.setDataContentType(msg.getEvent().getDataContentType());
        this.setDataRef(msg.getEvent().getDataRef());
        this.setTime(msg.getEvent().getTime());
        this.setData(msg.getEvent().getData());

        this.httpHeaders = includeHttpHeaders ? msg.getHttpHeaders() : null;
    }
}
