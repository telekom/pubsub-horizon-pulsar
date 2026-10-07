// Copyright 2026 Deutsche Telekom AG
//
// SPDX-License-Identifier: Apache-2.0

package de.telekom.horizon.pulsar.api;

import de.telekom.eni.pandora.horizon.exception.SubscriptionCacheReadException;
import de.telekom.eni.pandora.horizon.model.common.ProblemMessage;
import org.junit.jupiter.api.Test;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.http.HttpStatus;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.web.context.request.ServletWebRequest;

import static org.assertj.core.api.Assertions.assertThat;

class RestResponseEntityExceptionHandlerTest {

    @Test
    void subscriptionCacheReadFailureReturnsInternalServerErrorLikeOtherUnhandledErrors() {
        var handler = new RestResponseEntityExceptionHandler(new GenericApplicationContext());

        var response = handler.handleAny(
                new SubscriptionCacheReadException("cache unavailable"),
                new ServletWebRequest(new MockHttpServletRequest()));

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.INTERNAL_SERVER_ERROR);
        assertThat(response.getBody()).isInstanceOfSatisfying(ProblemMessage.class, message ->
                assertThat(message.getTitle()).isEqualTo(RestResponseEntityExceptionHandler.DEFAULT_ERROR_TITLE));
    }
}
