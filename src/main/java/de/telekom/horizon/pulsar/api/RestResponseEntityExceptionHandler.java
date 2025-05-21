// Copyright 2024 Deutsche Telekom IT GmbH
//
// SPDX-License-Identifier: Apache-2.0

package de.telekom.horizon.pulsar.api;

import com.mongodb.MongoCommandException;
import com.mongodb.MongoTimeoutException;
import de.telekom.eni.pandora.horizon.model.common.ProblemMessage;
import de.telekom.horizon.pulsar.exception.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.catalina.connector.ClientAbortException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.core.task.TaskRejectedException;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.lang.Nullable;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.context.request.WebRequest;
import org.springframework.web.servlet.mvc.method.annotation.ResponseEntityExceptionHandler;

/**
 * Global exception handler for RESTful services, providing custom responses for various exceptions.
 *
 * This class, annotated with {@code @RestControllerAdvice}, extends the Spring Framework's
 * {@link ResponseEntityExceptionHandler} to provide customized responses for specific exceptions.
 * It handles exceptions such as connection issues, forbidden access, service unavailability,
 * gateway timeout, and generic internal server errors. Additionally, it provides a default
 * exception handler for any unhandled exceptions.
 */
@Slf4j
@ControllerAdvice
public class RestResponseEntityExceptionHandler extends ResponseEntityExceptionHandler {

    public static final String HORIZON_ERROR_HANDLING_URL = "https://developer.telekom.de/docs/src/tardis_customer_handbook/horizon/#error-handling";

    public static final String DEFAULT_ERROR_TITLE = "Something went wrong.";

    public static final String TASK_NOT_STARTED_ERROR_TITLE = "Service is temporarily unable to process the request due to too many active connections. Please retry.  If this happens too often, please contact the support.";

    public static final String CACHE_INITIALIZATION_ERROR_TITLE = "Cache initialization failed. Please try again later.";

    public static final String CACHE_MONGODB_INITIALIZATION_ERROR_TITLE = "Cache and MongoDB initialization failed. Please try again later.";

    private final ApplicationContext applicationContext;

    /**
     * Constructs an instance of {@code RestResponseEntityExceptionHandler} with the specified application context.
     *
     * @param applicationContext The application context.
     */
    @Autowired
    public RestResponseEntityExceptionHandler(ApplicationContext applicationContext) {
        this.applicationContext = applicationContext;
    }

    /**
     * Handles exceptions related to connection cut out scenarios.
     *
     * @param e       The exception.
     * @param request The web request.
     * @return An HTTP 200 (OK) response.
     */
    @ExceptionHandler(value = {
            ConnectionCutOutException.class,
            StreamLimitExceededException.class
    })
    @ResponseStatus(HttpStatus.OK)
    protected ResponseEntity<Object> handleCutOut(Exception e, WebRequest request) {
        return ResponseEntity.ok().build();
    }

    /**
     * Handles exceptions related to forbidden access scenarios.
     *
     * @param e       The exception.
     * @param request The web request.
     * @return An HTTP 403 (Forbidden) response.
     */
    @ExceptionHandler(value = {
            SubscriberDoesNotMatchSubscriptionException.class,
    })
    @ResponseStatus(HttpStatus.FORBIDDEN)
    protected ResponseEntity<Object> handleForbidden(Exception e, WebRequest request) {
        return responseEntityForException(e, HttpStatus.FORBIDDEN, HttpStatus.FORBIDDEN.getReasonPhrase(), request, null);
    }

    /**
     * Handles exceptions related to service unavailability scenarios.
     *
     * @param e       The exception.
     * @param request The web request.
     * @return An HTTP 503 (Service Unavailable) response.
     */
    @ExceptionHandler(value = {
            TaskRejectedException.class,
            QueueWaitTimeoutException.class
    })
    @ResponseStatus(HttpStatus.SERVICE_UNAVAILABLE)
    protected ResponseEntity<Object> handleServiceUnavailable(Exception e, WebRequest request) {
        return responseEntityForException(e, HttpStatus.SERVICE_UNAVAILABLE, TASK_NOT_STARTED_ERROR_TITLE, request, null);
    }

    /**
     * Handles exceptions related to gateway timeout scenarios.
     *
     * @param e       The exception.
     * @param request The web request.
     * @return An HTTP 504 (Gateway Timeout) response.
     */
    @ExceptionHandler(value = {
            ConnectionTimeoutException.class,
    })
    @ResponseStatus(HttpStatus.GATEWAY_TIMEOUT)
    protected ResponseEntity<Object> handleTimeout(Exception e, WebRequest request) {
        return responseEntityForException(e, HttpStatus.GATEWAY_TIMEOUT, HttpStatus.GATEWAY_TIMEOUT.getReasonPhrase(), request, null);
    }

    /**
     * Handles exceptions related to client abort scenarios.
     *
     * @param e       The exception.
     * @param request The web request.
     */
    @ExceptionHandler(value = {
            ClientAbortException.class,
    })
    protected void handleClientAbortException(Exception e, WebRequest request) {
        log.debug("Error occurred: " + e.getMessage(), e);
    }

    /**
     * Handles exceptions related to cache initialization.
     *
     * @param e       The exception.
     * @param request The web request.
     */
    @ExceptionHandler(value = {
            CacheInitializationException.class,
    })
    protected ResponseEntity<Object> handleCacheInitializationException(Exception e, WebRequest request) {
        log.error("Could not initialize cache connection: " + e.getMessage(), e);
        return responseEntityForException(e, HttpStatus.INTERNAL_SERVER_ERROR, CACHE_INITIALIZATION_ERROR_TITLE, request, null);
    }

    /**
     * Handles exceptions related to cache and mongoDB initialization.
     *
     * @param e       The exception.
     * @param request The web request.
     */
    @ExceptionHandler(value = {
            MongoCommandException.class, MongoTimeoutException.class
    })
    protected ResponseEntity<Object> handleCacheAndMongoInitializationException(Exception e, WebRequest request) {
        log.error("Could not initialize cache and mongoDB connection: " + e.getMessage(), e);
        return responseEntityForException(e, HttpStatus.INTERNAL_SERVER_ERROR, CACHE_MONGODB_INITIALIZATION_ERROR_TITLE, request, null);
    }

    /**
     * Handles any unhandled exceptions, providing a default error response.
     *
     * @param e       The exception.
     * @param request The web request.
     * @return A customized error response.
     */
    @ExceptionHandler(Exception.class)
    protected ResponseEntity<Object> handleAny(Exception e, WebRequest request) {
        log.error("Error occurred: " + e.getMessage(), e);

        var headers = new HttpHeaders();

        HttpStatus status;
        try {
            ResponseEntity<?> responseEntity = super.handleException(e, request);

            if (responseEntity != null) {
                status = HttpStatus.valueOf(responseEntity.getStatusCode().value());
                headers = responseEntity.getHeaders();
            } else {
                status = HttpStatus.INTERNAL_SERVER_ERROR;
            }
        } catch(Exception unknownException) {
            status = HttpStatus.INTERNAL_SERVER_ERROR;
        }

        return responseEntityForException(e, status, DEFAULT_ERROR_TITLE, request, headers);
    }

    /**
     * Creates a customized response entity for the specified exception.
     *
     * @param e       The exception.
     * @param status  The HTTP status code.
     * @param title   The error title.
     * @param request The web request.
     * @param headers Additional HTTP headers.
     * @return A customized response entity.
     */
    private ResponseEntity<Object> responseEntityForException(Exception e, HttpStatus status, String title, WebRequest request, @Nullable HttpHeaders headers) {


        var message = new ProblemMessage(HORIZON_ERROR_HANDLING_URL, title);
        message.setStatus(status.value());
        message.setInstance(applicationContext.getId());

        var responseHeaders = new HttpHeaders();
        if (headers != null) {
            responseHeaders.addAll(headers);
        }

        responseHeaders.add(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_PROBLEM_JSON_VALUE);

        return handleExceptionInternal(e, message, responseHeaders, status, request);
    }
}