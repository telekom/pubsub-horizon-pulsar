// Copyright 2024 Deutsche Telekom IT GmbH
//
// SPDX-License-Identifier: Apache-2.0

package de.telekom.horizon.pulsar.helper;

import de.telekom.horizon.pulsar.exception.QueueWaitTimeoutException;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Async;
import org.springframework.web.servlet.mvc.method.annotation.ResponseBodyEmitter;

import java.time.Instant;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Container class representing the state of a Server-Sent Events (SSE) task.
 *
 * This class includes fields and methods for managing the state of an SSE task,
 * such as the response emitter, cancellation status, and a mechanism for setting the task as ready.
 * The class is annotated with Lombok's {@code @Slf4j} and {@code @Getter} annotations for
 * generating a logger and getter methods, respectively.
 */
@Slf4j
@Getter
public class SseTaskStateContainer {

    public static final String TIMEOUT_MESSAGE = "Task was waiting in the queue longer than %s ms.";

    private final ResponseBodyEmitter emitter = new ResponseBodyEmitter();

    private final AtomicBoolean canceled = new AtomicBoolean(false);

    private final AtomicBoolean running = new AtomicBoolean(false);

    /**
     * Sets the task as ready, completing the emitter after a specified timeout.
     *
     * @param timeout The maximum time to wait for the task to become ready, in milliseconds.
     */
    @Async
    public void setReady(long timeout) {
        var startingTime  = Instant.now();
        try {
            while(!running.get()) {
                if (startingTime.plusMillis(timeout).isBefore(Instant.now())) {
                    canceled.compareAndExchange(false, true);

                    var e = new QueueWaitTimeoutException(String.format(TIMEOUT_MESSAGE, timeout));

                    emitter.completeWithError(e);

                    return;
                }

                // busy waiting
                Thread.sleep(1000);
            }
        } catch (InterruptedException e) {
            canceled.compareAndExchange(false, true);
            emitter.completeWithError(e);

            log.error(e.getMessage(), e);
            Thread.currentThread().interrupt();
        }
    }
}
