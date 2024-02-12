package de.telekom.horizon.pulsar.helper;

import de.telekom.horizon.pulsar.exception.QueueWaitTimeoutException;
import de.telekom.horizon.pulsar.testutils.MockHelper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.web.servlet.mvc.method.annotation.ResponseBodyEmitter;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class SseTaskStateContainerTest {

    @BeforeEach
    void setupSseServiceTest() {
        MockHelper.init();
    }

    @Test
    void testSetReady() {
        var sseTaskStateContainer = new SseTaskStateContainer();

        // We mock the ResponseBodyEmitter, since we want to verify invocations on it
        var emitterMock = spy(ResponseBodyEmitter.class);

        // Since emitter is a private field that is initialized upon creation of SseTaskStateContainer, we need to inject
        // the emitter via ReflectionTestUtils
        ReflectionTestUtils.setField(sseTaskStateContainer, "emitter", emitterMock, ResponseBodyEmitter.class);

        // In another thread we trigger the termination condition of run method of SseTaskStateContainer
        // before we would run in a timeout
        new Thread(() -> {
            await().pollDelay(3, TimeUnit.SECONDS).untilTrue(new AtomicBoolean(true));
            sseTaskStateContainer.getRunning().compareAndExchange(false, true);
        }).start();

        // PUBLIC METHOD WE WANT TO TEST
        sseTaskStateContainer.setReady(10000);

        // We verify that the emitter was never completed with an error
        verify(emitterMock, never()).completeWithError(any());
    }

    @Test
    void testSetReadyWithTimeout() {
        var sseTaskStateContainer = new SseTaskStateContainer();

        // We mock the ResponseBodyEmitter, since we want to verify invocations on it
        var emitterMock = spy(ResponseBodyEmitter.class);

        // Since emitter is a private field that is initialized upon creation of SseTaskStateContainer, we need to inject
        // the emitter via ReflectionTestUtils
        ReflectionTestUtils.setField(sseTaskStateContainer, "emitter", emitterMock, ResponseBodyEmitter.class);

        // In another thread we simulate a timeout
        new Thread(() -> {
            await().pollDelay(10, TimeUnit.SECONDS).untilTrue(new AtomicBoolean(true));
            sseTaskStateContainer.getRunning().compareAndExchange(false, true);
        }).start();

        // PUBLIC METHOD WE WANT TO TEST
        sseTaskStateContainer.setReady(3000);

        // We verify that the emitter has been completed with an error due to the timeout
        verify(emitterMock).completeWithError(any(QueueWaitTimeoutException.class));
        // Task should be flagged as canceled due to the timeout
        assertThat(sseTaskStateContainer.getCanceled()).isTrue();
    }
}
