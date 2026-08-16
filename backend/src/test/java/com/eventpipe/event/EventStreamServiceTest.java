package com.eventpipe.event;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for the SSE broadcast hub. Emitters created outside a servlet
 * container are not initialized yet; the service must tolerate that and never
 * throw, keeping the registered emitters (which deliver once the HTTP response
 * initializes them — covered by {@code EventControllerSseTest}).
 */
class EventStreamServiceTest {

    private EventStreamService streamService;

    @BeforeEach
    void setUp() {
        streamService = new EventStreamService();
    }

    @Test
    void subscribeRegistersAnEmitter() {
        assertThat(streamService.subscriberCount()).isZero();
        streamService.subscribe();
        assertThat(streamService.subscriberCount()).isEqualTo(1);
    }

    @Test
    void broadcastToUninitializedEmittersDoesNotThrowAndKeepsThem() {
        streamService.subscribe();
        streamService.subscribe();
        assertThat(streamService.subscriberCount()).isEqualTo(2);

        streamService.broadcast(new EventEnvelope(
                UUID.randomUUID(), "test.event", Map.of("n", 1), Instant.now()));

        // Sends are buffered until the emitters are initialized by the servlet
        // container; nothing must be dropped or thrown.
        assertThat(streamService.subscriberCount()).isEqualTo(2);
    }

    @Test
    void broadcastWithNoSubscribersIsNoOp() {
        streamService.broadcast(new EventEnvelope(
                UUID.randomUUID(), "test.event", Map.of("n", 1), Instant.now()));
        assertThat(streamService.subscriberCount()).isZero();
    }
}
