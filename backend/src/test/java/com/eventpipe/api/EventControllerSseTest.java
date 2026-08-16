package com.eventpipe.api;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.request;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.Import;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;

import com.eventpipe.event.EventEnvelope;
import com.eventpipe.event.EventPublisher;
import com.eventpipe.event.EventStreamService;
import com.eventpipe.event.ProcessedEventService;

/**
 * Verifies the SSE endpoint with the real {@link EventStreamService}: a
 * subscription registers an emitter, and a broadcast is written to the open
 * stream as an {@code event.processed} SSE frame.
 */
@WebMvcTest(EventController.class)
@Import(EventStreamService.class)
class EventControllerSseTest {

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private EventStreamService eventStreamService;

    @MockBean
    private EventPublisher eventPublisher;

    @MockBean
    private ProcessedEventService processedEventService;

    @Test
    void streamRegistersSubscriberAndDeliversBroadcastFrames() throws Exception {
        MvcResult stream = mockMvc.perform(get("/api/events/stream"))
                .andExpect(request().asyncStarted())
                .andReturn();

        assertThat(eventStreamService.subscriberCount()).isEqualTo(1);

        EventEnvelope envelope = new EventEnvelope(
                UUID.randomUUID(), "order.created", Map.of("orderId", 42),
                Instant.parse("2026-08-16T12:00:00Z"));
        eventStreamService.broadcast(envelope);

        String frame = awaitSseFrame(stream, "event:" + EventStreamService.PROCESSED_EVENT);
        assertThat(frame).contains(envelope.eventId().toString());
        assertThat(frame).contains("\"order.created\"");
        assertThat(frame).contains("\"timestamp\":\"2026-08-16T12:00:00Z\"");

        // The client connection is still open.
        assertThat(eventStreamService.subscriberCount()).isEqualTo(1);
    }

    /** Polls the streamed response until it contains the expected SSE frame. */
    private String awaitSseFrame(MvcResult stream, String marker) throws Exception {
        long deadline = System.currentTimeMillis() + 5_000;
        String content = "";
        while (System.currentTimeMillis() < deadline) {
            content = stream.getResponse().getContentAsString();
            if (content.contains(marker)) {
                return content;
            }
            Thread.sleep(50);
        }
        throw new AssertionError("SSE stream never contained '%s'; got: %s".formatted(marker, content));
    }
}
