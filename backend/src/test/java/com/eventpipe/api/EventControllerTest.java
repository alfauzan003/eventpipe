package com.eventpipe.api;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

import com.eventpipe.event.EventEnvelope;
import com.eventpipe.event.EventPublisher;
import com.eventpipe.event.ProcessedEventService;

@WebMvcTest(EventController.class)
class EventControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @MockBean
    private EventPublisher eventPublisher;

    @MockBean
    private ProcessedEventService processedEventService;

    @Test
    void publishReturns202WithFullEnvelope() throws Exception {
        UUID eventId = UUID.fromString("11111111-2222-3333-4444-555555555555");
        EventEnvelope envelope = new EventEnvelope(eventId, "order.created",
                Map.of("orderId", 42), Instant.parse("2024-01-01T00:00:00Z"));
        when(eventPublisher.publish(eq("order.created"), any())).thenReturn(envelope);

        mockMvc.perform(post("/api/events")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"type\":\"order.created\",\"payload\":{\"orderId\":42}}"))
                .andExpect(status().isAccepted())
                .andExpect(jsonPath("$.eventId").value(eventId.toString()))
                .andExpect(jsonPath("$.type").value("order.created"))
                .andExpect(jsonPath("$.payload.orderId").value(42))
                .andExpect(jsonPath("$.timestamp").value("2024-01-01T00:00:00Z"));
    }

    @Test
    void publishRejectsBlankType() throws Exception {
        mockMvc.perform(post("/api/events")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"type\":\"   \",\"payload\":{\"orderId\":42}}"))
                .andExpect(status().isBadRequest());
    }

    @Test
    void publishRejectsMissingType() throws Exception {
        mockMvc.perform(post("/api/events")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"payload\":{\"orderId\":42}}"))
                .andExpect(status().isBadRequest());
    }

    @Test
    void publishRejectsMissingPayload() throws Exception {
        mockMvc.perform(post("/api/events")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"type\":\"order.created\"}"))
                .andExpect(status().isBadRequest());
    }

    @Test
    void countsReturnsTotalsAndByType() throws Exception {
        Map<String, Long> byType = new LinkedHashMap<>();
        byType.put("order.created", 2L);
        byType.put("order.shipped", 1L);
        Map<String, Object> counts = new LinkedHashMap<>();
        counts.put("totalProcessed", 3L);
        counts.put("byType", byType);
        when(processedEventService.counts()).thenReturn(counts);

        mockMvc.perform(get("/api/events/counts"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.totalProcessed").value(3))
                .andExpect(jsonPath("$.byType['order.created']").value(2))
                .andExpect(jsonPath("$.byType['order.shipped']").value(1));
    }
}
