package com.eventpipe;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.servlet.MockMvc;
import org.testcontainers.containers.RabbitMQContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import com.eventpipe.event.EventEnvelope;
import com.eventpipe.event.EventPublisher;
import com.eventpipe.event.ProcessedEventService;

/**
 * End-to-end messaging tests over a real RabbitMQ broker (Testcontainers):
 * REST publish -> broker -> consumer with manual ack, plus idempotency.
 */
@SpringBootTest
@AutoConfigureMockMvc
@Testcontainers
class RabbitMQIntegrationTest {

    private static final String RABBITMQ_IMAGE = "rabbitmq:3.13-alpine";

    @Container
    static final RabbitMQContainer RABBITMQ = new RabbitMQContainer(RABBITMQ_IMAGE);

    @DynamicPropertySource
    static void rabbitmqProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.rabbitmq.host", RABBITMQ::getHost);
        registry.add("spring.rabbitmq.port", RABBITMQ::getAmqpPort);
        registry.add("spring.rabbitmq.username", () -> "guest");
        registry.add("spring.rabbitmq.password", () -> "guest");
    }

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private EventPublisher eventPublisher;

    @Autowired
    private ProcessedEventService processedEventService;

    @BeforeEach
    void resetStore() {
        processedEventService.reset();
    }

    @Test
    void publishViaRestApiIsConsumedOverRealBroker() throws Exception {
        mockMvc.perform(post("/api/events")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"type\":\"order.created\",\"payload\":{\"orderId\":42}}"))
                .andExpect(status().isAccepted())
                .andExpect(jsonPath("$.eventId").isNotEmpty())
                .andExpect(jsonPath("$.type").value("order.created"))
                .andExpect(jsonPath("$.timestamp").isNotEmpty());

        awaitTotalProcessed(1);

        assertThat(processedEventService.totalProcessed()).isEqualTo(1);
        Map<String, Object> counts = processedEventService.counts();
        assertThat(counts.get("totalProcessed")).isEqualTo(1L);
        @SuppressWarnings("unchecked")
        Map<String, Long> byType = (Map<String, Long>) counts.get("byType");
        assertThat(byType).containsEntry("order.created", 1L);
        assertThat(processedEventService.last(10)).hasSize(1);
    }

    @Test
    void duplicateEventIdIsProcessedOnlyOnce() {
        EventEnvelope envelope = new EventEnvelope(
                UUID.randomUUID(), "duplicate.test", Map.of("n", 1), Instant.now());

        // Publish the exact same envelope twice (same eventId) over the real broker.
        eventPublisher.publish(envelope);
        eventPublisher.publish(envelope);

        // Allow both messages to be delivered, then assert only one was processed.
        try {
            Thread.sleep(1500);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }

        assertThat(processedEventService.totalProcessed()).isEqualTo(1);
        @SuppressWarnings("unchecked")
        Map<String, Long> byType = (Map<String, Long>) processedEventService.counts().get("byType");
        assertThat(byType).containsEntry("duplicate.test", 1L);
        assertThat(processedEventService.last(10)).hasSize(1);
    }

    private void awaitTotalProcessed(int expected) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 15_000;
        while (System.currentTimeMillis() < deadline) {
            if (processedEventService.totalProcessed() >= expected) {
                return;
            }
            Thread.sleep(100);
        }
        fail("Timed out waiting for %d processed events; counts=%s"
                .formatted(expected, processedEventService.counts()));
    }
}
