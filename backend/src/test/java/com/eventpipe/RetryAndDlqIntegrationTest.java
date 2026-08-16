package com.eventpipe;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.QueueInformation;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.support.AmqpHeaders;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.http.MediaType;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.servlet.MockMvc;
import org.testcontainers.containers.RabbitMQContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import com.eventpipe.event.EventEnvelope;
import com.eventpipe.event.ProcessedEventService;
import com.rabbitmq.client.Channel;

/**
 * Resilience tests over a real RabbitMQ broker (Testcontainers):
 * <ul>
 *   <li>retry with backoff: a consumer that fails the first attempts succeeds
 *       on a later attempt and the event is processed exactly once;</li>
 *   <li>dead-lettering: an event that always throws lands in the DLQ after the
 *       retries are exhausted, and the consumer keeps processing subsequent
 *       good events (never blocked).</li>
 * </ul>
 *
 * <p>A dedicated {@code eventpipe.test.retry} queue with a deliberately failing
 * listener drives the failures deterministically, so the main consumer's
 * behaviour is untouched. The test queue declares the same dead-letter args as
 * the main queue, so poison messages land in the shared DLQ
 * ({@code eventpipe.events.dlq}, declared by {@code RabbitConfig}).
 */
@SpringBootTest
@AutoConfigureMockMvc
@Testcontainers
class RetryAndDlqIntegrationTest {

    private static final String RABBITMQ_IMAGE = "rabbitmq:3.13-alpine";
    private static final String RETRY_TEST_QUEUE = "eventpipe.test.retry";
    private static final String DLQ = "eventpipe.events.dlq";

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
    private RabbitTemplate rabbitTemplate;

    @Autowired
    private RabbitAdmin rabbitAdmin;

    @Autowired
    private ProcessedEventService processedEventService;

    @Autowired
    private RetryTestListener retryTestListener;

    @TestConfiguration
    static class RetryTestConfig {

        @Bean
        Queue retryTestQueue() {
            return new Queue(RETRY_TEST_QUEUE, true, false, false, Map.of(
                    "x-dead-letter-exchange", "eventpipe.events.dlx",
                    "x-dead-letter-routing-key", "eventpipe.events.dlq"));
        }

        @Bean
        RetryTestListener retryTestListener() {
            return new RetryTestListener();
        }
    }

    @BeforeEach
    void resetStore() {
        processedEventService.reset();
    }

    @Test
    void retryThenSuccessIsProcessedExactlyOnce() throws Exception {
        UUID eventId = UUID.randomUUID();
        EventEnvelope envelope = new EventEnvelope(
                eventId, "retry.then.success", Map.of("n", 1), Instant.now());
        // Fail the first two deliveries; the third attempt must succeed.
        retryTestListener.failFirst(eventId, 2);

        rabbitTemplate.convertAndSend("", RETRY_TEST_QUEUE, envelope);

        await(() -> retryTestListener.succeeded(eventId),
                "retry-then-success event %s was never processed".formatted(eventId));

        // 1 initial delivery + 2 retries (max-attempts=3), no more.
        assertThat(retryTestListener.attempts(eventId)).isEqualTo(3);
        // The retried event must NOT have been dead-lettered.
        assertThat(dlqMessageCount()).isZero();
    }

    @Test
    void poisonMessageIsDeadLetteredAndConsumerStaysUnblocked() throws Exception {
        UUID poisonId = UUID.randomUUID();
        EventEnvelope poison = new EventEnvelope(
                poisonId, "poison.test", Map.of("poison", true), Instant.now());
        // Always throw: every attempt fails until retries are exhausted.
        retryTestListener.failFirst(poisonId, Integer.MAX_VALUE);

        rabbitTemplate.convertAndSend("", RETRY_TEST_QUEUE, poison);

        awaitDlqMessage(poisonId);

        // The consumer is NOT blocked: a good event published afterwards is
        // still consumed and processed by the main listener.
        mockMvc.perform(post("/api/events")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"type\":\"order.created\",\"payload\":{\"orderId\":42}}"))
                .andExpect(status().isAccepted());

        awaitTotalProcessed(1);
        assertThat(processedEventService.totalProcessed()).isEqualTo(1);
    }

    /** Test-only listener: throws for the configured eventIds up to a per-event attempt budget. */
    static class RetryTestListener {

        private final Map<UUID, Integer> attempts = new ConcurrentHashMap<>();
        private final Map<UUID, Integer> failFor = new ConcurrentHashMap<>();
        private final Map<UUID, Boolean> succeeded = new ConcurrentHashMap<>();

        @RabbitListener(queues = RETRY_TEST_QUEUE)
        public void onRetryTestEvent(EventEnvelope envelope, Channel channel,
                                     @Header(AmqpHeaders.DELIVERY_TAG) long deliveryTag) throws IOException {
            int attempt = attempts.merge(envelope.eventId(), 1, Integer::sum);
            int failBudget = failFor.getOrDefault(envelope.eventId(), 0);
            if (attempt <= failBudget) {
                throw new IllegalStateException(
                        "simulated failure for %s (attempt %d of %d)".formatted(
                                envelope.eventId(), attempt, failBudget));
            }
            succeeded.put(envelope.eventId(), Boolean.TRUE);
            channel.basicAck(deliveryTag, false);
        }

        void failFirst(UUID eventId, int attempts) {
            failFor.put(eventId, attempts);
        }

        boolean succeeded(UUID eventId) {
            return succeeded.containsKey(eventId);
        }

        int attempts(UUID eventId) {
            return attempts.getOrDefault(eventId, 0);
        }
    }

    private long dlqMessageCount() {
        QueueInformation queueInfo = rabbitAdmin.getQueueInfo(DLQ);
        return queueInfo == null ? 0 : queueInfo.getMessageCount();
    }

    private void awaitDlqMessage(UUID poisonId) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 20_000;
        while (System.currentTimeMillis() < deadline) {
            org.springframework.amqp.core.Message message = rabbitTemplate.receive(DLQ, 200);
            if (message != null) {
                String body = new String(message.getBody(), StandardCharsets.UTF_8);
                if (body.contains(poisonId.toString())) {
                    return;
                }
            }
            Thread.sleep(100);
        }
        fail("Poison event %s never reached the DLQ".formatted(poisonId));
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

    private void await(java.util.function.BooleanSupplier condition, String message)
            throws InterruptedException {
        long deadline = System.currentTimeMillis() + 20_000;
        while (System.currentTimeMillis() < deadline) {
            if (condition.getAsBoolean()) {
                return;
            }
            Thread.sleep(100);
        }
        fail(message);
    }
}
