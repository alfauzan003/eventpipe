package com.eventpipe.event;

import java.time.Instant;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

/**
 * Publishes {@link EventEnvelope}s to the {@code eventpipe.events} topic exchange,
 * using the event {@code type} as the routing key.
 */
@Service
public class EventPublisher {

    private static final Logger log = LoggerFactory.getLogger(EventPublisher.class);

    private final RabbitTemplate rabbitTemplate;
    private final String exchange;

    public EventPublisher(RabbitTemplate rabbitTemplate,
                          @Value("${eventpipe.exchange}") String exchange) {
        this.rabbitTemplate = rabbitTemplate;
        this.exchange = exchange;
    }

    /**
     * Builds a new envelope (server-assigned eventId and UTC timestamp) and publishes it.
     *
     * @return the published envelope, including the assigned eventId
     */
    public EventEnvelope publish(String type, Object payload) {
        EventEnvelope envelope = new EventEnvelope(UUID.randomUUID(), type, payload, Instant.now());
        return publish(envelope);
    }

    /**
     * Publishes a pre-built envelope (used by tests to replay the same eventId).
     */
    public EventEnvelope publish(EventEnvelope envelope) {
        rabbitTemplate.convertAndSend(exchange, envelope.type(), envelope);
        log.info("Published event eventId={} type={} to exchange={} routingKey={}",
                envelope.eventId(), envelope.type(), exchange, envelope.type());
        return envelope;
    }
}
