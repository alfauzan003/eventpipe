package com.eventpipe.event;

import java.time.Instant;
import java.util.UUID;

/**
 * The canonical event envelope exchanged over RabbitMQ.
 *
 * @param eventId   server-assigned unique identifier (used for idempotent processing)
 * @param type      event type, also used as the publish routing key
 * @param payload   arbitrary event payload
 * @param timestamp instant at which the event was published (UTC)
 */
public record EventEnvelope(UUID eventId, String type, Object payload, Instant timestamp) {
}
