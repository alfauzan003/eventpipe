package com.eventpipe.event;

/**
 * Request body for {@code POST /api/events}.
 *
 * @param type    event type (required, non-blank); used as the routing key
 * @param payload arbitrary event payload (required, non-null)
 */
public record PublishEventRequest(String type, Object payload) {
}
