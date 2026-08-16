package com.eventpipe.api;

import java.util.List;
import java.util.Map;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.eventpipe.event.EventEnvelope;
import com.eventpipe.event.EventPublisher;
import com.eventpipe.event.ProcessedEventService;
import com.eventpipe.event.PublishEventRequest;

/**
 * Publisher API: accepts events, validates them, publishes the envelope to
 * RabbitMQ, and exposes consumer observability endpoints.
 */
@RestController
@RequestMapping("/api/events")
public class EventController {

    private final EventPublisher eventPublisher;
    private final ProcessedEventService processedEventService;

    public EventController(EventPublisher eventPublisher, ProcessedEventService processedEventService) {
        this.eventPublisher = eventPublisher;
        this.processedEventService = processedEventService;
    }

    /**
     * Publishes an event. The server assigns {@code eventId} and {@code timestamp},
     * and responds {@code 202 Accepted} with the full envelope so the caller sees
     * the assigned eventId. {@code 400 Bad Request} on validation failure.
     */
    @PostMapping
    public ResponseEntity<?> publish(@RequestBody PublishEventRequest request) {
        if (request.type() == null || request.type().isBlank()) {
            return ResponseEntity.badRequest().body(Map.of("error", "type must not be blank"));
        }
        if (request.payload() == null) {
            return ResponseEntity.badRequest().body(Map.of("error", "payload must be present"));
        }
        EventEnvelope envelope = eventPublisher.publish(request.type().trim(), request.payload());
        return ResponseEntity.accepted().body(envelope);
    }

    /**
     * @return {@code { totalProcessed: n, byType: { "<type>": n, ... } }}
     */
    @GetMapping("/counts")
    public Map<String, Object> counts() {
        return processedEventService.counts();
    }

    /**
     * @return the most recently processed envelopes, newest first
     */
    @GetMapping("/last")
    public List<EventEnvelope> last(@RequestParam(name = "limit", defaultValue = "10") int limit) {
        return processedEventService.last(limit);
    }
}
