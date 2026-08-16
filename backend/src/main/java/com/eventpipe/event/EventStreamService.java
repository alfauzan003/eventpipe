package com.eventpipe.event;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

/**
 * Broadcasts every successfully processed event to all connected SSE clients
 * ({@code GET /api/events/stream}).
 *
 * <p>One {@link SseEmitter} is registered per client and removed when the
 * connection completes, times out or errors, so emitters never leak. Sending
 * happens on the RabbitMQ consumer thread; {@link SseEmitter} is thread-safe
 * and the subscriber list is a {@link CopyOnWriteArrayList}, so concurrent
 * consumers can broadcast concurrently.
 */
@Service
public class EventStreamService {

    private static final Logger log = LoggerFactory.getLogger(EventStreamService.class);

    /** No timeout: the stream stays open until the client disconnects. */
    private static final long SSE_TIMEOUT_MS = 0L;

    /** SSE event name used for processed-event frames. */
    public static final String PROCESSED_EVENT = "event.processed";

    private final List<SseEmitter> emitters = new CopyOnWriteArrayList<>();

    /**
     * Registers a new emitter for one client. The emitter is removed
     * automatically on completion, timeout or error.
     */
    public SseEmitter subscribe() {
        SseEmitter emitter = new SseEmitter(SSE_TIMEOUT_MS);
        emitters.add(emitter);
        emitter.onCompletion(() -> emitters.remove(emitter));
        emitter.onTimeout(() -> emitters.remove(emitter));
        emitter.onError(e -> emitters.remove(emitter));
        return emitter;
    }

    /**
     * Sends the envelope to every open emitter as an SSE frame named
     * {@value #PROCESSED_EVENT}. Emitters whose connection is gone are dropped.
     */
    public void broadcast(EventEnvelope envelope) {
        for (SseEmitter emitter : emitters) {
            try {
                emitter.send(SseEmitter.event().name(PROCESSED_EVENT).data(envelope));
            } catch (IOException | IllegalStateException e) {
                log.debug("Dropping closed SSE emitter: {}", e.getMessage());
                emitters.remove(emitter);
            }
        }
    }

    /** @return number of currently connected SSE clients (observability + tests) */
    public int subscriberCount() {
        return emitters.size();
    }
}
