package com.eventpipe.event;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicLong;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

/**
 * Thread-safe in-memory store backing idempotent processing and observability:
 * tracks seen {@code eventId}s, per-type and total processed counters, and the
 * most recently processed envelopes.
 */
@Service
public class ProcessedEventService {

    private final Set<UUID> processedEventIds = ConcurrentHashMap.newKeySet();
    private final Map<String, AtomicLong> processedByType = new ConcurrentHashMap<>();
    private final AtomicLong totalProcessed = new AtomicLong();
    private final ConcurrentLinkedDeque<EventEnvelope> recent = new ConcurrentLinkedDeque<>();
    private final int recentLimit;

    public ProcessedEventService(@Value("${eventpipe.last-limit:20}") int recentLimit) {
        this.recentLimit = recentLimit;
    }

    /**
     * Atomically marks an eventId as processed.
     *
     * @return {@code true} if this eventId was not seen before (first processing),
     *         {@code false} if it was already processed (duplicate)
     */
    public boolean markProcessedIfAbsent(UUID eventId) {
        return processedEventIds.add(eventId);
    }

    /**
     * Records a processed event: increments the total and per-type counters and
     * keeps it in the "last processed" window. Only called on first processing.
     */
    public void recordProcessed(EventEnvelope envelope) {
        totalProcessed.incrementAndGet();
        processedByType.computeIfAbsent(envelope.type(), key -> new AtomicLong()).incrementAndGet();
        recent.addFirst(envelope);
        while (recent.size() > recentLimit) {
            recent.pollLast();
        }
    }

    public long totalProcessed() {
        return totalProcessed.get();
    }

    /**
     * @return {@code { totalProcessed: n, byType: { "<type>": n, ... } }}
     */
    public Map<String, Object> counts() {
        Map<String, Long> byType = new LinkedHashMap<>();
        processedByType.forEach((type, count) -> byType.put(type, count.get()));
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("totalProcessed", totalProcessed.get());
        result.put("byType", byType);
        return result;
    }

    /**
     * @return the most recently processed envelopes, newest first (capped at the
     *         configured {@code eventpipe.last-limit})
     */
    public List<EventEnvelope> last(int limit) {
        int n = Math.max(1, Math.min(limit, recentLimit));
        List<EventEnvelope> result = new ArrayList<>(n);
        int i = 0;
        for (EventEnvelope envelope : recent) {
            if (i++ >= n) {
                break;
            }
            result.add(envelope);
        }
        return result;
    }

    /** Clears all state (used by tests). */
    public void reset() {
        processedEventIds.clear();
        processedByType.clear();
        totalProcessed.set(0);
        recent.clear();
    }
}
