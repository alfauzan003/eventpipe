package com.eventpipe.consumer;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.support.AmqpHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

import com.eventpipe.event.EventEnvelope;
import com.eventpipe.event.EventStreamService;
import com.eventpipe.event.ProcessedEventService;
import com.rabbitmq.client.Channel;

/**
 * Consumes events from the {@code eventpipe.events} queue with manual acks and
 * idempotent processing: a duplicate {@code eventId} is acked and skipped, never
 * processed twice.
 *
 * <p>Processing failures are deliberately <em>not</em> handled here: they
 * propagate to the retry advice configured on the listener container factory
 * (see {@code RabbitConfig#rabbitListenerContainerFactory()}), which retries
 * with exponential backoff and, after the attempts are exhausted, rejects the
 * message without requeue so it dead-letters to the DLQ instead of blocking
 * the queue.
 */
@Component
public class EventConsumer {

    private static final Logger log = LoggerFactory.getLogger(EventConsumer.class);

    private final ProcessedEventService processedEventService;
    private final EventStreamService eventStreamService;

    public EventConsumer(ProcessedEventService processedEventService,
                         EventStreamService eventStreamService) {
        this.processedEventService = processedEventService;
        this.eventStreamService = eventStreamService;
    }

    @RabbitListener(queues = "${eventpipe.queue}")
    public void onEvent(EventEnvelope envelope, Channel channel,
                        @Header(AmqpHeaders.DELIVERY_TAG) long deliveryTag) throws IOException {
        if (processedEventService.markProcessedIfAbsent(envelope.eventId())) {
            processedEventService.recordProcessed(envelope);
            eventStreamService.broadcast(envelope);
            log.info("Consumed event eventId={} type={} timestamp={}",
                    envelope.eventId(), envelope.type(), envelope.timestamp());
        } else {
            log.info("Skipped duplicate event eventId={} type={} (already processed)",
                    envelope.eventId(), envelope.type());
        }
        channel.basicAck(deliveryTag, false);
    }
}
