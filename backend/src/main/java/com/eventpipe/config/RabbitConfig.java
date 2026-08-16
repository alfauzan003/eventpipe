package com.eventpipe.config;

import java.util.Map;

import org.springframework.amqp.AmqpRejectAndDontRequeueException;
import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.TopicExchange;
import org.springframework.amqp.rabbit.config.RetryInterceptorBuilder;
import org.springframework.amqp.rabbit.config.SimpleRabbitListenerContainerFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.retry.MessageRecoverer;
import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Declares the durable RabbitMQ topology. Spring Boot's {@code RabbitAdmin}
 * auto-declares these beans on startup (exchanges, queues and bindings).
 *
 * <p>Resilience wiring (ticket 17):
 * <ul>
 *   <li>The main queue declares {@code x-dead-letter-exchange} /
 *       {@code x-dead-letter-routing-key}, so a rejected or expired message is
 *       routed to the dead-letter exchange and then the DLQ.</li>
 *   <li>A custom {@link SimpleRabbitListenerContainerFactory} keeps manual acks
 *       and wraps every listener in a stateless retry interceptor
 *       ({@code max-attempts=3}, exponential backoff 1s &rarr; 2s, capped at
 *       10s). A consumer that throws is retried with backoff; once attempts are
 *       exhausted the {@link ManualAckRejectRecoverer} throws a top-level
 *       {@link AmqpRejectAndDontRequeueException} with {@code rejectManual},
 *       which makes the manual-ack container nack the message without requeue
 *       so it dead-letters to the DLQ instead of blocking the queue forever
 *       (with the stock recoverer the exception is wrapped in a
 *       {@code ListenerExecutionFailedException}, which the manual-ack path
 *       does not nack).</li>
 * </ul>
 */
@Configuration
public class RabbitConfig {

    @Bean
    public TopicExchange eventExchange(@Value("${eventpipe.exchange}") String exchangeName) {
        return new TopicExchange(exchangeName, true, false);
    }

    @Bean
    public Queue eventQueue(@Value("${eventpipe.queue}") String queueName,
                            @Value("${eventpipe.dlx-exchange}") String dlxExchange,
                            @Value("${eventpipe.dlq-routing-key}") String dlqRoutingKey) {
        return new Queue(queueName, true, false, false, Map.of(
                "x-dead-letter-exchange", dlxExchange,
                "x-dead-letter-routing-key", dlqRoutingKey));
    }

    @Bean
    public TopicExchange deadLetterExchange(@Value("${eventpipe.dlx-exchange}") String dlxExchangeName) {
        return new TopicExchange(dlxExchangeName, true, false);
    }

    @Bean
    public Queue deadLetterQueue(@Value("${eventpipe.dlq}") String dlqName) {
        return new Queue(dlqName, true);
    }

    @Bean
    public Binding deadLetterBinding(Queue deadLetterQueue, TopicExchange deadLetterExchange,
                                     @Value("${eventpipe.dlq-routing-key}") String dlqRoutingKey) {
        return BindingBuilder.bind(deadLetterQueue).to(deadLetterExchange).with(dlqRoutingKey);
    }

    @Bean
    public Binding eventBinding(Queue eventQueue, TopicExchange eventExchange) {
        // Publishing uses the event type as routing key, so bind with "#" to
        // receive events of every type.
        return BindingBuilder.bind(eventQueue).to(eventExchange).with("#");
    }

    @Bean
    public MessageConverter messageConverter(ObjectMapper objectMapper) {
        return new Jackson2JsonMessageConverter(objectMapper);
    }

    /**
     * Listener container factory with manual acks + retry with exponential
     * backoff. A consumer that throws is retried up to {@code maxAttempts}
     * times; after exhaustion the {@link ManualAckRejectRecoverer} rejects the
     * message without requeue, so the dead-letter exchange routes it to the
     * DLQ and the queue keeps flowing.
     *
     * <p>Note: retry is deliberately wired here (not via
     * {@code spring.rabbitmq.listener.simple.retry.*} properties) so that the
     * recoverer and the manual-ack container are explicit and testable; the
     * properties route would apply to the auto-configured factory this bean
     * replaces.
     */
    @Bean
    public SimpleRabbitListenerContainerFactory rabbitListenerContainerFactory(
            ConnectionFactory connectionFactory, MessageConverter messageConverter) {
        SimpleRabbitListenerContainerFactory factory = new SimpleRabbitListenerContainerFactory();
        factory.setConnectionFactory(connectionFactory);
        factory.setMessageConverter(messageConverter);
        factory.setAcknowledgeMode(org.springframework.amqp.core.AcknowledgeMode.MANUAL);
        // Anything that still escapes after recovery must never be requeued
        // forever: reject it so the DLX routes it to the DLQ.
        factory.setDefaultRequeueRejected(false);
        factory.setAdviceChain(RetryInterceptorBuilder.stateless()
                .maxAttempts(3)
                .backOffOptions(1000, 2.0, 10000)
                .recoverer(new ManualAckRejectRecoverer())
                .build());
        return factory;
    }

    /**
     * Recoverer for the manual-ack listener containers: after the retries are
     * exhausted it throws a <em>top-level</em>
     * {@link AmqpRejectAndDontRequeueException} with {@code rejectManual=true}.
     * The manual-ack container only nacks without requeue when it sees exactly
     * that shape (the stock {@code RejectAndDontRequeueRecoverer} wraps it in a
     * {@code ListenerExecutionFailedException}, which is not nacked in manual
     * mode), so this recoverer is what makes poison messages land in the DLQ.
     */
    static final class ManualAckRejectRecoverer implements MessageRecoverer {

        @Override
        public void recover(org.springframework.amqp.core.Message message, Throwable cause) {
            throw new AmqpRejectAndDontRequeueException(
                    "Retries exhausted; rejecting message without requeue", true, cause);
        }
    }
}
