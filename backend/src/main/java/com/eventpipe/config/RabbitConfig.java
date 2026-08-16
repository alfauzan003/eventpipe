package com.eventpipe.config;

import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.TopicExchange;
import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Declares the durable RabbitMQ topology. Spring Boot's {@code RabbitAdmin}
 * auto-declares these beans on startup (exchange, queue and binding).
 */
@Configuration
public class RabbitConfig {

    @Bean
    public TopicExchange eventExchange(@Value("${eventpipe.exchange}") String exchangeName) {
        return new TopicExchange(exchangeName, true, false);
    }

    @Bean
    public Queue eventQueue(@Value("${eventpipe.queue}") String queueName) {
        return new Queue(queueName, true);
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
}
