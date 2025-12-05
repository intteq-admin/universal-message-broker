package com.intteq.universal_message_broker.rabbitmq;

import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConditionalOnProperty(name = "messaging.provider", havingValue = "rabbitmq", matchIfMissing = true)
public class RabbitMQConfig {

    @Bean
    public ConnectionFactory connectionFactory() {
        CachingConnectionFactory factory = new CachingConnectionFactory();
        factory.setHost(System.getProperty("spring.rabbitmq.host", "localhost"));
        factory.setPort(Integer.parseInt(System.getProperty("spring.rabbitmq.port", "5672")));
        factory.setUsername(System.getProperty("spring.rabbitmq.username", "guest"));
        factory.setPassword(System.getProperty("spring.rabbitmq.password", "guest"));
        factory.setConnectionTimeout(10000);
        factory.setRequestedHeartBeat(60);
        return factory;
    }

    @Bean
    public MessageConverter producerMessageConverter() {
        return new Jackson2JsonMessageConverter();
    }

    @Bean
    public RabbitTemplate rabbitTemplate(ConnectionFactory connectionFactory,
                                         MessageConverter messageConverter) {

        RabbitTemplate template = new RabbitTemplate(connectionFactory);
        template.setMessageConverter(messageConverter);
        return template;
    }
}