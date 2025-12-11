package com.intteq.universal.message.broker.rabbitmq;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory.CacheMode;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.connection.CorrelationData;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.boot.autoconfigure.amqp.RabbitProperties;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * RabbitMQ infrastructure configuration for the Universal Message Broker.
 *
 * SSL is supported automatically if spring.rabbitmq.ssl.enabled=true.
 * No custom SSLContext building is done here — Spring Boot handles that.
 */
@Configuration
@Slf4j
@ConditionalOnProperty(name = "messaging.provider", havingValue = "rabbitmq", matchIfMissing = true)
public class RabbitMQConfig {

    private final RabbitProperties rabbitProps;
    private final ObjectMapper objectMapper;

    public RabbitMQConfig(RabbitProperties rabbitProps, ObjectMapper objectMapper) {
        this.rabbitProps = rabbitProps;
        this.objectMapper = objectMapper;
    }

    @Bean
    public ConnectionFactory connectionFactory() {
        CachingConnectionFactory factory = new CachingConnectionFactory();

        factory.setHost(rabbitProps.getHost());
        factory.setPort(rabbitProps.getPort());
        factory.setUsername(rabbitProps.getUsername());
        factory.setPassword(rabbitProps.getPassword());

        var timeout = rabbitProps.getConnectionTimeout();
        factory.setConnectionTimeout(timeout != null ? (int) timeout.toMillis() : 10000);

        var heartbeat = rabbitProps.getRequestedHeartbeat();
        factory.setRequestedHeartBeat(heartbeat != null ? (int) heartbeat.getSeconds() : 60);

        factory.setCacheMode(CacheMode.CHANNEL);
        factory.setChannelCacheSize(50);
        factory.setChannelCheckoutTimeout(10_000);

        // Publisher confirms
        factory.setPublisherConfirmType(CachingConnectionFactory.ConfirmType.CORRELATED);
        factory.setPublisherReturns(true);

        // SSL handled automatically by Spring Boot when enabled
        if (Boolean.TRUE.equals(rabbitProps.getSsl().getEnabled())) {
            log.info("RabbitMQ SSL enabled by application properties");
        }

        log.info("RabbitMQ ConnectionFactory initialized: host={} port={}",
                rabbitProps.getHost(), rabbitProps.getPort());

        return factory;
    }

    @Bean
    public MessageConverter messageConverter() {
        Jackson2JsonMessageConverter converter = new Jackson2JsonMessageConverter(objectMapper);
        converter.setCreateMessageIds(true);
        return converter;
    }

    @Bean
    public RabbitTemplate rabbitTemplate(
            ConnectionFactory connectionFactory,
            MessageConverter messageConverter
    ) {
        RabbitTemplate template = new RabbitTemplate(connectionFactory);
        template.setMessageConverter(messageConverter);
        template.setMandatory(true);

        template.setConfirmCallback((CorrelationData cd, boolean ack, String cause) -> {
            if (ack) {
                log.debug("Publish confirmed: correlationId={}", cd != null ? cd.getId() : null);
            } else {
                log.error("Publish failed: correlationId={} cause={}",
                        cd != null ? cd.getId() : null, cause);
            }
        });

        template.setReturnsCallback(returned ->
                log.error("Returned message: exchange={} routingKey={} replyCode={} replyText={}",
                        returned.getExchange(),
                        returned.getRoutingKey(),
                        returned.getReplyCode(),
                        returned.getReplyText())
        );

        return template;
    }
}
