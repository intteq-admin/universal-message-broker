package com.intteq.universal.message.broker.rabbitmq;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.MeterRegistry;
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
import org.springframework.lang.Nullable;

/**
 * RabbitMQ infrastructure configuration for the Universal Message Broker.
 *
 * <p>Activated only when:
 * <pre>
 * messaging.provider = rabbitmq
 * </pre>
 *
 * <p>Capabilities:
 * <ul>
 *     <li>Creates a resilient pooled connection factory</li>
 *     <li>Provides a JSON message converter using the application ObjectMapper</li>
 *     <li>Configures RabbitTemplate with publisher confirms support</li>
 *     <li>Optional Micrometer instrumentation</li>
 * </ul>
 *
 * <p>This configuration does NOT create queues or bindings.
 * That functionality belongs in topology builders or auto-infrastructure modules.
 */
@Configuration
@Slf4j
@ConditionalOnProperty(name = "messaging.provider", havingValue = "rabbitmq", matchIfMissing = true)
public class RabbitMQConfig {

    private final RabbitProperties rabbitProps;
    private final ObjectMapper objectMapper;
    @Nullable
    private final MeterRegistry meterRegistry;

    public RabbitMQConfig(RabbitProperties rabbitProps,
                          ObjectMapper objectMapper,
                          @Nullable MeterRegistry meterRegistry) {
        this.rabbitProps = rabbitProps;
        this.objectMapper = objectMapper;
        this.meterRegistry = meterRegistry;
    }

    // -------------------------------------------------------------------------
    // CONNECTION FACTORY
    // -------------------------------------------------------------------------

    /**
     * Creates a resilient and pooled RabbitMQ connection factory.
     */
    @Bean
    public ConnectionFactory connectionFactory() {

        CachingConnectionFactory factory = new CachingConnectionFactory();

        // Basic connection settings
        factory.setHost(rabbitProps.getHost());
        factory.setPort(rabbitProps.getPort());
        factory.setUsername(rabbitProps.getUsername());
        factory.setPassword(rabbitProps.getPassword());

        // Null-safe: Boot 3.4.x does NOT default these values
        var timeout = rabbitProps.getConnectionTimeout();
        factory.setConnectionTimeout(timeout != null ? (int) timeout.toMillis() : 10000);

        var heartbeat = rabbitProps.getRequestedHeartbeat();
        factory.setRequestedHeartBeat(heartbeat != null ? (int) heartbeat.getSeconds() : 60);

        // Channel caching defaults
        factory.setCacheMode(CacheMode.CHANNEL);
        factory.setChannelCacheSize(50);
        factory.setChannelCheckoutTimeout(10_000);

        // Publisher confirms
        factory.setPublisherConfirmType(CachingConnectionFactory.ConfirmType.CORRELATED);
        factory.setPublisherReturns(true);

        // SSL (Spring Boot 3.x → getEnabled(), not isEnabled())
        Boolean sslEnabled = rabbitProps.getSsl().getEnabled();
        if (Boolean.TRUE.equals(sslEnabled)) {
            try {
                factory.getRabbitConnectionFactory().useSslProtocol();
                log.info("RabbitMQ SSL enabled");
            } catch (Exception e) {
                throw new IllegalStateException("Failed to enable RabbitMQ SSL", e);
            }
        }

        log.info("RabbitMQ ConnectionFactory initialized: host={} port={}",
                rabbitProps.getHost(), rabbitProps.getPort());

        return factory;
    }

    // -------------------------------------------------------------------------
    // MESSAGE CONVERTER
    // -------------------------------------------------------------------------

    /**
     * JSON converter leveraging the application's configured ObjectMapper.
     */
    @Bean
    public MessageConverter messageConverter() {
        Jackson2JsonMessageConverter converter =
                new Jackson2JsonMessageConverter(objectMapper);

        converter.setCreateMessageIds(true);
        return converter;
    }

    // -------------------------------------------------------------------------
    // RABBIT TEMPLATE
    // -------------------------------------------------------------------------

    /**
     * RabbitTemplate with:
     * - JSON message conversion
     * - Publisher confirms
     * - Mandatory routing (so unroutable messages raise an error)
     */
    @Bean
    public RabbitTemplate rabbitTemplate(
            ConnectionFactory connectionFactory,
            MessageConverter messageConverter
    ) {
        RabbitTemplate template = new RabbitTemplate(connectionFactory);
        template.setMessageConverter(messageConverter);
        template.setMandatory(true); // necessary for publisher returns

        // Publisher confirms
        template.setConfirmCallback((CorrelationData cd, boolean ack, String cause) -> {
            if (ack) {
                log.debug("Publish confirmed: correlationId={}", cd != null ? cd.getId() : null);
            } else {
                log.error("Publish failed: correlationId={} cause={}",
                        cd != null ? cd.getId() : null, cause);
            }
        });

        // Returned messages (unroutable)
        template.setReturnsCallback(returned -> {
            log.error(
                    "Returned message: exchange={} routingKey={} replyCode={} replyText={}",
                    returned.getExchange(),
                    returned.getRoutingKey(),
                    returned.getReplyCode(),
                    returned.getReplyText()
            );
        });

        log.info("RabbitTemplate configured with JSON conversion + publisher confirms");

        return template;
    }
}
