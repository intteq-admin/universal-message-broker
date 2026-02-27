package com.intteq.universal.message.broker.rabbitmq;

import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;

/**
 * Overrides RabbitMQ health indicator when messaging.provider=azure
 * to prevent Rabbit connection attempts.
 */
@AutoConfiguration
@ConditionalOnProperty(prefix = "messaging", name = "provider", havingValue = "azure")
public class OverrideRabbitHealthAutoConfiguration {

    /**
     * Overrides the default RabbitHealthIndicator provided by Spring Boot.
     */
    @Bean(name = "rabbitHealthIndicator")
    @Primary
    public HealthIndicator rabbitHealthIndicator() {
        return () -> Health.up()
                .withDetail("rabbit", "disabled by universal-message-broker (provider=azure)")
                .build();
    }
}
