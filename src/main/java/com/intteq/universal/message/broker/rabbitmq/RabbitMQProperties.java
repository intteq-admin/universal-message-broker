package com.intteq.universal.message.broker.rabbitmq;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

import java.util.HashMap;
import java.util.Map;

/**
 * RabbitMQ-specific properties.
 *
 * Prefix: messaging.rabbitmq.*
 */
@Getter
@Setter
@Validated
@ToString
@ConfigurationProperties(prefix = "messaging.rabbitmq")
@ConditionalOnProperty(name = "messaging.provider", havingValue = "rabbitmq")
public class RabbitMQProperties {

    /**
     * Queue definitions by logical name.
     */
    @Valid
    private Map<String, QueueConfig> queues = new HashMap<>();

    // ------------------------------------------------------

    @Getter
    @Setter
    @ToString
    public static class QueueConfig {

        /** Queue name (required). */
        @NotBlank(message = "rabbitmq.queue.name must not be blank")
        private String name;

        /** Routing key (required). */
        @NotBlank(message = "rabbitmq.queue.routingKey must not be blank")
        private String routingKey;

        /** Enable DLQ/DLX. */
        private boolean dlq = false;

        /** TTL in milliseconds (default = 2 days). */
        private Long ttl = 172_800_000L;

        public long getTtlOrDefault() {
            return ttl != null ? ttl : 172_800_000L;
        }
    }
}
