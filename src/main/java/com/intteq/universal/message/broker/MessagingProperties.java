package com.intteq.universal.message.broker;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Pattern;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

import java.util.HashMap;
import java.util.Map;

/**
 * Configuration properties for the Universal Message Broker.
 *
 * <p>Prefix: {@code messaging.*}
 *
 * <p>Examples:
 * <pre>
 * messaging.provider=rabbitmq
 * messaging.topics.order-events=orders.exchange
 *
 * messaging.rabbitmq.queues.new-orders.name=new-orders-queue
 * messaging.rabbitmq.queues.new-orders.routing-key=order.created
 * messaging.rabbitmq.queues.new-orders.ttl=86400000
 *
 * messaging.azure.subscriptions.order-events.name=order-sub
 * messaging.azure.subscriptions.order-events.topic=order-events
 * </pre>
 *
 * <p>These properties are validated at startup. Invalid configurations will cause
 * the application to fail fast.
 */
@Getter
@Setter
@Validated
@ToString
@ConfigurationProperties(prefix = "messaging")
public class MessagingProperties {

    /**
     * Messaging provider to use.
     *
     * <p>Allowed values:
     * <ul>
     *     <li>{@code rabbitmq}</li>
     *     <li>{@code azure}</li>
     * </ul>
     */
    @NotBlank(message = "messaging.provider must not be blank")
    @Pattern(regexp = "rabbitmq|azure", flags = Pattern.Flag.CASE_INSENSITIVE,
            message = "messaging.provider must be one of: rabbitmq, azure")
    private String provider = "rabbitmq";

    /**
     * Logical topic → physical topic/exchange mapping.
     *
     * <p>Example:
     * messaging.topics.course-events=course-events-exchange
     */
    @Valid
    private Map<String, String> topics = new HashMap<>();

    /**
     * RabbitMQ-specific configuration.
     */
    @Valid
    private final RabbitMQProperties rabbitmq = new RabbitMQProperties();

    /**
     * Azure Service Bus-specific configuration.
     */
    @Valid
    private final AzureProperties azure = new AzureProperties();

    // ========================================================================
    // RabbitMQ Properties
    // ========================================================================

    @Getter
    @Setter
    @ToString
    public static class RabbitMQProperties {

        /**
         * Queue definitions by logical queue key.
         */
        @Valid
        private Map<String, QueueConfig> queues = new HashMap<>();
    }

    /**
     * RabbitMQ queue-level configuration.
     */
    @Getter
    @Setter
    @ToString
    public static class QueueConfig {

        /** Queue name. Required. */
        @NotBlank(message = "queue.name must not be blank")
        private String name;

        /** Routing key for binding. Required. */
        @NotBlank(message = "queue.routingKey must not be blank")
        private String routingKey;

        /** Whether to create a Dead Letter Queue (DLQ) and Dead Letter Exchange (DLX) for this queue. */
        private boolean dlq = false;

        /** TTL in milliseconds (default = 2 days). */
        private Long ttl = 172_800_000L; // 2 days default

        public long getTtlOrDefault() {
            return ttl != null ? ttl : 172_800_000L;
        }
    }

    // ========================================================================
    // Azure Properties
    // ========================================================================

    @Getter
    @Setter
    @ToString
    public static class AzureProperties {

        /**
         * Azure subscriptions grouped by logical name.
         */
        @Valid
        private Map<String, SubscriptionConfig> subscriptions = new HashMap<>();
    }

    /**
     * Azure Service Bus subscription config.
     */
    @Getter
    @Setter
    @ToString
    public static class SubscriptionConfig {

        /** Logical topic name (required). */
        @NotBlank(message = "subscription.topic must not be blank")
        private String topic;

        /** Name of the subscription (required). */
        @NotBlank(message = "subscription.name must not be blank")
        private String name;
    }
}
