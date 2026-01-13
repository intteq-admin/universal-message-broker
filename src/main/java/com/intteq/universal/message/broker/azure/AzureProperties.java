package com.intteq.universal.message.broker.azure;

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
 * Azure Service Bus-specific properties.
 *
 * Prefix: messaging.azure.*
 */
@Getter
@Setter
@Validated
@ToString
@ConfigurationProperties(prefix = "messaging.azure")
@ConditionalOnProperty(name = "messaging.provider", havingValue = "azure")
public class AzureProperties {

    /**
     * Azure subscriptions grouped by logical name.
     */
    @Valid
    private Map<String, SubscriptionConfig> subscriptions = new HashMap<>();

    // ------------------------------------------------------

    @Getter
    @Setter
    @ToString
    public static class SubscriptionConfig {

        /** Logical topic key (required). */
        @NotBlank(message = "azure.subscription.topic must not be blank")
        private String topic;

        /** Subscription name (required). */
        @NotBlank(message = "azure.subscription.name must not be blank")
        private String name;
    }
}
