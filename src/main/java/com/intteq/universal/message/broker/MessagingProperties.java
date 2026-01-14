package com.intteq.universal.message.broker;

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
 * Core messaging properties (provider-agnostic).
 *
 * Prefix: messaging.*
 */
@Getter
@Setter
@Validated
@ToString
@ConfigurationProperties(prefix = "messaging")
public class MessagingProperties {

    /**
     * Messaging provider.
     *
     * Allowed values:
     *  - rabbitmq
     *  - azure
     */
    @NotBlank(message = "messaging.provider must not be blank")
    @Pattern(
            regexp = "rabbitmq|azure",
            flags = Pattern.Flag.CASE_INSENSITIVE,
            message = "messaging.provider must be one of: rabbitmq, azure"
    )
    private String provider = "azure";

    /**
     * Logical topic → physical topic/exchange mapping.
     *
     * Example:
     * messaging.topics.course-events=course-events-exchange
     */
    private Map<String, String> topics = new HashMap<>();
}
