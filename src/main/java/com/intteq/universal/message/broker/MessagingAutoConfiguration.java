package com.intteq.universal.message.broker;

import com.intteq.universal.message.broker.azure.AzureProperties;
import com.intteq.universal.message.broker.internal.MessagingProxyFactory;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.intteq.universal.message.broker.rabbitmq.RabbitMQProperties;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.lang.Nullable;

/**
 * Auto-configuration for the Universal Message Broker library.
 *
 * <p>This configuration exposes infrastructure beans required by the library. It is
 * enabled by default and can be disabled by setting:
 *
 * <pre>
 *   universal.message-broker.enabled = false
 * </pre>
 *
 * <p>The {@link MessagingProxyFactory} bean will be created and will be supplied with
 * the application's {@link ObjectMapper} when available. The {@link MeterRegistry}
 * is optional and will be injected only if present on the classpath / application.
 */
@Configuration(proxyBeanMethods = false)
@EnableConfigurationProperties({MessagingProperties.class, RabbitMQProperties.class, AzureProperties.class})
@ConditionalOnProperty(prefix = "universal.message-broker", name = "enabled", havingValue = "true", matchIfMissing = true)
public class MessagingAutoConfiguration {

    /**
     * Creates the {@link MessagingProxyFactory} used to produce interface-based publishers.
     * The meterRegistry parameter is optional — metrics will be a no-op if Micrometer is not present.
     *
     * @param props         validated messaging properties
     * @param ctx           application context
     * @param objectMapper  application ObjectMapper (optional but usually present in Spring Boot apps)
     * @param meterRegistry optional MeterRegistry; pass null if Micrometer is not available
     * @return configured MessagingProxyFactory
     */
    @Bean
    public MessagingProxyFactory messagingProxyFactory(
            MessagingProperties props,
            ApplicationContext ctx,
            @Nullable ObjectMapper objectMapper,
            @Nullable MeterRegistry meterRegistry) {

        // If the application does not provide an ObjectMapper, create a default one internally.
        ObjectMapper mapper = objectMapper != null ? objectMapper : new ObjectMapper();
        // If the application does not provide a MeterRegistry, use a no-op implementation.
        MeterRegistry registry = meterRegistry != null ? meterRegistry : new io.micrometer.core.instrument.simple.SimpleMeterRegistry();

        // MessagingProxyFactory constructor accepts (MessagingProperties, ApplicationContext, MeterRegistry, ObjectMapper)
        return new MessagingProxyFactory(props, ctx, registry, mapper);
    }
}