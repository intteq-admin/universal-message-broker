package com.intteq.universal.message.broker.azure;

import com.azure.core.credential.TokenCredential;
import com.azure.core.exception.ResourceNotFoundException;
import com.azure.identity.DefaultAzureCredentialBuilder;
import com.azure.messaging.servicebus.ServiceBusClientBuilder;
import com.azure.messaging.servicebus.administration.ServiceBusAdministrationClient;
import com.azure.messaging.servicebus.administration.ServiceBusAdministrationClientBuilder;
import com.intteq.universal.message.broker.MessagingProperties;
import io.micrometer.core.instrument.MeterRegistry;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.annotation.Order;
import org.springframework.lang.Nullable;

import java.security.SecureRandom;
import java.time.Duration;

/**
 * Azure Service Bus infrastructure auto-configuration.
 *
 * <p><b>Purpose</b></p>
 * <ul>
 *   <li>Provision Azure Service Bus topics</li>
 *   <li>Provision Azure Service Bus subscriptions</li>
 *   <li>Expose shared Azure Service Bus clients</li>
 * </ul>
 *
 * <p><b>Critical lifecycle guarantee</b></p>
 * <pre>
 * This class runs BEFORE any message listeners are started.
 * Topics and subscriptions are guaranteed to exist
 * before processors attach to them.
 * </pre>
 *
 * <p><b>Activation conditions</b></p>
 * <ul>
 *   <li>{@code messaging.provider=azure}</li>
 * </ul>
 *
 * <p><b>Why SmartInitializingSingleton?</b></p>
 * <pre>
 * ApplicationReadyEvent runs too late.
 * SmartInitializingSingleton runs immediately
 * after all singleton beans are created but
 * BEFORE message consumers start.
 * </pre>
 */
@Configuration
@Slf4j
@RequiredArgsConstructor
@Order(0) // MUST execute before Azure listeners
@ConditionalOnProperty(name = "messaging.provider", havingValue = "azure")
public class AzureInfrastructureAutoConfig implements SmartInitializingSingleton {

    /** Core messaging configuration (logical → physical topic mapping). */
    private final MessagingProperties core;

    /** Azure-specific subscription configuration. */
    private final AzureProperties azureProperties;
    private final ApplicationContext applicationContext;

    /** Optional Micrometer registry for infrastructure metrics. */
    @Nullable
    private final MeterRegistry meterRegistry;

    /** Azure Service Bus connection string (optional for Managed Identity). */
    @Value("${azure.servicebus.connection-string:}")
    private String connectionString;

    /** Fully qualified namespace (required for Managed Identity). */
    @Value("${azure.servicebus.namespace:}")
    private String namespace;

    private static final int MAX_RETRIES = 6;
    private static final Duration BASE_DELAY = Duration.ofSeconds(1);
    private static final SecureRandom RNG = new SecureRandom();

    // =====================================================
    // VALIDATION
    // =====================================================

    /**
     * Logs Azure Service Bus startup mode.
     * Does not perform provisioning.
     */
    @PostConstruct
    void validate() {
        log.info("Azure Service Bus infrastructure auto-configuration enabled.");
    }

    // =====================================================
    // BEANS
    // =====================================================

    /**
     * Provides Azure credentials.
     *
     * <p>Uses DefaultAzureCredential, supporting:</p>
     * <ul>
     *   <li>Managed Identity</li>
     *   <li>Environment variables</li>
     *   <li>Azure CLI login</li>
     * </ul>
     */
    @Bean
    public TokenCredential azureCredential() {
        return new DefaultAzureCredentialBuilder().build();
    }

    /**
     * Shared ServiceBusClientBuilder for producers and consumers.
     */
    @Bean
    public ServiceBusClientBuilder serviceBusClientBuilder(
            @Nullable TokenCredential credential
    ) {
        if (useManagedIdentity()) {

            if (namespace == null || namespace.isBlank()) {
                throw new IllegalStateException(
                        "azure.servicebus.namespace is required when using Managed Identity."
                );
            }

            log.info("Creating ServiceBusClientBuilder using Managed Identity.");

            return new ServiceBusClientBuilder()
                    .fullyQualifiedNamespace(namespace)
                    .credential(credential);
        }

        log.info("Creating ServiceBusClientBuilder using connection string.");

        return new ServiceBusClientBuilder()
                .connectionString(resolveConnectionString());
    }

    /**
     * Azure Service Bus administration client used for provisioning.
     */
    @Bean
    public ServiceBusAdministrationClient adminClient(
            @Nullable TokenCredential credential
    ) {
        if (useManagedIdentity()) {

            if (namespace == null || namespace.isBlank()) {
                throw new IllegalStateException(
                        "azure.servicebus.namespace is required for Managed Identity."
                );
            }

            String endpoint = "https://" + namespace;

            log.info("Creating ServiceBusAdministrationClient using Managed Identity.");

            return new ServiceBusAdministrationClientBuilder()
                    .endpoint(endpoint)
                    .credential(credential)
                    .buildClient();
        }

        log.info("Creating ServiceBusAdministrationClient using connection string.");

        return new ServiceBusAdministrationClientBuilder()
                .connectionString(resolveConnectionString())
                .buildClient();
    }

    // =====================================================
    // INFRASTRUCTURE INITIALIZATION (CORE FIX)
    // =====================================================

    /**
     * Executes after all singleton beans are instantiated but
     * before any messaging listeners start.
     *
     * <p>This guarantees that all required topics and subscriptions
     * exist before consumers attach.</p>
     */
    @Override
    public void afterSingletonsInstantiated() {
        ServiceBusAdministrationClient admin =
                applicationContext.getBean(ServiceBusAdministrationClient.class);

        log.info("Initializing Azure Service Bus infrastructure...");

        createTopics(admin);
        createSubscriptions(admin);

        log.info("Azure Service Bus infrastructure is ready.");
    }

    // =====================================================
    // TOPICS & SUBSCRIPTIONS
    // =====================================================

    /** Creates topics if they do not already exist. */
    private void createTopics(ServiceBusAdministrationClient admin) {
        core.getTopics().forEach((logical, physical) ->
                retry("CreateTopic:" + physical, () -> {
                    if (!topicExists(admin, physical)) {
                        log.info("Creating topic '{}'", physical);
                        admin.createTopic(physical);
                    }
                })
        );
    }

    /** Creates subscriptions if they do not already exist. */
    private void createSubscriptions(ServiceBusAdministrationClient admin) {
        azureProperties.getSubscriptions().values().forEach(sub -> {

            String physicalTopic = core.getTopics().get(sub.getTopic());
            if (physicalTopic == null) {
                log.warn(
                        "Skipping subscription '{}' — logical topic '{}' not mapped",
                        sub.getName(), sub.getTopic()
                );
                return;
            }

            retry("CreateSubscription:" + sub.getName(), () -> {
                if (!subscriptionExists(admin, physicalTopic, sub.getName())) {
                    log.info(
                            "Creating subscription '{}' on topic '{}'",
                            sub.getName(), physicalTopic
                    );
                    admin.createSubscription(physicalTopic, sub.getName());
                }
            });
        });
    }

    // =====================================================
    // HELPERS
    // =====================================================

    private boolean topicExists(ServiceBusAdministrationClient admin, String topic) {
        try {
            admin.getTopic(topic);
            return true;
        } catch (ResourceNotFoundException e) {
            return false;
        }
    }

    private boolean subscriptionExists(ServiceBusAdministrationClient admin, String topic, String subscription) {
        try {
            admin.getSubscription(topic, subscription);
            return true;
        } catch (ResourceNotFoundException e) {
            return false;
        }
    }

    private void retry(String label, Runnable action) {
        Duration delay = BASE_DELAY;

        for (int attempt = 1; attempt <= MAX_RETRIES; attempt++) {
            try {
                action.run();
                return;
            } catch (Exception ex) {
                if (attempt == MAX_RETRIES) {
                    throw ex;
                }
                sleep(delay.toMillis() + RNG.nextInt(300));
                delay = delay.multipliedBy(2);
            }
        }
    }

    private void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Infrastructure retry interrupted", e);
        }
    }

    private boolean useManagedIdentity() {
        return resolveConnectionString() == null;
    }

    private String resolveConnectionString() {
        if (connectionString != null && !connectionString.isBlank()) {
            return connectionString;
        }
        return System.getenv("AZURE_SERVICEBUS_CONNECTION_STRING");
    }

}
