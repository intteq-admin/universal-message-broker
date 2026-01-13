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
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.lang.Nullable;

import java.security.SecureRandom;
import java.time.Duration;

/**
 * Azure Service Bus provisioning and client configuration.
 *
 * <p>This module:
 * <ul>
 *     <li>Automatically creates topics and subscriptions</li>
 *     <li>Handles Azure authentication (Connection String or Managed Identity)</li>
 *     <li>Provides ServiceBusClientBuilder and ServiceBusAdministrationClient beans</li>
 * </ul>
 *
 * Activated only when:
 * <pre>
 * messaging.provider = azure
 * </pre>
 *
 * Supports Azure Service Bus SDK version: 7.17.x
 */
@Configuration
@Slf4j
@RequiredArgsConstructor
@ConditionalOnProperty(name = "messaging.provider", havingValue = "azure")
public class AzureInfrastructureAutoConfig {

    /** Core messaging properties (topics, provider) */
    private final MessagingProperties core;

    /** Azure-specific properties (subscriptions) */
    private final AzureProperties azure;

    /** Micrometer metrics – optional */
    @Nullable
    private final MeterRegistry meterRegistry;

    /** SAS connection string (optional when using Managed Identity) */
    @Value("${azure.servicebus.connection-string:}")
    private String connectionString;

    /**
     * Fully-qualified namespace for Managed Identity mode.
     * Example: my-namespace.servicebus.windows.net
     */
    @Value("${azure.servicebus.namespace:}")
    private String namespace;

    private volatile boolean initialized = false;

    private static final int MAX_RETRIES = 6;
    private static final SecureRandom RNG = new SecureRandom();
    private static final Duration BASE_DELAY = Duration.ofSeconds(1);

    // =====================================================
    // 1. CONFIG VALIDATION
    // =====================================================

    @PostConstruct
    void validate() {
        if (initialized) {
            return;
        }
        initialized = true;

        log.info("Azure Service Bus mode enabled. Validating configuration...");

        if (resolveConnectionString() == null) {
            log.warn("Using Managed Identity: no Azure Service Bus connection string found.");
        } else {
            log.info("Using SAS authentication for Azure Service Bus.");
        }
    }

    private String resolveConnectionString() {
        String env = System.getenv("AZURE_SERVICEBUS_CONNECTION_STRING");
        if (connectionString != null && !connectionString.isBlank()) {
            return connectionString;
        }
        if (env != null && !env.isBlank()) {
            return env;
        }
        return null;
    }

    private boolean useManagedIdentity() {
        return resolveConnectionString() == null;
    }

    // =====================================================
    // 2. BEANS
    // =====================================================

    /**
     * Shared token credential for Managed Identity authentication.
     */
    @Bean
    public TokenCredential azureCredential() {
        return new DefaultAzureCredentialBuilder().build();
    }

    /**
     * ServiceBusClientBuilder for producers and receivers.
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
     * ServiceBusAdministrationClient for topic/subscription provisioning.
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

            log.info("Creating ServiceBusAdministrationClient using Managed Identity.");

            return new ServiceBusAdministrationClientBuilder()
                    .credential(credential)
                    .buildClient();
        }

        log.info("Creating ServiceBusAdministrationClient using connection string.");

        return new ServiceBusAdministrationClientBuilder()
                .connectionString(resolveConnectionString())
                .buildClient();
    }

    // =====================================================
    // 3. INFRASTRUCTURE BOOTSTRAP
    // =====================================================

    @Bean
    public ApplicationListener<ApplicationReadyEvent> azureInfrastructureInitializer(
            ServiceBusAdministrationClient admin
    ) {
        return event -> initializeInfrastructure(admin);
    }

    private void initializeInfrastructure(ServiceBusAdministrationClient admin) {
        log.info("Initializing Azure Service Bus infrastructure...");

        createTopics(admin);
        createSubscriptions(admin);

        log.info("Azure Service Bus infrastructure is ready.");

        if (meterRegistry != null) {
            meterRegistry.counter("umb.azure.infra.ready").increment();
        }
    }

    // =====================================================
    // 4. TOPICS + SUBSCRIPTIONS CREATION
    // =====================================================

    private void createTopics(ServiceBusAdministrationClient admin) {
        core.getTopics().forEach((logical, physical) -> {

            log.info("Checking topic: logical='{}' physical='{}'", logical, physical);

            if (topicExists(admin, physical)) {
                return;
            }

            retry("CreateTopic:" + physical, () -> admin.createTopic(physical));
        });
    }

    private void createSubscriptions(ServiceBusAdministrationClient admin) {

        azure.getSubscriptions().values().forEach(sub -> {

            String physicalTopic = core.getTopics().get(sub.getTopic());

            if (physicalTopic == null) {
                log.warn("Skipping subscription '{}' — logical topic '{}' not mapped.",
                        sub.getName(), sub.getTopic());
                return;
            }

            log.info("Checking subscription: {} → {}", sub.getName(), physicalTopic);

            retry("CreateSubscription:" + sub.getName(), () -> {
                if (!subscriptionExists(admin, physicalTopic, sub.getName())) {
                    admin.createSubscription(physicalTopic, sub.getName());
                }
            });
        });
    }

    // =====================================================
    // 5. RETRY + EXISTENCE CHECKS
    // =====================================================

    private void retry(String label, Runnable action) {
        Duration delay = BASE_DELAY;

        for (int attempt = 1; attempt <= MAX_RETRIES; attempt++) {

            try {
                action.run();

                if (meterRegistry != null) {
                    meterRegistry.counter("umb.azure.infra.success", "label", label).increment();
                }
                return;

            } catch (Exception e) {

                if (attempt == MAX_RETRIES) {
                    log.error("Final retry failed for {}: {}", label, e.getMessage());
                    throw e;
                }

                long jitter = RNG.nextInt(300);
                long sleepMs = delay.toMillis() + jitter;

                log.warn("Retry {}/{} for {} failed: {} → retrying in {}ms",
                        attempt, MAX_RETRIES, label, e.getMessage(), sleepMs);

                sleep(sleepMs);
                delay = delay.multipliedBy(2);
            }
        }
    }

    private void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Retry sleep interrupted", e);
        }
    }

    private boolean topicExists(ServiceBusAdministrationClient admin, String topic) {
        try {
            admin.getTopic(topic);
            return true;
        } catch (ResourceNotFoundException e) {
            return false;
        }
    }

    private boolean subscriptionExists(ServiceBusAdministrationClient admin, String topic, String sub) {
        try {
            admin.getSubscription(topic, sub);
            return true;
        } catch (ResourceNotFoundException e) {
            return false;
        }
    }
}
