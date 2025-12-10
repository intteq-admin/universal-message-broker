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
 * Azure Service Bus infrastructure auto-configuration.
 *
 * <p>This component is activated only when:
 * <pre>
 * messaging.provider = azure
 * </pre>
 *
 * <p>Capabilities:
 * <ul>
 *     <li>Topic creation</li>
 *     <li>Subscription creation</li>
 *     <li>Backoff-based retry logic</li>
 *     <li>Supports both connection strings & Azure Managed Identity</li>
 *     <li>Optional Micrometer metrics for initialization</li>
 * </ul>
 */
@Configuration
@ConditionalOnProperty(name = "messaging.provider", havingValue = "azure")
@Slf4j
@RequiredArgsConstructor
public class AzureInfrastructureAutoConfig {

    private final MessagingProperties properties;

    /** Optional metric registry (library should not force Micrometer) */
    @Nullable
    private final MeterRegistry meterRegistry;

    /**
     * Optional connection string. If not provided, Managed Identity is used as fallback.
     */
    @Value("${azure.servicebus.connection-string:}")
    private String connectionString;

    private static final int MAX_RETRIES = 6;
    private static final Duration BASE_DELAY = Duration.ofSeconds(1);
    private static final SecureRandom RNG = new SecureRandom();

    // ===========================================================
    // Credential / Connection Resolution
    // ===========================================================

    private volatile boolean initialized;

    @PostConstruct
    void validate() {
        if (initialized) return;
        initialized = true;

        log.info("Azure Service Bus provider activated. Validating configuration...");

        if (connectionString == null || connectionString.isBlank()) {
            log.warn("No connection string provided → attempting to use Managed Identity (Azure AD).");
        } else {
            log.info("Azure Service Bus using connection string authentication.");
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
        return null; // indicates fallback to Managed Identity
    }

    private boolean useManagedIdentity() {
        return resolveConnectionString() == null;
    }

    // ===========================================================
    // Bean Definitions
    // ===========================================================

    @Bean
    public ServiceBusClientBuilder serviceBusClientBuilder() {
        if (useManagedIdentity()) {
            log.info("Azure Service Bus client using Managed Identity auth.");
            TokenCredential credential = new DefaultAzureCredentialBuilder().build();
            return new ServiceBusClientBuilder().credential(credential);
        }
        return new ServiceBusClientBuilder().connectionString(resolveConnectionString());
    }

    @Bean
    public ServiceBusAdministrationClient adminClient() {
        if (useManagedIdentity()) {
            log.info("Azure Service Bus admin client using Managed Identity auth.");
            TokenCredential credential = new DefaultAzureCredentialBuilder().build();
            return new ServiceBusAdministrationClientBuilder()
                    .credential(credential)
                    .buildClient();
        }
        return new ServiceBusAdministrationClientBuilder()
                .connectionString(resolveConnectionString())
                .buildClient();
    }

    // ===========================================================
    // Infrastructure Initialization
    // ===========================================================

    @Bean
    public ApplicationListener<ApplicationReadyEvent> azureInfrastructureInitializer(
            ServiceBusAdministrationClient admin
    ) {
        return event -> initializeAzureInfrastructure(admin);
    }

    private void initializeAzureInfrastructure(ServiceBusAdministrationClient admin) {
        log.info("Beginning Azure Service Bus infrastructure initialization…");

        createTopics(admin);
        createSubscriptions(admin);

        log.info("Azure Service Bus infrastructure ready.");
        if (meterRegistry != null) {
            meterRegistry.counter("umb.azure.infra.ready").increment();
        }
    }

    // ===========================================================
    // Topic & Subscription Creation
    // ===========================================================

    private void createTopics(ServiceBusAdministrationClient admin) {
        properties.getTopics().forEach((logical, physical) -> {
            log.info("Ensuring topic exists: logical='{}' physical='{}'", logical, physical);

            if (topicExists(admin, physical)) {
                log.info("Topic already exists: {}", physical);
                return;
            }

            retry("CreateTopic:" + physical, () -> admin.createTopic(physical));
        });
    }

    private void createSubscriptions(ServiceBusAdministrationClient admin) {
        properties.getAzure().getSubscriptions().values().forEach(sub -> {
            String physicalTopic = properties.getTopics().get(sub.getTopic());

            if (physicalTopic == null) {
                log.warn("No topic mapping found for subscription '{}' on logical topic '{}' → skipping.",
                        sub.getName(), sub.getTopic());
                return;
            }

            log.info("Ensuring subscription exists: {} → {}", sub.getName(), physicalTopic);

            retry("CreateSubscription:" + sub.getName(), () -> {
                if (!subscriptionExists(admin, physicalTopic, sub.getName())) {
                    admin.createSubscription(physicalTopic, sub.getName());
                }
            });
        });
    }

    // ===========================================================
    // Retry with jitter exponential backoff
    // ===========================================================

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
            throw new IllegalStateException("Retry interrupted", e);
        }
    }

    // ===========================================================
    // Existence Checks
    // ===========================================================

    private boolean topicExists(ServiceBusAdministrationClient admin, String name) {
        try {
            admin.getTopic(name);
            return true;
        } catch (ResourceNotFoundException e) {
            return false;
        } catch (Exception e) {
            log.error("Topic existence check failed: {}", name, e);
            throw e;
        }
    }

    private boolean subscriptionExists(ServiceBusAdministrationClient admin, String topic, String sub) {
        try {
            admin.getSubscription(topic, sub);
            return true;
        } catch (ResourceNotFoundException ignored) {
            return false;
        } catch (Exception e) {
            log.error("Subscription existence check failed: {} / {}", topic, sub, e);
            throw e;
        }
    }
}
