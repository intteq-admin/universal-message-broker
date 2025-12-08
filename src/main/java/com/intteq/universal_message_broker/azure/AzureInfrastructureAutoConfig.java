package com.intteq.universal_message_broker.azure;

import com.azure.core.exception.ResourceNotFoundException;
import com.azure.messaging.servicebus.ServiceBusClientBuilder;
import com.azure.messaging.servicebus.administration.ServiceBusAdministrationClient;
import com.azure.messaging.servicebus.administration.ServiceBusAdministrationClientBuilder;
import com.intteq.universal_message_broker.MessagingProperties;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
@Configuration
@ConditionalOnProperty(name = "messaging.provider", havingValue = "azure")
@Slf4j
public class AzureInfrastructureAutoConfig {
    private final MessagingProperties props;

    @Value("${azure.servicebus.connection-string:}")
    private String connectionString;

    public AzureInfrastructureAutoConfig(MessagingProperties props) {
        this.props = props;
    }

    @Bean
    public ServiceBusClientBuilder azureSenderFactory() {
        return new ServiceBusClientBuilder().connectionString(resolveConnection());
    }

    @Bean
    public ServiceBusAdministrationClient adminClient() {
        return new ServiceBusAdministrationClientBuilder()
                .connectionString(resolveConnection())
                .buildClient();
    }

    private String resolveConnection() {
        String conn = connectionString.isBlank()
                ? System.getenv("AZURE_SERVICEBUS_CONNECTION_STRING")
                : connectionString;

        if (conn == null || conn.isBlank()) {
            throw new IllegalStateException("Azure Service Bus connection string not found!");
        }
        return conn;
    }

    @Bean
    public ApplicationListener<ApplicationReadyEvent> azureInfraInitializer(
            ServiceBusAdministrationClient admin
    ) {
        return event -> {

            log.info("Initializing Azure Service Bus infrastructure with retry support…");

            props.getTopics().forEach((logical, physical) -> {
                if (!topicExists(admin, physical)) {
                    log.info("Creating Azure topic: {}", physical);
                    retryWithBackoff(
                            () -> admin.createTopic(physical),
                            "CreateTopic:" + physical
                    );
                } else {
                    log.info("Topic already exists: {}", physical);
                }
            });

            props.getAzure().getSubscriptions().values().forEach(sub -> {
                String physicalTopic = props.getTopics().get(sub.getTopic());
                if (physicalTopic == null) return;

                if (!subscriptionExists(admin, physicalTopic, sub.getName())) {
                    log.info("Creating subscription: {} → {}", sub.getName(), physicalTopic);
                    retryWithBackoff(
                            () -> admin.createSubscription(physicalTopic, sub.getName()),
                            "CreateSubscription:" + sub.getName()
                    );
                } else {
                    log.info("Subscription already exists: {}", sub.getName());
                }
            });

            log.info("Azure Service Bus infrastructure ready!");
        };
    }

    private void retryWithBackoff(Runnable action, String label) {
        int maxAttempts = 6;
        long delay = 1000;

        for (int attempt = 1; attempt <= maxAttempts; attempt++) {
            try {
                action.run();
                return;

            } catch (Exception ex) {
                if (attempt == maxAttempts) {
                    log.error("Final retry failed for {}: {}", label, ex.getMessage());
                    throw ex;
                }

                log.warn("Retry {}/{} for {} failed: {} → retrying in {}ms",
                        attempt, maxAttempts, label, ex.getMessage(), delay);

                try {
                    Thread.sleep(delay);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException(
                            "Retry interrupted for: " + label,
                            e
                    );
                }

                delay *= 2;
            }
        }
    }

    private boolean topicExists(ServiceBusAdministrationClient admin, String topic) {
        try {
            admin.getTopic(topic);
            return true;

        } catch (ResourceNotFoundException e) {
            return false;

        } catch (Exception e) {
            log.error("Error checking topic existence: {}", topic, e);
            throw e;
        }
    }

    private boolean subscriptionExists(ServiceBusAdministrationClient admin, String topic, String sub) {
        try {
            admin.getSubscription(topic, sub);
            return true;

        } catch (ResourceNotFoundException e) {
            return false;

        } catch (Exception e) {
            log.error("Error checking subscription existence: {} / {}", topic, sub, e);
            throw e;
        }
    }
}
