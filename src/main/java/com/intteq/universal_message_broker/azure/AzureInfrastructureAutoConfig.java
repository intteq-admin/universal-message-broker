package com.intteq.universal_message_broker.azure;

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

    // Sender factory
    @Bean
    public ServiceBusClientBuilder azureSenderFactory() {
        return new ServiceBusClientBuilder().connectionString(resolveConnection());
    }

    // Admin client
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

    // THIS RUNS AFTER SPRING STARTS (safe)
    @Bean
    public ApplicationListener<ApplicationReadyEvent> azureInfraInitializer(
            ServiceBusAdministrationClient admin
    ) {
        return event -> {
            log.info("Initializing Azure Service Bus infrastructure…");

            // 1. Create topics if missing
            props.getTopics().forEach((logical, physical) -> {
                if (!topicExists(admin, physical)) {
                    log.info("Creating Azure topic: {}", physical);
                    admin.createTopic(physical);
                } else {
                    log.info("Topic already exists: {}", physical);
                }
            });

            // 2. Create subscriptions if missing
            props.getAzure().getSubscriptions().values().forEach(sub -> {
                String physicalTopic = props.getTopics().get(sub.getTopic());
                if (physicalTopic == null) return;

                if (!subscriptionExists(admin, physicalTopic, sub.getName())) {
                    log.info("Creating subscription: {} → {}", sub.getName(), physicalTopic);
                    admin.createSubscription(physicalTopic, sub.getName());
                } else {
                    log.info("Subscription already exists: {}", sub.getName());
                }
            });

            log.info("Azure Service Bus infrastructure ready!");
        };
    }

    // ========== Helper methods ==========

    private boolean topicExists(ServiceBusAdministrationClient admin, String topic) {
        try {
            admin.getTopic(topic);
            return true;
        } catch (Exception ignored) {
            return false;
        }
    }

    private boolean subscriptionExists(ServiceBusAdministrationClient admin, String topic, String sub) {
        try {
            admin.getSubscription(topic, sub);
            return true;
        } catch (Exception ignored) {
            return false;
        }
    }
}
