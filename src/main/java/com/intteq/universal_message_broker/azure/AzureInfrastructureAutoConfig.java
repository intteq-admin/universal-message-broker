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
            ServiceBusAdministrationClient adminClient
    ) {
        return event -> {

            log.info("Initializing Azure Service Bus infrastructure…");

            // 1. Create topics
            props.getTopics().forEach((logical, physical) -> {
                if (!adminClient.getTopicExists(physical)) {
                    log.info("Creating Azure topic: {}", physical);
                    adminClient.createTopic(physical);
                }
            });

            // 2. Create subscriptions
            props.getAzure().getSubscriptions().values().forEach(sub -> {
                String physicalTopic = props.getTopics().get(sub.getTopic());
                if (physicalTopic == null) return;

                if (!adminClient.getSubscriptionExists(physicalTopic, sub.getName())) {
                    log.info("Creating subscription: {} → {}", sub.getName(), physicalTopic);
                    adminClient.createSubscription(physicalTopic, sub.getName());
                }
            });

            log.info("Azure Service Bus infrastructure ready!");
        };
    }
}
