package com.intteq.universal_message_broker;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.Map;

@ConfigurationProperties(prefix = "messaging")
public class MessagingProperties {
    private String provider = "rabbitmq";

    // Logical topics → physical exchange (RabbitMQ) or topic (Azure)
    private Map<String, String> topics = Map.of();

    private final RabbitMQ rabbitmq = new RabbitMQ();
    private final Azure azure = new Azure();

    // Getters & setters
    public String getProvider() { return provider == null ? "rabbitmq" : provider.toLowerCase(); }
    public void setProvider(String provider) { this.provider = provider; }
    public Map<String, String> getTopics() { return topics; }
    public void setTopics(Map<String, String> topics) { this.topics = topics; }
    public RabbitMQ getRabbitmq() { return rabbitmq; }
    public Azure getAzure() { return azure; }

    public static class RabbitMQ {
        private Map<String, QueueConfig> queues = Map.of();

        public Map<String, QueueConfig> getQueues() { return queues; }
        public void setQueues(Map<String, QueueConfig> queues) { this.queues = queues; }
    }

    public static class Azure {
        private Map<String, SubscriptionConfig> subscriptions = Map.of();
        public Map<String, SubscriptionConfig> getSubscriptions() { return subscriptions; }
        public void setSubscriptions(Map<String, SubscriptionConfig> subscriptions) { this.subscriptions = subscriptions; }
    }

    // ← TTL configurable + default 2 days (172800000 ms)
    public static class QueueConfig {
        private String name;
        private String routingKey;
        private boolean dlq = false;
        private Long ttl = 172_800_000L; // ← DEFAULT = 2 days

        // Getters & setters
        public String getName() { return name; }
        public void setName(String name) { this.name = name; }
        public String getRoutingKey() { return routingKey; }
        public void setRoutingKey(String routingKey) { this.routingKey = routingKey; }
        public boolean isDlq() { return dlq; }
        public void setDlq(boolean dlq) { this.dlq = dlq; }
        public Long getTtl() { return ttl != null ? ttl : 172_800_000L; } // fallback
        public void setTtl(Long ttl) { this.ttl = ttl; }
    }

    public static class SubscriptionConfig {
        private String topic;   // logical name (e.g. "course-events")
        private String name;
        public String getTopic() { return topic; }
        public void setTopic(String topic) { this.topic = topic; }
        public String getName() { return name; }
        public void setName(String name) { this.name = name; }
    }
}