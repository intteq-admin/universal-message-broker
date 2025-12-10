package com.intteq.universal.message.broker.rabbitmq;

import com.intteq.universal.message.broker.MessagingProperties;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.*;
import org.springframework.amqp.core.Queue;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.*;

/**
 * Auto-configuration responsible for building RabbitMQ infrastructure:
 * <ul>
 *     <li>Creates durable topic exchanges for each logical topic</li>
 *     <li>Creates durable queues based on MessagingProperties.rabbitmq.queues.*</li>
 *     <li>Optionally creates DLX + DLQ for queues marked as {@code dlq=true}</li>
 *     <li>Creates bindings from queues to exchanges using explicit routing keys</li>
 *     <li>Automatically creates a fallback “auto queue” per logical topic when no queue exists</li>
 * </ul>
 *
 * <p>This class contains no consumer logic — only pure topology creation.
 */
@Slf4j
@Configuration
@ConditionalOnProperty(name = "messaging.provider", havingValue = "rabbitmq", matchIfMissing = true)
public class RabbitMQInfrastructureAutoConfig {

    @Bean
    public Declarables rabbitDeclarables(MessagingProperties props) {

        List<Declarable> declarables = new ArrayList<>();

        // ---------------------------------------------------------------------
        // 1. Create all topic exchanges
        // ---------------------------------------------------------------------
        Map<String, TopicExchange> exchanges = new HashMap<>();

        props.getTopics().forEach((logical, physical) -> {
            TopicExchange exchange = new TopicExchange(physical, true, false);
            exchanges.put(physical, exchange);
            declarables.add(exchange);

            log.info("Declared exchange: logical='{}' physical='{}'", logical, physical);
        });

        // ---------------------------------------------------------------------
        // 2. Create all configured queues
        // ---------------------------------------------------------------------
        props.getRabbitmq().getQueues().forEach((logicalQueue, cfg) -> {

            String queueName = cfg.getName();
            long ttl = cfg.getTtlOrDefault();
            boolean hasDlq = cfg.isDlq();

            QueueBuilder queueBuilder = QueueBuilder.durable(queueName)
                    .withArgument("x-message-ttl", ttl);

            // ------------------------------
            // DLX + DLQ support
            // ------------------------------
            if (hasDlq) {
                String dlxName = queueName + ".dlx";
                String dlqName = queueName + ".dlq";

                TopicExchange dlxExchange = new TopicExchange(dlxName, true, false);
                declarables.add(dlxExchange);

                Queue dlq = QueueBuilder.durable(dlqName).build();
                declarables.add(dlq);

                declarables.add(BindingBuilder.bind(dlq).to(dlxExchange).with("#"));
                queueBuilder.withArgument("x-dead-letter-exchange", dlxName);

                log.info("DLQ enabled → queue={} dlx={} dlq={}", queueName, dlxName, dlqName);
            }

            Queue queue = queueBuilder.build();
            declarables.add(queue);
            log.info("Declared queue: {}", queueName);

            // -----------------------------------------------------------------
            // 3. Determine exchange based on topic -> routingKey match
            // -----------------------------------------------------------------
            String routingKey = cfg.getRoutingKey();

            String targetExchange = resolveExchangeNameForRoutingKey(props, routingKey)
                    .orElseThrow(() -> new IllegalStateException(
                            "No matching topic exchange found for routingKey=" + routingKey +
                                    ". Ensure messaging.topics.* maps correctly."
                    ));

            TopicExchange exchange = exchanges.get(targetExchange);

            Binding binding = BindingBuilder.bind(queue)
                    .to(exchange)
                    .with(routingKey);

            declarables.add(binding);

            log.info("Binding created: queue={} exchange={} routingKey={}", queueName, targetExchange, routingKey);
        });

        // ---------------------------------------------------------------------
        // 4. Create auto queues per logical topic when no explicit queue exists
        // ---------------------------------------------------------------------
        props.getTopics().forEach((logicalTopic, physicalExchange) -> {

            String autoQueueName = logicalTopic + ".auto.queue";

            boolean explicitQueueExists = props.getRabbitmq()
                    .getQueues()
                    .values()
                    .stream()
                    .anyMatch(cfg -> cfg.getName().equals(autoQueueName));

            if (explicitQueueExists) {
                return;
            }

            log.warn("No queue configured for logical topic '{}' → Creating auto queue {}", logicalTopic, autoQueueName);

            // DLQ for auto queues
            String dlx = autoQueueName + ".dlx";
            TopicExchange dlxExchange = new TopicExchange(dlx, true, false);
            declarables.add(dlxExchange);

            Queue dlq = QueueBuilder.durable(autoQueueName + ".dlq").build();
            declarables.add(dlq);
            declarables.add(BindingBuilder.bind(dlq).to(dlxExchange).with("#"));

            Queue autoQueue = QueueBuilder.durable(autoQueueName)
                    .withArgument("x-message-ttl", 172_800_000L)
                    .withArgument("x-dead-letter-exchange", dlx)
                    .build();

            declarables.add(autoQueue);

            TopicExchange exchange = exchanges.get(physicalExchange);

            Binding autoBinding = BindingBuilder.bind(autoQueue)
                    .to(exchange)
                    .with(logicalTopic + ".*");

            declarables.add(autoBinding);

            log.info("Auto queue created: queue={} exchange={} pattern={}",
                    autoQueueName, physicalExchange, logicalTopic + ".*");
        });

        return new Declarables(declarables);
    }

    // -------------------------------------------------------------------------
    // Exchange resolution logic
    // -------------------------------------------------------------------------

    /**
     * Resolve the RabbitMQ exchange based on routing key.
     * Instead of brittle prefix matching, this method uses:
     *
     *     routingKey = "<logicalTopic>.<event>"
     *
     * which is the same convention publishers follow.
     *
     * @return Optional physical exchange name
     */
    private Optional<String> resolveExchangeNameForRoutingKey(MessagingProperties props, String routingKey) {
        if (routingKey == null || !routingKey.contains(".")) {
            return Optional.empty();
        }

        String logicalTopic = routingKey.substring(0, routingKey.indexOf('.'));

        return Optional.ofNullable(props.getTopics().get(logicalTopic));
    }
}
