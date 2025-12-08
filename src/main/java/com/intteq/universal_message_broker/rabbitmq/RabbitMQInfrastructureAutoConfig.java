package com.intteq.universal_message_broker.rabbitmq;

import com.intteq.universal_message_broker.MessagingProperties;
import org.springframework.amqp.core.*;
import org.springframework.amqp.core.Queue;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.*;
@Configuration
@ConditionalOnProperty(name = "messaging.provider", havingValue = "rabbitmq", matchIfMissing = true)
public class RabbitMQInfrastructureAutoConfig {

    @Bean
    public Declarables rabbitDeclarables(MessagingProperties props) {

        List<Declarable> declarables = new ArrayList<>();
        Map<String, TopicExchange> exchanges = new HashMap<>();

        props.getTopics().values().forEach(exchangeName -> {
            TopicExchange exchange = new TopicExchange(exchangeName, true, false);
            exchanges.put(exchangeName, exchange);
            declarables.add(exchange);
        });

        props.getRabbitmq().getQueues().values().forEach(cfg -> {

            QueueBuilder builder = QueueBuilder
                    .durable(cfg.getName())
                    .withArgument("x-message-ttl", cfg.getTtl());

            if (cfg.isDlq()) {

                String dlx = cfg.getName() + ".dlx";
                builder.withArgument("x-dead-letter-exchange", dlx);

                TopicExchange dlxExchange = new TopicExchange(dlx, true, false);
                declarables.add(dlxExchange);

                String dlqName = cfg.getName() + ".dlq";
                Queue dlq = QueueBuilder.durable(dlqName).build();
                declarables.add(dlq);

                declarables.add(BindingBuilder.bind(dlq).to(dlxExchange).with("#"));
            }

            Queue queue = builder.build();
            declarables.add(queue);


            String routingKeyPrefix = cfg.getRoutingKey().contains(".")
                    ? cfg.getRoutingKey().substring(0, cfg.getRoutingKey().indexOf('.'))
                    : cfg.getRoutingKey();

            String exchangeName = props.getTopics().entrySet().stream()
                    .filter(e -> {
                        String topic = e.getKey();
                        String topicPrefix = topic.contains("-")
                                ? topic.substring(0, topic.indexOf('-'))
                                : topic;
                        return routingKeyPrefix.equals(topicPrefix);
                    })
                    .map(Map.Entry::getValue)
                    .findFirst()
                    .orElseThrow(() ->
                            new IllegalStateException(
                                    "No exchange found for routing key: " + cfg.getRoutingKey()
                            )
                    );

            TopicExchange exchange = exchanges.get(exchangeName);

            declarables.add(
                    BindingBuilder.bind(queue)
                            .to(exchange)
                            .with(cfg.getRoutingKey())
            );
        });

        props.getTopics().forEach((logicalTopic, physicalExchange) -> {

            String autoQueueName = logicalTopic + ".auto.queue";

            boolean exists = props.getRabbitmq().getQueues().values()
                    .stream()
                    .anyMatch(cfg -> cfg.getName().equals(autoQueueName));

            if (!exists) {

                String dlx = autoQueueName + ".dlx";
                TopicExchange dlxExchange = new TopicExchange(dlx, true, false);
                declarables.add(dlxExchange);

                Queue autoQueue = QueueBuilder.durable(autoQueueName)
                        .withArgument("x-message-ttl", 172_800_000L) // 2 days
                        .withArgument("x-dead-letter-exchange", dlx)
                        .build();

                declarables.add(autoQueue);

                Queue dlq = QueueBuilder.durable(autoQueueName + ".dlq").build();
                declarables.add(dlq);

                declarables.add(BindingBuilder.bind(dlq).to(dlxExchange).with("#"));

                TopicExchange mainExchange = exchanges.get(physicalExchange);

                declarables.add(
                        BindingBuilder.bind(autoQueue)
                                .to(mainExchange)
                                .with(logicalTopic + ".*")
                );
            }
        });

        return new Declarables(declarables);
    }
}