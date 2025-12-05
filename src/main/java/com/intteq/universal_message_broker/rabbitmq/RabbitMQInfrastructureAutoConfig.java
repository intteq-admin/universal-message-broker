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

        var declarables = new ArrayList<Declarable>();
        var exchanges = new HashMap<String, TopicExchange>();


        // 1️⃣ Create all topic exchanges
        props.getTopics().values().forEach(exchangeName -> {
            TopicExchange exchange = new TopicExchange(exchangeName, true, false);
            exchanges.put(exchangeName, exchange);
            declarables.add(exchange);
        });


        // 2️⃣ Create all manually registered queues
        props.getRabbitmq().getQueues().values().forEach(cfg -> {

            QueueBuilder builder = QueueBuilder.durable(cfg.getName())
                    .withArgument("x-message-ttl", cfg.getTtl());

            if (cfg.isDlq()) {
                String dlx = cfg.getName() + ".dlx";
                builder.withArgument("x-dead-letter-exchange", dlx);

                // create DLX
                TopicExchange dlxExchange = new TopicExchange(dlx, true, false);
                declarables.add(dlxExchange);

                // create DLQ
                String dlqName = cfg.getName() + ".dlq";
                Queue dlq = QueueBuilder.durable(dlqName).build();
                declarables.add(dlq);

                // bind DLQ → DLX
                declarables.add(BindingBuilder.bind(dlq).to(dlxExchange).with("#"));
            }

            Queue queue = builder.build();
            declarables.add(queue);


            // Find matching exchange based on prefix
            String exchangeName = props.getTopics().entrySet().stream()
                    .filter(e -> cfg.getRoutingKey()
                            .startsWith(e.getKey().split("-")[0] + "."))
                    .map(Map.Entry::getValue)
                    .findFirst()
                    .orElseThrow(() ->
                            new IllegalStateException("No exchange found for routing key: " + cfg.getRoutingKey()));

            TopicExchange exchange = exchanges.get(exchangeName);

            declarables.add(BindingBuilder.bind(queue).to(exchange).with(cfg.getRoutingKey()));
        });


        // 3️⃣ Auto-create queues for topics that do not have queues defined
        props.getTopics().forEach((logical, physicalExchange) -> {

            String autoQueueName = logical + ".auto.queue";

            boolean isDefined = props.getRabbitmq().getQueues().values()
                    .stream()
                    .anyMatch(cfg -> cfg.getName().equals(autoQueueName));

            if (!isDefined) {

                // Auto queue DLX
                String dlx = autoQueueName + ".dlx";

                Queue autoQueue = QueueBuilder.durable(autoQueueName)
                        .withArgument("x-message-ttl", 172_800_000L)
                        .withArgument("x-dead-letter-exchange", dlx)
                        .build();

                declarables.add(autoQueue);

                // Create DLX exchange
                TopicExchange dlxExchange = new TopicExchange(dlx, true, false);
                declarables.add(dlxExchange);

                // Create DLQ
                Queue dlq = QueueBuilder.durable(autoQueueName + ".dlq").build();
                declarables.add(dlq);

                // Bind DLQ → DLX
                declarables.add(BindingBuilder.bind(dlq).to(dlxExchange).with("#"));


                // Now bind autoQueue → topic exchange
                TopicExchange mainExchange = exchanges.get(physicalExchange);
                declarables.add(BindingBuilder.bind(autoQueue)
                        .to(mainExchange)
                        .with(logical + ".*"));
            }
        });

        return new Declarables(declarables);
    }
}