package com.intteq.universal_message_broker.internal;
import com.azure.messaging.servicebus.*;
import com.azure.messaging.servicebus.models.DeadLetterOptions;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.intteq.universal_message_broker.MessageContext;
import com.intteq.universal_message_broker.MessagingProperties;
import com.intteq.universal_message_broker.annotation.EventHandler;
import com.intteq.universal_message_broker.annotation.MessagingListener;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.*;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer;
import org.springframework.amqp.rabbit.listener.api.ChannelAwareMessageListener;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.stereotype.Component;
import com.rabbitmq.client.Channel;

import java.util.concurrent.ConcurrentHashMap;

@Component
@RequiredArgsConstructor
@Slf4j
public class ManualAckListenerProcessor implements SmartInitializingSingleton {

    private final MessagingProperties props;
    private final ApplicationContext ctx;
    private final ObjectMapper mapper = new ObjectMapper();
    private final ConcurrentHashMap<String, ServiceBusProcessorClient> azureProcessors = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, SimpleMessageListenerContainer> rabbitContainers = new ConcurrentHashMap<>();

    private ConfigurableApplicationContext configurableCtx;

    @PostConstruct
    private void init() {
        this.configurableCtx = (ConfigurableApplicationContext) ctx;
    }

    @Override
    public void afterSingletonsInstantiated() {
        ctx.getBeansWithAnnotation(MessagingListener.class).values().forEach(bean -> {
            Class<?> clazz = bean.getClass();
            MessagingListener ann = clazz.getAnnotation(MessagingListener.class);
            String logicalTopic = ann.topic();
            String physicalTopic = props.getTopics().getOrDefault(logicalTopic, logicalTopic);
            String channelName = ann.channel();

            for (var method : clazz.getDeclaredMethods()) {
                if (!method.isAnnotationPresent(EventHandler.class)) continue;
                if (method.getParameterCount() != 2) {
                    log.warn("Invalid handler method: {} in {}", method.getName(), clazz.getSimpleName());
                    continue;
                }

                String eventType = method.getAnnotation(EventHandler.class).value();
                String routingKey = logicalTopic + "." + eventType;

                if ("rabbitmq".equalsIgnoreCase(props.getProvider())) {
                    registerRabbitListener(physicalTopic, routingKey, channelName, bean, method);
                } else {
                    registerAzureListener(physicalTopic, routingKey, channelName, bean, method);
                }
            }
        });
    }

    // ====================== RABBITMQ — FINAL & BULLETPROOF ======================
    private void registerRabbitListener(String exchange, String routingKey, String channelName, Object handler, java.lang.reflect.Method method) {
        String queueName = channelName + ".queue";
        String declarableBeanName = queueName + "_decl";
        String containerBeanName = "rabbitContainer_" + queueName;

        // 1. Declare queue + exchange + binding (idempotent)
        if (!configurableCtx.getBeanFactory().containsBean(declarableBeanName)) {
            Declarables declarables = new Declarables(
                    new Queue(queueName, true, false, false), // durable, not exclusive, not auto-delete
                    new TopicExchange(exchange, true, false),
                    BindingBuilder.bind(new Queue(queueName))
                            .to(new TopicExchange(exchange))
                            .with(routingKey)
            );
            configurableCtx.getBeanFactory().registerSingleton(declarableBeanName, declarables);
            log.info("Declared RabbitMQ: {} → {} (exchange: {})", routingKey, queueName, exchange);
        }

        // 2. Register listener container only once
        if (rabbitContainers.containsKey(queueName)) {
            log.debug("RabbitMQ listener already active: {}", queueName);
            return;
        }

        ConnectionFactory connectionFactory = ctx.getBean(ConnectionFactory.class);
        SimpleMessageListenerContainer container = new SimpleMessageListenerContainer(connectionFactory);
        container.setQueueNames(queueName);
        container.setAcknowledgeMode(AcknowledgeMode.MANUAL);
        container.setPrefetchCount(1);
        container.setRecoveryInterval(0L); // Prevents restart loop
        container.setMissingQueuesFatal(false);

        container.setMessageListener((ChannelAwareMessageListener) (Message message, Channel channel) -> {
            long deliveryTag = message.getMessageProperties().getDeliveryTag();
            try {
                Object payload = mapper.readValue(message.getBody(), method.getParameterTypes()[0]);
                MessageContext context = new MessageContext(channel, deliveryTag);
                method.invoke(handler, payload, context);
            } catch (Exception e) {
                log.error("Handler failed in queue {} → NACK + DLQ", queueName, e);
                try {
                    channel.basicNack(deliveryTag, false, false);
                } catch (Exception ignored) {}
            }
        });

        container.afterPropertiesSet();
        container.start();

        // Save container for lifecycle management
        rabbitContainers.put(queueName, container);
        configurableCtx.getBeanFactory().registerSingleton(containerBeanName, container);

        log.info("RabbitMQ listener STARTED: {} → {}", routingKey, queueName);
    }

    // ====================== AZURE SERVICE BUS — FINAL ======================
    private void registerAzureListener(String topic, String routingKey, String channelName, Object handler, java.lang.reflect.Method method) {
        String subscription = channelName + "-sub";

        if (azureProcessors.containsKey(subscription)) {
            log.debug("Azure subscription already active: {}", subscription);
            return;
        }

        ServiceBusProcessorClient processor = ctx.getBean(ServiceBusClientBuilder.class)
                .processor()
                .topicName(topic)
                .subscriptionName(subscription)
                .disableAutoComplete()
                .maxConcurrentCalls(1)
                .processMessage(context -> {
                    ServiceBusReceivedMessage msg = context.getMessage();
                    try {
                        Object payload = mapper.readValue(msg.getBody().toBytes(), method.getParameterTypes()[0]);
                        MessageContext msgCtx = new MessageContext(context);
                        method.invoke(handler, payload, msgCtx);
                    } catch (Exception e) {
                        log.error("Azure handler failed → dead-lettering", e);
                        DeadLetterOptions options = new DeadLetterOptions()
                                .setDeadLetterReason("processing-exception")
                                .setDeadLetterErrorDescription(e.getMessage() != null ? e.getMessage() : "Unknown");
                        context.deadLetter(options);
                    }
                })
                .processError(err -> log.error("Azure Service Bus error: {}", err.getException().getMessage()))
                .buildProcessorClient();

        processor.start();
        azureProcessors.put(subscription, processor);
        log.info("Azure listener STARTED: {} → {}/{}", routingKey, topic, subscription);
    }
}
