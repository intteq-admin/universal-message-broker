package com.intteq.universal_message_broker.internal;

import com.azure.messaging.servicebus.ServiceBusClientBuilder;
import com.azure.messaging.servicebus.ServiceBusProcessorClient;
import com.azure.messaging.servicebus.ServiceBusReceivedMessage;
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
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.core.annotation.AnnotationUtils;
import org.springframework.stereotype.Component;

import java.util.concurrent.ConcurrentHashMap;

@Component
@RequiredArgsConstructor
@Slf4j
public class ManualAckListenerProcessor implements SmartInitializingSingleton, DisposableBean {

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
    public void destroy() {
        log.info("Shutting down message listeners...");

        // Stop Azure processors
        azureProcessors.values().forEach(processor -> {
            try {
                log.info("Stopping Azure processor: {}", processor.getSubscriptionName());
                processor.stop();
                processor.close();
            } catch (Exception e) {
                log.warn("Error closing Azure processor", e);
            }
        });

        // Stop RabbitMQ containers
        rabbitContainers.values().forEach(container -> {
            try {
                log.info("Stopping Rabbit listener: {}", String.join(",", container.getQueueNames()));
                container.stop();
            } catch (Exception e) {
                log.warn("Error stopping Rabbit container", e);
            }
        });

        log.info("Message listeners stopped successfully.");
    }

    @Override
    public void afterSingletonsInstantiated() {
        ctx.getBeansWithAnnotation(MessagingListener.class).values().forEach(bean -> {
            Class<?> clazz = bean.getClass();

            MessagingListener ann = AnnotationUtils.findAnnotation(clazz, MessagingListener.class);
            if (ann == null) {
                log.warn("MessagingListener annotation not found on bean {}. Skipping...", clazz.getName());
                return;
            }
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
    private void registerRabbitListener(
            String exchange,
            String routingKey,
            String channelName,
            Object handler,
            java.lang.reflect.Method method
    ) {
        String queueName = channelName + ".queue";
        String containerBeanName = "rabbitContainer_" + queueName;

        // ========== 1. DECLARE QUEUE, EXCHANGE & BINDING ==========
        AmqpAdmin admin = ctx.getBean(AmqpAdmin.class);

        try {
            Queue queue = QueueBuilder.durable(queueName).build();
            TopicExchange ex = new TopicExchange(exchange, true, false);
            Binding binding = BindingBuilder.bind(queue).to(ex).with(routingKey);

            admin.declareQueue(queue);
            admin.declareExchange(ex);
            admin.declareBinding(binding);

            log.info("RabbitMQ OK → queue={}, exchange={}, routingKey={}",
                    queueName, exchange, routingKey);

        } catch (Exception e) {
            log.error("Failed to declare RabbitMQ infra for queue={}", queueName, e);
            return;
        }

        // ========== 2. ATOMIC: CREATE LISTENER ONLY IF ABSENT ==========
        SimpleMessageListenerContainer container =
                rabbitContainers.computeIfAbsent(queueName, q -> createRabbitContainer(
                        q, handler, method
                ));

        // Prevent duplicate Spring bean registration
        if (!configurableCtx.getBeanFactory().containsBean(containerBeanName)) {
            configurableCtx.getBeanFactory().registerSingleton(containerBeanName, container);
        }

        log.info("RabbitMQ Listener STARTED → queue={}", queueName);
    }


    private SimpleMessageListenerContainer createRabbitContainer(
            String queueName,
            Object handler,
            java.lang.reflect.Method method
    ) {
        ConnectionFactory connectionFactory = ctx.getBean(ConnectionFactory.class);

        SimpleMessageListenerContainer container =
                new SimpleMessageListenerContainer(connectionFactory);

        container.setQueueNames(queueName);
        container.setAcknowledgeMode(AcknowledgeMode.MANUAL);
        container.setPrefetchCount(1);
        container.setMissingQueuesFatal(false);
        container.setRecoveryInterval(3000);

        container.setMessageListener((ChannelAwareMessageListener) (message, channel) -> {
            long tag = message.getMessageProperties().getDeliveryTag();

            try {
                Object payload = mapper.readValue(
                        message.getBody(),
                        method.getParameterTypes()[0]
                );
                MessageContext mc = new MessageContext(channel, tag);
                method.invoke(handler, payload, mc);

            } catch (Exception ex) {
                log.error("Listener failed on queue {} → sending to DLQ", queueName, ex);
                channel.basicNack(tag, false, false);
            }
        });

        container.afterPropertiesSet();
        container.start();

        return container;
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

        try {
            processor.start();
        } catch (Exception e) {
            log.error("Failed to start Azure processor for subscription {}", subscription, e);
        }
        azureProcessors.put(subscription, processor);
        log.info("Azure listener STARTED: {} → {}/{}", routingKey, topic, subscription);
    }
}
