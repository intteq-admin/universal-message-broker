package com.intteq.universal.message.broker.internal;

import com.azure.messaging.servicebus.ServiceBusClientBuilder;
import com.azure.messaging.servicebus.ServiceBusProcessorClient;
import com.azure.messaging.servicebus.ServiceBusReceivedMessage;
import com.azure.messaging.servicebus.models.DeadLetterOptions;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.intteq.universal.message.broker.MessageContext;
import com.intteq.universal.message.broker.MessagingProperties;
import com.intteq.universal.message.broker.annotation.EventHandler;
import com.intteq.universal.message.broker.annotation.MessagingListener;
import io.micrometer.core.instrument.MeterRegistry;
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

import java.lang.reflect.Method;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * Responsible for discovering @MessagingListener beans and registering message listeners
 * for both RabbitMQ and Azure Service Bus with manual acknowledgment semantics.
 *
 * <p>Key Features:
 * <ul>
 *     <li>Dynamic listener registration per event method</li>
 *     <li>Manual acknowledgment mode for both providers</li>
 *     <li>DLQ forwarding on handler failure</li>
 *     <li>Thread-safe lifecycle tracking for listeners</li>
 *     <li>Configurable concurrency for Azure and RabbitMQ</li>
 *     <li>Micrometer metrics for consume, failure, and processing latency</li>
 * </ul>
 *
 * <p>This class runs after all singletons are loaded and automatically wires message consumers.
 * It also performs graceful shutdown.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class ManualAckListenerProcessor implements SmartInitializingSingleton, DisposableBean {

    private final MessagingProperties properties;
    private final ApplicationContext context;
    private final ObjectMapper objectMapper;
    private final MeterRegistry meterRegistry;

    /** Thread-safe caches for active message processors */
    private final Map<String, ServiceBusProcessorClient> azureProcessors = new ConcurrentHashMap<>();
    private final Map<String, SimpleMessageListenerContainer> rabbitContainers = new ConcurrentHashMap<>();

    private ConfigurableApplicationContext configurableContext;

    /** Default concurrency - can be made configurable in MessagingProperties */
    private static final int DEFAULT_RABBIT_PREFETCH = 10;
    private static final int DEFAULT_AZURE_MAX_CONCURRENT_CALLS = 5;

    @PostConstruct
    private void init() {
        this.configurableContext = (ConfigurableApplicationContext) context;
    }

    // ========================================================================
    //   LIFECYCLE MANAGEMENT
    // ========================================================================

    @Override
    public void afterSingletonsInstantiated() {
        log.info("Scanning for @MessagingListener beans...");

        context.getBeansWithAnnotation(MessagingListener.class).values().forEach(bean -> {
            Class<?> beanClass = bean.getClass();

            MessagingListener listenerAnn = AnnotationUtils.findAnnotation(beanClass, MessagingListener.class);
            if (listenerAnn == null) return;

            String logicalTopic = listenerAnn.topic();
            String resolvedTopic = properties.getTopics().getOrDefault(logicalTopic, logicalTopic);
            String channel = listenerAnn.channel();

            for (Method method : beanClass.getDeclaredMethods()) {
                if (!method.isAnnotationPresent(EventHandler.class)) continue;
                if (method.getParameterCount() != 2) {
                    log.warn("Skipping method {}.{} → invalid signature. Expected (Payload, MessageContext).",
                            beanClass.getSimpleName(), method.getName());
                    continue;
                }

                String eventType = method.getAnnotation(EventHandler.class).value();
                String routingKey = logicalTopic + "." + eventType;

                if ("rabbitmq".equalsIgnoreCase(properties.getProvider())) {
                    registerRabbitListener(resolvedTopic, routingKey, channel, bean, method);
                } else {
                    registerAzureListener(resolvedTopic, routingKey, channel, bean, method);
                }
            }
        });
    }

    @Override
    public void destroy() {
        log.info("Shutting down message listeners...");

        azureProcessors.forEach((sub, processor) -> {
            try {
                log.info("Stopping Azure processor: {}", sub);
                processor.stop();
                processor.close();
            } catch (Exception e) {
                log.warn("Failed to stop Azure processor {}", sub, e);
            }
        });

        rabbitContainers.forEach((queue, container) -> {
            try {
                log.info("Stopping RabbitMQ listener: {}", queue);
                container.stop();
            } catch (Exception e) {
                log.warn("Failed to stop RabbitMQ container {}", queue, e);
            }
        });

        log.info("All message listeners stopped successfully.");
    }

    // ========================================================================
    //   RABBITMQ
    // ========================================================================

    private void registerRabbitListener(
            String exchange,
            String routingKey,
            String channel,
            Object handler,
            Method method
    ) {
        String queueName = channel + ".queue";

        AmqpAdmin admin = context.getBean(AmqpAdmin.class);

        try {
            Queue queue = QueueBuilder.durable(queueName).build();
            TopicExchange ex = new TopicExchange(exchange, true, false);
            Binding binding = BindingBuilder.bind(queue).to(ex).with(routingKey);

            admin.declareQueue(queue);
            admin.declareExchange(ex);
            admin.declareBinding(binding);

            log.info("RabbitMQ infrastructure created → queue={}, exchange={}, routingKey={}",
                    queueName, exchange, routingKey);

        } catch (Exception e) {
            log.error("RabbitMQ infrastructure creation failed for queue={}", queueName, e);
            return;
        }

        rabbitContainers.computeIfAbsent(queueName, q ->
                createRabbitContainer(q, handler, method)
        );
    }

    private SimpleMessageListenerContainer createRabbitContainer(
            String queueName,
            Object handler,
            Method method
    ) {
        ConnectionFactory connectionFactory = context.getBean(ConnectionFactory.class);

        SimpleMessageListenerContainer container = new SimpleMessageListenerContainer(connectionFactory);
        container.setQueueNames(queueName);
        container.setAcknowledgeMode(AcknowledgeMode.MANUAL);
        container.setPrefetchCount(DEFAULT_RABBIT_PREFETCH);
        container.setMissingQueuesFatal(false);
        container.setRecoveryInterval(3000);

        container.setMessageListener((ChannelAwareMessageListener) (message, channel) -> {
            long tag = message.getMessageProperties().getDeliveryTag();

            try {
                Object payload = objectMapper.readValue(
                        message.getBody(),
                        method.getParameterTypes()[0]
                );

                MessageContext mc = MessageContext.forRabbitMQ(channel, tag);

                long start = System.nanoTime();
                method.invoke(handler, payload, mc);
                long duration = System.nanoTime() - start;

                meterRegistry.timer("umb.consume.latency", "queue", queueName)
                        .record(duration, TimeUnit.NANOSECONDS);

                meterRegistry.counter("umb.consume.success", "queue", queueName).increment();

            } catch (Exception e) {
                log.error("RabbitMQ listener failed for queue={} → sending to DLQ", queueName, e);
                meterRegistry.counter("umb.consume.failure", "queue", queueName).increment();
                channel.basicNack(tag, false, false); // DLQ
            }
        });

        container.afterPropertiesSet();
        container.start();

        log.info("RabbitMQ listener STARTED → queue={}", queueName);
        return container;
    }

    // ========================================================================
    //   AZURE SERVICE BUS
    // ========================================================================

    private void registerAzureListener(
            String topic,
            String routingKey,
            String channel,
            Object handler,
            Method method
    ) {
        String subscription = channel + "-sub";

        if (azureProcessors.containsKey(subscription)) {
            log.info("Azure subscription already active → {}", subscription);
            return;
        }

        ServiceBusProcessorClient processor = context.getBean(ServiceBusClientBuilder.class)
                .processor()
                .topicName(topic)
                .subscriptionName(subscription)
                .disableAutoComplete()
                .maxConcurrentCalls(DEFAULT_AZURE_MAX_CONCURRENT_CALLS)
                .processMessage(processContext -> {
                    ServiceBusReceivedMessage msg = processContext.getMessage();

                    try {
                        Object payload = objectMapper.readValue(
                                msg.getBody().toBytes(),
                                method.getParameterTypes()[0]
                        );

                        MessageContext mc = MessageContext.forAzureProcessor(processContext);

                        long start = System.nanoTime();
                        method.invoke(handler, payload, mc);
                        long duration = System.nanoTime() - start;

                        meterRegistry.timer("umb.consume.latency", "subscription", subscription)
                                .record(duration, TimeUnit.NANOSECONDS);

                        meterRegistry.counter("umb.consume.success", "subscription", subscription).increment();

                    } catch (Exception e) {
                        meterRegistry.counter("umb.consume.failure", "subscription", subscription).increment();
                        log.error("Azure handler failure → dead-lettering message", e);

                        DeadLetterOptions options = new DeadLetterOptions()
                                .setDeadLetterReason("processing-exception")
                                .setDeadLetterErrorDescription(e.getMessage());

                        processContext.deadLetter(options);
                    }
                })
                .processError(err -> log.error("Azure Service Bus error: {}", err.getException().getMessage()))
                .buildProcessorClient();

        processor.start();
        azureProcessors.put(subscription, processor);

        log.info("Azure listener STARTED → topic={}, subscription={}", topic, subscription);
    }
}
