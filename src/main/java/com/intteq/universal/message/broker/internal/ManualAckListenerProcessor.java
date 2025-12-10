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
import jakarta.annotation.Nullable;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.*;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer;
import org.springframework.amqp.rabbit.listener.api.ChannelAwareMessageListener;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.context.ApplicationContext;
import org.springframework.core.annotation.AnnotationUtils;
import org.springframework.stereotype.Component;

import java.lang.reflect.Method;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;


/**
 * Discovers all {@link MessagingListener} beans and registers message listeners dynamically
 * for both RabbitMQ and Azure Service Bus using manual-acknowledgment semantics.
 *
 * <p>Features:
 * <ul>
 *     <li>Dynamic discovery of event handlers annotated with {@link EventHandler}</li>
 *     <li>Provider-agnostic MessageContext abstraction</li>
 *     <li>Manual acknowledgment for RabbitMQ and Azure</li>
 *     <li>Per-listener concurrency and prefetch configuration</li>
 *     <li>Micrometer metrics (optional)</li>
 *     <li>Automatic DLQ routing on handler failure</li>
 *     <li>Graceful shutdown on application exit</li>
 * </ul>
 *
 * <p>This processor is initialized once all Spring singletons are created.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class ManualAckListenerProcessor implements SmartInitializingSingleton, DisposableBean {

    private final MessagingProperties properties;
    private final ApplicationContext context;
    private final ObjectMapper objectMapper;

    /** Optional — available only when the application includes Micrometer + Actuator. */
    @Nullable
    private final MeterRegistry meterRegistry;

    /** Active Azure processor clients (key = subscription name). */
    private final Map<String, ServiceBusProcessorClient> azureProcessors = new ConcurrentHashMap<>();

    /** Active RabbitMQ containers (key = queue name). */
    private final Map<String, SimpleMessageListenerContainer> rabbitContainers = new ConcurrentHashMap<>();

    private static final int DEFAULT_RABBIT_PREFETCH = 10;
    private static final int DEFAULT_AZURE_CONCURRENCY = 5;

    // ========================================================================
    //   BEAN DISCOVERY & INITIALIZATION
    // ========================================================================

    /**
     * Discovers all beans annotated with {@link MessagingListener} and registers all
     * {@link EventHandler} methods as active message listeners.
     */
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

            int prefetch = listenerAnn.prefetch();
            int concurrency = listenerAnn.concurrency();

            for (Method method : beanClass.getDeclaredMethods()) {
                if (!method.isAnnotationPresent(EventHandler.class)) continue;

                if (method.getParameterCount() != 2) {
                    log.warn("Skipping {}.{} — invalid signature. Expected (Payload, MessageContext).",
                            beanClass.getSimpleName(), method.getName());
                    continue;
                }

                String eventType = method.getAnnotation(EventHandler.class).value();
                String routingKey = logicalTopic + "." + eventType;

                if ("rabbitmq".equalsIgnoreCase(properties.getProvider())) {
                    registerRabbitListener(resolvedTopic, routingKey, channel, bean, method, prefetch);
                } else {
                    registerAzureListener(resolvedTopic, routingKey, channel, bean, method, concurrency);
                }
            }
        });
    }

    // ========================================================================
    //   SHUTDOWN LOGIC
    // ========================================================================

    /**
     * Gracefully shuts down all listeners.
     */
    @Override
    public void destroy() {
        log.info("Stopping all messaging listeners...");

        azureProcessors.forEach((sub, processor) -> {
            try {
                processor.stop();
                processor.close();
                log.info("Stopped Azure subscription: {}", sub);
            } catch (Exception e) {
                log.warn("Failed stopping Azure processor {}", sub, e);
            }
        });

        rabbitContainers.forEach((queue, container) -> {
            try {
                container.stop();
                log.info("Stopped RabbitMQ listener: {}", queue);
            } catch (Exception e) {
                log.warn("Failed stopping RabbitMQ listener {}", queue, e);
            }
        });
    }

    // ========================================================================
    //   RABBITMQ REGISTRATION
    // ========================================================================

    /**
     * Registers a RabbitMQ listener for the given handler method.
     */
    private void registerRabbitListener(
            String exchange,
            String routingKey,
            String channel,
            Object handler,
            Method method,
            int requestedPrefetch
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

            log.info("RabbitMQ infra OK → queue={}, exchange={}, routingKey={}", queueName, exchange, routingKey);
        } catch (Exception e) {
            log.error("Failed to create RabbitMQ infra for queue={}", queueName, e);
            return;
        }

        rabbitContainers.computeIfAbsent(queueName, q ->
                createRabbitContainer(q, handler, method, requestedPrefetch)
        );
    }

    /**
     * Creates and starts a RabbitMQ listener container.
     */
    private SimpleMessageListenerContainer createRabbitContainer(
            String queueName,
            Object handler,
            Method method,
            int requestedPrefetch
    ) {
        ConnectionFactory connectionFactory = context.getBean(ConnectionFactory.class);

        int finalPrefetch = requestedPrefetch > 0 ? requestedPrefetch : DEFAULT_RABBIT_PREFETCH;

        SimpleMessageListenerContainer container = new SimpleMessageListenerContainer(connectionFactory);
        container.setQueueNames(queueName);
        container.setAcknowledgeMode(AcknowledgeMode.MANUAL);
        container.setPrefetchCount(finalPrefetch);
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

                recordSuccessMetric("queue", queueName, duration);

            } catch (Exception ex) {
                recordFailureMetric("queue", queueName);
                log.error("RabbitMQ handler error → DLQ (queue={})", queueName, ex);

                try {
                    channel.basicNack(tag, false, false); // dead-letter
                } catch (Exception nackErr) {
                    log.warn("Failed to nack message queue={}", queueName, nackErr);
                }
            }
        });

        container.afterPropertiesSet();
        container.start();

        log.info("RabbitMQ listener STARTED → queue={} prefetch={}", queueName, finalPrefetch);
        return container;
    }

    // ========================================================================
    //   AZURE SERVICE BUS REGISTRATION
    // ========================================================================

    /**
     * Registers an Azure Service Bus listener for the given handler.
     */
    private void registerAzureListener(
            String topic,
            String routingKey,
            String channel,
            Object handler,
            Method method,
            int requestedConcurrency
    ) {
        String subscription = channel + "-sub";

        if (azureProcessors.containsKey(subscription)) {
            log.info("Azure subscription already active: {}", subscription);
            return;
        }

        int concurrency = requestedConcurrency > 0 ? requestedConcurrency : DEFAULT_AZURE_CONCURRENCY;

        ServiceBusProcessorClient processor = context.getBean(ServiceBusClientBuilder.class)
                .processor()
                .topicName(topic)
                .subscriptionName(subscription)
                .disableAutoComplete()
                .maxConcurrentCalls(concurrency)
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

                        recordSuccessMetric("subscription", subscription, duration);

                    } catch (Exception handlerErr) {
                        recordFailureMetric("subscription", subscription);
                        log.error("Azure handler exception → DLQ", handlerErr);

                        DeadLetterOptions opts = new DeadLetterOptions()
                                .setDeadLetterReason("processing-exception")
                                .setDeadLetterErrorDescription(
                                        handlerErr.getMessage() != null ? handlerErr.getMessage() : "Handler exception");

                        try {
                            processContext.deadLetter(opts);
                        } catch (Exception dlqErr) {
                            log.warn("Failed to DLQ message subscription={}", subscription, dlqErr);
                        }
                    }
                })
                .processError(err -> log.error("Azure processor error: {}", err.getException().getMessage()))
                .buildProcessorClient();

        processor.start();
        azureProcessors.put(subscription, processor);

        log.info("Azure listener STARTED → topic={}, subscription={}, concurrency={}",
                topic, subscription, concurrency);
    }

    // ========================================================================
    //   METRICS HELPERS
    // ========================================================================

    private void recordSuccessMetric(String type, String id, long durationNs) {
        if (meterRegistry == null) return;

        meterRegistry.timer("umb.consume.latency", type, id)
                .record(durationNs, TimeUnit.NANOSECONDS);

        meterRegistry.counter("umb.consume.success", type, id).increment();
    }

    private void recordFailureMetric(String type, String id) {
        if (meterRegistry == null) return;
        meterRegistry.counter("umb.consume.failure", type, id).increment();
    }
}
