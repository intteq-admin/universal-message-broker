package com.intteq.universal.message.broker.internal;

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
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.ApplicationContext;
import org.springframework.core.annotation.AnnotationUtils;
import org.springframework.stereotype.Component;

import java.lang.reflect.Method;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * RabbitMQ-specific manual-ack listener processor.
 *
 * <p><b>Activation rules:</b></p>
 * <ul>
 *     <li>Loaded ONLY when {@code messaging.provider=rabbitmq}</li>
 *     <li>Requires RabbitMQ classes on the classpath</li>
 *     <li>NEVER loads when Azure is the active provider</li>
 * </ul>
 *
 * <p><b>Responsibilities:</b></p>
 * <ul>
 *     <li>Discovers {@link MessagingListener} beans</li>
 *     <li>Registers {@link EventHandler} methods as RabbitMQ consumers</li>
 *     <li>Uses MANUAL acknowledgment semantics</li>
 *     <li>Handles DLQ routing on handler failure</li>
 *     <li>Records Micrometer metrics when available</li>
 * </ul>
 *
 * <p><b>Critical architectural guarantee:</b></p>
 * <pre>
 * This class is the ONLY place where Rabbit listener containers are created.
 * If this bean is not loaded, Rabbit health checks will NEVER activate.
 * </pre>
 */
@Slf4j
@Component
@RequiredArgsConstructor
@ConditionalOnProperty(prefix = "messaging", name = "provider", havingValue = "rabbitmq")
@ConditionalOnClass(SimpleMessageListenerContainer.class)
public class RabbitManualAckListenerProcessor
        implements SmartInitializingSingleton, DisposableBean {

    private final MessagingProperties properties;
    private final ApplicationContext context;
    private final ObjectMapper objectMapper;

    /** Optional Micrometer registry (null-safe). */
    @Nullable
    private final MeterRegistry meterRegistry;

    /** Active listener containers keyed by queue name. */
    private final Map<String, SimpleMessageListenerContainer> containers =
            new ConcurrentHashMap<>();

    private static final int DEFAULT_PREFETCH = 10;

    // =====================================================================
    // INITIALIZATION
    // =====================================================================

    /**
     * Discovers all {@link MessagingListener} beans and registers their
     * {@link EventHandler} methods as RabbitMQ consumers.
     *
     * <p>This method is invoked once all singletons are fully initialized,
     * guaranteeing that infrastructure beans (ConnectionFactory, AmqpAdmin)
     * are already available.</p>
     */
    @Override
    public void afterSingletonsInstantiated() {
        log.info("Initializing RabbitMQ manual-ack listeners...");

        context.getBeansWithAnnotation(MessagingListener.class)
                .values()
                .forEach(this::registerListenerBean);
    }

    // =====================================================================
    // BEAN DISCOVERY
    // =====================================================================

    private void registerListenerBean(Object bean) {
        Class<?> clazz = bean.getClass();

        MessagingListener listener =
                AnnotationUtils.findAnnotation(clazz, MessagingListener.class);

        if (listener == null) {
            return;
        }

        String logicalTopic = listener.topic();
        String exchange =
                properties.getTopics().getOrDefault(logicalTopic, logicalTopic);

        for (Method method : clazz.getDeclaredMethods()) {

            EventHandler handler = method.getAnnotation(EventHandler.class);
            if (handler == null) {
                continue;
            }

            validateHandlerSignature(clazz, method);

            String routingKey = logicalTopic + "." + handler.value();
            String queueName = listener.channel() + ".queue";
            int prefetch = listener.prefetch();

            registerQueueAndContainer(
                    exchange,
                    routingKey,
                    queueName,
                    bean,
                    method,
                    prefetch
            );
        }
    }

    // =====================================================================
    // RABBIT REGISTRATION
    // =====================================================================

    /**
     * Declares RabbitMQ infrastructure and starts a listener container.
     */
    private void registerQueueAndContainer(
            String exchange,
            String routingKey,
            String queueName,
            Object handler,
            Method method,
            int requestedPrefetch
    ) {

        AmqpAdmin admin = context.getBean(AmqpAdmin.class);

        // Declare infrastructure idempotently
        Queue queue = QueueBuilder.durable(queueName).build();
        TopicExchange topicExchange = new TopicExchange(exchange, true, false);
        Binding binding =
                BindingBuilder.bind(queue).to(topicExchange).with(routingKey);

        admin.declareQueue(queue);
        admin.declareExchange(topicExchange);
        admin.declareBinding(binding);

        containers.computeIfAbsent(queueName,
                q -> createAndStartContainer(
                        q,
                        handler,
                        method,
                        requestedPrefetch
                )
        );
    }

    /**
     * Creates and starts a {@link SimpleMessageListenerContainer}
     * configured for MANUAL acknowledgment.
     */
    private SimpleMessageListenerContainer createAndStartContainer(
            String queueName,
            Object handler,
            Method method,
            int requestedPrefetch
    ) {

        ConnectionFactory connectionFactory =
                context.getBean(ConnectionFactory.class);

        int prefetch =
                requestedPrefetch > 0 ? requestedPrefetch : DEFAULT_PREFETCH;

        SimpleMessageListenerContainer container =
                new SimpleMessageListenerContainer(connectionFactory);

        container.setQueueNames(queueName);
        container.setAcknowledgeMode(AcknowledgeMode.MANUAL);
        container.setPrefetchCount(prefetch);
        container.setMissingQueuesFatal(false);
        container.setRecoveryInterval(3000);

        container.setMessageListener(
                (ChannelAwareMessageListener) (message, channel) -> {

                    long tag =
                            message.getMessageProperties().getDeliveryTag();

                    try {
                        Object payload =
                                objectMapper.readValue(
                                        message.getBody(),
                                        method.getParameterTypes()[0]
                                );

                        MessageContext mc =
                                MessageContext.forRabbitMQ(channel, tag);

                        long start = System.nanoTime();
                        method.invoke(handler, payload, mc);
                        long duration = System.nanoTime() - start;

                        recordSuccess(queueName, duration);

                    } catch (Exception ex) {
                        recordFailure(queueName);
                        log.error(
                                "RabbitMQ handler failed → dead-lettering (queue={})",
                                queueName,
                                ex
                        );

                        channel.basicNack(tag, false, false);
                    }
                }
        );

        container.afterPropertiesSet();
        container.start();

        log.info(
                "RabbitMQ listener started → queue={} prefetch={}",
                queueName,
                prefetch
        );

        return container;
    }

    // =====================================================================
    // VALIDATION
    // =====================================================================

    private void validateHandlerSignature(Class<?> clazz, Method method) {
        if (method.getParameterCount() != 2) {
            throw new IllegalStateException(
                    "Invalid @EventHandler signature: "
                            + clazz.getName() + "#" + method.getName()
                            + " — expected (Payload, MessageContext)"
            );
        }
    }

    // =====================================================================
    // METRICS
    // =====================================================================

    private void recordSuccess(String queue, long durationNs) {
        if (meterRegistry == null) return;

        meterRegistry.timer(
                        "umb.rabbit.consume.latency",
                        "queue",
                        queue
                )
                .record(durationNs, TimeUnit.NANOSECONDS);

        meterRegistry.counter(
                        "umb.rabbit.consume.success",
                        "queue",
                        queue
                )
                .increment();
    }

    private void recordFailure(String queue) {
        if (meterRegistry == null) return;

        meterRegistry.counter(
                        "umb.rabbit.consume.failure",
                        "queue",
                        queue
                )
                .increment();
    }

    // =====================================================================
    // SHUTDOWN
    // =====================================================================

    /**
     * Gracefully stops all RabbitMQ listener containers.
     */
    @Override
    public void destroy() {
        log.info("Stopping RabbitMQ listener containers...");

        containers.forEach((queue, container) -> {
            try {
                container.stop();
                log.info("Stopped RabbitMQ listener → queue={}", queue);
            } catch (Exception e) {
                log.warn(
                        "Failed to stop RabbitMQ listener → queue={}",
                        queue,
                        e
                );
            }
        });

        containers.clear();
    }
}
