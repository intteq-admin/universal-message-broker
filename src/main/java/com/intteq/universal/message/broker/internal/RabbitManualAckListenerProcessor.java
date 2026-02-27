package com.intteq.universal.message.broker.internal;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.intteq.universal.message.broker.MessageContext;
import com.intteq.universal.message.broker.MessagingProperties;
import com.intteq.universal.message.broker.annotation.EventHandler;
import com.intteq.universal.message.broker.annotation.MessagingListener;
import com.intteq.universal.message.broker.rabbitmq.RabbitMQProperties;
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
import org.springframework.context.EmbeddedValueResolverAware;
import org.springframework.core.annotation.AnnotationUtils;
import org.springframework.stereotype.Component;
import org.springframework.util.ReflectionUtils;
import org.springframework.util.StringValueResolver;

import java.lang.reflect.InvocationTargetException;
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
        implements SmartInitializingSingleton, DisposableBean, EmbeddedValueResolverAware {

    private final MessagingProperties properties;
    private final RabbitMQProperties rabbitProperties;
    private final ApplicationContext context;
    private final ObjectMapper objectMapper;

    /** Optional Micrometer registry (null-safe). */
    @Nullable
    private final MeterRegistry meterRegistry;

    /** Active listener containers keyed by queue name. */
    private final Map<String, SimpleMessageListenerContainer> containers =
            new ConcurrentHashMap<>();

    /** Handlers registry keyed by queue name and then by routing key. */
    private final Map<String, Map<String, HandlerMethod>> handlersByQueue =
            new ConcurrentHashMap<>();

    private StringValueResolver resolver;

    @Override
    public void setEmbeddedValueResolver(StringValueResolver resolver) {
        this.resolver = resolver;
    }

    private String resolve(String value) {
        return (resolver != null && value != null) ? resolver.resolveStringValue(value) : value;
    }

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

        String logicalTopic = resolve(listener.topic());
        String exchange =
                properties.getTopics().getOrDefault(logicalTopic, logicalTopic);

        ReflectionUtils.doWithMethods(clazz, method -> {
            EventHandler handler = method.getAnnotation(EventHandler.class);
            if (handler == null) {
                return;
            }

            validateHandlerSignature(clazz, method);

            String routingKey = logicalTopic + "." + resolve(handler.value());

            String queueName = rabbitProperties.getQueues().values().stream()
                    .filter(cfg -> routingKey.equals(cfg.getRoutingKey()))
                    .map(RabbitMQProperties.QueueConfig::getName)
                    .findFirst()
                    .orElse(logicalTopic + ".auto.queue");

            int prefetch = listener.prefetch();

            registerQueueAndContainer(
                    exchange,
                    routingKey,
                    queueName,
                    bean,
                    method,
                    prefetch
            );
        });
    }


    // =====================================================================
    // RABBIT REGISTRATION
    // =====================================================================

    /**
     * Registers a handler for a specific queue and routing key.
     * The infrastructure (queue, exchange, binding) is expected to be
     * declared by RabbitMQInfrastructureAutoConfig.
     */
    private void registerQueueAndContainer(
            String exchange,
            String routingKey,
            String queueName,
            Object handler,
            Method method,
            int requestedPrefetch
    ) {
        // Register handler for the specific routing key on this queue
        handlersByQueue.computeIfAbsent(queueName, q -> new ConcurrentHashMap<>())
                .put(routingKey, new HandlerMethod(handler, method));

        containers.computeIfAbsent(queueName,
                q -> createAndStartContainer(
                        q,
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
                    MessageContext mc = null;

                    try {
                        String receivedRoutingKey = message.getMessageProperties().getReceivedRoutingKey();
                        Map<String, HandlerMethod> handlers = handlersByQueue.get(queueName);

                        if (handlers == null || !handlers.containsKey(receivedRoutingKey)) {
                            log.warn(
                                    "No handler found for routing key {} on queue {}",
                                    receivedRoutingKey,
                                    queueName
                            );
                            channel.basicAck(tag, false);
                            return;
                        }

                        HandlerMethod handlerMethod = handlers.get(receivedRoutingKey);
                        Object handler = handlerMethod.handler();
                        Method method = handlerMethod.method();

                        Object payload =
                                objectMapper.readValue(
                                        message.getBody(),
                                        method.getParameterTypes()[0]
                                );
                        mc = MessageContext.forRabbitMQ(channel, tag);
                        long start = System.nanoTime();
                        method.invoke(handler, payload, mc);
                        long duration = System.nanoTime() - start;
                        if (!mc.isSettled()) {
                            channel.basicAck(tag, false);
                        }
                        recordSuccess(queueName, duration);
                    } catch (Exception ex) {
                        Throwable root = unwrapInvocationTargetException(ex);
                        recordFailure(queueName);
                        log.error(
                                "RabbitMQ handler failed → dead-lettering (queue={})",
                                queueName,
                                root
                        );
                        // Handler may have already settled (ack/nack/dead-letter).
                        // Avoid double-settlement because RabbitMQ will close channel on duplicate ack/nack.
                        if (mc == null || !mc.isSettled()) {
                            channel.basicNack(tag, false, false);
                        }
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
        Class<?>[] parameterTypes = method.getParameterTypes();
        if (parameterTypes.length != 2
                || !MessageContext.class.isAssignableFrom(parameterTypes[1])) {
            throw new IllegalStateException(
                    "Invalid @EventHandler signature: "
                            + clazz.getName() + "#" + method.getName()
                            + " — expected (Payload, MessageContext)"
            );
        }
    }

    private Throwable unwrapInvocationTargetException(Throwable throwable) {
        if (throwable instanceof InvocationTargetException ite && ite.getTargetException() != null) {
            return ite.getTargetException();
        }
        return throwable;
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

    /** Helper to store handler instance and method. */
    private record HandlerMethod(Object handler, Method method) {}

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
