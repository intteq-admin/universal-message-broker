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
 * Azure Service Bus manual-ack listener processor.
 *
 * <p><b>Activation rules:</b></p>
 * <ul>
 *   <li>Loaded ONLY when {@code messaging.provider=azure}</li>
 *   <li>Requires Azure Service Bus SDK on classpath</li>
 *   <li>NEVER initializes RabbitMQ components</li>
 * </ul>
 *
 * <p><b>Responsibilities:</b></p>
 * <ul>
 *   <li>Discovers {@link MessagingListener} beans</li>
 *   <li>Registers {@link EventHandler} methods as Azure processors</li>
 *   <li>Uses MANUAL settlement semantics</li>
 *   <li>Routes failures to DLQ</li>
 *   <li>Records Micrometer metrics when available</li>
 * </ul>
 *
 * <p><b>Critical guarantee:</b></p>
 * <pre>
 * This class is the ONLY place where Azure processors are created.
 * If this bean is not loaded, Azure Service Bus is never touched.
 * </pre>
 */
@Slf4j
@Component
@RequiredArgsConstructor
@ConditionalOnProperty(prefix = "messaging", name = "provider", havingValue = "azure")
@ConditionalOnClass(ServiceBusProcessorClient.class)
public class AzureManualAckListenerProcessor
        implements SmartInitializingSingleton, DisposableBean {

    private final MessagingProperties properties;
    private final ApplicationContext context;
    private final ObjectMapper objectMapper;

    /** Optional Micrometer registry (null-safe). */
    @Nullable
    private final MeterRegistry meterRegistry;

    /** Active Azure processors keyed by subscription name. */
    private final Map<String, ServiceBusProcessorClient> processors =
            new ConcurrentHashMap<>();

    private static final int DEFAULT_CONCURRENCY = 5;

    // =====================================================================
    // INITIALIZATION
    // =====================================================================

    /**
     * Discovers all {@link MessagingListener} beans and registers their
     * {@link EventHandler} methods as Azure Service Bus processors.
     *
     * <p>Executed after all singletons are created to ensure
     * {@link ServiceBusClientBuilder} is available.</p>
     */
    @Override
    public void afterSingletonsInstantiated() {
        log.info("Initializing Azure Service Bus manual-ack listeners...");

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
        String topicName =
                properties.getTopics().getOrDefault(logicalTopic, logicalTopic);

        int concurrency =
                listener.concurrency() > 0
                        ? listener.concurrency()
                        : DEFAULT_CONCURRENCY;

        for (Method method : clazz.getDeclaredMethods()) {

            EventHandler handler = method.getAnnotation(EventHandler.class);
            if (handler == null) {
                continue;
            }

            validateHandlerSignature(clazz, method);

            String subscription = listener.channel() + "-sub";

            processors.computeIfAbsent(
                    subscription,
                    s -> createAndStartProcessor(
                            topicName,
                            subscription,
                            bean,
                            method,
                            concurrency
                    )
            );
        }
    }

    // =====================================================================
    // AZURE REGISTRATION
    // =====================================================================

    /**
     * Creates and starts an Azure {@link ServiceBusProcessorClient}
     * configured for MANUAL settlement.
     */
    private ServiceBusProcessorClient createAndStartProcessor(
            String topic,
            String subscription,
            Object handler,
            Method method,
            int concurrency
    ) {

        ServiceBusClientBuilder builder =
                context.getBean(ServiceBusClientBuilder.class);

        ServiceBusProcessorClient processor =
                builder.processor()
                        .topicName(topic)
                        .subscriptionName(subscription)
                        .disableAutoComplete()
                        .maxConcurrentCalls(concurrency)
                        .processMessage(ctx -> handleMessage(
                                ctx.getMessage(),
                                ctx,
                                handler,
                                method,
                                subscription
                        ))
                        .processError(err ->
                                log.error(
                                        "Azure processor error (subscription={})",
                                        subscription,
                                        err.getException()
                                ))
                        .buildProcessorClient();

        processor.start();

        log.info(
                "Azure listener started → topic={} subscription={} concurrency={}",
                topic,
                subscription,
                concurrency
        );

        return processor;
    }

    // =====================================================================
    // MESSAGE HANDLING
    // =====================================================================

    private void handleMessage(
            ServiceBusReceivedMessage message,
            com.azure.messaging.servicebus.ServiceBusReceivedMessageContext ctx,
            Object handler,
            Method method,
            String subscription
    ) {

        try {
            Object payload =
                    objectMapper.readValue(
                            message.getBody().toBytes(),
                            method.getParameterTypes()[0]
                    );

            MessageContext mc =
                    MessageContext.forAzureProcessor(ctx);

            long start = System.nanoTime();
            method.invoke(handler, payload, mc);
            long duration = System.nanoTime() - start;

            recordSuccess(subscription, duration);

        } catch (Exception ex) {
            recordFailure(subscription);
            log.error(
                    "Azure handler failed → dead-lettering (subscription={})",
                    subscription,
                    ex
            );

            DeadLetterOptions opts =
                    new DeadLetterOptions()
                            .setDeadLetterReason("handler-exception")
                            .setDeadLetterErrorDescription(
                                    ex.getMessage() != null
                                            ? ex.getMessage()
                                            : "Handler execution failed"
                            );

            ctx.deadLetter(opts);
        }
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

    private void recordSuccess(String subscription, long durationNs) {
        if (meterRegistry == null) return;

        meterRegistry.timer(
                        "umb.azure.consume.latency",
                        "subscription",
                        subscription
                )
                .record(durationNs, TimeUnit.NANOSECONDS);

        meterRegistry.counter(
                        "umb.azure.consume.success",
                        "subscription",
                        subscription
                )
                .increment();
    }

    private void recordFailure(String subscription) {
        if (meterRegistry == null) return;

        meterRegistry.counter(
                        "umb.azure.consume.failure",
                        "subscription",
                        subscription
                )
                .increment();
    }

    // =====================================================================
    // SHUTDOWN
    // =====================================================================

    /**
     * Gracefully stops all Azure processors.
     */
    @Override
    public void destroy() {
        log.info("Stopping Azure Service Bus processors...");

        processors.forEach((sub, processor) -> {
            try {
                processor.stop();
                processor.close();
                log.info("Stopped Azure processor → subscription={}", sub);
            } catch (Exception e) {
                log.warn(
                        "Failed to stop Azure processor → subscription={}",
                        sub,
                        e
                );
            }
        });

        processors.clear();
    }
}
