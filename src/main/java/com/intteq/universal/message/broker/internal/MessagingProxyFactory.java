package com.intteq.universal.message.broker.internal;

import com.azure.messaging.servicebus.ServiceBusClientBuilder;
import com.azure.messaging.servicebus.ServiceBusMessage;
import com.azure.messaging.servicebus.ServiceBusSenderClient;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.intteq.universal.message.broker.MessagingProperties;
import com.intteq.universal.message.broker.annotation.MessagingEvent;
import com.intteq.universal.message.broker.annotation.MessagingTopic;
import com.intteq.universal.message.broker.exception.MessagingPublishException;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.context.ApplicationContext;
import org.springframework.util.Assert;

import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.time.Duration;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Factory responsible for creating dynamic proxy-based message publishers.
 * This class abstracts broker-specific publishing logic for providers such as Azure Service Bus
 * and RabbitMQ. The proxies allow users to define interface-based publishers, where each method
 * corresponds to a message event.
 *
 * <p>Key Capabilities:
 * <ul>
 *     <li>Dynamic proxy creation for publisher interfaces</li>
 *     <li>Thread-safe Azure sender caching</li>
 *     <li>Retry policies for transient failures</li>
 *     <li>Micrometer metrics for publish operations</li>
 *     <li>Structured logging with correlation metadata</li>
 *     <li>Graceful shutdown of all Azure senders</li>
 * </ul>
 *
 * <p>This class is part of the internal infrastructure layer and is not intended
 * for direct use by application developers.
 */
@Slf4j
@RequiredArgsConstructor
public class MessagingProxyFactory implements DisposableBean {

    private final MessagingProperties properties;
    private final ApplicationContext ctx;
    private final MeterRegistry meterRegistry;
    private final ObjectMapper objectMapper;

    /** Thread-safe cache of Azure Service Bus senders */
    private final ConcurrentHashMap<String, ServiceBusSenderClient> senderCache = new ConcurrentHashMap<>();

    /** Default retry configuration (can be externalized later) */
    private static final int MAX_RETRIES = 3;
    private static final Duration RETRY_BACKOFF = Duration.ofMillis(200);

    @SuppressWarnings("unchecked")
    public <T> T createPublisher(Class<T> interfaceType) {

        validatePublisherInterface(interfaceType);

        MessagingTopic topicAnn = interfaceType.getAnnotation(MessagingTopic.class);
        String logicalTopic = topicAnn.value();

        String physicalTopic = properties.getTopics()
                .getOrDefault(logicalTopic, logicalTopic);

        boolean useAzure = "azure".equalsIgnoreCase(properties.getProvider());

        RabbitTemplate rabbitTemplate = useAzure ? null : ctx.getBean(RabbitTemplate.class);

        return (T) Proxy.newProxyInstance(
                interfaceType.getClassLoader(),
                new Class[]{interfaceType},
                (proxy, method, args) -> {

                    if (isObjectMethod(method)) {
                        return handleJavaObjectMethods(proxy, method, args);
                    }

                    validatePublisherMethod(method, args);

                    Object payload = args[0];
                    String eventType = resolveEventType(method);
                    String routingKey = logicalTopic + "." + eventType;

                    meterRegistry.counter("umb.publish.attempt", "topic", logicalTopic).increment();

                    try {
                        if (useAzure) {
                            publishAzure(physicalTopic, eventType, payload);
                        } else {
                            publishRabbitMQ(rabbitTemplate, physicalTopic, routingKey, payload);
                        }

                        meterRegistry.counter("umb.publish.success", "topic", logicalTopic).increment();
                        return null;

                    } catch (Exception ex) {
                        meterRegistry.counter("umb.publish.failure", "topic", logicalTopic).increment();
                        log.error("Failed to publish message to topic={} eventType={}", logicalTopic, eventType, ex);
                        throw new MessagingPublishException("Failed to publish message", ex);
                    }
                }
        );
    }

    // ========================================================================
    //   Broker Publishing Implementations
    // ========================================================================

    private void publishAzure(String topic, String eventType, Object payload) {
        retry(() -> {
            try {
                ServiceBusSenderClient sender = getOrCreateSender(topic);

                byte[] body = objectMapper.writeValueAsBytes(payload);

                ServiceBusMessage message = new ServiceBusMessage(body);
                message.getApplicationProperties().put("eventType", eventType);
                message.setSubject(eventType);

                sender.sendMessage(message);
            } catch (Exception e) {
                throw new RuntimeException("Failed to serialize or send message", e);
            }
        });
    }
    private void publishRabbitMQ(RabbitTemplate template, String topic, String routingKey, Object payload) {
        retry(() -> template.convertAndSend(topic, routingKey, payload));
    }

    // ========================================================================
    //   Validation Helpers
    // ========================================================================

    private <T> void validatePublisherInterface(Class<T> interfaceType) {
        Assert.isTrue(interfaceType.isInterface(),
                "Publisher must be an interface");
        Assert.isTrue(interfaceType.isAnnotationPresent(MessagingTopic.class),
                "Interface must be annotated with @MessagingTopic");
    }

    private void validatePublisherMethod(Method method, Object[] args) {
        if (args == null || args.length != 1) {
            throw new IllegalArgumentException(
                    "Publisher method must have exactly one parameter: " + method.getName()
            );
        }
    }

    private String resolveEventType(Method method) {
        return method.isAnnotationPresent(MessagingEvent.class)
                ? method.getAnnotation(MessagingEvent.class).value()
                : method.getName();
    }

    private boolean isObjectMethod(Method method) {
        return method.getDeclaringClass() == Object.class;
    }

    private Object handleJavaObjectMethods(Object proxy, Method method, Object[] args) {
        return switch (method.getName()) {
            case "equals" -> proxy == (args != null && args.length > 0 ? args[0] : null);
            case "hashCode" -> System.identityHashCode(proxy);
            case "toString" -> "MessagingProxy[" + proxy.getClass().getName() + "]";
            default -> null;
        };
    }

    // ========================================================================
    //   Azure Sender Cache / Resource Management
    // ========================================================================

    private ServiceBusSenderClient getOrCreateSender(String topicName) {
        return senderCache.computeIfAbsent(topicName, topic -> {
            log.info("Creating Azure Service Bus sender for topic={}", topic);
            ServiceBusClientBuilder builder = ctx.getBean(ServiceBusClientBuilder.class);
            return builder.sender().topicName(topic).buildClient();
        });
    }

    @Override
    public void destroy() {
        senderCache.forEach((topic, sender) -> {
            try {
                log.info("Closing Azure sender for topic {}", topic);
                sender.close();
            } catch (Exception e) {
                log.warn("Error closing sender for topic {}", topic, e);
            }
        });
        senderCache.clear();
    }

    // ========================================================================
    //   Retry Logic (Lightweight)
    // ========================================================================

    private void retry(Runnable action) {
        int attempt = 1;

        while (true) {
            try {
                action.run();
                return;

            } catch (Exception ex) {
                if (attempt >= MAX_RETRIES) {
                    throw ex;
                }

                log.warn("Publish attempt {} failed. Retrying in {} ms",
                        attempt, RETRY_BACKOFF.toMillis(), ex);

                sleep(RETRY_BACKOFF);
                attempt++;
            }
        }
    }

    private void sleep(Duration duration) {
        try {
            Thread.sleep(duration.toMillis());
        } catch (InterruptedException ignored) {
            Thread.currentThread().interrupt();
        }
    }
}
