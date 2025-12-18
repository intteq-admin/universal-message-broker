package com.intteq.universal.message.broker.internal;

import com.azure.messaging.servicebus.ServiceBusClientBuilder;
import com.azure.messaging.servicebus.ServiceBusMessage;
import com.azure.messaging.servicebus.ServiceBusSenderClient;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.intteq.universal.message.broker.MessagePublisherFactory;
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
 * Factory that creates dynamic proxy-based message publishers for both Azure Service Bus
 * and RabbitMQ. Each method in a publisher interface corresponds to a single message event.
 *
 * <p>Supports:
 * <ul>
 *     <li>Dynamic proxy creation</li>
 *     <li>Retry with exponential backoff</li>
 *     <li>Azure sender caching</li>
 *     <li>Micrometer metrics</li>
 *     <li>Graceful shutdown of Azure senders</li>
 * </ul>
 */
@Slf4j
@RequiredArgsConstructor
public class MessagingProxyFactory implements MessagePublisherFactory, DisposableBean {

    private final MessagingProperties properties;
    private final ApplicationContext ctx;
    private final MeterRegistry meterRegistry;
    private final ObjectMapper objectMapper;

    /** Thread-safe Azure sender cache */
    private final ConcurrentHashMap<String, ServiceBusSenderClient> senderCache = new ConcurrentHashMap<>();

    private static final int MAX_RETRIES = 3;
    private static final Duration RETRY_BACKOFF = Duration.ofMillis(200);

    // ========================================================================
    //   Publisher Creation
    // ========================================================================

    @SuppressWarnings("unchecked")
    @Override
    public <T> T createPublisher(Class<T> interfaceType) {

        validatePublisherInterface(interfaceType);

        MessagingTopic topicAnn = interfaceType.getAnnotation(MessagingTopic.class);
        String logicalTopic = topicAnn.value();
        String physicalTopic = properties.getTopics().getOrDefault(logicalTopic, logicalTopic);

        boolean useAzure = "azure".equalsIgnoreCase(properties.getProvider());

        RabbitTemplate rabbitTemplate = null;
        if (!useAzure) {
            try {
                rabbitTemplate = ctx.getBean(RabbitTemplate.class);
            } catch (Exception ex) {
                throw new IllegalStateException(
                        "RabbitTemplate bean not found while provider=rabbitmq. Ensure configuration is correct.",
                        ex
                );
            }
        }

        // FIX: Make variables effectively final for lambda
        final RabbitTemplate finalRabbit = rabbitTemplate;
        final boolean finalUseAzure = useAzure;
        final String finalLogicalTopic = logicalTopic;
        final String finalPhysicalTopic = physicalTopic;

        return (T) Proxy.newProxyInstance(
                interfaceType.getClassLoader(),
                new Class[]{interfaceType},
                (proxy, method, args) -> {

                    // Handle toString(), equals(), hashCode()
                    if (isObjectMethod(method)) {
                        return handleJavaObjectMethods(proxy, method, args);
                    }

                    validatePublisherMethod(method, args);

                    Object payload = args[0];
                    String eventType = resolveEventType(method);
                    String routingKey = finalLogicalTopic + "." + eventType;

                    meterRegistry.counter("umb.publish.attempt", "topic", finalLogicalTopic).increment();

                    try {
                        if (finalUseAzure) {
                            publishAzure(finalPhysicalTopic, eventType, payload);
                        } else {
                            publishRabbitMQ(finalRabbit, finalPhysicalTopic, routingKey, payload);
                        }

                        meterRegistry.counter("umb.publish.success", "topic", finalLogicalTopic).increment();
                        return null;

                    } catch (Exception ex) {
                        meterRegistry.counter("umb.publish.failure", "topic", finalLogicalTopic).increment();
                        log.error("Failed to publish message topic={} event={}", finalLogicalTopic, eventType, ex);
                        throw new MessagingPublishException("Failed to publish message", ex);
                    }
                }
        );
    }

    // ========================================================================
    //   Broker Implementations
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

            } catch (com.fasterxml.jackson.core.JsonProcessingException e) {
                throw new IllegalArgumentException(
                        "Failed to serialize message payload to JSON: " + payload.getClass().getName(), e);
            } catch (Exception e) {
                throw new RuntimeException(
                        "Failed to send message to Azure Service Bus topic: " + topic, e);
            }
        });
    }

    private void publishRabbitMQ(RabbitTemplate template, String topic, String routingKey, Object payload) {
        retry(() -> template.convertAndSend(topic, routingKey, payload));
    }

    // ========================================================================
    //   Validation
    // ========================================================================

    private <T> void validatePublisherInterface(Class<T> interfaceType) {
        Assert.isTrue(interfaceType.isInterface(), "Publisher must be an interface");
        Assert.isTrue(interfaceType.isAnnotationPresent(MessagingTopic.class),
                "Publisher interface must have @MessagingTopic");
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
    //   Azure Sender Management
    // ========================================================================

    private ServiceBusSenderClient getOrCreateSender(String topicName) {
        return senderCache.computeIfAbsent(topicName, topic -> {
            log.info("Creating Azure Service Bus sender for topic {}", topic);
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
                log.warn("Failed to close Azure sender for topic {}", topic, e);
            }
        });
        senderCache.clear();
    }

    // ========================================================================
    //   Retry Logic
    // ========================================================================

    private void retry(Runnable action) {
        int attempt = 1;

        while (true) {
            try {
                action.run();
                return;

            } catch (Exception ex) {
                if (isNonTransient(ex)) {
                    throw ex;
                }

                if (attempt >= MAX_RETRIES) {
                    throw ex;
                }

                log.warn("Publish attempt {} failed. Retrying in {}ms",
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

    private boolean isNonTransient(Exception ex) {
        return ex instanceof IllegalArgumentException
                || ex instanceof com.fasterxml.jackson.core.JsonProcessingException
                || (ex.getCause() instanceof java.nio.channels.UnresolvedAddressException);
    }
}
