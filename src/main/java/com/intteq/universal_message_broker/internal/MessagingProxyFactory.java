package com.intteq.universal_message_broker.internal;

import com.azure.messaging.servicebus.ServiceBusClientBuilder;
import com.azure.messaging.servicebus.ServiceBusMessage;
import com.azure.messaging.servicebus.ServiceBusSenderClient;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.intteq.universal_message_broker.MessagingProperties;
import com.intteq.universal_message_broker.annotation.MessagingEvent;
import com.intteq.universal_message_broker.annotation.MessagingTopic;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.context.ApplicationContext;

import java.lang.reflect.Proxy;
import java.util.concurrent.ConcurrentHashMap;

@RequiredArgsConstructor
@Slf4j
public class MessagingProxyFactory implements DisposableBean {

    private final MessagingProperties props;
    private final ApplicationContext ctx;

    private static final ObjectMapper MAPPER = new ObjectMapper();

    // Cache of Azure senders by topic
    private final ConcurrentHashMap<String, ServiceBusSenderClient> senderCache = new ConcurrentHashMap<>();

    @SuppressWarnings("unchecked")
    public <T> T createPublisher(Class<T> interfaceType) {

        if (!interfaceType.isAnnotationPresent(MessagingTopic.class)) {
            throw new IllegalArgumentException(
                    "Interface must be annotated with @MessagingTopic"
            );
        }

        MessagingTopic topicAnn = interfaceType.getAnnotation(MessagingTopic.class);
        String logicalTopic = topicAnn.value();
        String physicalTopic = props.getTopics().getOrDefault(logicalTopic, logicalTopic);

        boolean azure = "azure".equalsIgnoreCase(props.getProvider());

        RabbitTemplate rabbitTemplate = azure ? null : ctx.getBean(RabbitTemplate.class);

        return (T) Proxy.newProxyInstance(
                interfaceType.getClassLoader(),
                new Class<?>[]{interfaceType},
                (proxy, method, args) -> {

                    // 1. Handle Object methods safely
                    switch (method.getName()) {
                        case "equals":
                            return proxy == (args != null && args.length > 0 ? args[0] : null);

                        case "hashCode":
                            return System.identityHashCode(proxy);

                        case "toString":
                            return "MessagingProxy[" + interfaceType.getName() + "]";

                        default:
                            // Continue to messaging logic
                    }

                    // 2. Validate publisher method signature
                    if (args == null || args.length != 1) {
                        throw new IllegalArgumentException(
                                "Publisher method must have exactly one parameter: " + method.getName()
                        );
                    }

                    Object payload = args[0];

                    // 3. Resolve event type
                    String eventType = method.isAnnotationPresent(MessagingEvent.class)
                            ? method.getAnnotation(MessagingEvent.class).value()
                            : method.getName();

                    String routingKey = logicalTopic + "." + eventType;

                    // 4. Azure publish path
                    if (azure) {
                        ServiceBusSenderClient sender = getSender(physicalTopic);

                        ServiceBusMessage msg =
                                new ServiceBusMessage(MAPPER.writeValueAsBytes(payload));

                        msg.getApplicationProperties().put("eventType", eventType);
                        msg.setSubject(eventType);

                        sender.sendMessage(msg);
                        return null;
                    }

                    // 5. RabbitMQ publish path
                    rabbitTemplate.convertAndSend(
                            physicalTopic,
                            routingKey,
                            payload
                    );

                    return null;
                }
        );
    }

    // ========== Azure sender cache utilities ==========

    private ServiceBusSenderClient getSender(String topicName) {
        return senderCache.computeIfAbsent(topicName, topic -> {
            log.info("Creating Azure Service Bus sender for topic {}", topic);
            ServiceBusClientBuilder builder = ctx.getBean(ServiceBusClientBuilder.class);
            return builder.sender().topicName(topic).buildClient();
        });
    }

    // ========== Graceful shutdown: close Azure senders ==========
    @Override
    public void destroy() {
        senderCache.forEach((topic, sender) -> {
            try {
                log.info("Closing Azure Service Bus sender for topic {}", topic);
                sender.close();
            } catch (Exception e) {
                log.warn("Error closing sender for topic {}", topic, e);
            }
        });
        senderCache.clear();
    }
}