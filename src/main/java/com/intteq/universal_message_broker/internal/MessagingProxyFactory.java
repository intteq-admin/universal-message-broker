package com.intteq.universal_message_broker.internal;

import com.azure.messaging.servicebus.ServiceBusClientBuilder;
import com.azure.messaging.servicebus.ServiceBusMessage;
import com.azure.messaging.servicebus.ServiceBusSenderClient;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.intteq.universal_message_broker.MessagingProperties;
import com.intteq.universal_message_broker.annotation.MessagingEvent;
import com.intteq.universal_message_broker.annotation.MessagingTopic;
import lombok.RequiredArgsConstructor;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.context.ApplicationContext;

import java.lang.reflect.Proxy;
@RequiredArgsConstructor
public class MessagingProxyFactory {

    private final MessagingProperties props;
    private final ApplicationContext ctx;
    private static final ObjectMapper MAPPER = new ObjectMapper();

    @SuppressWarnings("unchecked")
    public <T> T createPublisher(Class<T> interfaceType) {
        if (!interfaceType.isAnnotationPresent(MessagingTopic.class)) {
            throw new IllegalArgumentException("Interface must be annotated with @MessagingTopic");
        }

        MessagingTopic topicAnn = interfaceType.getAnnotation(MessagingTopic.class);
        String logicalTopic = topicAnn.value();
        String physicalTopic = props.getTopics().getOrDefault(logicalTopic, logicalTopic);

        return (T) Proxy.newProxyInstance(
                interfaceType.getClassLoader(),
                new Class<?>[]{interfaceType},
                (proxy, method, args) -> {
                    if (args == null || args.length == 0) {
                        throw new IllegalArgumentException("Publisher method must have exactly one parameter");
                    }

                    String eventType = method.getName(); // fallback
                    if (method.isAnnotationPresent(MessagingEvent.class)) {
                        eventType = method.getAnnotation(MessagingEvent.class).value();
                    }

                    String routingKey = logicalTopic + "." + eventType;
                    Object payload = args[0];

                    if ("azure".equals(props.getProvider())) {
                        ServiceBusClientBuilder builder = ctx.getBean(ServiceBusClientBuilder.class);
                        try (ServiceBusSenderClient sender = builder.sender()
                                .topicName(physicalTopic)
                                .buildClient()) {

                            ServiceBusMessage message = new ServiceBusMessage(MAPPER.writeValueAsBytes(payload));
                            message.getApplicationProperties().put("eventType", eventType);
                            message.setSubject(eventType); // optional

                            sender.sendMessage(message);
                        }
                    } else {
                        RabbitTemplate rabbitTemplate = ctx.getBean(RabbitTemplate.class);
                        rabbitTemplate.convertAndSend(physicalTopic, routingKey, payload);
                    }
                    return null;
                }
        );
    }
}