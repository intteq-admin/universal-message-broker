package com.intteq.universal_message_broker;

import com.azure.messaging.servicebus.ServiceBusReceivedMessage;
import com.azure.messaging.servicebus.ServiceBusReceivedMessageContext;
import com.azure.messaging.servicebus.ServiceBusReceiverClient;
import com.azure.messaging.servicebus.models.DeadLetterOptions;
import com.rabbitmq.client.Channel;
import lombok.Getter;

@Getter
public class MessageContext {

    private final Channel rabbitChannel;
    private final long rabbitDeliveryTag;
    private final ServiceBusReceivedMessageContext azureProcessorContext;
    private final ServiceBusReceivedMessage azureMessage;
    private final ServiceBusReceiverClient azureReceiver;

    // RabbitMQ
    public MessageContext(Channel channel, long deliveryTag) {
        this.rabbitChannel = channel;
        this.rabbitDeliveryTag = deliveryTag;
        this.azureProcessorContext = null;
        this.azureMessage = null;
        this.azureReceiver = null;
    }

    // Azure — Processor mode
    public MessageContext(ServiceBusReceivedMessageContext context) {
        this.rabbitChannel = null;
        this.rabbitDeliveryTag = 0;
        this.azureProcessorContext = context;
        this.azureMessage = context.getMessage();
        this.azureReceiver = null;
    }

    // Azure — Manual receiver mode
    public MessageContext(ServiceBusReceivedMessage message, ServiceBusReceiverClient receiver) {
        this.rabbitChannel = null;
        this.rabbitDeliveryTag = 0;
        this.azureProcessorContext = null;
        this.azureMessage = message;
        this.azureReceiver = receiver;
    }

    public void ack() throws Exception {
        if (rabbitChannel != null) {
            rabbitChannel.basicAck(rabbitDeliveryTag, false);
        } else if (azureProcessorContext != null) {
            azureProcessorContext.complete();
        } else if (azureReceiver != null && azureMessage != null) {
            azureReceiver.complete(azureMessage);
        } else {
            throw new IllegalStateException("No messaging context available to acknowledge");
        }
    }

    public void nack() throws Exception {
        if (rabbitChannel != null) {
            rabbitChannel.basicNack(rabbitDeliveryTag, false, true);
        } else if (azureProcessorContext != null) {
            azureProcessorContext.abandon();
        } else if (azureReceiver != null && azureMessage != null) {
            azureReceiver.abandon(azureMessage);
        }
    }

    public void deadLetter() throws Exception {
        if (rabbitChannel != null) {
            rabbitChannel.basicNack(rabbitDeliveryTag, false, false);
        }
        else if (azureProcessorContext != null) {
            DeadLetterOptions options = new DeadLetterOptions()
                    .setDeadLetterReason("processing-failed")
                    .setDeadLetterErrorDescription("Manual dead-letter from application");
            azureProcessorContext.deadLetter(options);
        }
        else if (azureReceiver != null && azureMessage != null) {
            DeadLetterOptions options = new DeadLetterOptions()
                    .setDeadLetterReason("processing-failed")
                    .setDeadLetterErrorDescription("Manual dead-letter from application");
            azureReceiver.deadLetter(azureMessage, options);
        }
    }

    // Optional: deadLetter with custom reason/description
    public void deadLetter(String reason, String description) throws Exception {
        if (rabbitChannel != null) {
            rabbitChannel.basicNack(rabbitDeliveryTag, false, false);
        }
        else if (azureProcessorContext != null) {
            DeadLetterOptions options = new DeadLetterOptions()
                    .setDeadLetterReason(reason)
                    .setDeadLetterErrorDescription(description);
            azureProcessorContext.deadLetter(options);
        }
        else if (azureReceiver != null && azureMessage != null) {
            DeadLetterOptions options = new DeadLetterOptions()
                    .setDeadLetterReason(reason)
                    .setDeadLetterErrorDescription(description);
            azureReceiver.deadLetter(azureMessage, options);
        }
    }
}
