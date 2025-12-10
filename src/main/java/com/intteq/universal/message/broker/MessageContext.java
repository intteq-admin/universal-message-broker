package com.intteq.universal.message.broker;

import com.azure.messaging.servicebus.ServiceBusReceivedMessage;
import com.azure.messaging.servicebus.ServiceBusReceivedMessageContext;
import com.azure.messaging.servicebus.ServiceBusReceiverClient;
import com.azure.messaging.servicebus.models.DeadLetterOptions;
import com.rabbitmq.client.Channel;
import lombok.Getter;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;

import java.util.Objects;

/**
 * Unified message-processing context that abstracts acknowledgment /
 * settlement operations for RabbitMQ and Azure Service Bus.
 *
 * <p>Usage:</p>
 *
 * <pre>
 * // RabbitMQ
 * MessageContext mc = MessageContext.forRabbitMQ(channel, tag);
 * mc.ack();
 * mc.nack();
 * mc.deadLetter();
 *
 * // Azure Processor
 * MessageContext mc = MessageContext.forAzureProcessor(processContext);
 * mc.ack();
 * mc.nack();
 * mc.deadLetter("reason", "desc");
 *
 * // Azure receiver client
 * MessageContext mc = MessageContext.forAzureReceiver(msg, receiverClient);
 * mc.ack();
 * </pre>
 */
@Getter
@Accessors(fluent = true)
@Slf4j
public final class MessageContext {

    // ------------ RabbitMQ ------------
    private final Channel rabbitChannel;
    private final long rabbitDeliveryTag;

    // ------------ Azure Processor ------------
    private final ServiceBusReceivedMessageContext azureProcessorContext;

    // ------------ Azure Receiver Model ------------
    private final ServiceBusReceivedMessage azureMessage;
    private final ServiceBusReceiverClient azureReceiver;

    // =====================================================================
    //  FACTORY METHODS
    // =====================================================================

    /** RabbitMQ manual-ack mode */
    public static MessageContext forRabbitMQ(Channel channel, long deliveryTag) {
        Objects.requireNonNull(channel, "RabbitMQ channel must not be null");
        return new MessageContext(channel, deliveryTag, null, null, null);
    }

    /** Azure Service Bus Processor mode */
    public static MessageContext forAzureProcessor(ServiceBusReceivedMessageContext ctx) {
        Objects.requireNonNull(ctx, "ServiceBusReceivedMessageContext must not be null");
        return new MessageContext(null, 0, ctx, ctx.getMessage(), null);
    }

    /** Azure manual receiver client mode */
    public static MessageContext forAzureReceiver(ServiceBusReceivedMessage msg, ServiceBusReceiverClient client) {
        Objects.requireNonNull(msg, "ServiceBusReceivedMessage must not be null");
        Objects.requireNonNull(client, "ServiceBusReceiverClient must not be null");
        return new MessageContext(null, 0, null, msg, client);
    }

    // =====================================================================
    //  PRIVATE CANONICAL CONSTRUCTOR
    // =====================================================================

    private MessageContext(
            Channel rabbitChannel,
            long rabbitDeliveryTag,
            ServiceBusReceivedMessageContext azureProcessorContext,
            ServiceBusReceivedMessage azureMessage,
            ServiceBusReceiverClient azureReceiver
    ) {
        this.rabbitChannel = rabbitChannel;
        this.rabbitDeliveryTag = rabbitDeliveryTag;
        this.azureProcessorContext = azureProcessorContext;
        this.azureMessage = azureMessage;
        this.azureReceiver = azureReceiver;
    }

    // =====================================================================
    //  PROVIDER CHECKS
    // =====================================================================

    public boolean isRabbit() {
        return rabbitChannel != null;
    }

    public boolean isAzureProcessor() {
        return azureProcessorContext != null;
    }

    public boolean isAzureReceiver() {
        return azureReceiver != null;
    }

    // =====================================================================
    //  ACKNOWLEDGEMENT OPERATIONS
    // =====================================================================

    public void ack() {
        try {
            if (isRabbit()) {
                rabbitChannel.basicAck(rabbitDeliveryTag, false);
            } else if (isAzureProcessor()) {
                azureProcessorContext.complete();
            } else if (isAzureReceiver()) {
                azureReceiver.complete(azureMessage);
            } else {
                throw new IllegalStateException("ack() called with no messaging context available");
            }
        } catch (Exception e) {
            throw new MessagingOperationException("Failed to ack message", e);
        }
    }

    public void nack() {
        try {
            if (isRabbit()) {
                rabbitChannel.basicNack(rabbitDeliveryTag, false, true);
            } else if (isAzureProcessor()) {
                azureProcessorContext.abandon();
            } else if (isAzureReceiver()) {
                azureReceiver.abandon(azureMessage);
            } else {
                throw new IllegalStateException("nack() called with no messaging context available");
            }
        } catch (Exception e) {
            throw new MessagingOperationException("Failed to nack message", e);
        }
    }

    public void deadLetter() {
        deadLetter("processing-failed", "Dead-lettered by application");
    }

    public void deadLetter(String reason, String description) {
        try {
            if (isRabbit()) {
                rabbitChannel.basicNack(rabbitDeliveryTag, false, false);
            } else if (isAzureProcessor()) {
                DeadLetterOptions options = new DeadLetterOptions()
                        .setDeadLetterReason(reason)
                        .setDeadLetterErrorDescription(description);

                azureProcessorContext.deadLetter(options);
            } else if (isAzureReceiver()) {
                DeadLetterOptions options = new DeadLetterOptions()
                        .setDeadLetterReason(reason)
                        .setDeadLetterErrorDescription(description);

                azureReceiver.deadLetter(azureMessage, options);
            } else {
                throw new IllegalStateException("deadLetter() called with no messaging context available");
            }
        } catch (Exception e) {
            throw new MessagingOperationException("Failed to dead-letter message", e);
        }
    }

    // =====================================================================
    //  UTILITIES
    // =====================================================================

    private String safeAzureMessageId() {
        try {
            if (azureMessage != null) return azureMessage.getMessageId();
        } catch (Exception ignored) { }
        return "<n/a>";
    }

    // =====================================================================
    //  CUSTOM EXCEPTION
    // =====================================================================

    public static class MessagingOperationException extends RuntimeException {
        public MessagingOperationException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
