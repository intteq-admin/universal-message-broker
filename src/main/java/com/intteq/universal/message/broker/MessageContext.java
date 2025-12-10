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
 * Unified message-processing context that abstracts provider-specific acknowledgement
 * operations for RabbitMQ and Azure Service Bus.
 *
 * <p>Usage (recommended):
 * <pre>
 *   // RabbitMQ
 *   MessageContext ctx = MessageContext.forRabbitMQ(channel, deliveryTag);
 *   ctx.ack();           // acknowledge
 *   ctx.nack();          // requeue (or according to implementation)
 *   ctx.deadLetter();    // send to DLQ
 *
 *   // Azure (processor)
 *   MessageContext ctx = MessageContext.forAzureProcessor(processContext);
 *   ctx.ack();           // complete
 *   ctx.nack();          // abandon
 *   ctx.deadLetter("reason","desc");
 * </pre>
 *
 * <p>Notes:
 * <ul>
 *     <li>Instances are immutable and thread-safe.</li>
 *     <li>Factory methods are the recommended way to obtain instances.</li>
 *     <li>All I/O/SDK checked exceptions are wrapped in {@link MessagingOperationException}.</li>
 * </ul>
 */
@Getter
@Accessors(fluent = true)
@Slf4j
public class MessageContext {

    private final Channel rabbitChannel;
    private final long rabbitDeliveryTag;

    private final ServiceBusReceivedMessageContext azureProcessorContext;
    private final ServiceBusReceivedMessage azureMessage;
    private final ServiceBusReceiverClient azureReceiver;

    // -----------------------
    // Factory methods
    // -----------------------

    /**
     * Create a context for RabbitMQ manual-ack processing.
     *
     * @param channel     RabbitMQ channel (must not be null)
     * @param deliveryTag delivery tag from the envelope
     * @return a new {@link MessageContext}
     */
    public static MessageContext forRabbitMQ(Channel channel, long deliveryTag) {
        Objects.requireNonNull(channel, "channel must not be null");
        return new MessageContext(channel, deliveryTag, null, null, null);
    }

    /**
     * Create a context for Azure Service Bus when using the processor client model
     * (i.e. callbacks that receive {@link ServiceBusReceivedMessageContext}).
     *
     * @param context ServiceBusReceivedMessageContext (must not be null)
     * @return a new {@link MessageContext}
     */
    public static MessageContext forAzureProcessor(ServiceBusReceivedMessageContext context) {
        Objects.requireNonNull(context, "ServiceBusReceivedMessageContext must not be null");
        return new MessageContext(null, 0L, context, context.getMessage(), null);
    }

    /**
     * Create a context for Azure Service Bus when using a manual receiver client
     * (i.e. you received a {@link ServiceBusReceivedMessage} and have a {@link ServiceBusReceiverClient}).
     *
     * @param message  the received Service Bus message (must not be null)
     * @param receiver the client that can complete/abandon/dead-letter the message (must not be null)
     * @return a new {@link MessageContext}
     */
    public static MessageContext forAzureReceiver(ServiceBusReceivedMessage message, ServiceBusReceiverClient receiver) {
        Objects.requireNonNull(message, "ServiceBusReceivedMessage must not be null");
        Objects.requireNonNull(receiver, "ServiceBusReceiverClient must not be null");
        return new MessageContext(null, 0L, null, message, receiver);
    }

    // Private canonical constructor
    private MessageContext(Channel rabbitChannel,
                           long rabbitDeliveryTag,
                           ServiceBusReceivedMessageContext azureProcessorContext,
                           ServiceBusReceivedMessage azureMessage,
                           ServiceBusReceiverClient azureReceiver) {
        this.rabbitChannel = rabbitChannel;
        this.rabbitDeliveryTag = rabbitDeliveryTag;
        this.azureProcessorContext = azureProcessorContext;
        this.azureMessage = azureMessage;
        this.azureReceiver = azureReceiver;
    }

    // -----------------------
    // Convenience checks
    // -----------------------

    public boolean isRabbit() {
        return rabbitChannel != null;
    }

    public boolean isAzureProcessor() {
        return azureProcessorContext != null;
    }

    public boolean isAzureReceiver() {
        return azureReceiver != null && azureMessage != null;
    }

    // -----------------------
    // Acknowledge operations
    // -----------------------

    /**
     * Acknowledge/Complete the message for the underlying provider.
     *
     * @throws MessagingOperationException if the underlying SDK call fails or if no context is available
     */
    public void ack() {
        try {
            if (isRabbit()) {
                rabbitChannel.basicAck(rabbitDeliveryTag, false);
                log.debug("RabbitMQ ack successful (tag={})", rabbitDeliveryTag);
            } else if (isAzureProcessor()) {
                azureProcessorContext.complete();
                log.debug("Azure processor complete successful (messageId={})", safeAzureMessageId());
            } else if (isAzureReceiver()) {
                azureReceiver.complete(azureMessage);
                log.debug("Azure receiver complete successful (messageId={})", safeAzureMessageId());
            } else {
                throw new IllegalStateException("No messaging context available to acknowledge");
            }
        } catch (Exception e) {
            log.error("Failed to ack message (id={})", safeAzureMessageId(), e);
            throw new MessagingOperationException("Failed to ack message", e);
        }
    }

    /**
     * Negative-acknowledge / Abandon the message. Behavior differs by provider:
     * <ul>
     *     <li>RabbitMQ: Nack with requeue = true</li>
     *     <li>Azure Processor: abandon()</li>
     *     <li>Azure Receiver: abandon(message)</li>
     * </ul>
     *
     * @throws MessagingOperationException in case of underlying failure
     */
    public void nack() {
        try {
            if (isRabbit()) {
                rabbitChannel.basicNack(rabbitDeliveryTag, false, true);
                log.debug("RabbitMQ nack (requeued) issued (tag={})", rabbitDeliveryTag);
            } else if (isAzureProcessor()) {
                azureProcessorContext.abandon();
                log.debug("Azure processor abandon issued (messageId={})", safeAzureMessageId());
            } else if (isAzureReceiver()) {
                azureReceiver.abandon(azureMessage);
                log.debug("Azure receiver abandon issued (messageId={})", safeAzureMessageId());
            } else {
                throw new IllegalStateException("No messaging context available to negative-acknowledge");
            }
        } catch (Exception e) {
            log.error("Failed to nack/abandon message (id={})", safeAzureMessageId(), e);
            throw new MessagingOperationException("Failed to nack/abandon message", e);
        }
    }

    /**
     * Dead-letter the message with a default reason/description.
     *
     * @throws MessagingOperationException in case of underlying failure
     */
    public void deadLetter() {
        deadLetter("processing-failed", "Dead-lettered by application");
    }

    /**
     * Dead-letter the message with custom reason and description.
     *
     * @param reason      reason for dead-lettering
     * @param description human-friendly description
     * @throws MessagingOperationException in case of underlying failure
     */
    public void deadLetter(String reason, String description) {
        try {
            if (isRabbit()) {
                // RabbitMQ does not have a first-class dead-letter API here — nack with requeue=false
                rabbitChannel.basicNack(rabbitDeliveryTag, false, false);
                log.debug("RabbitMQ dead-letter simulated via basicNack (tag={})", rabbitDeliveryTag);
            } else if (isAzureProcessor()) {
                DeadLetterOptions options = new DeadLetterOptions()
                        .setDeadLetterReason(reason)
                        .setDeadLetterErrorDescription(description);
                azureProcessorContext.deadLetter(options);
                log.debug("Azure processor dead-lettered message (messageId={}) reason={}", safeAzureMessageId(), reason);
            } else if (isAzureReceiver()) {
                DeadLetterOptions options = new DeadLetterOptions()
                        .setDeadLetterReason(reason)
                        .setDeadLetterErrorDescription(description);
                azureReceiver.deadLetter(azureMessage, options);
                log.debug("Azure receiver dead-lettered message (messageId={}) reason={}", safeAzureMessageId(), reason);
            } else {
                throw new IllegalStateException("No messaging context available to dead-letter");
            }
        } catch (Exception e) {
            log.error("Failed to dead-letter message (id={})", safeAzureMessageId(), e);
            throw new MessagingOperationException("Failed to dead-letter message", e);
        }
    }

    // -----------------------
    // Utilities
    // -----------------------

    private String safeAzureMessageId() {
        try {
            if (azureMessage != null) return azureMessage.getMessageId();
        } catch (Exception ignored) { /* best-effort */ }
        return "<n/a>";
    }

    // -----------------------
    // Nested runtime exception used to wrap provider SDK checked exceptions
    // -----------------------

    /**
     * Runtime exception used by {@link MessageContext} to wrap provider SDK checked exceptions.
     */
    public static class MessagingOperationException extends RuntimeException {
        public MessagingOperationException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
