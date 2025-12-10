package com.intteq.universal.message.broker.exception;

/**
 * Exception thrown when the publisher proxy fails to send a message
 * after retry attempts.
 */
public class MessagingPublishException extends RuntimeException {

    public MessagingPublishException(String message) {
        super(message);
    }

    public MessagingPublishException(String message, Throwable cause) {
        super(message, cause);
    }
}
