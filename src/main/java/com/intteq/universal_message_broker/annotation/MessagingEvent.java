package com.intteq.universal_message_broker.annotation;

import java.lang.annotation.*;

@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface MessagingEvent {
    /**
     * The event type name (e.g. "gradeCreated", "studentDeleted")
     * This becomes part of the routing key: topic.eventType
     */
    String value();
}
