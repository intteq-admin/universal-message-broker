package com.intteq.universal_message_broker.annotation;

import java.lang.annotation.*;

@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface EventHandler {
    /**
     * The event method name from the publisher interface
     * e.g. "studentCreated", "gradeUpdated"
     */
    String value();
}
