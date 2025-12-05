package com.intteq.universal_message_broker.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface MessagingListener {
    String topic();        // logical topic name
    /**
     * Unified channel name (e.g. "course.grade", "exam.grade")
     * RabbitMQ → becomes queue:  course.grade.queue
     * Azure    → becomes sub:    course.grade-sub
     */
    String channel();
}
