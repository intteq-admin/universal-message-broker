package com.intteq.universal.message.broker.annotation;

import java.lang.annotation.*;

/**
 * Declares a messaging event published by a method inside a publisher interface.
 *
 * <p>This annotation is used in conjunction with {@code @MessagingTopic} on publisher
 * interfaces. The event name (the {@link #value()}) becomes part of the routing key:
 *
 * <pre>
 *   routingKey = <logicalTopic> + "." + <eventName>
 * </pre>
 *
 * <p>Example:
 * <pre>
 * {@code
 * @MessagingTopic("course-events")
 * public interface CourseEventsPublisher {
 *
 *     @MessagingEvent("gradeCreated")
 *     void gradeCreated(GradeCreatedPayload payload);
 * }
 * }
 * </pre>
 *
 * <p>The annotation also provides optional metadata such as:
 * <ul>
 *     <li>{@link #routingKey()} — override default routing key structure</li>
 *     <li>{@link #schemaVersion()} — event schema versioning</li>
 *     <li>{@link #description()} — human-readable description for documentation tools</li>
 * </ul>
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface MessagingEvent {

    /**
     * Canonical event name (e.g. "studentCreated", "gradeUpdated").
     * Used by default to build routing keys unless overridden via {@link #routingKey()}.
     */
    String value();

    /**
     * Optional explicit routing key. If left empty, the default is:
     *
     * <pre>
     *     <logicalTopic> + "." + value()
     * </pre>
     *
     * <p>Example:
     * <pre>
     * @MessagingEvent(value = "gradeCreated", routingKey = "grades.created.v1")
     * </pre>
     */
    String routingKey() default "";

    /**
     * Optional schema version tag. Useful when evolving event formats while
     * maintaining backward compatibility between producer and consumers.
     *
     * <p>Example values: {@code "v1"}, {@code "v2"}, {@code "2024-01"}.
     *
     * <p>Default: empty (unspecified).
     */
    String schemaVersion() default "";

    /**
     * Optional human-readable description. Useful for:
     * <ul>
     *     <li>API documentation</li>
     *     <li>Event catalogs</li>
     *     <li>Message governance tools</li>
     * </ul>
     *
     * <p>Default: empty.
     */
    String description() default "";
}
