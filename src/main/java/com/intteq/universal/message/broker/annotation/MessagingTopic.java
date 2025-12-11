package com.intteq.universal.message.broker.annotation;

import java.lang.annotation.*;

/**
 * Marks an interface as a message publisher for a logical topic.
 *
 * <p>The logical topic maps to a physical topic or exchange
 * via {@code messaging.topics.*} in {@link com.intteq.universal.message.broker.MessagingProperties}.
 *
 * <p>Example:
 * <pre>
 * {@code
 * @MessagingTopic("course-events")
 * public interface CourseEventsPublisher {
 *
 *     @MessagingEvent("gradeCreated")
 *     void gradeCreated(GradeCreatedPayload payload);
 *
 *     @MessagingEvent("gradeUpdated")
 *     void gradeUpdated(GradeUpdatedPayload payload);
 * }
 * }
 * </pre>
 *
 * <p>Optional metadata fields such as {@link #description()} and
 * {@link #schemaVersion()} allow documentation tools, governance dashboards,
 * and cataloging systems to auto-describe your event streams.
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface MessagingTopic {

    /**
     * Logical topic name (e.g., "student-events", "course-events").
     *
     * <p>This value must match a key defined under:
     * <pre>
     * messaging.topics.<logicalName>=<physicalTopicOrExchangeName>
     * </pre>
     */
    String value();

    /**
     * Optional human-readable description of the topic.
     * Useful for event catalogs, documentation generators,
     * or observability dashboards.
     */
    String description() default "";

    /**
     * Optional schema version of the message contract for this topic.
     *
     * <p>Example formats:
     * <ul>
     *     <li>"v1"</li>
     *     <li>"v2"</li>
     *     <li>"2024.01"</li>
     * </ul>
     *
     * This can be surfaced in headers during publishing if desired.
     */
    String schemaVersion() default "";
}
