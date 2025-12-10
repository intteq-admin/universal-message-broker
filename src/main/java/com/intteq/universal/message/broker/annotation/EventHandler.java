package com.intteq.universal.message.broker.annotation;


import java.lang.annotation.*;

/**
 * Marks a method as an event handler for a specific messaging event.
 *
 * <p>Typical usage:
 * <pre>
 * {@code
 * @MessagingListener(topic = "course-events", channel = "grading")
 * public class GradingListener {
 *
 *     @EventHandler("gradeUpdated")
 *     public void onGradeUpdated(GradePayload payload, MessageContext ctx) {
 *         // handle event...
 *     }
 * }
 * }
 * </pre>
 *
 * <p>Notes:
 * <ul>
 *   <li>{@code value} is the canonical event name and is used by default to build the routing key
 *       (logicalTopic + "." + value) unless {@link #routingKey()} is provided.</li>
 *   <li>{@link #deadLetterOnFailure()} is a hint to the listener framework to dead-letter a message
 *       when the handler throws an exception. Concrete implementations may also implement retries
 *       based on {@link #maxAttempts()} before dead-lettering.</li>
 *   <li>All additional attributes are optional and have safe defaults to preserve backward compatibility.</li>
 * </ul>
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface EventHandler {

    /**
     * The event name (typically the publisher method name or explicit event type).
     *
     * <p>Example: {@code "studentCreated"} or {@code "order.shipped"}.
     */
    String value();

    /**
     * Optional explicit routing key for this handler. If empty, the framework will
     * typically use {@code <logicalTopic> + "." + value()}.
     *
     * <p>Default: empty string (framework chooses routing key).
     */
    String routingKey() default "";

    /**
     * Whether the framework should dead-letter the message automatically if the handler
     * throws an exception and retries are exhausted.
     *
     * <p>Default: {@code true}.
     */
    boolean deadLetterOnFailure() default true;

    /**
     * A hint for the maximum number of attempts the framework should make for this handler
     * before considering the message failed and (optionally) dead-lettering it.
     *
     * <p>Default: {@code 1} meaning "no retries" — handlers that wish to enable retries
     * should set this to a higher value. Concrete retry behaviour must be implemented
     * by the listener infrastructure (e.g., ManualAckListenerProcessor).
     */
    int maxAttempts() default 1;
}
