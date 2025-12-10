package com.intteq.universal.message.broker.annotation;

import java.lang.annotation.*;

/**
 * Marks a class as a message listener for a specific logical topic and channel.
 *
 * <p>A listener class contains one or more {@link EventHandler} methods which receive
 * (payload, MessageContext) arguments for each event.
 *
 * <h2>Topic & Channel Mapping</h2>
 * <ul>
 *   <li><strong>topic</strong> → logical topic name used by publishers (mapped via MessagingProperties)</li>
 *   <li><strong>channel</strong> → consumer group identifier used to create queues/subscriptions</li>
 * </ul>
 *
 * <h3>Provider-specific behavior:</h3>
 * <ul>
 *   <li>RabbitMQ → queue name becomes: {@code <channel>.queue}</li>
 *   <li>Azure Service Bus → subscription name becomes: {@code <channel>-sub}</li>
 * </ul>
 *
 * <p>Example:
 * <pre>
 * {@code
 * @MessagingListener(topic = "course-events", channel = "grading")
 * public class GradingEventListener {
 *
 *     @EventHandler("gradeUpdated")
 *     public void onGradeUpdated(GradeUpdatedPayload payload, MessageContext ctx) {
 *         // business logic...
 *     }
 * }
 * }
 * </pre>
 *
 * <p>Advanced attributes (optional):
 * <ul>
 *     <li>{@link #concurrency()} → desired concurrency level for this listener</li>
 *     <li>{@link #prefetch()} → RabbitMQ prefetch control per listener</li>
 *     <li>{@link #description()} → human-readable description (useful for governance)</li>
 * </ul>
 *
 * All optional fields have safe defaults and preserve backward compatibility.
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface MessagingListener {

    /**
     * Logical topic name that maps to a physical exchange or topic.
     * This must match a key under {@code messaging.topics.*}.
     */
    String topic();

    /**
     * Unified channel name (consumer group).
     *
     * <p>Provider-specific behavior:
     * <ul>
     *     <li>RabbitMQ → queue = {@code <channel>.queue}</li>
     *     <li>Azure → subscription = {@code <channel>-sub}</li>
     * </ul>
     */
    String channel();

    /**
     * Optional concurrency hint. The default value (1) matches Azure Service Bus's
     * safe default and RabbitMQ's single-thread processing.
     *
     * <p>The infrastructure layer may or may not strictly enforce this value,
     * but it provides an important configuration hint.
     *
     * @return desired maximum parallel handler executions
     */
    int concurrency() default 1;

    /**
     * Optional per-listener RabbitMQ prefetch override.
     *
     * <p>If set to a value > 0, the listener infrastructure should honor this
     * prefetch count for RabbitMQ consumers.
     *
     * <p>Ignored by Azure.
     *
     * @return the prefetch value, or 0 to use framework default
     */
    int prefetch() default 0;

    /**
     * Optional human-readable documentation for developers or monitoring systems.
     *
     * @return description of what this listener handles
     */
    String description() default "";
}
