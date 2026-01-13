package com.intteq.universal.message.broker;

public interface MessagePublisherFactory {
    <T> T createPublisher(Class<T> publisherInterface);
}
