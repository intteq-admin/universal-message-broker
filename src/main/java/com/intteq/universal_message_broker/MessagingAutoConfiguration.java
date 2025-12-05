package com.intteq.universal_message_broker;

import com.intteq.universal_message_broker.internal.MessagingProxyFactory;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration(proxyBeanMethods = false)
@EnableConfigurationProperties(MessagingProperties.class)
public class MessagingAutoConfiguration {
    @Bean
    public MessagingProxyFactory messagingProxyFactory(
            MessagingProperties props,
            ApplicationContext ctx) {
        return new MessagingProxyFactory(props, ctx);
    }
}