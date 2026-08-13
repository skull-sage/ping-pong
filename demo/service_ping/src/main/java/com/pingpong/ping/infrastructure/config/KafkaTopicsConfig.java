package com.pingpong.ping.infrastructure.config;

import com.pingpong.common.Topics;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

/** Declares the topics so they exist deterministically instead of relying on broker auto-create. */
@Configuration
public class KafkaTopicsConfig {

    @Bean
    NewTopic ping_events_topic() {
        return TopicBuilder.name(Topics.PING_EVENTS).partitions(3).replicas(1).build();
    }

    @Bean
    NewTopic pong_events_topic() {
        return TopicBuilder.name(Topics.PONG_EVENTS).partitions(3).replicas(1).build();
    }

    @Bean
    NewTopic ping_faults_topic() {
        return TopicBuilder.name(Topics.PING_FAULTS).partitions(3).replicas(1).build();
    }
}
