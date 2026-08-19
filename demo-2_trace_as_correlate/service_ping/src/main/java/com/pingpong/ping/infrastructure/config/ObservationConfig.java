package com.pingpong.ping.infrastructure.config;

import io.micrometer.observation.ObservationRegistry;
import io.micrometer.observation.aop.ObservedAspect;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Enables Micrometer's {@link io.micrometer.observation.annotation.Observed @Observed} annotation.
 *
 * <p>This replaces the old OpenTelemetry {@code @WithSpan} + hand-rolled {@code WithSpanAspect}.
 * {@link ObservedAspect} is a Spring-AOP around-advice that wraps every {@code @Observed} method in
 * an {@link io.micrometer.observation.Observation}. Because Micrometer Tracing (the OTel bridge)
 * registers a tracing {@code ObservationHandler} on the auto-configured {@code ObservationRegistry},
 * each observation becomes BOTH a timer metric (named by {@code @Observed(name=...)}) AND a child
 * span (named by {@code @Observed(contextualName=...)}) inside the CURRENT trace — so the local
 * span inherits the single trace_id and shows up in the same Grafana/Tempo waterfall.
 *
 * <p>Spring Boot does not auto-register this aspect, so we declare it explicitly.
 */
@Configuration
public class ObservationConfig {

    @Bean
    ObservedAspect observedAspect(ObservationRegistry observationRegistry) {
        return new ObservedAspect(observationRegistry);
    }
}
