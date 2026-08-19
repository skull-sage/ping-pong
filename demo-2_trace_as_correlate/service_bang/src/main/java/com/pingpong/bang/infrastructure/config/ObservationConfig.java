package com.pingpong.bang.infrastructure.config;

import io.micrometer.observation.ObservationRegistry;
import io.micrometer.observation.aop.ObservedAspect;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Enables Micrometer's {@link io.micrometer.observation.annotation.Observed @Observed} annotation
 * (replaces the old OpenTelemetry {@code @WithSpan} + {@code WithSpanAspect}).
 *
 * <p>{@link ObservedAspect} wraps each {@code @Observed} method in an
 * {@link io.micrometer.observation.Observation}; via the tracing {@code ObservationHandler} it
 * becomes a child span inside the CURRENT trace, so local work inherits the single trace_id.
 */
@Configuration
public class ObservationConfig {

    @Bean
    ObservedAspect observedAspect(ObservationRegistry observationRegistry) {
        return new ObservedAspect(observationRegistry);
    }
}
