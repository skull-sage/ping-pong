package com.pingpong.bang.infrastructure.config;

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.instrumentation.logback.appender.v1_0.OpenTelemetryAppender;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.stereotype.Component;

/** Wires the OTel SDK into the logback OTEL appender so log records ship over OTLP to Loki. */
@Component
class OpenTelemetryLogbackInstaller implements InitializingBean {

    private final OpenTelemetry openTelemetry;

    OpenTelemetryLogbackInstaller(OpenTelemetry openTelemetry) {
        this.openTelemetry = openTelemetry;
    }

    @Override
    public void afterPropertiesSet() {
        OpenTelemetryAppender.install(this.openTelemetry);
    }
}
