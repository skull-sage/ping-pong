package com.pingpong.ping.infrastructure.config;

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Scope;
import io.opentelemetry.instrumentation.annotations.WithSpan;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.reflect.MethodSignature;
import org.springframework.stereotype.Component;

/**
 * Makes the {@code @WithSpan} annotation work without the OpenTelemetry Java agent: a Spring-AOP
 * around-advice opens an INTERNAL span for every annotated method using the SDK's tracer. The span
 * name is the annotation value, or {@code ClassName.method} when unset.
 */
@Aspect
@Component
public class WithSpanAspect {

    private final Tracer tracer;

    public WithSpanAspect(OpenTelemetry openTelemetry) {
        this.tracer = openTelemetry.getTracer("with-span-aspect");
    }

    @Around("@annotation(withSpan)")
    public Object trace(ProceedingJoinPoint pjp, WithSpan withSpan) throws Throwable {
        MethodSignature signature = (MethodSignature) pjp.getSignature();
        String name = withSpan.value().isBlank()
                ? signature.getDeclaringType().getSimpleName() + "." + signature.getName()
                : withSpan.value();

        Span span = tracer.spanBuilder(name).setSpanKind(SpanKind.INTERNAL).startSpan();
        try (Scope scope = span.makeCurrent()) {
            return pjp.proceed();
        } catch (Throwable t) {
            span.recordException(t);
            span.setStatus(StatusCode.ERROR);
            throw t;
        } finally {
            span.end();
        }
    }
}
