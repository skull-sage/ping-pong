# service_pong — Trace / Log / Metric Guide

Everything below is emitted **by this service only**, identified in Grafana by
**`service.name = service-pong`** (from `spring.application.name`) and
**`service.namespace = pingpong`**. All three pillars are pushed over **OTLP** to the Grafana LGTM
container (`OTLP_ENDPOINT`, default `http://localhost:4318`):

| Pillar | Backend | How it is produced |
|---|---|---|
| Traces | Tempo | Manual CONSUMER/PRODUCER spans + `@WithSpan` INTERNAL spans |
| Logs | Loki | logback `OpenTelemetryAppender` (OTLP); trace id + `correlationId`/`causationId` in MDC |
| Metrics | Prometheus | `micrometer-registry-otlp` push every 5s |

## 1. What this service does (span map)

This is the **middle link of the ping → pong → bang chain**: it consumes `ping.events` from
service_ping and publishes `pong.events`, which service_bang then consumes.

```
process ping.events (chain middle)        ← CONSUMER span, NEW ROOT trace, linked to ping's publish
 └─ HandlePingCommandHandler
     ├─ PongRepository.save                ← @WithSpan INTERNAL span (~60–140ms sleep)
     ├─ PongProcessingService.process      ← @WithSpan INTERNAL span (~120–260ms sleep, main hazard)
     └─ publish pong.events                ← PRODUCER span (injects traceparent; consumed by service_bang)
```

- Consumes topic **`ping.events`** (consumer group `service-pong`); produces topic **`pong.events`**.
- Bounded context **`pong`** (`ddd.bounded_context`).
- `correlationId` is kept static across the whole chain; `causationId` = the inbound ping event id.
- Duplicate ping ids are dropped and marked with a `messaging.duplicate_detected` span event (RC-4).

## 2. Traces (Tempo) — Explore → Tempo

| Goal | Query |
|---|---|
| All traces from this service | `{ resource.service.name = "service-pong" }` |
| The consumer span | `{ resource.service.name = "service-pong" && name = "process ping.events" }` |
| Follow ONE whole saga across services/traces | `{ span.messaging.message.conversation_id = "<correlationId>" }` |
| Only this context | `{ span.ddd.bounded_context = "pong" }` |
| Slow internal processing | `{ name = "PongProcessingService.process" && duration > 200ms }` |
| Duplicates skipped | `{ resource.service.name = "service-pong" && event.name = "messaging.duplicate_detected" }` |

## 3. `@WithSpan` functions in this service

| `@WithSpan` function | Span name (kind) | OTel specifics used | Find / filter in Grafana (Tempo TraceQL) |
|---|---|---|---|
| `PongRepository.save` | `PongRepository.save` (INTERNAL) | `WithSpanAspect` → `Tracer.spanBuilder(name).setSpanKind(INTERNAL)`; simulated write latency | `{ name = "PongRepository.save" }` |
| `PongProcessingService.process` | `PongProcessingService.process` (INTERNAL) | Same aspect; longer sleep = dominant latency | `{ name = "PongProcessingService.process" }` |

> `@WithSpan` is `io.opentelemetry.instrumentation.annotations.WithSpan`, woven by the Spring-AOP
> `WithSpanAspect` (no Java agent), using the SDK `OpenTelemetry` bean. These INTERNAL spans appear
> as children of this service's `process ping.events` CONSUMER span.

## 4. Logs (Loki) — Explore → Loki

| Goal | LogQL |
|---|---|
| All logs from this service | `{service_name="service-pong"}` |
| One saga's logs | `{service_name="service-pong"} |= "<correlationId>"` |
| Errors only | `{service_name="service-pong"} | level="ERROR"` |

Each line carries `traceId`, `spanId`, `correlationId`, `causationId` (MDC).

## 5. Metrics (Prometheus) — Explore → Prometheus

Label **`job="pingpong/service-pong"`** (`service.namespace/service.name`).

| Goal | PromQL |
|---|---|
| Kafka consumer throughput/health | `{job="pingpong/service-pong"}` (browse `spring_kafka_*` / `kafka_consumer_*`) |
| JVM heap used | `jvm_memory_used_bytes{job="pingpong/service-pong", area="heap"}` |
| CPU usage | `process_cpu_usage{job="pingpong/service-pong"}` |
| GC pauses | `rate(jvm_gc_pause_seconds_sum{job="pingpong/service-pong"}[5m])` |

## 6. Notes for clarity (no other docs needed)

- **service_name** isolates this service everywhere: `service.name` (traces), `service_name` (Loki
  label), `job=pingpong/service-pong` (Prometheus).
- This service starts a **new trace** per consumed ping (async fan-out) and **links** back to the
  producer's publish span — so its traces are separate from service_ping's but joinable by
  `correlationId` (the saga id) via the TraceQL in §2.
- Metrics are **push** (OTLP every 5s), not scraped.
- A long `PongProcessingService.process` span is the injected performance hazard, not a real fault.
