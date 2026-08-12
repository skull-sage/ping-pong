# service_bang — Trace / Log / Metric Guide

Everything below is emitted **by this service only**, identified in Grafana by
**`service.name = service-bang`** (from `spring.application.name`) and
**`service.namespace = pingpong`**. All three pillars are pushed over **OTLP** to the Grafana LGTM
container (`OTLP_ENDPOINT`, default `http://localhost:4318`):

| Pillar | Backend | How it is produced |
|---|---|---|
| Traces | Tempo | Manual CONSUMER span + `@WithSpan` INTERNAL spans (terminal — no PRODUCER span) |
| Logs | Loki | logback `OpenTelemetryAppender` (OTLP); trace id + `correlationId`/`causationId` in MDC |
| Metrics | Prometheus | `micrometer-registry-otlp` push every 5s |

## 1. What this service does (span map)

This is the **terminal link of the ping → pong → bang chain**. It consumes `pong.events` (the
`PongResponded` fact emitted by `service_pong`) and finalizes the flow — it publishes nothing.

```
process pong.events (chain terminal)      ← CONSUMER span, NEW ROOT trace, linked to pong's publish
 └─ HandlePongCommandHandler
     ├─ PongRepository.save                ← @WithSpan INTERNAL span (~60–140ms sleep)
     └─ PongProcessingService.process      ← @WithSpan INTERNAL span (~120–260ms sleep, main hazard)
```

- Consumes topic **`pong.events`** (consumer group `service-bang`). Produces **nothing** (terminal).
- Bounded context **`bang`** (`ddd.bounded_context`).
- `correlationId` is kept static across the whole chain (`ping.events` → `pong.events`), so a single
  saga id ties ping, pong, and bang together; `causationId` = the inbound pong event id.
- Duplicate pong ids are dropped and marked with a `messaging.duplicate_detected` span event (RC-4).

## 2. Traces (Tempo) — Explore → Tempo

| Goal | Query |
|---|---|
| All traces from this service | `{ resource.service.name = "service-bang" }` |
| The consumer span | `{ resource.service.name = "service-bang" && name = "process pong.events" }` |
| Follow ONE whole saga across services/traces | `{ span.messaging.message.conversation_id = "<correlationId>" }` |
| Only this context | `{ span.ddd.bounded_context = "bang" }` |
| Slow internal processing | `{ name = "PongProcessingService.process" && duration > 200ms }` |
| Duplicates skipped | `{ resource.service.name = "service-bang" && event.name = "messaging.duplicate_detected" }` |

## 3. `@WithSpan` functions in this service

| `@WithSpan` function | Span name (kind) | OTel specifics used | Find / filter in Grafana (Tempo TraceQL) |
|---|---|---|---|
| `PongRepository.save` | `PongRepository.save` (INTERNAL) | `WithSpanAspect` → `Tracer.spanBuilder(name).setSpanKind(INTERNAL)`; simulated write latency | `{ name = "PongRepository.save" }` |
| `PongProcessingService.process` | `PongProcessingService.process` (INTERNAL) | Same aspect; longer sleep = dominant latency | `{ name = "PongProcessingService.process" }` |

> `@WithSpan` is `io.opentelemetry.instrumentation.annotations.WithSpan`, woven by the Spring-AOP
> `WithSpanAspect` (no Java agent), using the SDK `OpenTelemetry` bean. These INTERNAL spans appear
> as children of this service's `process pong.events` CONSUMER span.

## 4. Logs (Loki) — Explore → Loki

| Goal | LogQL |
|---|---|
| All logs from this service | `{service_name="service-bang"}` |
| One saga's logs | `{service_name="service-bang"} |= "<correlationId>"` |
| Errors only | `{service_name="service-bang"} | level="ERROR"` |

Each line carries `traceId`, `spanId`, `correlationId`, `causationId` (MDC).

## 5. Metrics (Prometheus) — Explore → Prometheus

Label **`job="pingpong/service-bang"`** (`service.namespace/service.name`).

| Goal | PromQL |
|---|---|
| Kafka consumer throughput/health | `{job="pingpong/service-bang"}` (browse `spring_kafka_*` / `kafka_consumer_*`) |
| JVM heap used | `jvm_memory_used_bytes{job="pingpong/service-bang", area="heap"}` |
| CPU usage | `process_cpu_usage{job="pingpong/service-bang"}` |
| GC pauses | `rate(jvm_gc_pause_seconds_sum{job="pingpong/service-bang"}[5m])` |

## 6. Notes for clarity (no other docs needed)

- **service_name** isolates this service everywhere: `service.name` (traces), `service_name` (Loki
  label), `job=pingpong/service-bang` (Prometheus).
- This service starts a **new trace** per consumed pong (async hop) and **links** back to
  service_pong's publish span — joinable to the rest of the chain by `correlationId` via §2.
- Metrics are **push** (OTLP every 5s), not scraped.
- A long `PongProcessingService.process` span is the injected performance hazard, not a real fault.
