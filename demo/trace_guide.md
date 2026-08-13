# Ping-Pong Distributed-Tracing Guide

One guide for all three services. Each has its own section below; the shared conventions and the
CR-2 **failure / back-tracing** pipeline are documented once at the end so nothing is duplicated.

All three pillars are pushed over **OTLP** to the Grafana LGTM container (`OTLP_ENDPOINT`, default
`http://localhost:4318`) and every signal is isolated per service by its resource attribute
**`service.name`** (from `spring.application.name`) and **`service.namespace = pingpong`**:

| In Grafana | Traces (Tempo) | Logs (Loki) | Metrics (Prometheus) |
|---|---|---|---|
| Service key | `resource.service.name` | `service_name` label | `job = pingpong/<service.name>` |
| service_ping | `service-ping` | `service-ping` | `pingpong/service-ping` |
| service_pong | `service-pong` | `service-pong` | `pingpong/service-pong` |
| service_bang | `service-bang` | `service-bang` | `pingpong/service-bang` |

The chain is **Ping → Pong → Bang**. The saga id **`correlationId`** stays constant across every
hop (so one id ties all three services together); **`causationId`** chains cause → effect. Each
async hop starts a **new root trace linked** to the upstream publish span, so traces are separate
per service but joinable by `correlationId`.

---

## 1. service_ping

Producer-only edge of the chain. Opens the trace on HTTP and publishes to `ping.events` (happy path)
or `ping.faults` (CR-2 failure path). Consumes nothing.

**Span map**

```
HTTP POST /api/ping                     ← SERVER span (trace starts here)
 └─ SendPingCommandHandler
     ├─ PingRepository.save             ← @WithSpan INTERNAL (~60–140ms sleep)
     ├─ PingAuditService.record         ← @WithSpan INTERNAL (~120–260ms sleep, main hazard)
     └─ publish ping.events             ← PRODUCER span (injects traceparent)

HTTP POST /api/ping/fail                ← SERVER span (CR-2 failure pipeline)
 └─ TriggerFaultCommandHandler
     └─ publish ping.faults             ← PRODUCER span (consumed by service_pong, which throws)
```

**Traces (Tempo) — Explore → Tempo**

| Goal | TraceQL |
|---|---|
| All traces from this service | `{ resource.service.name = "service-ping" }` |
| HTTP entry span only | `{ resource.service.name = "service-ping" && name = "POST /api/ping" }` |
| Follow ONE whole saga across services | `{ span.messaging.message.conversation_id = "<correlationId>" }` |
| The slow internal work | `{ resource.service.name = "service-ping" && name = "PingAuditService.record" }` |
| Slow requests only | `{ resource.service.name = "service-ping" && duration > 200ms }` |
| The fault-publish span | `{ resource.service.name = "service-ping" && name = "publish ping.faults" }` |

The `<correlationId>` is returned in the HTTP response body and printed in every log line.

**`@WithSpan` functions**

| `@WithSpan` function | Span name (kind) | OTel specifics | Find / filter (Tempo TraceQL) |
|---|---|---|---|
| `PingRepository.save` | `PingRepository.save` (INTERNAL) | `WithSpanAspect` → `Tracer.spanBuilder(name).setSpanKind(INTERNAL)`; simulated DB write latency | `{ name = "PingRepository.save" }` |
| `PingAuditService.record` | `PingAuditService.record` (INTERNAL) | Same aspect; longer sleep = dominant latency | `{ name = "PingAuditService.record" && duration > 200ms }` |

**Logs (Loki)**

| Goal | LogQL |
|---|---|
| All logs | `{service_name="service-ping"}` |
| One saga's logs | `{service_name="service-ping"} |= "<correlationId>"` |
| Errors only | `{service_name="service-ping"} | level="ERROR"` |

**Metrics (Prometheus)** — label `job="pingpong/service-ping"`

| Goal | PromQL |
|---|---|
| HTTP request rate | `rate(http_server_requests_seconds_count{job="pingpong/service-ping"}[1m])` |
| HTTP p95 latency | `histogram_quantile(0.95, sum by (le) (rate(http_server_requests_seconds_bucket{job="pingpong/service-ping"}[5m])))` |
| JVM heap used | `jvm_memory_used_bytes{job="pingpong/service-ping", area="heap"}` |
| CPU usage | `process_cpu_usage{job="pingpong/service-ping"}` |

---

## 2. service_pong

Middle link. Consumes `ping.events`, does `@WithSpan` work, publishes `pong.events`. Also runs the
CR-2 failure consumer: it consumes `ping.faults` and deliberately throws (see §4).

**Span map**

```
process ping.events (chain middle)      ← CONSUMER span, NEW ROOT trace, linked to ping's publish
 └─ HandlePingCommandHandler
     ├─ PongRepository.save             ← @WithSpan INTERNAL (~60–140ms sleep)
     ├─ PongProcessingService.process   ← @WithSpan INTERNAL (~120–260ms sleep, main hazard)
     └─ publish pong.events             ← PRODUCER span (consumed by service_bang)

process ping.faults (CR-2 failure)      ← CONSUMER span, NEW ROOT trace, linked to ping's publish
 └─ HandleFaultCommandHandler
     ├─ PongProcessingService.process   ← @WithSpan INTERNAL
     └─ throws FaultSimulationException ← span status = ERROR + ERROR log line
```

**Traces (Tempo)**

| Goal | TraceQL |
|---|---|
| All traces from this service | `{ resource.service.name = "service-pong" }` |
| The happy-path consumer span | `{ resource.service.name = "service-pong" && name = "process ping.events" }` |
| Follow ONE whole saga across services | `{ span.messaging.message.conversation_id = "<correlationId>" }` |
| Only this context | `{ span.ddd.bounded_context = "pong" }` |
| Slow internal processing | `{ name = "PongProcessingService.process" && duration > 200ms }` |
| Duplicates skipped | `{ resource.service.name = "service-pong" && event.name = "messaging.duplicate_detected" }` |
| The errored fault span | `{ resource.service.name = "service-pong" && name = "process ping.faults" && status = error }` |

**`@WithSpan` functions**

| `@WithSpan` function | Span name (kind) | OTel specifics | Find / filter (Tempo TraceQL) |
|---|---|---|---|
| `PongRepository.save` | `PongRepository.save` (INTERNAL) | `WithSpanAspect` → INTERNAL span; simulated write latency | `{ name = "PongRepository.save" }` |
| `PongProcessingService.process` | `PongProcessingService.process` (INTERNAL) | Same aspect; longer sleep = dominant latency | `{ name = "PongProcessingService.process" }` |

**Logs (Loki)**

| Goal | LogQL |
|---|---|
| All logs | `{service_name="service-pong"}` |
| One saga's logs | `{service_name="service-pong"} |= "<correlationId>"` |
| Errors only (incl. the simulated fault) | `{service_name="service-pong"} | level="ERROR"` |

**Metrics (Prometheus)** — label `job="pingpong/service-pong"`

| Goal | PromQL |
|---|---|
| Kafka consumer health | `{job="pingpong/service-pong"}` (browse `spring_kafka_*` / `kafka_consumer_*`) |
| JVM heap used | `jvm_memory_used_bytes{job="pingpong/service-pong", area="heap"}` |
| CPU usage | `process_cpu_usage{job="pingpong/service-pong"}` |
| GC pauses | `rate(jvm_gc_pause_seconds_sum{job="pingpong/service-pong"}[5m])` |

---

## 3. service_bang

Terminal link. Consumes `pong.events` and finalizes the flow; publishes nothing.

**Span map**

```
process pong.events (chain terminal)    ← CONSUMER span, NEW ROOT trace, linked to pong's publish
 └─ HandlePongCommandHandler
     ├─ PongRepository.save             ← @WithSpan INTERNAL (~60–140ms sleep)
     └─ PongProcessingService.process   ← @WithSpan INTERNAL (~120–260ms sleep, main hazard)
```

**Traces (Tempo)**

| Goal | TraceQL |
|---|---|
| All traces from this service | `{ resource.service.name = "service-bang" }` |
| The consumer span | `{ resource.service.name = "service-bang" && name = "process pong.events" }` |
| Follow ONE whole saga across services | `{ span.messaging.message.conversation_id = "<correlationId>" }` |
| Only this context | `{ span.ddd.bounded_context = "bang" }` |
| Slow internal processing | `{ name = "PongProcessingService.process" && duration > 200ms }` |
| Duplicates skipped | `{ resource.service.name = "service-bang" && event.name = "messaging.duplicate_detected" }` |

**`@WithSpan` functions**

| `@WithSpan` function | Span name (kind) | OTel specifics | Find / filter (Tempo TraceQL) |
|---|---|---|---|
| `PongRepository.save` | `PongRepository.save` (INTERNAL) | `WithSpanAspect` → INTERNAL span; simulated write latency | `{ name = "PongRepository.save" }` |
| `PongProcessingService.process` | `PongProcessingService.process` (INTERNAL) | Same aspect; longer sleep = dominant latency | `{ name = "PongProcessingService.process" }` |

**Logs (Loki)**

| Goal | LogQL |
|---|---|
| All logs | `{service_name="service-bang"}` |
| One saga's logs | `{service_name="service-bang"} |= "<correlationId>"` |
| Errors only | `{service_name="service-bang"} | level="ERROR"` |

**Metrics (Prometheus)** — label `job="pingpong/service-bang"`

| Goal | PromQL |
|---|---|
| Kafka consumer health | `{job="pingpong/service-bang"}` (browse `spring_kafka_*` / `kafka_consumer_*`) |
| JVM heap used | `jvm_memory_used_bytes{job="pingpong/service-bang", area="heap"}` |
| CPU usage | `process_cpu_usage{job="pingpong/service-bang"}` |
| GC pauses | `rate(jvm_gc_pause_seconds_sum{job="pingpong/service-bang"}[5m])` |

---

## 4. Failure pipeline & back-tracing (CR-2)

A dedicated error path so you can practise finding an error log and back-tracing it to its origin.

**Flow:** `POST /api/ping/fail` on service_ping → publishes a `FaultRequested` event to
**`ping.faults`** → service_pong's `FaultEventListener` consumes it → `HandleFaultCommandHandler`
throws `FaultSimulationException` → the listener logs it at ERROR and marks the consumer span
`status = error`.

**Trigger it**

`run-simulation.sh` already drives this endpoint continuously — the trigger hits `/api/ping` and
`/api/ping/fail` concurrently at a **4:1** mix (adjust with `--ping-per-fail`), so ERROR logs appear
without any manual step. To fire a single fault by hand:

```bash
curl -X POST http://localhost:8080/api/ping/fail \
  -H 'Content-Type: application/json' \
  -d '{"reason":"demo-error"}'
# -> {"status":"fault-dispatched","correlationId":"service-ping.fault-saga.XXXXXXXX"}
```

Copy the returned `correlationId` — it is the single key for back-tracing across both services.

**Step 1 — find the error log (Loki, Explore → Loki)**

```logql
{service_name="service-pong"} | level="ERROR"
```

The matching line reads `Fault pipeline failed corr=... eventId=... Simulated downstream failure ...`
and carries the full `FaultSimulationException` stack trace. It also carries `traceId`, `spanId`,
`correlationId`, and `causationId` in its fields (from MDC).

**Step 2 — back-trace to the errored span (Loki → Tempo)**

Click the **`traceId`** on that log line to jump straight into the trace in Tempo, or query Tempo
directly for errored fault spans:

```traceql
{ resource.service.name = "service-pong" && name = "process ping.faults" && status = error }
```

The span shows the recorded `FaultSimulationException` (exception event) and `status = error`.

**Step 3 — back-trace to the origin (across services)**

Use the `correlationId` to pull both hops of the failed saga — the ping publish span and the pong
error span — regardless of which trace each lives in:

```traceql
{ span.messaging.message.conversation_id = "<correlationId>" }
```

That takes you from the logged error all the way back to the `POST /api/ping/fail` request that
started it.

> Note: the exception is logged and swallowed (not rethrown) so each fault request produces exactly
> one clean ERROR — no Kafka redelivery storm. The consumer group `service-pong-faults` is separate
> from the happy-path group `service-pong`, so the failure path never interferes with normal traffic.

---

## 5. Notes for clarity (no other docs needed)

- **service_name** is the one key that isolates a service in every pillar: `service.name` (traces
  resource attr), `service_name` (Loki label), `job=pingpong/<name>` (Prometheus).
- Metrics are **push** (OTLP every 5s) — there is no `/actuator/prometheus` scrape.
- `@WithSpan` here is `io.opentelemetry.instrumentation.annotations.WithSpan`, woven by a Spring-AOP
  `WithSpanAspect` (no Java agent) using the SDK `OpenTelemetry` bean; each annotated call becomes an
  INTERNAL span named after the annotation value.
- To exemplar-pivot on the happy path: a metric spike → open a matching trace (share
  `correlationId`) → filter Loki by that `correlationId`.
- A long `*ProcessingService.process` / `PingAuditService.record` span is the injected performance
  hazard, not a real outage. A `process ping.faults` span with `status = error` is the injected
  fault from §4, not a real fault.
