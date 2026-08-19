# Ping-Pong Distributed-Tracing Guide

One guide for all three services. Each has its own section below; the shared conventions and the
failure / back-tracing pipeline are documented once at the end so nothing is duplicated.

All three pillars are pushed over **OTLP** to the Grafana LGTM container (`OTLP_ENDPOINT`, default
`http://localhost:4318`) and every signal is isolated per service by its resource attribute
**`service.name`** (from `spring.application.name`) and **`service.namespace = pingpong`**:

| In Grafana | Traces (Tempo) | Logs (Loki) | Metrics (Prometheus) |
|---|---|---|---|
| Service key | `resource.service.name` | `service_name` label | `job = pingpong/<service.name>` |
| service_ping | `service-ping` | `service-ping` | `pingpong/service-ping` |
| service_pong | `service-pong` | `service-pong` | `pingpong/service-pong` |
| service_bang | `service-bang` | `service-bang` | `pingpong/service-bang` |

---

## 0. The trace model (read this first)

This is the heart of the refactor. We follow the **OpenTelemetry standard**: a **single `trace_id`**
is created **when the HTTP request lands on service_ping's REST controller** and that *same*
`trace_id` correlates the **entire lifecycle** — every service, every async Kafka hop, every local
span — end to end.

```
POST /api/ping                         ← SERVER span. trace_id is BORN here (auto-instrumented).
   │  (same trace_id from here on ↓↓↓)
service-ping  publish ping.events      ← PRODUCER span, CHILD of the SERVER span
   │  W3C traceparent rides in Kafka headers
service-pong  process ping.events      ← CONSUMER span, CONTINUES the trace (parent = extracted ctx)
   │
service-pong  publish pong.events      ← PRODUCER span, CHILD of the consumer span
   │  W3C traceparent rides in Kafka headers
service-bang  process pong.events      ← CONSUMER span, CONTINUES the trace
```

**How it works (the three critical points, all commented in the code):**

1. **Birth** — Spring Boot + Micrometer auto-instrument the inbound HTTP request as a SERVER
   `Observation`; the Micrometer→OpenTelemetry bridge turns it into the **root span of a new trace**.
   That is the single `trace_id`. See `PingController` (it also returns the `traceId` in the response).
2. **Propagation** — on publish, `KafkaEventPublisher` opens a PRODUCER span **as a child of the
   current span** (never a new trace) and uses the Micrometer `Propagator` to **inject** the active
   W3C context (`traceparent`/`tracestate`) into the Kafka record headers. See `KafkaTracingSupport`.
3. **Continuation** — on receive, each listener uses `Propagator.extract(headers, GETTER)`, which
   returns a `Span.Builder` **whose parent is the extracted remote context**. Calling `.start()`
   therefore **continues the same trace** instead of starting a new one. This single line is what
   keeps one `trace_id` alive across the whole ping → pong → bang flow.

> **What changed vs. the old design.** Previously each async hop called `.setNoParent().addLink(...)`,
> so every service got its **own** `trace_id` and they were only joinable by a business
> `correlationId`. Now consumers **continue** the trace, so there is **one unbroken trace** and one
> `trace_id` for the whole request — exactly what the OTel spec prescribes.

**The business saga id is OTel Baggage now, not a body field.** Per the OTel model the `trace_id`
is the correlator; there is no bespoke "correlation id". For human convenience we still carry a
readable saga id, but the standard way: as **Baggage** (key `correlationId`), set once at the
controller. It rides inside the propagated trace context — including across the Kafka hop, since the
publisher injects it into the record headers — and is mirrored into the `correlationId` MDC field by
`management.tracing.baggage.correlation.fields`, so every service's logs show it with no manual
`MDC.put`. It is returned to the caller for convenience. `causationId` (which message caused this)
stays in the `EventEnvelope` as pure **domain** metadata — a business fact distinct from the trace's
own parent/child structure.

**Instrumentation is 100% Micrometer:**
- **Local spans** use Micrometer's `@Observed` (not OTel's `@WithSpan`). `ObservedAspect`
  (registered in each `ObservationConfig`) wraps the method in an `Observation`, which becomes both
  a **timer metric** (`@Observed(name=...)`) and a **child span** (`@Observed(contextualName=...)`)
  inside the current trace.
- **Messaging spans** use the Micrometer `Tracer` + `Propagator` beans (bridged to OTLP by
  `micrometer-tracing-bridge-otel`, shipped inside `spring-boot-starter-opentelemetry`).

**One trace, one query.** Because it is a single trace, you no longer stitch services together by
hand — open the trace once in Tempo and the whole ping → pong → bang waterfall (all three services)
is right there.

---

## 1. service_ping

Producer-only edge of the chain **and the origin of the trace**. Both endpoints publish the SAME
`PingCreated` event to the SAME `ping.events` topic — `/api/ping/fail` just sets `faulty=true` on it.
Consumes nothing.

**Span map (the head of the single trace)**

```
POST /api/ping                          ← SERVER span — trace_id created here
 └─ SendPingCommandHandler
     ├─ PingRepository.save             ← @Observed child span (~60–140ms sleep)
     ├─ PingAuditService.record         ← @Observed child span (~120–260ms sleep, main hazard)
     └─ publish ping.events             ← PRODUCER span (injects traceparent into Kafka headers)

POST /api/ping/fail                     ← SERVER span (failure-visualization path)
 └─ SendPingCommandHandler              (same handler, faulty=true)
     ├─ PingRepository.save             ← @Observed INTERNAL (same processing as happy path)
     ├─ PingAuditService.record         ← @Observed INTERNAL
     └─ publish ping.events             ← PRODUCER span, faulty=true (service_pong logs an ERROR)
```

**Traces (Tempo) — Explore → Tempo**

| Goal | TraceQL |
|---|---|
| All traces from this service | `{ resource.service.name = "service-ping" }` |
| HTTP entry span only | `{ resource.service.name = "service-ping" && name = "POST /api/ping" }` |
| Open ONE whole request end-to-end (all 3 services) | search by **Trace ID** = the `traceId` returned in the HTTP response |
| The slow internal work | `{ resource.service.name = "service-ping" && name = "PingAuditService.record" }` |
| Slow requests only | `{ resource.service.name = "service-ping" && duration > 200ms }` |
| A faulty ping's publish span | `{ resource.service.name = "service-ping" && name = "publish ping.events" }` (the fault rides this same topic) |

The response body returns both `traceId` (the single lifecycle correlator — paste it into Tempo) and
`correlationId` (business saga id). Every log line also prints `traceId`.

**`@Observed` functions**

| `@Observed` method | Span name (kind) | Metric (Micrometer timer) | Find / filter (Tempo TraceQL) |
|---|---|---|---|
| `PingRepository.save` | `PingRepository.save` (INTERNAL) | `ping.repository.save` | `{ name = "PingRepository.save" }` |
| `PingAuditService.record` | `PingAuditService.record` (INTERNAL) | `ping.audit.record` | `{ name = "PingAuditService.record" && duration > 200ms }` |

**Logs (Loki)**

| Goal | LogQL |
|---|---|
| All logs | `{service_name="service-ping"}` |
| One request's logs | `{service_name="service-ping"} |= "<traceId>"` |
| Errors only | `{service_name="service-ping"} | level="ERROR"` |

**Metrics (Prometheus)** — label `job="pingpong/service-ping"`

| Goal | PromQL |
|---|---|
| HTTP request rate | `rate(http_server_requests_seconds_count{job="pingpong/service-ping"}[1m])` |
| HTTP p95 latency | `histogram_quantile(0.95, sum by (le) (rate(http_server_requests_seconds_bucket{job="pingpong/service-ping"}[5m])))` |
| Audit-step p95 (the `@Observed` timer) | `histogram_quantile(0.95, sum by (le) (rate(ping_audit_record_seconds_bucket{job="pingpong/service-ping"}[5m])))` |
| JVM heap used | `jvm_memory_used_bytes{job="pingpong/service-ping", area="heap"}` |

---

## 2. service_pong

Middle link. Consumes `ping.events`, does `@Observed` work. For a normal event it publishes
`pong.events` (onward to service_bang); for a `faulty=true` event it logs an ERROR and stops (see §4).

**Span map (continues the same trace)**

```
process ping.events                     ← CONSUMER span, CONTINUES ping's trace (same trace_id)
 └─ HandlePingCommandHandler
     ├─ PongRepository.save             ← @Observed child span (~60–140ms sleep)
     ├─ PongProcessingService.process   ← @Observed child span (~120–260ms sleep, main hazard)
     └─ publish pong.events             ← PRODUCER span (hands the same trace to service_bang)

process ping.events (faulty=true)       ← CONSUMER span, CONTINUES ping's trace
 └─ HandlePingCommandHandler (faulty branch)
     ├─ PongProcessingService.process   ← @Observed child span
     └─ throws FaultSimulationException ← span status = ERROR + ERROR log line (with trace_id)
```

**Traces (Tempo)**

| Goal | TraceQL |
|---|---|
| All traces touching this service | `{ resource.service.name = "service-pong" }` |
| The happy-path consumer span | `{ resource.service.name = "service-pong" && name = "process ping.events" }` |
| Only this context | `{ span.ddd.bounded_context = "pong" }` |
| Slow internal processing | `{ name = "PongProcessingService.process" && duration > 200ms }` |
| Duplicates skipped | `{ resource.service.name = "service-pong" && event = "messaging.duplicate_detected" }` |
| The errored (faulty) spans | `{ resource.service.name = "service-pong" && name = "process ping.events" && status = error }` |

**`@Observed` functions**

| `@Observed` method | Span name (kind) | Metric (Micrometer timer) | Find / filter (Tempo TraceQL) |
|---|---|---|---|
| `PongRepository.save` | `PongRepository.save` (INTERNAL) | `pong.repository.save` | `{ name = "PongRepository.save" }` |
| `PongProcessingService.process` | `PongProcessingService.process` (INTERNAL) | `pong.processing.process` | `{ name = "PongProcessingService.process" }` |

**Logs (Loki)**

| Goal | LogQL |
|---|---|
| All logs | `{service_name="service-pong"}` |
| One request's logs | `{service_name="service-pong"} |= "<traceId>"` |
| Errors only (incl. the simulated fault) | `{service_name="service-pong"} | level="ERROR"` |

**Metrics (Prometheus)** — label `job="pingpong/service-pong"`

| Goal | PromQL |
|---|---|
| Kafka consumer health | `{job="pingpong/service-pong"}` (browse `spring_kafka_*` / `kafka_consumer_*`) |
| Processing-step p95 (`@Observed` timer) | `histogram_quantile(0.95, sum by (le) (rate(pong_processing_process_seconds_bucket{job="pingpong/service-pong"}[5m])))` |
| JVM heap used | `jvm_memory_used_bytes{job="pingpong/service-pong", area="heap"}` |

---

## 3. service_bang

Terminal link. Consumes `pong.events` and finalizes the flow; publishes nothing.

**Span map (the tail of the single trace)**

```
process pong.events                     ← CONSUMER span, CONTINUES the trace (same trace_id)
 └─ HandlePongCommandHandler
     ├─ PongRepository.save             ← @Observed child span (~60–140ms sleep)
     └─ PongProcessingService.process   ← @Observed child span (~120–260ms sleep, main hazard)
```

**Traces (Tempo)**

| Goal | TraceQL |
|---|---|
| All traces touching this service | `{ resource.service.name = "service-bang" }` |
| The consumer span | `{ resource.service.name = "service-bang" && name = "process pong.events" }` |
| Only this context | `{ span.ddd.bounded_context = "bang" }` |
| Slow internal processing | `{ name = "PongProcessingService.process" && duration > 200ms }` |
| Duplicates skipped | `{ resource.service.name = "service-bang" && event = "messaging.duplicate_detected" }` |

**`@Observed` functions**

| `@Observed` method | Span name (kind) | Metric (Micrometer timer) | Find / filter (Tempo TraceQL) |
|---|---|---|---|
| `PongRepository.save` | `PongRepository.save` (INTERNAL) | `bang.repository.save` | `{ name = "PongRepository.save" }` |
| `PongProcessingService.process` | `PongProcessingService.process` (INTERNAL) | `bang.processing.process` | `{ name = "PongProcessingService.process" }` |

**Logs (Loki)**

| Goal | LogQL |
|---|---|
| All logs | `{service_name="service-bang"}` |
| One request's logs | `{service_name="service-bang"} |= "<traceId>"` |
| Errors only | `{service_name="service-bang"} | level="ERROR"` |

**Metrics (Prometheus)** — label `job="pingpong/service-bang"`

| Goal | PromQL |
|---|---|
| Kafka consumer health | `{job="pingpong/service-bang"}` (browse `spring_kafka_*` / `kafka_consumer_*`) |
| Processing-step p95 (`@Observed` timer) | `histogram_quantile(0.95, sum by (le) (rate(bang_processing_process_seconds_bucket{job="pingpong/service-bang"}[5m])))` |
| JVM heap used | `jvm_memory_used_bytes{job="pingpong/service-bang", area="heap"}` |

---

## 4. Failure pipeline & back-tracing

A failure-visualization path that uses the **exact same request and event flow** as the happy path,
so you can practise finding an error log and back-tracing it to its origin. Because the trace is
continuous, back-tracing is a single click.

**Flow:** `POST /api/ping/fail` on service_ping → the same `SendPingCommandHandler` does the same
processing and publishes the same `PingCreated` event to **`ping.events`**, only flagged
`faulty=true` → service_pong's `PingEventListener` consumes it (**continuing the same trace**) →
`HandlePingCommandHandler` sees the flag and throws `FaultSimulationException` after its `@Observed`
work → the listener marks the span `status = error` and logs it at ERROR with the **`trace_id`,
`span_id` and service name**. Nothing is published onward, so the chain deliberately stops at the
pong error (service_bang is not involved). Happy-path pings on the very same topic sail through to
service_bang untouched.

**Trigger it**

`run-simulation.sh` / `run-simulation.ps1` drive `/api/ping` and `/api/ping/fail` concurrently at a
`-PingPerFail`:1 mix (default 4:1), so ERROR logs appear without any manual step. Both endpoints
return `202`; the failure surfaces asynchronously as a pong ERROR. To fire a single fault by hand:

```bash
curl -X POST http://localhost:8080/api/ping/fail \
  -H 'Content-Type: application/json' \
  -d '{"reason":"demo-error"}'
# -> {"status":"fault-dispatched","traceId":"<32-hex-trace-id>","correlationId":"service-ping.fault-saga.XXXXXXXX"}
```

Copy the returned **`traceId`** — that single id spans both the ping request and the pong failure.

**Step 1 — find the error log (Loki, Explore → Loki)**

```logql
{service_name="service-pong"} | level="ERROR"
```

The matching line reads
`Ping processing failed (simulated) service=service-pong traceId=... spanId=... saga=... eventId=... Simulated downstream failure ...`.
It is self-sufficient for back-tracing: it names **the service (where)**, the **`trace_id` (the whole
flow)**, the **`span_id` (the exact operation)**, and the **`saga` id** (the readable Baggage id).

**Step 2 — back-trace the whole flow (Loki → Tempo, one click)**

Click the **`traceId`** on that log line to jump into the trace in Tempo. Because ping and pong share
**one trace**, the waterfall shows the entire journey in a single view:

```
POST /api/ping/fail (service-ping, SERVER)
 ├─ PingRepository.save / PingAuditService.record (service-ping, INTERNAL)
 └─ publish ping.events [faulty=true] (service-ping, PRODUCER)
     └─ process ping.events (service-pong, CONSUMER)   ← status = error, exception recorded here
         └─ PongProcessingService.process (service-pong, INTERNAL)
```

No cross-trace stitching needed. If you prefer a query over a click:

```traceql
{ resource.service.name = "service-pong" && name = "process ping.events" && status = error }
```

**Step 3 — pull every log for that request across all services**

```logql
{service_name=~"service-.*"} |= "<traceId>"
```

That gives you the ping publish log and the pong error log (and any bang logs) for the exact request
— one `trace_id`, every pillar.

> Note: the `FaultSimulationException` is logged and swallowed (not rethrown) so each faulty ping
> produces exactly one clean ERROR — no Kafka redelivery storm. Only `FaultSimulationException` is
> swallowed; any other (real) exception is rethrown so Kafka can retry. Happy-path and faulty pings
> share the one `service-pong` consumer group and topic, differing only by the event's `faulty` flag.

---

## 5. Notes for clarity (no other docs needed)

- **`trace_id` is the one correlator.** It is created at the REST controller and continues, unbroken,
  through every Kafka hop and every local span, across all three services. Find it in the HTTP
  response, in every log line, and as the Tempo trace id.
- **service_name** isolates a service within that shared trace/logs/metrics: `service.name` (traces
  resource attr), `service_name` (Loki label), `job=pingpong/<name>` (Prometheus).
- **Micrometer everywhere.** Local spans come from `@Observed` (woven by `ObservedAspect`), messaging
  spans from the Micrometer `Tracer` + `Propagator`. The `micrometer-tracing-bridge-otel` inside
  `spring-boot-starter-opentelemetry` exports them as OTLP traces; `@Observed` also yields a timer
  metric per method (dots become underscores in Prometheus, e.g. `ping.audit.record` →
  `ping_audit_record_seconds`).
- **Kafka headers carry the propagated context** (`traceparent`/`tracestate` + the `baggage` header
  with the saga id), via `KafkaTracingSupport`. The readable saga id is Baggage (key `correlationId`);
  only `causationId` remains in the `EventEnvelope` body as domain metadata.
- **traceId/spanId in logs** are populated automatically by Micrometer Tracing whenever a span is in
  scope — that is why the log pattern's `%X{traceId}` / `%X{spanId}` are always filled.
- Metrics are **push** (OTLP every 5s) — there is no `/actuator/prometheus` scrape.
- A long `*ProcessingService.process` / `PingAuditService.record` span is the injected performance
  hazard, not a real outage. A `process ping.events` span with `status = error` is the injected
  (faulty=true) fault from §4, not a real fault.
