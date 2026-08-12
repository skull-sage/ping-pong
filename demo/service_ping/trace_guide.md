# service_ping — Trace / Log / Metric Guide

Everything below is emitted **by this service only**. It is identified everywhere in Grafana by its
resource attribute **`service.name = service-ping`** (set from `spring.application.name` in
`application.yml`) and **`service.namespace = pingpong`**. All three pillars are pushed over **OTLP**
to the Grafana LGTM container (`OTLP_ENDPOINT`, default `http://localhost:4318`):

| Pillar | Backend | How it is produced |
|---|---|---|
| Traces | Tempo | Micrometer/OTel auto SERVER span + manual PRODUCER span + `@WithSpan` INTERNAL spans |
| Logs | Loki | logback `OpenTelemetryAppender` (OTLP); trace id + `correlationId`/`causationId` in MDC |
| Metrics | Prometheus | `micrometer-registry-otlp` push every 5s |

## 1. What this service does (span map)

```
HTTP POST /api/ping                     ← SERVER span  (trace starts here)
 └─ SendPingCommandHandler
     ├─ PingRepository.save             ← @WithSpan INTERNAL span (~60–140ms sleep)
     ├─ PingAuditService.record         ← @WithSpan INTERNAL span (~120–260ms sleep, main hazard)
     └─ publish ping.events             ← PRODUCER span (injects traceparent)
```

- Produces to topic **`ping.events`**. This service does **not** consume any topic (it is a
  producer-only edge of the flow); the pong services consume `ping.events` and publish `pong.events`.
- The saga id **`correlationId`** stays constant across every hop; **`causationId`** chains cause→effect.

## 2. Traces (Tempo) — Explore → Tempo

| Goal | Query type | Query |
|---|---|---|
| All traces from this service | TraceQL | `{ resource.service.name = "service-ping" }` |
| The HTTP entry span only | TraceQL | `{ resource.service.name = "service-ping" && name = "POST /api/ping" }` |
| Follow ONE whole saga across services/traces | TraceQL | `{ span.messaging.message.conversation_id = "<correlationId>" }` |
| Find the slow internal work | TraceQL | `{ resource.service.name = "service-ping" && name = "PingAuditService.record" }` |
| Slow requests only | TraceQL | `{ resource.service.name = "service-ping" && duration > 200ms }` |

The `<correlationId>` is returned in the HTTP response body and printed in every log line for the flow.

## 3. `@WithSpan` functions in this service

| `@WithSpan` function | Span name (kind) | OTel specifics used | Find / filter in Grafana (Tempo TraceQL) |
|---|---|---|---|
| `PingRepository.save` | `PingRepository.save` (INTERNAL) | Woven by `WithSpanAspect` → `Tracer.spanBuilder(name).setSpanKind(INTERNAL)`; simulates DB write latency | `{ name = "PingRepository.save" }` |
| `PingAuditService.record` | `PingAuditService.record` (INTERNAL) | Same aspect; longer sleep = the dominant latency contributor | `{ name = "PingAuditService.record" }` or `{ name = "PingAuditService.record" && duration > 200ms }` |

> `@WithSpan` here is `io.opentelemetry.instrumentation.annotations.WithSpan`. There is **no Java
> agent**; a Spring-AOP aspect (`WithSpanAspect`) turns each annotated call into an INTERNAL span
> using the SDK `OpenTelemetry` bean. The span name is the annotation value.

## 4. Logs (Loki) — Explore → Loki

| Goal | LogQL |
|---|---|
| All logs from this service | `{service_name="service-ping"}` |
| One saga's logs | `{service_name="service-ping"} |= "<correlationId>"` |
| Errors only | `{service_name="service-ping"} | level="ERROR"` |

Each line carries `traceId`, `spanId`, `correlationId`, `causationId` (MDC). Click a `traceId` in a
log line to jump straight to the trace in Tempo.

## 5. Metrics (Prometheus) — Explore → Prometheus

This service pushes Micrometer metrics via OTLP; in Prometheus they carry the label
**`job="pingpong/service-ping"`** (i.e. `service.namespace/service.name`).

| Goal | PromQL |
|---|---|
| HTTP request rate | `rate(http_server_requests_seconds_count{job="pingpong/service-ping"}[1m])` |
| HTTP p95 latency | `histogram_quantile(0.95, sum by (le) (rate(http_server_requests_seconds_bucket{job="pingpong/service-ping"}[5m])))` |
| JVM heap used | `jvm_memory_used_bytes{job="pingpong/service-ping", area="heap"}` |
| CPU usage | `process_cpu_usage{job="pingpong/service-ping"}` |

## 6. Notes for clarity (no other docs needed)

- **service_name** is the single key that isolates this service in every pillar: `service.name`
  (traces resource attr), `service_name` (Loki label), `job=pingpong/service-ping` (Prometheus).
- To exemplar-pivot: a metric spike → open a matching trace (share `correlationId`) → filter Loki
  by that `correlationId`.
- Metrics are **push** (OTLP every 5s), not scraped — there is no `/actuator/prometheus` scrape here.
- If a trace shows a long `PingAuditService.record` span, that is the injected performance hazard,
  not a real outage.
