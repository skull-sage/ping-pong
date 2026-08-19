# Ping-Pong Distributed Tracing Demo

A runnable simulation of the requirements in [`../analysis/distributed_tracing.md`](../analysis/distributed_tracing.md):
event-driven, DDD/CQRS microservices on **Spring Boot 4** + **Kafka**, with the full **Grafana LGTM**
stack (Loki, Grafana, Tempo, Prometheus) receiving traces, logs, and metrics over OTLP.

## What it does

A linear **event chain**: `ping → pong → bang`.

```
 trigger_ping ──HTTP POST /api/ping──▶ service_ping ──publish ping.events──▶ Kafka
                                     (trace_id BORN here)                        │
                                                                                ▼
                                                                          service_pong        (consumes ping.events;
                                                                  CONTINUES the same trace)     publishes pong.events)
                                                                                │ publish pong.events
                                                                                ▼
                                                                          service_bang        (consumes pong.events;
                                                                  CONTINUES the same trace)     terminal — publishes nothing)
```

- A single **`trace_id`** is created when the request lands on service_ping's REST controller and
  **continues, unbroken, across every Kafka hop** — it is the one value that correlates the entire
  lifecycle (per the OpenTelemetry standard). Open the trace once in Tempo and all three services
  appear in one waterfall. The `trace_id` is returned in the HTTP response.
- A readable business **saga id** rides as OpenTelemetry **Baggage** (key `correlationId`), set once
  at the controller; it auto-propagates in the trace context (incl. over Kafka) and shows in every
  service's logs via MDC. It's for convenience only — the `trace_id` is the observability correlator.
  `causationId` (which message caused this) stays in the `EventEnvelope` as domain metadata.
- Consumers **continue** the trace (`Propagator.extract(...)` → parent context), so async latency is
  visible as one end-to-end trace instead of separate per-service traces.

## Layout (clean architecture per service)

```
demo/
├── common/                 # shared contract: EventEnvelope, TracingAttributes catalog, ReadableId, Topics
├── service_ping/           # HTTP entry + ping.events producer (chain start)
│   └── src/main/java/com/pingpong/ping/{domain,application,infrastructure,presentation}
├── service_pong/          # ping.events consumer + pong.events producer (chain middle)
├── service_bang/          # pong.events consumer — terminal, publishes nothing (chain end)
├── trigger_ping/Trigger.java   # dependency-free concurrent HTTP load trigger
├── Dockerfile              # shared multi-stage build (MODULE build-arg)
└── docker-compose.yml      # kafka + otel-lgtm + the 3 services
```

`domain` is pure (no Spring, no tracing types — OQ-2). All tracing lives in `infrastructure/messaging`
(Micrometer `Tracer` + `Propagator`) and in `@Observed`-annotated infrastructure services.

## Prerequisites

- Docker + Docker Compose (Docker Desktop is fine)
- A JDK 21+ (used by the Gradle wrapper to run the services locally, and to run the trigger)

## Quick start (one command, cross-platform)

Spins up everything: starts Kafka + Grafana LGTM containers, launches the three services locally via
the Gradle wrapper (messages flow over Kafka), opens the Grafana dashboard, then runs a continuous
trigger to generate live traffic. Ctrl+C stops the trigger; the stack keeps running.

```bash
# macOS / Linux
cd demo
./run-simulation.sh                         # defaults: 12 concurrent users, 200ms think time
./run-simulation.sh --concurrency 25 --think-ms 100
./run-simulation.sh --duration-sec 120      # auto-stop the trigger after 120s
```

```powershell
# Windows (PowerShell)
cd demo
./run-simulation.ps1
./run-simulation.ps1 -Concurrency 25 -ThinkMs 100
./run-simulation.ps1 -DurationSec 120
```

Shut everything down (services + containers):

```bash
./stop-simulation.sh      # macOS / Linux
./stop-simulation.ps1     # Windows
```

Services: `service_ping` :8080, `service_pong` :8081, `service_bang` :8082.
Grafana: http://localhost:3000 (anonymous admin, opens automatically).

> Prefer full containerization instead of local Gradle? Use the manual Docker path below.

## 1. Start the stack (manual / all-in-Docker)

From this `demo/` folder:

```bash
docker compose up --build
```

First run builds the three service images (downloads Gradle deps once) and pulls Kafka + otel-lgtm.
Wait until the three `service_*` containers log `Started ...Application`.

Services: `service_ping` :8080, `service_pong` :8081, `service_bang` :8082.
Grafana: http://localhost:3000 (anonymous admin, no login).

## 2. Fire pings (concurrent)

The trigger is a single Java file — run it directly:

```bash
cd trigger_ping
java Trigger.java                                   # 20 requests, 5 concurrent (defaults)
java Trigger.java --requests 200 --concurrency 20   # heavier concurrent load
java Trigger.java --url http://localhost:8080/api/ping --requests 50 --concurrency 10
```

- `--requests`     total number of pings to send
- `--concurrency`  number of parallel worker threads (adjust the simulation load here)
- `--url`          target endpoint (defaults to `http://localhost:8080/api/ping`)

Or a single manual ping:

```bash
curl -X POST http://localhost:8080/api/ping -H "Content-Type: application/json" -d '{"note":"hello"}'
```

## 3. View the results in Grafana

Open http://localhost:3000, then:

**Traces (Tempo)** — Explore → data source **Tempo**:
- **Search** tab: pick Service Name `service-ping` (or `service-pong` / `service-bang`), Run query,
  open a trace to see the waterfall. PRODUCER (publish) and CONSUMER (process) spans carry the
  `messaging.*`, `ddd.*`, `app.causation_id`, and `event.type` attributes.
- **Follow a whole request end-to-end** — grab the `traceId` from the HTTP response (or any log
  line) and search it by **Trace ID** in Tempo. Because it is a single continuous trace, the one
  waterfall shows ping → pong → bang across all three services (no cross-trace stitching needed).

**Logs (Loki)** — Explore → data source **Loki**:
- Query `{service_name="service-pong"}` (or any service). Each line carries the service name,
  `traceId`, `spanId`, `correlationId`, and `causationId`. Click a `traceId` to jump straight to its
  trace, or run `{service_name=~"service-.*"} |= "<traceId>"` to pull every service's logs for one
  request.

**Metrics (Prometheus)** — Explore → data source **Prometheus**:
- Try `http_server_requests_seconds_count` or `kafka_*` / `jvm_*` series exported by the services.

## 4. Stop / reset

```bash
docker compose down          # stop
docker compose down -v       # stop + wipe volumes
```

## Adjusting things

| Want to... | Do this |
|---|---|
| Increase simulation load | `java Trigger.java --requests N --concurrency C` |
| Change a service port | edit `server.port` in that service's `application.yml` and the compose `ports` |
| Point a locally-run service at the containers | set env `KAFKA_BOOTSTRAP=localhost:29092` and `OTLP_ENDPOINT=http://localhost:4318` |
| Run only infra (Kafka + Grafana) | `docker compose up kafka otel-lgtm` |

## How this maps to the requirements

| Requirement | Where it shows up |
|---|---|
| **CR-1** propagate on every broker hop | `KafkaEventPublisher` injects `traceparent` via Micrometer `Propagator`; listeners extract it (`KafkaTracingSupport`) |
| **CR-2** single trace_id for the whole lifecycle | listeners `Propagator.extract(...)` → parent context, so consumers **continue** the trace born at the REST controller |
| **CR-3** ids on every messaging span | `TracingAttributes` stamped in publishers/listeners; saga id via Baggage, `causationId` (domain) in `EventEnvelope` |
| **CR-4** three-pillar correlation | service name + `traceId`/`spanId` (auto) + `correlationId`/`causationId` in the log MDC and OTLP logs |
| **Local spans** via Micrometer | `@Observed` on repositories/services, woven by `ObservedAspect` (see `ObservationConfig`) |
| **DD-1 / DD-2** DDD tags + domain vs integration | `ddd.bounded_context`, `ddd.aggregate.*`, `message.category` on spans |
| **RC-4** idempotency / duplicate detection | pong listeners dedup on `messaging.message.id`, emit `messaging.duplicate_detected` |
| **OQ-2** clean-architecture separation | `domain` imports no OTel/Spring; tracing lives only in `infrastructure` adapters |
| **OQ-4** shared semantic-convention catalog | single `common/TracingAttributes` class used by all services |

## Notes & production deltas (kept out of the demo for simplicity)

- Sampling is 100% here. In production use head sampling at the producer + **tail sampling** at the
  collector to always keep errors and slow traces (OQ-1, §17).
- The `otel-lgtm` image bundles a pre-wired collector; a standalone collector with tail-sampling and
  PII scrubbing (OQ-5) is shown in `distributed_tracing.md` §17.
- Idempotency uses an in-memory set; production uses a persistent inbox table (§8.5–§8.6).
