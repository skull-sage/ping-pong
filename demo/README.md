# Ping-Pong Distributed Tracing Demo

A runnable simulation of the requirements in [`../analysis/distributed_tracing.md`](../analysis/distributed_tracing.md):
event-driven, DDD/CQRS microservices on **Spring Boot 4** + **Kafka**, with the full **Grafana LGTM**
stack (Loki, Grafana, Tempo, Prometheus) receiving traces, logs, and metrics over OTLP.

## What it does

A linear **event chain**: `ping → pong → bang`.

```
 trigger_ping ──HTTP POST /api/ping──▶ service_ping ──publish ping.events──▶ Kafka
                                                                                │
                                                                                ▼
                                                                          service_pong        (consumes ping.events;
                                                                    new root trace, LINK back)  publishes pong.events)
                                                                                │ publish pong.events
                                                                                ▼
                                                                          service_bang        (consumes pong.events;
                                                                    new root trace, LINK back)  terminal — publishes nothing)
```

- One **`correlationId`** (the saga id) is minted at the HTTP entry and stays constant across every
  hop and every separate trace — it is the single value you search on to see the whole chain.
- **`causationId`** chains cause → effect: the ping event causes the pong; the pong event causes the bang step.
- Events use the **new-root-trace + link** model (CR-2), so async latency never distorts the producer's trace.

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

`domain` is pure (no Spring, no OpenTelemetry — OQ-2). All tracing lives in `infrastructure/messaging`.

## Prerequisites

- Docker + Docker Compose (Docker Desktop is fine)
- A JDK (21+) to run the trigger. No local Gradle needed — services build inside the Gradle image.

## 1. Start the stack

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
- **Follow a whole saga across traces** — use the TraceQL tab and query by the saga id:
  ```
  { span.messaging.message.conversation_id = "PASTE_correlationId_HERE" }
  ```
  (the `correlationId` is returned in the HTTP response and printed in the logs). You'll see the
  separate ping / pongA / pongB / fan-in traces, joined by **span links**.

**Logs (Loki)** — Explore → data source **Loki**:
- Query `{service_name="service-pong"}` (or any service). Each line carries `traceId`, `spanId`,
  `correlationId`, and `causationId` (CR-4). Click a `traceId` to jump straight to its trace.

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
| **CR-1** propagate on every broker hop | `KafkaEventPublisher` injects `traceparent`; listeners extract it (`KafkaTracingSupport`) |
| **CR-2** events = new root trace + link | `PingEventListener` / `PongEventListener` use `setNoParent().addLink(producerSpan)` |
| **CR-3** four ids on every messaging span + envelope | `TracingAttributes` stamped in publishers/listeners; ids carried in `EventEnvelope` body |
| **CR-4** three-pillar correlation | `traceId`/`spanId` + `correlationId`/`causationId` in the log MDC and OTLP logs |
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
