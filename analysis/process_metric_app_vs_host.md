You've changed direction, so let me stop the in-progress image pull first (layers are cached, so you can resume `docker compose up` later without re-downloading).

Good question. There are two distinct layers to "CPU and RAM per application," and OTel handles them differently. Let me lay out both, then give you something concrete you can run without waiting on containers.

## The two layers

**1. App-reported (from inside the JVM) — you already have this**

Your Spring services include `micrometer-registry-otlp`, so each one already pushes its own runtime metrics over OTLP to the LGTM stack:

- `process.cpu.usage` — this JVM process's CPU share (0–1)
- `system.cpu.usage` — whole-machine CPU as the JVM sees it
- `jvm.memory.used` / `jvm.memory.committed` / `jvm.memory.max` (tagged by `area=heap|nonheap`, `id=<pool>`)
- `jvm.threads.live`, `jvm.gc.pause`, etc.

This is the most accurate view of *heap/GC* behavior, and it's labeled with `service.name`, so per-application slicing is automatic. Limitation: it reports JVM-internal memory, not the OS resident set (RSS), and only covers apps you instrument.

**2. Host/process-reported (from the OS) — the OTel-native way to see *any* app**

The OpenTelemetry Collector's `hostmetrics` receiver has a `process` scraper that reads the OS process table and emits per-process:

- `process.cpu.time` (counter, by `state=user|system`) → rate it for CPU %
- `process.cpu.utilization` (0–1, must be enabled)
- `process.memory.usage` (RSS bytes)
- `process.memory.virtual`
- `process.threads`, `process.disk.io`, `process.open_file_descriptors`

This works for *every* process on the machine (Java, Node, Postgres, anything), not just instrumented ones.

## Important Windows/Docker caveat

You're on Windows with Docker Desktop. A **containerized** collector runs inside the Linux VM, so its `hostmetrics` would report the VM's processes, not your Windows host apps. To track apps running on the **Windows machine**, run the collector as a **native Windows binary** (`otelcol-contrib.exe`), not in a container. It still ships metrics over OTLP to the `otel-lgtm` container on `localhost:4318`.

## Concrete setup (native collector on Windows)

Download `otelcol-contrib` (the `hostmetrics` process scraper is in the *contrib* distribution, not core), then use this config:

```yaml
# hostmetrics-collector.yaml
receivers:
  hostmetrics:
    collection_interval: 10s
    scrapers:
      cpu:            # whole-machine CPU
      memory:         # whole-machine RAM
      load:
      process:        # per-application CPU + RAM
        mute_process_name_error: true
        # optional: only scrape the apps you care about
        include:
          match_type: regexp
          names: ["java.exe", "node.exe"]
        metrics:
          process.cpu.utilization:
            enabled: true
          process.memory.utilization:
            enabled: true

processors:
  batch: {}
  resourcedetection:
    detectors: [system]   # stamps host.name so you can filter by machine

exporters:
  otlphttp:
    endpoint: http://localhost:4318   # your running otel-lgtm container

service:
  pipelines:
    metrics:
      receivers: [hostmetrics]
      processors: [resourcedetection, batch]
      exporters: [otlphttp]
```

Run it:

```powershell
otelcol-contrib.exe --config hostmetrics-collector.yaml
```

Metrics flow into the LGTM stack's Prometheus, so no change to your services is needed. Note: some Windows processes (system/other-user) require the collector to run elevated to read their CPU/memory; your own apps are fine unprivileged.

## Viewing in Grafana

Explore → Prometheus:

- Per-app RAM (RSS): `process_memory_usage_bytes` — group/legend by `process_executable_name` or `process_command_line`
- Per-app CPU %: `rate(process_cpu_time_seconds_total[1m])` by process, or `process_cpu_utilization` if enabled
- JVM heap per service (from Micrometer): `jvm_memory_used_bytes{area="heap"}` by `service_name`
- JVM process CPU per service: `process_cpu_usage` by `service_name`

(Exact metric names depend on the OTLP→Prometheus naming; dots become underscores and counters get a `_total` suffix.)

## Which should you use?

- Want deep JVM/GC/heap insight for your three services → the Micrometer path you already have.
- Want a uniform "how much CPU/RAM is each app on this box using" view (including non-JVM apps) → add the native `hostmetrics` collector.
- Best coverage → run both; they answer different questions and share the same Grafana.
 