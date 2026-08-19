# Demo Troubleshooting Log

A record of the issues that consumed the most time while preparing this distributed-tracing demo
(OpenTelemetry → Micrometer refactor + single `trace_id` correlation), what actually caused them,
how they were resolved, and how to avoid them next time.

The single biggest lesson: **most of the lost time was environment/tooling noise, not application
bugs.** Stale processes, Kafka replay, Windows PowerShell quirks, and a slow Grafana repeatedly made
correct code *look* broken and sent us chasing ghosts.

---

## Quick reference

| # | Issue | Symptom | Root cause | Fix |
|---|-------|---------|-----------|-----|
| 1 | Runaway / stale processes | Every test hit old code; "all pings fail"; false health checks | An old continuous `Trigger.java` (no `--duration-sec`) + stale `bootRun` services held ports 8080-8082; new launches couldn't bind and died silently | Kill by port/《command-line》 + record PIDs in `.sim_pids`; `stop-simulation.ps1` now tree-kills |
| 2 | Kafka message replay | Consumer counts >> messages sent; fresh messages "stuck" | `auto-offset-reset: earliest` + Kafka container reused across runs → full topic replay on restart | Recreate the Kafka container (no volume mount ⇒ fresh = empty) for clean tests |
| 3 | `docker compose down -v` didn't wipe | Replay persisted after a "wipe" | A lingering externally-created `pingpong-lgtm` container caused a name conflict that aborted the compose command | Force-remove containers explicitly (`docker rm -f`) before recreating |
| 4 | Grafana "stuck / not healthy" | Launcher aborted on "Grafana did not become healthy"; page shows "isn't working" | `:latest` = otel-lgtm v0.30 with **Grafana 13**, whose SQLite startup takes ~5 min on Windows Docker Desktop (WSL2 I/O-bound, *not* RAM) — far past the wait window | Pin `grafana/otel-lgtm:0.8.1` (Grafana 11, fast); make the launcher **non-blocking** on the Grafana UI |
| 5 | Grafana hard-fail blocked everything | "all pings fail" | Launcher `exit 1` on the Grafana gate ran **before** starting the services → nothing listening | Gate only on Kafka + OTLP collector; Grafana UI is best-effort (warn & continue) |
| 6 | Wrong Grafana readiness signal | False "healthy" and false "down" | Docker HEALTHCHECK reports healthy *before* Grafana HTTP binds; PowerShell `Invoke-WebRequest` reports false "connection closed" against Grafana | Probe with `curl.exe /api/health == 200` |
| 7 | PowerShell native-stderr abort | Script died at `docker compose ...` | Docker writes progress to stderr; under `$ErrorActionPreference='Stop'` that became terminating | `$PSNativeCommandUseErrorActionPreference=$false`; don't pipe the script with `2>&1` |
| 8 | PowerShell 5.1 syntax / capture quirks | Commands silently no-op or error | `if` used as an inline expression (7+ only); multi-line inline scripts truncated in the console | Write helper `.ps1` files and read output from a file |
| 9 | No service logs on Windows | Couldn't see startup/consumption/errors | `run-simulation.ps1` launched services in separate windows with no captured output | Launch headless, redirect to `run_<svc>.log`; surface the tail on health-check failure |
| 10 | Independent `correlationId` vs `trace_id` | Two competing correlation keys (design confusion) | Legacy design minted a business `correlationId` unrelated to the trace | `trace_id` is the sole correlator; readable saga id moved to **OTel Baggage** |
| 11 | Fault scenario on a divergent path | `/ping/fail` never exercised the real pipeline | It used a separate `ping.faults` topic + `FaultEventListener` | Unified: `/ping/fail` rides the same `ping.events` flow with a `faulty` flag; pong logs the error |
| 12 | Envelope schema change risk | Potential consumer deserialization failure | Removing `correlationId` from `EventEnvelope` would break older JSON still in Kafka | Consumers set `FAIL_ON_UNKNOWN_PROPERTIES=false` |
| 13 | Stale IDE diagnostics | `PING_FAULTS cannot be resolved` errors | Stale language-server index (code compiled fine) | Ignored; verified via `gradlew compileJava` |

---

## Detailed notes

### A. Environment & process management (the biggest time sink)

- **Runaway trigger + stale services (Issue 1).** A previous run left a `Trigger.java` process running
  *without* `--duration-sec` (default concurrency 12), plus three `bootRun` service JVMs, all still
  alive and holding ports 8080-8082. Because the ports were taken, freshly launched (refactored)
  services failed to bind and exited — but the health checks still returned `200` from the **old**
  processes. Net effect: hours of testing were unknowingly run against stale code, and traffic from
  the runaway trigger (id counters in the hundreds of thousands) drowned out our test requests.
  - *Tell-tale signs we should have caught sooner:* log lines with an old message format, event ids
    far higher than what our short runs could produce, and consumer counts wildly exceeding sends.
  - *Fixes:* `stop-simulation.ps1` now reads `.sim_pids` and `taskkill /T`, plus a command-line sweep
    for `bootRun` / `com.pingpong.` JVMs; `run-simulation.ps1` records launched PIDs.

- **Kafka replay (Issues 2-3).** The consumer groups use `auto-offset-reset: earliest`, and Kafka
  retained messages across runs. On restart, consumers replayed the entire backlog, so counts were
  meaningless and freshly published test messages sat behind thousands of old ones. A `docker compose
  down -v` appeared to wipe volumes but silently aborted because a lingering `pingpong-lgtm`
  container name conflicted. Kafka in this compose file has **no volume mount**, so the reliable
  reset is to remove the Kafka *container* (`docker rm -f pingpong-kafka`) and recreate it.

### B. Windows / PowerShell tooling friction (Issues 6-8)

- `Invoke-WebRequest` returns "The underlying connection was closed" against Grafana even when it is
  up — a false negative. `curl.exe` is reliable; use it for HTTP health probes.
- Docker Compose prints progress to **stderr**. With `$ErrorActionPreference='Stop'`, and especially
  when the script's output is piped with `2>&1`, that stderr became a terminating error and killed
  the launcher. Set `$PSNativeCommandUseErrorActionPreference = $false` and avoid `2>&1` capture.
- The shell is **Windows PowerShell 5.1**: `if(){}else{}` is not a valid inline expression, and long
  multi-line inline scripts were frequently truncated/garbled in the console. Writing a small `.ps1`
  file and reading results back from a file was the only consistently reliable pattern.

### C. Docker / Grafana startup (Issues 4-6) — and why macOS was fine

- `image: grafana/otel-lgtm:latest` resolved on Windows to **v0.30.x bundling Grafana 13**. Grafana
  13's unified-storage/SQLite startup migrations are heavy and, on Docker Desktop for Windows (WSL2),
  are **disk-I/O bound** — it took ~5 minutes to bind port 3000 (confirmed: `docker stats` showed
  only ~250 MiB used of 31 GiB, so it was *not* a memory problem). Prometheus/Loki/Tempo/OTelcol all
  came up in seconds; only Grafana lagged.
- The launcher then **hard-failed** on the Grafana gate and `exit 1`'d *before* launching the
  services — which is why the same incident also presented as "all pings fail" (nothing was
  listening).
- **Two wrong turns before the right fix:** (a) waiting on the Docker HEALTHCHECK — it reports
  `healthy` *before* Grafana's HTTP is actually up (false positive); (b) `Invoke-WebRequest` probing —
  false "connection closed" (false negative).
- **Final fix:** gate startup only on **Kafka (29092) + the OTLP collector (4318)** — the only things
  the services need, both up in seconds — and treat the **Grafana UI as best-effort** (probe with
  `curl`, warn and continue). Also pin `grafana/otel-lgtm:0.8.1` (Grafana 11, starts fast, and
  multi-platform so it behaves the same on Windows and Apple-silicon Macs).
- **Why `run-simulation.sh` on macOS never hit this:** (1) `:latest` is a moving tag — the earlier
  Mac run pulled a lighter Grafana (11/12); the recent Windows run got Grafana 13. (2) Container
  filesystem I/O is faster on macOS/Apple-silicon virtualization than on Windows WSL2, so Grafana
  finished within the 90 s window. (3) Both scripts had the *same* 90 s hard-fail — Mac just never
  tripped it. (4) `.sh` used `curl` (reliable) whereas `.ps1` used `Invoke-WebRequest` (false fails).

### D. Observability & design decisions (Issues 10-12)

- **Correlation model.** Per the OpenTelemetry traces spec, cross-service correlation is the job of
  the propagated `trace_id`; there is no "correlation id" concept. We removed the independently
  minted `correlationId` and made `trace_id` the sole correlator. A human-readable saga id is carried
  the OTel-native way, as **Baggage** (key `correlationId`), set once at the controller — it
  propagates in the trace context (incl. across Kafka) and mirrors into the `correlationId` MDC field.
- **Fault scenario.** The original `/ping/fail` published a `FaultRequested` event to a *separate*
  `ping.faults` topic consumed by a *separate* `FaultEventListener` — a divergent path that never
  exercised the real `ping → pong → bang` flow. It now rides the **same** `ping.events` flow with a
  `faulty=true` flag; pong runs the same processing and then logs an ERROR (recorded on the span,
  same `trace_id`) instead of responding onward. Removed `ping.faults`, `FaultEventListener`,
  `HandleFaultCommand(Handler)`, `TriggerFaultCommand(Handler)`, and `FaultRequested`.
- **Schema safety.** Removing `correlationId` from `EventEnvelope` could break deserialization of
  older JSON still sitting in Kafka, so consumers now disable `FAIL_ON_UNKNOWN_PROPERTIES`.

### E. IDE noise (Issue 13)

- The language server repeatedly flagged `Topics.PING_FAULTS cannot be resolved` even though the
  field existed and `gradlew compileJava` succeeded — a stale index artifact. Trust the compiler over
  transient IDE diagnostics.

---

## Verification approach that finally worked

1. **Clean slate:** kill all project JVMs, recreate the Kafka container (empty), start the three
   refactored services and confirm health on 8080/8081/8082.
2. **Fresh, single requests** (never a replay): the controller returns the `traceId`; grep all three
   `run_service_*.log` files for that exact id.
3. **Result:** one `trace_id` + one Baggage saga id flow `ping → pong → bang` for normal pings; a
   faulty ping appears in `ping + pong` (pong logs the error) but **not** `bang`. Trigger shows a
   correct 3:1 send ratio with `0` HTTP failures.

---

## Prevention checklist (do this first next time)

- [ ] Before any run/verify: confirm ports 8080-8082 are free and no stray `Trigger`/`bootRun` JVMs
      exist (`stop-simulation.ps1` handles this).
- [ ] For deterministic tests, recreate the Kafka container so there is no replay backlog.
- [ ] Use `curl.exe` (not `Invoke-WebRequest`) for HTTP health checks on Windows.
- [ ] Never block startup on the Grafana UI; gate on Kafka + OTLP collector only.
- [ ] Pin infra image versions — avoid `:latest` for reproducibility across machines.
- [ ] Capture service stdout to log files so "nothing shows up" is diagnosable.
- [ ] For multi-line PowerShell, put it in a `.ps1` file and read results from a file.
- [ ] Verify with a **fresh single request** and its returned `trace_id`, not with aggregate counts.
