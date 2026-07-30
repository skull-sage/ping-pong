
# OpenTelemetry for all three pillars (Metrics, Logging, Tracing)

In the Java and Spring Boot ecosystem, you typically have two main ways to approach this:

1. **Zero-Code Instrumentation (Recommended):** You run the official **OpenTelemetry Java Agent** (`opentelemetry-javaagent.jar`) attached to your JVM. It intercepts HTTP requests, Spring MVC controllers, database calls (JDBC/Hibernate), and Logback loggers at runtime. It automatically attaches `trace_id` and `span_id` to your logs, converts Micrometer metrics to OTLP, and handles context propagation seamlessly.
2. **Library / Starter-Based Instrumentation:** You use Spring Boot’s native Observability stack (**Micrometer + Micrometer Tracing**) and configure it to export via the **OTLP (OpenTelemetry Protocol)** exporter.

---

## Code Example: Spring Boot 3 & OpenTelemetry

The standard practice in Java is using the **Java Agent** because it avoids boilerplate code and works across all Spring libraries seamlessly.

Here is a standard Spring Boot application configured to emit traces, logs, and custom metrics using the OpenTelemetry API and Micrometer.

### 1. Dependencies (`pom.xml`)

No special OpenTelemetry dependencies are required for base automatic instrumentation if you use the Java Agent. You only need standard Spring Boot Starter Web and `micrometer-observation` (or Spring Boot Actuator) if you want custom application metrics.

```xml
<dependencies>
    <!-- Standard Spring Boot Web -->
    <dependency>
        <groupId>org.springframework.boot</groupId>
        <artifactId>spring-boot-starter-web</artifactId>
    </dependency>

    <!-- Spring Boot Actuator for application metrics -->
    <dependency>
        <groupId>org.springframework.boot</groupId>
        <artifactId>spring-boot-starter-actuator</artifactId>
    </dependency>

    <!-- OpenTelemetry API for creating custom spans manually (Optional) -->
    <dependency>
        <groupId>io.opentelemetry</groupId>
        <artifactId>opentelemetry-api</artifactId>
    </dependency>
</dependencies>

```

### 2. Java Sample Code (`OrderController.java`)

Spring Boot logs will automatically include `trace_id` and `span_id` inserted into the MDC context by the agent.

```java
package com.example.demo;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.opentelemetry.instrumentation.annotations.WithSpan;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/orders")
public class OrderController {

    private static final Logger log = LoggerFactory.getLogger(OrderController.class);
    private final Counter orderCounter;

    public OrderController(MeterRegistry meterRegistry) {
        // Custom Metric: OpenTelemetry Agent captures Micrometer metrics & sends via OTLP
        this.orderCounter = meterRegistry.counter("orders.created.total");
    }

    @PostMapping("/{id}")
    public String createOrder(@PathVariable String id) {
        // Log line: automatically annotated with trace_id and span_id by OTel
        log.info("Processing order request for ID: {}", id);
        
        orderCounter.increment();
        
        processBusinessLogic(id);

        return "Order " + id + " created successfully";
    }

    // Custom Span: Automatically tracked as an internal span in the distributed trace
    @WithSpan
    private void processBusinessLogic(String orderId) {
        log.info("Executing database & payment logic for order {}", orderId);
        try {
            Thread.sleep(100); // Simulate work
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}

```

### 3. Logback Configuration (`src/main/resources/logback-spring.xml`)

Format your logs as JSON and include the `trace_id` and `span_id` injected into MDC.

```xml
<?xml version="1.0" encoding="UTF-8"?>
<configuration>
    <appender name="CONSOLE" class="ch.qos.logback.core.ConsoleAppender">
        <encoder>
            <pattern>
                %d{yyyy-MM-dd HH:mm:ss.SSS} [%thread] %-5level %logger{36} - [traceId=%X{trace_id} spanId=%X{span_id}] - %msg%n
            </pattern>
        </encoder>
    </appender>

    <root level="INFO">
        <appender-ref ref="CONSOLE" />
    </root>
</configuration>

```

---

### How to Run

Download the official OpenTelemetry Java Agent JAR:

```bash
wget https://github.com/open-telemetry/opentelemetry-java-instrumentation/releases/latest/download/opentelemetry-javaagent.jar

```

Execute your compiled JAR while attaching the javaagent and directing the telemetry signal output (metrics, logs, traces) to an OpenTelemetry Collector:

```bash
java -javaagent:opentelemetry-javaagent.jar \
     -Dotel.service.name=order-service \
     -Dotel.exporter.otlp.endpoint=http://localhost:4317 \
     -Dotel.exporter.otlp.protocol=grpc \
     -Dotel.logs.exporter=otlp \
     -Dotel.metrics.exporter=otlp \
     -Dotel.traces.exporter=otlp \
     -jar target/demo-0.0.1-SNAPSHOT.jar

```

When you hit the `/orders/123` endpoint:

1. **Trace:** A trace is started at the HTTP level and cascaded into `processBusinessLogic`.
2. **Log:** Logback outputs log lines annotated with `trace_id` and `span_id`.
3. **Metric:** Micrometer increments `orders.created.total`, and OTel batches/exports the metric stream via OTLP.

## Zero-Code Instrumentation

**Zero-code instrumentation** (also known as automatic instrumentation) allows you to collect traces, metrics, and logs from a Spring Boot application **without modifying or recompiling its source code**.

Instead of adding SDK dependencies, writing boilerplate span code, or manually wiring up exporters, zero-code instrumentation relies on external tooling to observe your app at runtime.

---

### 1. How It Works Under the Hood

The underlying mechanism in Java relies on **Bytecode Manipulation** using the standard Java Instrumentation API (`java.lang.instrument`).

1. **Agent Attachment:** You attach the `opentelemetry-javaagent.jar` to the Java Virtual Machine (JVM) using the `-javaagent` startup flag.
2. **Bytecode Interception:** When the JVM loads compiled `.class` files into memory, the agent intercepts class loading.
3. **Dynamic Patching:** The agent inspects known libraries (such as Spring MVC, Tomcat, HikariCP, JDBC drivers, OkHttp) and dynamically injects telemetry collection hooks into method entry and exit points.
4. **Context Propagation & Export:** The agent injects W3C trace headers into outgoing HTTP/gRPC requests and exports telemetry via OTLP directly to a backend or OpenTelemetry Collector.

---

### 2. What Gets Instrumented Automatically

When you run a Spring Boot application with the OpenTelemetry Java Agent, it automatically instruments the surrounding framework ecosystem:

* **Inbound & Outbound HTTP Calls:**
* Spring Web (`@RestController`, `@RequestMapping`)
* Spring WebFlux & Reactive handlers
* `RestTemplate`, `WebClient`, Apache HttpClient, OkHttp

* **Database & Persistence:**
* JDBC calls, JPA / Hibernate, HikariCP connection pools
* NoSQL drivers (Redis, MongoDB, Cassandra)

* **Messaging & Async Queues:**
* Spring Kafka, RabbitMQ, JMS queues
* `@Async` annotated methods

* **Logs & Correlation:**
* Intercepts SLF4J / Logback logs and dynamically injects `trace_id` and `span_id` into the MDC context

---

### 3. How to Deploy It

#### Option A: Direct JVM Startup Command

Download the agent JAR and pass configuration properties via system properties (`-D`) or environment variables (`OTEL_*`):

```bash
# Download the agent jar
curl -L https://github.com/open-telemetry/opentelemetry-java-instrumentation/releases/latest/download/opentelemetry-javaagent.jar -o opentelemetry-javaagent.jar

# Run application with agent attached
java -javaagent:opentelemetry-javaagent.jar \
     -Dotel.service.name=order-service \
     -Dotel.exporter.otlp.endpoint=http://otel-collector:4318 \
     -Dotel.exporter.otlp.protocol=http/protobuf \
     -jar target/order-service-0.0.1-SNAPSHOT.jar

```

#### Option B: Containerized / Docker Deployment

Pack the agent directly inside your Docker container image:

```dockerfile
FROM eclipse-temurin:21-jre
WORKDIR /app

# Download OpenTelemetry Agent
ADD https://github.com/open-telemetry/opentelemetry-java-instrumentation/releases/latest/download/opentelemetry-javaagent.jar /app/opentelemetry-javaagent.jar

COPY target/app.jar app.jar

ENV OTEL_SERVICE_NAME="payment-service"
ENV OTEL_EXPORTER_OTLP_ENDPOINT="http://otel-collector:4318"

ENTRYPOINT ["java", "-javaagent:/app/opentelemetry-javaagent.jar", "-jar", "app.jar"]

```

#### Option C: Kubernetes (Zero-Touch via OpenTelemetry Operator)

If you deploy microservices to Kubernetes, the **OpenTelemetry Operator** can automatically inject the Java Agent into your pods via sidecars or init containers without you changing Dockerfiles or startup commands.

You simply add an annotation to your Kubernetes `Deployment`:

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: user-service
  annotations:
    # Automatically injects Java OTel Agent into pod
    instrumentation.opentelemetry.io/inject-java: "true"
spec:
  template:
    ...

```

---

### 4. Pros and Cons of Zero-Code Instrumentation

#### Advantages

* **Instant Time-to-Value:** Instrument legacy or third-party Spring apps in minutes without touch-editing codebase files.
* **Separation of Concerns:** Developers don't need to maintain observability code or update SDK versions inside application `pom.xml` or `build.gradle`.
* **Consistency:** Ensures standard span names, trace IDs, and HTTP/database metrics are formatted uniformly across all services.

#### Limitations & Trade-Offs

* **Overhead:** Slight startup delay (~50–100ms) and modest memory overhead (~50–150MB heap) due to dynamic class transformation.
* **Lack of Domain Business Context:** The agent captures low-level framework operations (e.g., `SELECT * FROM orders`), but won't capture domain-specific data (e.g., `user.tier = VIP` or shopping cart item counts) without hybrid manual annotations like `@WithSpan`.
* **GraalVM Native Image Incompatibility:** The Java Agent relies on dynamic bytecode manipulation at runtime, which is incompatible with GraalVM Ahead-Of-Time (AOT) native compilation (if using Spring Boot Native Images, you should use the OpenTelemetry Spring Boot starter or native Micrometer tracing instead).

---