### What is a Span in Distributed Tracing?

In distributed tracing, a **Span** represents a single, contiguous unit of work or time within a system.

When a single user request enters a microservices system, it triggers a chain of operations across multiple services, databases, and message queues. The entire journey of that request is called a **Trace**.

A **Trace** is essentially a directed acyclic graph (DAG) made up of individual **Spans**.

```text
[Trace: User Checkout Request (ID: 4bf92f35...)]
│
├── [Span 1: HTTP POST /checkout] (API Gateway) — duration: 250ms
│   │
│   ├── [Span 2: HTTP POST /payment] (Payment Service) — duration: 180ms
│   │   └── [Span 3: SELECT FROM accounts] (DB) — duration: 20ms
│   │
│   └── [Span 4: Publish Event "order.created"] (Kafka) — duration: 30ms

```

#### What Data Does a Span Contain?

Every span contains metadata critical for diagnosing performance issues and debugging errors:

* **Name:** A human-readable identifier (e.g., `POST /checkout` or `SELECT FROM accounts`).
* **Trace ID:** A unique string identifying the entire request chain across all services.
* **Span ID:** A unique identifier for that specific operation.
* **Parent Span ID:** Identifies which span initiated this operation (enables the parent-child tree hierarchy).
* **Timestamps:** Start time and end time (from which total duration is calculated).
* **Attributes / Tags:** Custom key-value pairs carrying contextual context (e.g., `http.status_code = 200`, `db.statement = "SELECT..."`, `user.id = 4821`).
* **Events / Logs:** In-line timestamps for events within the span (e.g., exception stack traces).
* **Status:** `OK`, `UNSET`, or `ERROR`.

---

### Understanding `@WithSpan`

While automatic instrumentation (such as the OpenTelemetry Java Agent) automatically creates spans for inbound HTTP endpoints, database queries, and outgoing REST requests, it **cannot know your application's internal business domain logic**.

The `@WithSpan` annotation allows you to create **custom, in-process spans** around specific Java methods without writing manual OpenTelemetry SDK boilerplate.

#### Why Use `@WithSpan`?

1. **Isolate Performance Bottlenecks:** Break down a slow 500ms API controller response to see exactly which internal Java method (e.g., fraud calculation, image processing) is taking up time.
2. **Contextualizing Domain Logic:** Track internal microservices functions that don't trigger external network traffic or DB queries.
3. **Clean Code:** Avoid polluting business code with programmatic tracer boilerplate like `tracer.spanBuilder("name").startSpan()`.

---

### Detailed Code Example with Spring Boot

Consider an e-commerce order service. The automatic instrumentation creates a span for the controller HTTP request and the final database save query. However, there is complex CPU-heavy business calculation happening in the middle.

#### 1. Implementation Code

```java
package com.example.orderservice.service;

import io.opentelemetry.instrumentation.annotations.WithSpan;
import io.opentelemetry.instrumentation.annotations.SpanAttribute;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

@Service
public class OrderProcessingService {

    private static final Logger log = LoggerFactory.getLogger(OrderProcessingService.class);

    // Creates an automatic child span named "OrderProcessingService.processOrder"
    @WithSpan
    public void processOrder(String orderId, String customerTier) {
        log.info("Starting processing for order: {}", orderId);

        // Sub-operation 1: Calculate discount
        double discount = calculateCustomDiscount(customerTier);

        // Sub-operation 2: Validate inventory
        validateInventory(orderId);
    }

    // You can explicitly set a custom span name using the 'value' parameter
    // You can capture method arguments directly into the span attributes using @SpanAttribute
    @WithSpan("calculate-tier-discount")
    public double calculateCustomDiscount(@SpanAttribute("customer.tier") String customerTier) {
        log.info("Calculating discount for customer tier: {}", customerTier);
        
        // Complex internal calculation (not captured by auto-instrumentation default)
        if ("VIP".equals(customerTier)) {
            simulateHeavyWork(150); // 150ms work
            return 0.20;
        }
        return 0.05;
    }

    @WithSpan("inventory-validation")
    private void validateInventory(@SpanAttribute("order.id") String orderId) {
        simulateHeavyWork(50); // 50ms work
    }

    private void simulateHeavyWork(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}

```

---

### Visualizing the Result in a Tracing UI (e.g., Jaeger / Grafana Tempo)

Without `@WithSpan`, your trace waterfall would look like an empty gap between the controller and the DB query:

```text
[HTTP POST /orders] ---------------------------------------- (Total: 220ms)
   └── [JDBC INSERT INTO orders] ------------------ (20ms)

```

*(Notice: You have no visibility into what happened during the missing ~200ms of application code execution).*

With `@WithSpan` and `@SpanAttribute`, your trace waterfall expands into granular segments:

```text
[HTTP POST /orders] -------------------------------------------------------- (Total: 220ms)
   │
   └── [OrderProcessingService.processOrder] --------------------------------- (200ms)
        │
        ├── [calculate-tier-discount] --------------------------- (150ms)
        │     Attributes: { customer.tier: "VIP" }
        │
        └── [inventory-validation] ---------------- (50ms)
              Attributes: { order.id: "ORD-991" }

```

---

### Real-World Use Cases for `@WithSpan`

1. **Debugging Slow Internal Algorithms:** Identifying high latency inside internal computational loops (e.g., risk analysis algorithms, ML model inferences running in Java, complex PDF generation).
2. **Batch Processing:** Tracking performance when iterating through batches of objects inside a single background process or scheduled task (`@Scheduled`).
3. **Method-Level Error Attribution:** If a specific internal method throws an exception (e.g., a custom validation failure), `@WithSpan` automatically captures the exception event, stack trace, and marks *that specific span* with status `ERROR`, helping locate exact failure points without parsing raw log files.