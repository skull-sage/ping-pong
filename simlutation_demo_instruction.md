
# introduction

Our System is distributed microservices and being developed based on DDD methods, Event Driven & CQRS. 

you will study  /analysis/distributed_tracing.md and implement a demo project inside /demo simulating all aspects of distributed tracig:
- you will write 3 services: service_ping, service_pongA, service_pongB
- you will use Spring Boot 4 and Clean Architecture pattern
- Kafka for cross service communication and Grafana LGTM stack
- you will implement all aspects of requiremetns from distributed_tracing.md 

For simulation and end-to-end visualtization: 
- you will implement a http ping triggering project using simple java inside  /trigger_ping 
- you will address all aspect of requiremetns. Group closely relevant examples in class and follow good naming principle for example function in Snake Case. 
- must have concurrent feature for proper simulation and  provide option to adjust concurrency
- you will generate a compact guideline on running the simulation, adjusting concurrency, running kafka and grafana LGTM containers etc
- you will setup the simulation and run  kafka and grafana LGTM stacks locally with container such as docker compose
- you will give simple steps with clarity to load grafana visualization to test the simulation 

## Change Request - 08.12

### Phase 1 

Refactor the codebase service_ping, service_pongA, service_pongB according to CQRS pattern and following guideline:
- RestController belongs to Presentation layer
- you will remove port type structure rather Application Layer will have Command handler, commands that are emitted from presentation layer on REST request
  
- you will implement a minimalistic aggregate root with two sample actions that raise event on mutation
- you will implement a dummy infrastructure service & a repository to simulate performance tracing with local span annotation @WithSpan, use Thread.sleep to mimick a performance hazard. 

-  You will place Event Listener code base in Presentation Layer and Event Publishing codebase in Infrastructure layer

- you will implement a command handler that is just dummy a regular command handler with success response,

- you will implement a command handler that will allow tracing the Span functionality in action  

NOTE: you will make the implementation compact and not too abstract, rather core functionality in priortiy.  


## Phase 2 

In each service, you will generate a trace_guide doc describing a compact summary report to what metrics, logs and trace to look for in grafana and where to filter them along with required TraceQL.

Then, in tabular format:  @WithSpan annotated function,  OTEL specifics used for tracing, and how to find/filter on grafana in dashboard. 

List any more instruction you feel needed for clarity so that anyone can understand how each service metrics are collected along with service_name without looking into another code or docs. 
 
