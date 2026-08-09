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