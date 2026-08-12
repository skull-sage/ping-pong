# Optional container build (the primary local flow uses the Gradle wrapper + local JDK 25).
# Selects the service to build with the MODULE build-arg.
FROM gradle:9.1.0 AS build
WORKDIR /workspace
COPY . .
ARG MODULE
RUN gradle :${MODULE}:bootJar --no-daemon

FROM eclipse-temurin:25-jre AS runtime
ARG MODULE
WORKDIR /app
COPY --from=build /workspace/${MODULE}/build/libs/*.jar app.jar
ENTRYPOINT ["java", "-jar", "/app/app.jar"]
