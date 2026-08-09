# Shared multi-stage build for all three services. The service to build is selected with the
# MODULE build-arg (service_ping | service_pongA | service_pongB). No local Gradle/JDK required —
# the build runs inside the Gradle image.
FROM gradle:8.14-jdk21 AS build
WORKDIR /workspace
COPY . .
ARG MODULE
RUN gradle :${MODULE}:bootJar --no-daemon

FROM eclipse-temurin:21-jre AS runtime
ARG MODULE
WORKDIR /app
COPY --from=build /workspace/${MODULE}/build/libs/*.jar app.jar
ENTRYPOINT ["java", "-jar", "/app/app.jar"]
