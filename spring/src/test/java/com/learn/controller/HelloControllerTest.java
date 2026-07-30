package com.learn.controller;

import io.restassured.RestAssured;
import io.restassured.http.ContentType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.test.context.TestPropertySource;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.*;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@TestPropertySource(properties = {
    "spring.security.enabled=false"
})
class HelloControllerTest {

    @LocalServerPort
    private int port;

    @BeforeEach
    void setUp() {
        RestAssured.port = port;
        RestAssured.baseURI = "http://localhost";
    }

    @Test
    void shouldReturnHelloWithDefaultName() {
        given()
            .contentType(ContentType.JSON)
        .when()
            .get("/api/hello")
        .then()
            .statusCode(200)
            .body("message", equalTo("Hello, World!"))
            .body("framework", equalTo("Spring Boot"))
            .body("timestamp", notNullValue());
    }

    @Test
    void shouldReturnHelloWithCustomName() {
        given()
            .contentType(ContentType.JSON)
            .queryParam("name", "Spring")
        .when()
            .get("/api/hello")
        .then()
            .statusCode(200)
            .body("message", equalTo("Hello, Spring!"))
            .body("framework", equalTo("Spring Boot"));
    }

    @Test
    void shouldReturnHealthStatus() {
        given()
            .contentType(ContentType.JSON)
        .when()
            .get("/api/health")
        .then()
            .statusCode(200)
            .body("status", equalTo("UP"));
    }
}
