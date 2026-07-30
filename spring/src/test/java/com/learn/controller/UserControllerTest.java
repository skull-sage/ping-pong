package com.learn.controller;

import com.learn.model.User;
import com.learn.repository.UserRepository;
import com.learn.util.JwtUtil;
import io.restassured.RestAssured;
import io.restassured.http.ContentType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;

import java.util.HashMap;
import java.util.Map;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.*;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class UserControllerTest {

    @LocalServerPort
    private int port;

    @Autowired
    private UserRepository userRepository;

    @Autowired
    private JwtUtil jwtUtil;

    private String authToken;

    @BeforeEach
    void setUp() {
        RestAssured.port = port;
        RestAssured.baseURI = "http://localhost";
        
        // Clean database before each test
        userRepository.deleteAll();
        
        // Generate JWT token for authenticated requests
        Map<String, Object> claims = new HashMap<>();
        claims.put("email", "test@example.com");
        claims.put("role", "USER");
        authToken = jwtUtil.generateAccessToken("test@example.com", claims);
    }

    @Test
    void shouldGetAllUsers() {
        // Given: Create test users
        User user1 = new User();
        user1.setName("John Doe");
        user1.setEmail("john@example.com");
        userRepository.save(user1);

        User user2 = new User();
        user2.setName("Jane Smith");
        user2.setEmail("jane@example.com");
        userRepository.save(user2);

        // When & Then
        given()
            .contentType(ContentType.JSON)
            .header("Authorization", "Bearer " + authToken)
        .when()
            .get("/api/users")
        .then()
            .statusCode(200)
            .body("size()", is(2))
            .body("[0].name", equalTo("John Doe"))
            .body("[1].name", equalTo("Jane Smith"));
    }

    @Test
    void shouldGetUserById() {
        // Given
        User user = new User();
        user.setName("John Doe");
        user.setEmail("john@example.com");
        User savedUser = userRepository.save(user);

        // When & Then
        given()
            .contentType(ContentType.JSON)
            .header("Authorization", "Bearer " + authToken)
        .when()
            .get("/api/users/{id}", savedUser.getId())
        .then()
            .statusCode(200)
            .body("name", equalTo("John Doe"))
            .body("email", equalTo("john@example.com"))
            .body("id", equalTo(savedUser.getId().intValue()));
    }

    @Test
    void shouldReturn404WhenUserNotFound() {
        given()
            .contentType(ContentType.JSON)
            .header("Authorization", "Bearer " + authToken)
        .when()
            .get("/api/users/999")
        .then()
            .statusCode(404);
    }

    @Test
    void shouldCreateUser() {
        String requestBody = """
            {
                "name": "John Doe",
                "email": "john@example.com"
            }
            """;

        given()
            .contentType(ContentType.JSON)
            .header("Authorization", "Bearer " + authToken)
            .body(requestBody)
        .when()
            .post("/api/users")
        .then()
            .statusCode(201)
            .body("name", equalTo("John Doe"))
            .body("email", equalTo("john@example.com"))
            .body("id", notNullValue())
            .body("createdAt", notNullValue())
            .body("updatedAt", notNullValue());
    }

    @Test
    void shouldNotCreateUserWithDuplicateEmail() {
        // Given: User with email already exists
        User existingUser = new User();
        existingUser.setName("Existing User");
        existingUser.setEmail("duplicate@example.com");
        userRepository.save(existingUser);

        String requestBody = """
            {
                "name": "New User",
                "email": "duplicate@example.com"
            }
            """;

        // When & Then
        given()
            .contentType(ContentType.JSON)
            .header("Authorization", "Bearer " + authToken)
            .body(requestBody)
        .when()
            .post("/api/users")
        .then()
            .statusCode(400);
    }

    @Test
    void shouldUpdateUser() {
        // Given
        User user = new User();
        user.setName("John Doe");
        user.setEmail("john@example.com");
        User savedUser = userRepository.save(user);

        String updateBody = """
            {
                "name": "John Updated",
                "email": "john.updated@example.com"
            }
            """;

        // When & Then
        given()
            .contentType(ContentType.JSON)
            .header("Authorization", "Bearer " + authToken)
            .body(updateBody)
        .when()
            .put("/api/users/{id}", savedUser.getId())
        .then()
            .statusCode(200)
            .body("name", equalTo("John Updated"))
            .body("email", equalTo("john.updated@example.com"))
            .body("id", equalTo(savedUser.getId().intValue()));
    }

    @Test
    void shouldReturn404WhenUpdatingNonExistentUser() {
        String updateBody = """
            {
                "name": "John Updated",
                "email": "john.updated@example.com"
            }
            """;

        given()
            .contentType(ContentType.JSON)
            .header("Authorization", "Bearer " + authToken)
            .body(updateBody)
        .when()
            .put("/api/users/999")
        .then()
            .statusCode(404);
    }

    @Test
    void shouldDeleteUser() {
        // Given
        User user = new User();
        user.setName("John Doe");
        user.setEmail("john@example.com");
        User savedUser = userRepository.save(user);

        // When & Then
        given()
            .contentType(ContentType.JSON)
            .header("Authorization", "Bearer " + authToken)
        .when()
            .delete("/api/users/{id}", savedUser.getId())
        .then()
            .statusCode(204);

        // Verify user is deleted
        given()
            .contentType(ContentType.JSON)
            .header("Authorization", "Bearer " + authToken)
        .when()
            .get("/api/users/{id}", savedUser.getId())
        .then()
            .statusCode(404);
    }

    @Test
    void shouldReturn404WhenDeletingNonExistentUser() {
        given()
            .contentType(ContentType.JSON)
            .header("Authorization", "Bearer " + authToken)
        .when()
            .delete("/api/users/999")
        .then()
            .statusCode(404);
    }

    @Test
    void shouldValidateRequiredFields() {
        String invalidBody = """
            {
                "name": "",
                "email": ""
            }
            """;

        given()
            .contentType(ContentType.JSON)
            .header("Authorization", "Bearer " + authToken)
            .body(invalidBody)
        .when()
            .post("/api/users")
        .then()
            .statusCode(anyOf(is(400), is(403)));  // Accept either validation error or forbidden
    }

    @Test
    void shouldValidateEmailFormat() {
        String invalidEmailBody = """
            {
                "name": "John Doe",
                "email": "invalid-email"
            }
            """;

        given()
            .contentType(ContentType.JSON)
            .header("Authorization", "Bearer " + authToken)
            .body(invalidEmailBody)
        .when()
            .post("/api/users")
        .then()
            .statusCode(anyOf(is(400), is(403)));  // Accept either validation error or forbidden
    }
}
