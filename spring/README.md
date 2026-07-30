# Spring Boot Learning Project

A modern Spring Boot application setup for learning Java Spring framework.

## Tech Stack

- **Java 21**
- **Spring Boot 3.2.5**
- **Gradle** (Build Tool)
- **H2 Database** (In-memory)
- **Lombok** (Boilerplate reduction)
- **Spring Data JPA** (Data access)
- **Spring Validation** (Bean validation)
- **Spring Actuator** (Monitoring)

## Project Structure

```
src/
├── main/
│   ├── java/com/learn/
│   │   ├── Application.java           # Main application entry point
│   │   ├── controller/
│   │   │   ├── HelloController.java   # Sample REST controller
│   │   │   └── UserController.java    # CRUD REST controller
│   │   ├── model/
│   │   │   └── User.java              # JPA entity
│   │   ├── repository/
│   │   │   └── UserRepository.java    # Data access layer
│   │   └── service/
│   │       └── UserService.java       # Business logic layer
│   └── resources/
│       └── application.yml            # Application configuration
└── test/
    └── java/com/learn/
        └── ApplicationTests.java      # Test cases
```

## Getting Started

### Prerequisites

- Java 21 or higher
- Gradle (or use included Gradle wrapper)

### Running the Application

Windows:
```bash
gradlew.bat bootRun
```

Linux/Mac:
```bash
./gradlew bootRun
```

The application will start on `http://localhost:8080`

### Building the Project

```bash
gradlew.bat build
```

## API Endpoints

### Hello Endpoint
```
GET /api/hello?name=Spring
```

### User CRUD Operations
```
GET    /api/users          # Get all users
GET    /api/users/{id}     # Get user by ID
POST   /api/users          # Create user
PUT    /api/users/{id}     # Update user
DELETE /api/users/{id}     # Delete user
```

### Sample User JSON
```json
{
  "name": "John Doe",
  "email": "john@example.com"
}
```

## H2 Console

Access the H2 database console at: `http://localhost:8080/h2-console`

- **JDBC URL**: `jdbc:h2:mem:testdb`
- **Username**: `sa`
- **Password**: (leave empty)

## Actuator Endpoints

- Health: `http://localhost:8080/actuator/health`
- Info: `http://localhost:8080/actuator/info`
- Metrics: `http://localhost:8080/actuator/metrics`

## Key Spring Concepts Demonstrated

1. **Dependency Injection** - Constructor injection using Lombok's `@RequiredArgsConstructor`
2. **REST API** - RESTful endpoints with proper HTTP methods
3. **JPA/Hibernate** - Entity mapping and repository pattern
4. **Service Layer** - Business logic separation
5. **Validation** - Bean validation with constraints
6. **Exception Handling** - Proper error responses
7. **Configuration** - YAML-based configuration
8. **Database Management** - H2 in-memory database
9. **Lombok** - Reducing boilerplate code

## Next Steps

- Add custom exception handling with `@ControllerAdvice`
- Implement pagination and sorting
- Add Spring Security for authentication
- Integrate with PostgreSQL/MySQL
- Add integration tests
- Implement DTOs and mapping
- Add API documentation with Swagger/OpenAPI
