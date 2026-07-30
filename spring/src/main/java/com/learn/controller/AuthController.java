package com.learn.controller;

import com.learn.config.JwtConfig;
import com.learn.dto.AuthRequest;
import com.learn.dto.AuthResponse;
import com.learn.util.JwtUtil;
import jakarta.validation.Valid;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.Map;

@RestController
@RequestMapping("/api/auth")
public class AuthController {

    private final JwtUtil jwtUtil;
    private final JwtConfig jwtConfig;

    public AuthController(JwtUtil jwtUtil, JwtConfig jwtConfig) {
        this.jwtUtil = jwtUtil;
        this.jwtConfig = jwtConfig;
    }

    /**
     * Generate JWT tokens using HMAC-SHA256
     */
    @PostMapping("/login")
    public ResponseEntity<AuthResponse> login(@Valid @RequestBody AuthRequest request) {
        // TODO: Implement actual authentication against database
        // For now, this is a demo endpoint showing JWT generation
        
        String email = request.getEmail();
        
        // Create custom claims
        Map<String, Object> claims = new HashMap<>();
        claims.put("email", email);
        claims.put("role", "USER");
        
        // Generate tokens using HMAC symmetric encryption
        String accessToken = jwtUtil.generateAccessToken(email, claims);
        String refreshToken = jwtUtil.generateRefreshToken(email);
        
        AuthResponse response = new AuthResponse(
            accessToken, 
            refreshToken, 
            jwtConfig.getExpiration() / 1000  // Convert to seconds
        );
        
        return ResponseEntity.ok(response);
    }

    /**
     * Refresh access token using refresh token
     */
    @PostMapping("/refresh")
    public ResponseEntity<AuthResponse> refresh(@RequestParam String refreshToken) {
        if (!jwtUtil.validateToken(refreshToken) || jwtUtil.isTokenExpired(refreshToken)) {
            return ResponseEntity.badRequest().build();
        }

        String username = jwtUtil.extractUsername(refreshToken);
        
        // Generate new access token
        Map<String, Object> claims = new HashMap<>();
        claims.put("email", username);
        claims.put("role", "USER");
        
        String newAccessToken = jwtUtil.generateAccessToken(username, claims);
        
        AuthResponse response = new AuthResponse(
            newAccessToken,
            refreshToken,  // Keep same refresh token
            jwtConfig.getExpiration() / 1000
        );
        
        return ResponseEntity.ok(response);
    }

    /**
     * Validate token endpoint
     */
    @GetMapping("/validate")
    public ResponseEntity<Map<String, Object>> validateToken(@RequestHeader("Authorization") String authHeader) {
        String token = jwtUtil.extractTokenFromHeader(authHeader);
        
        if (token == null) {
            return ResponseEntity.badRequest().body(Map.of("valid", false, "message", "No token provided"));
        }

        boolean isValid = jwtUtil.validateToken(token) && !jwtUtil.isTokenExpired(token);
        
        Map<String, Object> response = new HashMap<>();
        response.put("valid", isValid);
        
        if (isValid) {
            response.put("username", jwtUtil.extractUsername(token));
            response.put("claims", jwtUtil.getClaims(token));
        }
        
        return ResponseEntity.ok(response);
    }
}
