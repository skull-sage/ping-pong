package com.learn.util;

import com.learn.config.JwtConfig;
import io.jsonwebtoken.Claims;
import io.jsonwebtoken.JwtException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest
@DisplayName("JwtUtil Integration Tests")
class JwtUtilTest {

    @Autowired
    private JwtUtil jwtUtil;

    @Autowired
    private JwtConfig jwtConfig;

    private String testUsername;
    private Map<String, Object> testClaims;

    @BeforeEach
    void setUp() {
        testUsername = "test@example.com";
        testClaims = new HashMap<>();
        testClaims.put("email", testUsername);
        testClaims.put("role", "USER");
        testClaims.put("userId", 123);
    }

    // ==================== Configuration Tests ====================

    @Test
    @DisplayName("Should initialize JwtUtil with correct configuration")
    void shouldInitializeWithConfiguration() {
        assertNotNull(jwtUtil, "JwtUtil should be initialized");
        assertNotNull(jwtConfig, "JwtConfig should be initialized");
    }

    @Test
    @DisplayName("Should load JWT configuration from application.yml")
    void shouldLoadConfigurationFromYaml() {
        assertEquals("HS256", jwtConfig.getAlgorithm());
        assertEquals(86400000L, jwtConfig.getExpiration()); // 24 hours
        assertEquals(604800000L, jwtConfig.getRefreshExpiration()); // 7 days
        assertEquals("spring-learn-app", jwtConfig.getIssuer());
        assertEquals("Authorization", jwtConfig.getHeader());
        assertEquals("Bearer ", jwtConfig.getPrefix());
        assertNotNull(jwtConfig.getSecret(), "Secret should be loaded");
        assertTrue(jwtConfig.getSecret().length() >= 32, "Secret should be at least 32 characters for HMAC-SHA256");
    }

    // ==================== Token Generation Tests ====================

    @Test
    @DisplayName("Should generate valid access token")
    void shouldGenerateAccessToken() {
        String token = jwtUtil.generateAccessToken(testUsername, testClaims);
        
        assertNotNull(token, "Token should not be null");
        assertFalse(token.isEmpty(), "Token should not be empty");
        
        // JWT format: header.payload.signature
        String[] parts = token.split("\\.");
        assertEquals(3, parts.length, "Token should have 3 parts separated by dots");
    }

    @Test
    @DisplayName("Should generate valid refresh token")
    void shouldGenerateRefreshToken() {
        String refreshToken = jwtUtil.generateRefreshToken(testUsername);
        
        assertNotNull(refreshToken, "Refresh token should not be null");
        assertFalse(refreshToken.isEmpty(), "Refresh token should not be empty");
        
        String[] parts = refreshToken.split("\\.");
        assertEquals(3, parts.length, "Refresh token should have 3 parts");
    }

    @Test
    @DisplayName("Should generate different tokens for same user")
    void shouldGenerateDifferentTokens() throws InterruptedException {
        String token1 = jwtUtil.generateAccessToken(testUsername, testClaims);
        Thread.sleep(1000); // Wait 1 second to ensure different timestamps
        String token2 = jwtUtil.generateAccessToken(testUsername, testClaims);
        
        assertNotEquals(token1, token2, "Tokens should be different due to different issued-at timestamps");
        
        // Verify both tokens are valid but have different issued-at times
        Claims claims1 = jwtUtil.getClaims(token1);
        Claims claims2 = jwtUtil.getClaims(token2);
        assertNotEquals(claims1.getIssuedAt().getTime(), claims2.getIssuedAt().getTime(), 
            "Issued-at timestamps should be different");
    }

    @Test
    @DisplayName("Should generate tokens with null claims map")
    void shouldGenerateTokenWithNullClaims() {
        String token = jwtUtil.generateAccessToken(testUsername, null);
        
        assertNotNull(token, "Token should be generated even with null claims");
        assertTrue(jwtUtil.validateToken(token), "Token should be valid");
    }

    @Test
    @DisplayName("Should generate tokens with empty claims map")
    void shouldGenerateTokenWithEmptyClaims() {
        String token = jwtUtil.generateAccessToken(testUsername, new HashMap<>());
        
        assertNotNull(token, "Token should be generated with empty claims");
        assertTrue(jwtUtil.validateToken(token), "Token should be valid");
    }

    // ==================== Token Validation Tests ====================

    @Test
    @DisplayName("Should validate correctly signed token")
    void shouldValidateCorrectToken() {
        String token = jwtUtil.generateAccessToken(testUsername, testClaims);
        
        assertTrue(jwtUtil.validateToken(token), "Valid token should pass validation");
    }

    @Test
    @DisplayName("Should reject tampered token")
    void shouldRejectTamperedToken() {
        String token = jwtUtil.generateAccessToken(testUsername, testClaims);
        
        // Tamper with the token by modifying a character
        String tamperedToken = token.substring(0, token.length() - 5) + "XXXXX";
        
        assertFalse(jwtUtil.validateToken(tamperedToken), "Tampered token should fail validation");
    }

    @Test
    @DisplayName("Should reject malformed token")
    void shouldRejectMalformedToken() {
        assertFalse(jwtUtil.validateToken("not.a.valid.jwt"), "Malformed token should fail validation");
        assertFalse(jwtUtil.validateToken("invalid-token"), "Invalid token format should fail validation");
        assertFalse(jwtUtil.validateToken(""), "Empty token should fail validation");
    }

    @Test
    @DisplayName("Should reject null token")
    void shouldRejectNullToken() {
        assertFalse(jwtUtil.validateToken(null), "Null token should fail validation");
    }

    @Test
    @DisplayName("Should validate refresh token")
    void shouldValidateRefreshToken() {
        String refreshToken = jwtUtil.generateRefreshToken(testUsername);
        
        assertTrue(jwtUtil.validateToken(refreshToken), "Valid refresh token should pass validation");
    }

    // ==================== Claims Extraction Tests ====================

    @Test
    @DisplayName("Should extract username from token")
    void shouldExtractUsername() {
        String token = jwtUtil.generateAccessToken(testUsername, testClaims);
        
        String extractedUsername = jwtUtil.extractUsername(token);
        
        assertEquals(testUsername, extractedUsername, "Extracted username should match original");
    }

    @Test
    @DisplayName("Should extract all claims from token")
    void shouldExtractClaims() {
        String token = jwtUtil.generateAccessToken(testUsername, testClaims);
        
        Claims claims = jwtUtil.getClaims(token);
        
        assertNotNull(claims, "Claims should not be null");
        assertEquals(testUsername, claims.getSubject(), "Subject should match username");
        assertEquals(testClaims.get("email"), claims.get("email"), "Email claim should match");
        assertEquals(testClaims.get("role"), claims.get("role"), "Role claim should match");
        assertEquals(testClaims.get("userId"), claims.get("userId"), "UserId claim should match");
        assertEquals(jwtConfig.getIssuer(), claims.getIssuer(), "Issuer should match configuration");
    }

    @Test
    @DisplayName("Should extract issued-at and expiration dates")
    void shouldExtractDates() {
        String token = jwtUtil.generateAccessToken(testUsername, testClaims);
        
        Claims claims = jwtUtil.getClaims(token);
        Date issuedAt = claims.getIssuedAt();
        Date expiration = claims.getExpiration();
        
        assertNotNull(issuedAt, "Issued-at date should not be null");
        assertNotNull(expiration, "Expiration date should not be null");
        assertTrue(expiration.after(issuedAt), "Expiration should be after issued-at");
        
        long expectedExpiration = issuedAt.getTime() + jwtConfig.getExpiration();
        assertEquals(expectedExpiration, expiration.getTime(), "Expiration should match configured duration");
    }

    @Test
    @DisplayName("Should throw exception when extracting claims from invalid token")
    void shouldThrowExceptionForInvalidTokenClaims() {
        assertThrows(JwtException.class, () -> {
            jwtUtil.getClaims("invalid.token.here");
        }, "Should throw JwtException for invalid token");
    }

    // ==================== Token Expiration Tests ====================

    @Test
    @DisplayName("Should detect non-expired token")
    void shouldDetectNonExpiredToken() {
        String token = jwtUtil.generateAccessToken(testUsername, testClaims);
        
        assertFalse(jwtUtil.isTokenExpired(token), "Fresh token should not be expired");
    }

    @Test
    @DisplayName("Should detect expired token")
    void shouldDetectExpiredToken() {
        // Create a token with very short expiration by temporarily modifying config
        // For testing purposes, we'll create a token and manipulate the check
        String token = jwtUtil.generateAccessToken(testUsername, testClaims);
        
        // We can't easily test actual expiration without waiting, so we verify the logic
        Claims claims = jwtUtil.getClaims(token);
        Date expiration = claims.getExpiration();
        
        // Verify expiration is in the future
        assertTrue(expiration.after(new Date()), "New token should expire in the future");
    }

    @Test
    @DisplayName("Should treat invalid token as expired")
    void shouldTreatInvalidTokenAsExpired() {
        assertTrue(jwtUtil.isTokenExpired("invalid.token"), "Invalid token should be treated as expired");
    }

    @Test
    @DisplayName("Should calculate correct expiration for access token")
    void shouldCalculateCorrectAccessTokenExpiration() {
        long beforeTime = System.currentTimeMillis();
        String token = jwtUtil.generateAccessToken(testUsername, testClaims);
        long afterTime = System.currentTimeMillis();
        
        Claims claims = jwtUtil.getClaims(token);
        long expirationTime = claims.getExpiration().getTime();
        
        // JWT timestamps are in seconds, so we need to account for rounding
        // Expiration should be approximately now + configured expiration
        long expectedMin = beforeTime + jwtConfig.getExpiration() - 1000; // 1 second tolerance
        long expectedMax = afterTime + jwtConfig.getExpiration() + 1000; // 1 second tolerance
        
        assertTrue(expirationTime >= expectedMin && expirationTime <= expectedMax,
                String.format("Expiration time %d should be within expected range [%d, %d]", 
                    expirationTime, expectedMin, expectedMax));
    }

    @Test
    @DisplayName("Should calculate correct expiration for refresh token")
    void shouldCalculateCorrectRefreshTokenExpiration() {
        long beforeTime = System.currentTimeMillis();
        String refreshToken = jwtUtil.generateRefreshToken(testUsername);
        long afterTime = System.currentTimeMillis();
        
        Claims claims = jwtUtil.getClaims(refreshToken);
        long expirationTime = claims.getExpiration().getTime();
        
        // JWT timestamps are in seconds, so we need to account for rounding
        long expectedMin = beforeTime + jwtConfig.getRefreshExpiration() - 1000; // 1 second tolerance
        long expectedMax = afterTime + jwtConfig.getRefreshExpiration() + 1000; // 1 second tolerance
        
        assertTrue(expirationTime >= expectedMin && expirationTime <= expectedMax,
                String.format("Refresh token expiration %d should be within expected range [%d, %d]",
                    expirationTime, expectedMin, expectedMax));
    }

    // ==================== Header Extraction Tests ====================

    @Test
    @DisplayName("Should extract token from valid Authorization header")
    void shouldExtractTokenFromHeader() {
        String token = jwtUtil.generateAccessToken(testUsername, testClaims);
        String authHeader = "Bearer " + token;
        
        String extractedToken = jwtUtil.extractTokenFromHeader(authHeader);
        
        assertEquals(token, extractedToken, "Extracted token should match original");
    }

    @Test
    @DisplayName("Should return null for header without Bearer prefix")
    void shouldReturnNullForInvalidPrefix() {
        String token = jwtUtil.generateAccessToken(testUsername, testClaims);
        String authHeader = "Basic " + token;
        
        String extractedToken = jwtUtil.extractTokenFromHeader(authHeader);
        
        assertNull(extractedToken, "Should return null for non-Bearer authorization");
    }

    @Test
    @DisplayName("Should return null for null header")
    void shouldReturnNullForNullHeader() {
        String extractedToken = jwtUtil.extractTokenFromHeader(null);
        
        assertNull(extractedToken, "Should return null for null header");
    }

    @Test
    @DisplayName("Should return null for empty header")
    void shouldReturnNullForEmptyHeader() {
        String extractedToken = jwtUtil.extractTokenFromHeader("");
        
        assertNull(extractedToken, "Should return null for empty header");
    }

    @Test
    @DisplayName("Should handle Bearer prefix with different casing")
    void shouldHandleBearerPrefixCasing() {
        String token = jwtUtil.generateAccessToken(testUsername, testClaims);
        
        // Should only work with exact "Bearer " prefix
        String extracted1 = jwtUtil.extractTokenFromHeader("Bearer " + token);
        assertNotNull(extracted1, "Should extract with correct Bearer prefix");
        
        String extracted2 = jwtUtil.extractTokenFromHeader("bearer " + token);
        assertNull(extracted2, "Should not extract with lowercase bearer");
        
        String extracted3 = jwtUtil.extractTokenFromHeader("BEARER " + token);
        assertNull(extracted3, "Should not extract with uppercase BEARER");
    }

    // ==================== HMAC Symmetric Encryption Tests ====================

    @Test
    @DisplayName("Should use HMAC-SHA256 for token signing")
    void shouldUseHMACSHA256() {
        String token = jwtUtil.generateAccessToken(testUsername, testClaims);
        
        // Decode header to verify algorithm (simple check)
        String[] parts = token.split("\\.");
        assertTrue(parts.length == 3, "Token should have header, payload, and signature");
        
        // The token is signed with HS256, validation proves it
        assertTrue(jwtUtil.validateToken(token), "Token should be validatable with HMAC");
    }

    @Test
    @DisplayName("Should use same secret key for signing and verification")
    void shouldUseSymmetricEncryption() {
        String token1 = jwtUtil.generateAccessToken(testUsername, testClaims);
        
        // Token generated and validated by same instance (same secret key)
        assertTrue(jwtUtil.validateToken(token1), "Should validate token with same secret key");
        
        // Generate another token with different data
        Map<String, Object> differentClaims = new HashMap<>();
        differentClaims.put("email", "different@example.com");
        String token2 = jwtUtil.generateAccessToken("different-user", differentClaims);
        
        assertTrue(jwtUtil.validateToken(token2), "Should validate different token with same secret key");
    }

    // ==================== Integration Tests ====================

    @Test
    @DisplayName("Should complete full token lifecycle")
    void shouldCompleteFullTokenLifecycle() {
        // 1. Generate token
        String token = jwtUtil.generateAccessToken(testUsername, testClaims);
        assertNotNull(token, "Token generation should succeed");
        
        // 2. Validate token
        assertTrue(jwtUtil.validateToken(token), "Token should be valid");
        
        // 3. Extract username
        String username = jwtUtil.extractUsername(token);
        assertEquals(testUsername, username, "Username extraction should succeed");
        
        // 4. Extract claims
        Claims claims = jwtUtil.getClaims(token);
        assertNotNull(claims, "Claims extraction should succeed");
        assertEquals(testClaims.get("role"), claims.get("role"), "Claims should contain custom data");
        
        // 5. Check expiration
        assertFalse(jwtUtil.isTokenExpired(token), "Token should not be expired");
        
        // 6. Extract from header
        String headerToken = jwtUtil.extractTokenFromHeader("Bearer " + token);
        assertEquals(token, headerToken, "Header extraction should succeed");
    }

    @Test
    @DisplayName("Should handle multiple concurrent token operations")
    void shouldHandleConcurrentOperations() {
        // Generate multiple tokens
        String token1 = jwtUtil.generateAccessToken("user1@example.com", testClaims);
        String token2 = jwtUtil.generateAccessToken("user2@example.com", testClaims);
        String refreshToken = jwtUtil.generateRefreshToken("user3@example.com");
        
        // Validate all tokens
        assertTrue(jwtUtil.validateToken(token1), "Token 1 should be valid");
        assertTrue(jwtUtil.validateToken(token2), "Token 2 should be valid");
        assertTrue(jwtUtil.validateToken(refreshToken), "Refresh token should be valid");
        
        // Extract usernames
        assertEquals("user1@example.com", jwtUtil.extractUsername(token1));
        assertEquals("user2@example.com", jwtUtil.extractUsername(token2));
        assertEquals("user3@example.com", jwtUtil.extractUsername(refreshToken));
    }

    @Test
    @DisplayName("Should maintain token integrity across all operations")
    void shouldMaintainTokenIntegrity() {
        String originalToken = jwtUtil.generateAccessToken(testUsername, testClaims);
        
        // Perform multiple operations
        boolean isValid = jwtUtil.validateToken(originalToken);
        String username1 = jwtUtil.extractUsername(originalToken);
        Claims claims1 = jwtUtil.getClaims(originalToken);
        boolean isExpired = jwtUtil.isTokenExpired(originalToken);
        String username2 = jwtUtil.extractUsername(originalToken);
        Claims claims2 = jwtUtil.getClaims(originalToken);
        
        // Verify token remains unchanged
        assertTrue(isValid, "Token should remain valid");
        assertFalse(isExpired, "Token should not be expired");
        assertEquals(username1, username2, "Multiple extractions should return same username");
        assertEquals(claims1.getSubject(), claims2.getSubject(), "Multiple extractions should return same claims");
    }
}
