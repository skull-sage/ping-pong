package com.learn.util;

import com.learn.config.JwtConfig;
import io.jsonwebtoken.*;
import io.jsonwebtoken.security.Keys;
import org.springframework.stereotype.Component;

import javax.crypto.SecretKey;
import java.nio.charset.StandardCharsets;
import java.util.Date;
import java.util.Map;

/**
 * JWT Utility using HMAC-SHA256 symmetric encryption
 */
@Component
public class JwtUtil {

    private final JwtConfig jwtConfig;
    private final SecretKey secretKey;

    public JwtUtil(JwtConfig jwtConfig) {
        this.jwtConfig = jwtConfig;
        // Create HMAC key from secret string
        this.secretKey = Keys.hmacShaKeyFor(jwtConfig.getSecret().getBytes(StandardCharsets.UTF_8));
    }

    /**
     * Generate access token with HMAC-SHA256
     */
    public String generateAccessToken(String username, Map<String, Object> claims) {
        return Jwts.builder()
                .subject(username)
                .claims(claims)
                .issuer(jwtConfig.getIssuer())
                .issuedAt(new Date())
                .expiration(new Date(System.currentTimeMillis() + jwtConfig.getExpiration()))
                .signWith(secretKey, Jwts.SIG.HS256)  // HMAC with SHA-256
                .compact();
    }

    /**
     * Generate refresh token with HMAC-SHA256
     */
    public String generateRefreshToken(String username) {
        return Jwts.builder()
                .subject(username)
                .issuer(jwtConfig.getIssuer())
                .issuedAt(new Date())
                .expiration(new Date(System.currentTimeMillis() + jwtConfig.getRefreshExpiration()))
                .signWith(secretKey, Jwts.SIG.HS256)  // HMAC with SHA-256
                .compact();
    }

    /**
     * Validate token signature using HMAC
     */
    public boolean validateToken(String token) {
        try {
            Jwts.parser()
                    .verifyWith(secretKey)  // Verify HMAC signature
                    .build()
                    .parseSignedClaims(token);
            return true;
        } catch (JwtException | IllegalArgumentException e) {
            return false;
        }
    }

    /**
     * Extract username from token
     */
    public String extractUsername(String token) {
        return getClaims(token).getSubject();
    }

    /**
     * Extract claims from token
     */
    public Claims getClaims(String token) {
        return Jwts.parser()
                .verifyWith(secretKey)  // Verify HMAC signature
                .build()
                .parseSignedClaims(token)
                .getPayload();
    }

    /**
     * Check if token is expired
     */
    public boolean isTokenExpired(String token) {
        try {
            Date expiration = getClaims(token).getExpiration();
            return expiration.before(new Date());
        } catch (JwtException e) {
            return true;
        }
    }

    /**
     * Extract token from Authorization header
     */
    public String extractTokenFromHeader(String header) {
        if (header != null && header.startsWith(jwtConfig.getPrefix())) {
            return header.substring(jwtConfig.getPrefix().length());
        }
        return null;
    }
}
