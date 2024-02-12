// Copyright 2024 Deutsche Telekom IT GmbH
//
// SPDX-License-Identifier: Apache-2.0

package de.telekom.horizon.pulsar.service.impl;

import de.telekom.horizon.pulsar.service.TokenService;
import jakarta.servlet.http.HttpServletRequest;
import org.springframework.context.annotation.Profile;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.security.oauth2.server.resource.authentication.JwtAuthenticationToken;
import org.springframework.stereotype.Service;

import java.security.Principal;
import java.util.Optional;



/**
 * Service class providing token-related functionality for authentication.
 *
 * This class, annotated with {@code @Service}, implements the {@link TokenService} interface
 * to handle token-related operations. It specifically caters to scenarios where the application
 * is not in the "publisher-mock" profile. The primary method, {@code getSubscriberId()}, retrieves
 * the subscriberId from the JWT token in the current user's principal.
 */
@Profile("!publisher-mock")
@Service
public class TokenServiceImpl implements TokenService {

    private final HttpServletRequest request;

    /**
     * Constructs an instance of {@code TokenServiceImpl} with the specified HttpServletRequest.
     *
     * @param request The HttpServletRequest representing the current HTTP request.
     */
    public TokenServiceImpl(HttpServletRequest request) {
        this.request = request;
    }

    /**
     * Gets the JwtAuthenticationToken from the provided principal.
     *
     * @param principal The principal representing the authenticated user.
     * @return The JwtAuthenticationToken, or null if the principal is not an instance of JwtAuthenticationToken.
     */
    private JwtAuthenticationToken getJwtAuthentication(Principal principal) {
        return principal instanceof JwtAuthenticationToken jwtAuthPrincipal ? jwtAuthPrincipal : null;
    }

    /**
     * Gets the Jwt token from the JwtAuthenticationToken.
     *
     * @param principal The principal representing the authenticated user.
     * @return The Jwt token, or null if the principal is not an instance of JwtAuthenticationToken.
     */
    private Jwt getToken(Principal principal) {
        JwtAuthenticationToken authentication = getJwtAuthentication(principal);

        return authentication != null ? authentication.getToken() : null;
    }

    /**
     * Retrieves the JWT token from the user's principal and extracts the subscriberId.
     *
     * @return The subscriberId extracted from the JWT token, or null if the token is not available or does not contain the expected claims.
     */
    @Override
    public String getSubscriberId() {
        var principal = request.getUserPrincipal();
        Jwt token = getToken(principal);

        if (token == null) {
            return null;
        }

        return Optional.ofNullable(token.getClaimAsString("clientId")).orElseGet(() -> token.getClaimAsString("azp"));
    }
}
