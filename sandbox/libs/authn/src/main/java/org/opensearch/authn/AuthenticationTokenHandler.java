/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.authn;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.shiro.authc.AuthenticationToken;
import org.apache.shiro.authc.BearerToken;
import org.apache.shiro.authc.UsernamePasswordToken;
import org.opensearch.authn.tokens.BasicAuthToken;
import org.opensearch.authn.tokens.BearerAuthToken;
import org.apache.cxf.rs.security.jose.jwt.JwtToken;
import org.opensearch.authn.jwt.BadCredentialsException;
import org.opensearch.authn.jwt.JwtVerifier;

import java.nio.charset.StandardCharsets;
import java.util.Base64;

/**
 * Extracts Shiro's {@link AuthenticationToken} from different types of auth headers
 *
 * @opensearch.experimental
 */
public class AuthenticationTokenHandler {

    private static final Logger logger = LogManager.getLogger(AuthenticationTokenHandler.class);

    /**
     * Extracts shiro auth token from the given header token
     */
    public static AuthenticationToken extractShiroAuthToken(org.opensearch.authn.tokens.AuthenticationToken authenticationToken)
        throws BadCredentialsException {
        AuthenticationToken authToken = null;

        if (authenticationToken instanceof BasicAuthToken) { // if (headerToken.getHeaderValue().contains("Basic"))
            authToken = handleBasicAuth((BasicAuthToken) authenticationToken);
        }

        if (authenticationToken instanceof BearerAuthToken) { // if header contains "Bearer"
            authToken = handleBearerAuth((BearerAuthToken) authenticationToken);
        }

        // TODO: check for other type of Headers

        return authToken;
    }

    /**
     * Returns auth token extracted from basic auth header
     * @param token the basic auth token
     * @return the extracted auth token
     */
    private static AuthenticationToken handleBasicAuth(final BasicAuthToken token) {

        final byte[] decodedAuthHeader = Base64.getDecoder().decode(token.getHeaderValue().substring("Basic".length()).trim());
        String decodedHeader = new String(decodedAuthHeader, StandardCharsets.UTF_8);

        final String[] decodedUserNamePassword = decodedHeader.split(":");

        // Malformed AuthHeader strings
        if (decodedUserNamePassword.length != 2) return null;

        logger.info("Logging in as: " + decodedUserNamePassword[0]);

        return new UsernamePasswordToken(decodedUserNamePassword[0], decodedUserNamePassword[1]);
    }

    private static AuthenticationToken handleBearerAuth(final BearerAuthToken token) throws BadCredentialsException {
        // Can be moved into the InternalRealms.java class
        // Can add a positive and negative case for testing this -- a valid bearer token and then a malformed token without bearer in the
        // header
        // Tokens should like `curl -XGET -H "Authorization: Bearer ${ACCESS_TOKEN}" http://localhost:9200`

        String encodedJWT = token.getHeaderValue().substring("Bearer".length()).trim(); // Still may need to base64 decode this

        try {
            JwtToken jwtToken = JwtVerifier.getVerifiedJwtToken(encodedJWT);
        } catch (BadCredentialsException e) {
            throw (e); // Could not verify the JWT token--throw this error to prevent the retur
        }

        return new BearerToken(encodedJWT);
    }
}
