/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.identity;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.shiro.authc.AuthenticationToken;
import org.apache.shiro.authc.BearerToken;
import org.apache.shiro.authc.UsernamePasswordToken;
import org.apache.cxf.rs.security.jose.jwt.JwtToken;
import org.apache.cxf.jaxrs.json.basic.JsonMapObject;
import org.opensearch.authn.HttpHeaderToken;

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
     * @param authenticationToken the token from which to extract
     * @return the extracted shiro auth token to be used to perform login
     */
    public static AuthenticationToken extractShiroAuthToken(org.opensearch.authn.AuthenticationToken authenticationToken) {
        AuthenticationToken authToken = null;

        if (authenticationToken instanceof HttpHeaderToken) {
            final HttpHeaderToken headerToken = (HttpHeaderToken) authenticationToken;

            if (headerToken.getHeaderValue().contains("Basic")) {
                authToken = handleBasicAuth(headerToken);
            }

            if (headerToken.getHeaderValue().contains("Bearer")) {
                authToken = handleBearerAuth(headerToken);
            }

            // TODO: check for other type of Headers
        }
        // TODO: Handle other type of auths, and see if we can use switch case here

        return authToken;
    }

    /**
     * Returns auth token extracted from basic auth header
     * @param token the basic auth token
     * @return the extracted auth token
     */
    private static AuthenticationToken handleBasicAuth(final HttpHeaderToken token) {

        final byte[] decodedAuthHeader = Base64.getDecoder().decode(token.getHeaderValue().substring("Basic".length()).trim());
        String decodedHeader = new String(decodedAuthHeader, StandardCharsets.UTF_8);
        final String[] decodedUserNamePassword = decodedHeader.split(":");

        logger.info("Logging in as: " + decodedUserNamePassword[0]);

        return new UsernamePasswordToken(decodedUserNamePassword[0], decodedUserNamePassword[1]);
    }

    private static AuthenticationToken handleBearerAuth(final HttpHeaderToken token) { // Can be moved into the InternalRealms.java class
        // Can add a positive and negative case for testing this -- a valid bearer token and then a malformed token without bearer in the header
        // Tokens should like `curl -XGET -H "Authorization: Bearer ${ACCESS_TOKEN}" http://localhost:9200`

        String encodedJWT = token.getHeaderValue().substring("Bearer".length()).trim();
        JwtToken jwtToken;

        try {
            jwtToken = JwtVerifier.getVerifiedJwtToken(encodedJWT);
        } catch (BadCredentialsException e) {
            throw new Error(e); // Could not verify the JWT token
        }

        return new BearerToken(encodedJWT);
    }
}
