/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.identity;

import org.apache.shiro.authc.AuthenticationToken;
import org.apache.shiro.authc.UsernamePasswordToken;
import org.opensearch.authn.HttpHeaderToken;

import java.util.Base64;

/**
 * Extracts Shiro's {@link AuthenticationToken} from different types of auth headers
 *
 * @opensearch.experimental
 */
public class AuthenticationTokenHandler {

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
        final String[] decodedUserNamePassword = decodedAuthHeader.toString().split(":");

        return new UsernamePasswordToken(decodedUserNamePassword[0], decodedUserNamePassword[1]);
    }
}
