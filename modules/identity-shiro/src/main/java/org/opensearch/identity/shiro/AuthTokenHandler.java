/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.identity.shiro;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.shiro.authc.AuthenticationToken;
import org.apache.shiro.authc.UsernamePasswordToken;
import org.opensearch.identity.tokens.BasicAuthToken;

/**
 * Extracts Shiro's {@link AuthenticationToken} from different types of auth headers
 *
 * @opensearch.experimental
 */
class AuthTokenHandler {

    private static final Logger logger = LogManager.getLogger(AuthTokenHandler.class);

    /**
     * Translates shiro auth token from the given header token
     * @param authenticationToken the token from which to translate
     * @return the shiro auth token to be for login
     */
    public AuthenticationToken translateAuthToken(org.opensearch.identity.tokens.AuthToken authenticationToken) {
        final AuthenticationToken authToken = null;

        if (authenticationToken instanceof BasicAuthToken) {
            final BasicAuthToken basicAuthToken = (BasicAuthToken) authenticationToken;
            return new UsernamePasswordToken(basicAuthToken.getUser(), basicAuthToken.getPassword());
        }

        return authToken;
    }
}
