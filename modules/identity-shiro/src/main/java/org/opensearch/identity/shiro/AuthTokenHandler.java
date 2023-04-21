/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.identity.shiro;

import java.util.Optional;

import org.apache.shiro.authc.AuthenticationToken;
import org.apache.shiro.authc.UsernamePasswordToken;
import org.opensearch.identity.tokens.BasicAuthToken;

/**
 * Extracts Shiro's {@link AuthenticationToken} from different types of auth headers
 *
 * @opensearch.experimental
 */
class AuthTokenHandler {

    /**
     * Translates into shiro auth token from the given header token
     * @param authenticationToken the token from which to translate
     * @return An optional of the shiro auth token for login
     */
    public Optional<AuthenticationToken> translateAuthToken(org.opensearch.identity.tokens.AuthToken authenticationToken) {
        if (authenticationToken instanceof BasicAuthToken) {
            final BasicAuthToken basicAuthToken = (BasicAuthToken) authenticationToken;
            return Optional.of(new UsernamePasswordToken(basicAuthToken.getUser(), basicAuthToken.getPassword()));
        }

        return Optional.empty();
    }
}
