/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.identity.noop;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.identity.IdentityService;
import org.opensearch.identity.tokens.AuthToken;
import org.opensearch.identity.tokens.TokenManager;

/**
 * This class represents a Noop Token Manager
 */
public class NoopTokenManager implements TokenManager {

    private static final Logger log = LogManager.getLogger(IdentityService.class);

    /**
     * Issue a new Noop Token
     * @return a new Noop Token
     */
    @Override
    public AuthToken issueToken() {
        return new AuthToken() {
        };
    }

    /**
     * Validate a token
     * @param token The token to be validated
     * @return true
     */
    @Override
    public boolean validateToken(AuthToken token) {
        log.info("Validating a token with NoopTokenManager");
        return true;
    }

    /**
     * Get token class
     * @param token The auth token to be parsed
     * @return A description of the token's type
     */
    @Override
    public String getTokenInfo(AuthToken token) {
        return "Token is of type: " + token.getClass();
    }

    /**
     * Revoking a Noop Token should not do anything
     * @param token The Auth Token to be revoked
     */
    @Override
    public void revokeToken(AuthToken token) {
        log.info("Revoke operation is not supported for NoopTokens");
        return;
    }

    /**
     * Refreshing a NoopToken also not do anything
     * @param token The token to be refreshed
     */
    @Override
    public void resetToken(AuthToken token) {
        log.info("Reset operation is not supported for NoopTokens");
        return;
    }
}
