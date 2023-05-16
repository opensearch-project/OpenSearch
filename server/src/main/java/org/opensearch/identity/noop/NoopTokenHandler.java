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
import org.opensearch.OpenSearchException;
import org.opensearch.identity.IdentityService;
import org.opensearch.identity.tokens.AuthToken;
import org.opensearch.identity.tokens.NoopToken;
import org.opensearch.identity.tokens.TokenManager;

/**
 * This class represents a Noop Token Manager
 */
public class NoopTokenHandler implements TokenManager {

    private static final Logger log = LogManager.getLogger(IdentityService.class);

    /**
     * Issue a new Noop Token
     * @return a new Noop Token
     */
    @Override
    public AuthToken issueToken() {
        return new NoopToken();
    }

    /**
     * Validate a token
     * @param token The token to be validated
     * @return If the token is a Noop Token, then pass with True; otherwise fail with False.
     */
    @Override
    public boolean validateToken(AuthToken token) {
        if (token instanceof NoopToken) {
            return true;
        }
        return false;
    }

    /**
     * Get token info, there should not be any token info so just return whether the token is a NoopToken
     * @param token The auth token to be parsed
     * @return A String stating the token is a NoopToken or is not a NopToken
     */
    @Override
    public String getTokenInfo(AuthToken token) {
        if (token instanceof NoopToken) {
            return "Token is NoopToken";
        }
        return "Token is not a NoopToken";
    }

    /**
     * Revoking a Noop Token should not do anything
     * @param token The Auth Token to be revoked
     */
    @Override
    public void revokeToken(AuthToken token) {
        if (token instanceof NoopToken) {
            log.info("Revoke operation is not supported for NoopTokens");
            return;
        }
        throw new OpenSearchException("Token is not a NoopToken");
    }

    /**
     * Refreshing a NoopToken also not do anything
     * @param token The token to be refreshed
     */
    @Override
    public void resetToken(AuthToken token) {
        if (token instanceof NoopToken) {
            log.info("Reset operation is not supported for NoopTokens");
            return;
        }
        throw new OpenSearchException("Token is not a NoopToken");
    }
}
