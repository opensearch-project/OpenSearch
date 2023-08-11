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
import org.opensearch.identity.Subject;
import org.opensearch.identity.tokens.AuthToken;
import org.opensearch.identity.tokens.OnBehalfOfClaims;
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
    public AuthToken issueOnBehalfOfToken(final Subject subject, final OnBehalfOfClaims claims) {
        return new AuthToken() {
            @Override
            public String asAuthHeaderValue() {
                return "noopToken";
            }
        };
    }

    @Override
    public Subject authenticateToken(AuthToken authToken) {
        return null;
    }
}
