/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.identity.noop;

import com.fasterxml.jackson.databind.node.ObjectNode;
import org.opensearch.identity.ServiceAccountManager;
import org.opensearch.identity.tokens.AuthToken;

/**
 * A Noop implementation of a service account manager
 */
public class NoopServiceAccountManager implements ServiceAccountManager {

    /**
     * Returns an empty auth token
     * @param principal A principal an auth token should be created for
     * @return An empty auth token object
     */
    @Override
    public AuthToken resetServiceAccountToken(String principal) {
        return new AuthToken() {
            @Override
            public String toString() {
                return "";
            }
        };
    }

    /**
     * Pass through that always views a token as valid
     * @param token The token to be verified
     * @return True stating the token is valid
     */
    @Override
    public Boolean isValidToken(AuthToken token) {
        return true;
    }

    /**
     * A method to update a service account with the provided content
     * @param contentAsNode The content the account should be updated with
     */
    @Override
    public void updateServiceAccount(ObjectNode contentAsNode) {}
}
