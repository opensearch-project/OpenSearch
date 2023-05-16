/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.identity.tokens;

/**
 * This interface defines the expected methods of a token manager
 */
public interface TokenManager {

    /**
     * Create a new auth token
     * @return A new auth token
     */
    public AuthToken issueToken();

    /**
     * Validate an auth token based on the rules associated with its format
     * @param token The token to validate
     * @return True if the token is valid; False if the token is not valid
     */
    public boolean validateToken(AuthToken token);

    /**
     * Fetch the info from a token
     * @param token The auth token to be parsed
     * @return A String representing the info associated with the token
     */
    public String getTokenInfo(AuthToken token);

    /**
     * Revoke a token that should no longer be treated as valid
     * @param token The Auth Token to be revoked
     */
    public void revokeToken(AuthToken token);

    /**
     * Updates a token to be valid for a greater period of time or to have different attributes.
     * @param token The token to be refreshed
     */
    public void resetToken(AuthToken token);
}
