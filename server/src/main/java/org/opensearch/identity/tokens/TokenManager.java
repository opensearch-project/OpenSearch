/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.identity.tokens;

import org.opensearch.identity.Subject;

/**
 * This interface defines the expected methods of a token manager
 */
public interface TokenManager {

    /**
     * Create a new on behalf of token
     *
     * @param claims: A list of claims for the token to be generated with
     * @return A new auth token
     */
    public AuthToken issueOnBehalfOfToken(final Subject subject, final OnBehalfOfClaims claims);

    /**
     * Authenticates a provided authToken
     * @param authToken: The authToken to authenticate
     * @return The authenticated subject
     */
    public Subject authenticateToken(AuthToken authToken);
}
