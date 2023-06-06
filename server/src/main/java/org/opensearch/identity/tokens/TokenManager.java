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
     * @param audience: The audience for the token
     * @return A new auth token
     */
    public AuthToken issueToken(String audience);
}
