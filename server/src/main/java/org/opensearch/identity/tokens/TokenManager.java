/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.identity.tokens;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.identity.Subject;

/**
 * This interface defines the expected methods of a token manager
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public interface TokenManager {

    /**
     * Create a new on behalf of token
     *
     * @param claims: A list of claims for the token to be generated with
     * @return A new auth token
     */
    public AuthToken issueOnBehalfOfToken(final Subject subject, final OnBehalfOfClaims claims);

    /**
     * Create a new service account token
     *
     * @param audience: A string representing the unique id of the extension for which a service account token should be generated
     * @return a new auth token
     */
    public AuthToken issueServiceAccountToken(final String audience);
}
