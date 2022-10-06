/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.identity;

import org.opensearch.authn.Subject;

/**
 * Authentication management for OpenSearch.
 *
 * Retrieve the current subject or switch to a subject
 *
 * @opensearch.experimental
 * */
public interface AuthenticationManager {

    /**
     * Get the current subject
     * */
    public Subject getSubject();

    /**
     * Get an access token manager
     * */
    public AccessTokenManager getAccessTokenManager();
}
