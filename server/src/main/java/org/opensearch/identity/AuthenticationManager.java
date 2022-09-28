/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.identity;

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
