/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.identity.noop;

import org.opensearch.identity.AccessToken;
import org.opensearch.identity.AccessTokenManager;

/**
 * Implementation of access token manager that does not enforce authentication
 *
 * This class and related classes in this package will not return nulls or fail permissions checks
 *
 * @opensearch.internal
 */
public class NoopAccessTokenManager implements AccessTokenManager {

    @Override
    public void expireAllTokens() {
        // Tokens cannot be expired
    }

    @Override
    public AccessToken generate() {
        return new AccessToken();
    }

    @Override
    public AccessToken refresh(final AccessToken token) {
        return new AccessToken();
    }

}
