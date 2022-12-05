/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.authn.internal;

import org.opensearch.authn.AccessTokenManager;
import org.opensearch.authn.tokens.AccessToken;

/**
 * Implementation of access token manager that does not enforce authentication
 *
 * This class and related classes in this package will not return nulls or fail permissions checks
 *
 * @opensearch.internal
 */
public class InternalAccessTokenManager implements AccessTokenManager {

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
