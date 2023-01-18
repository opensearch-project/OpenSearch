/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.identity.authmanager.securityplugin;

import org.opensearch.authn.AccessTokenManager;
import org.opensearch.authn.tokens.AccessToken;

public class SecurityPluginAccessTokenManager implements AccessTokenManager {
    @Override
    public void expireAllTokens() {
        return;
    }

    @Override
    public AccessToken generate() {
        return null;
    }

    @Override
    public AccessToken refresh(AccessToken token) {
        return null;
    }
}
