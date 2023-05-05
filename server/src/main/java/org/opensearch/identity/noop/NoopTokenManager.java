/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.identity.noop;

import org.opensearch.identity.tokens.AuthToken;
import org.opensearch.identity.tokens.NoopToken;
import org.opensearch.identity.tokens.TokenManager;

public class NoopTokenManager implements TokenManager {
    @Override
    public AuthToken generateToken() {
        return new NoopToken();
    }

    @Override
    public boolean validateToken(AuthToken token) {
        if (token instanceof NoopToken){
            return true;
        }
        return false;
    }

    @Override
    public String getTokenInfo(AuthToken token) {
        return "Token is NoopToken";
    }

    @Override
    public void revokeToken(AuthToken token) {

    }

    @Override
    public void refreshToken(AuthToken token) {

    }
}
