/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.identity.tokens;

public interface TokenManager {

    public AuthToken generateToken();

    public boolean validateToken(AuthToken token);

    public String getTokenInfo(AuthToken token);

    public void revokeToken(AuthToken token);

    public void refreshToken(AuthToken token);
}
