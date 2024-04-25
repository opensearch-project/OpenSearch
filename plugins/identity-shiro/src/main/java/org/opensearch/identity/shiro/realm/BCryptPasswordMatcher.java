/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.identity.shiro.realm;

import org.apache.shiro.authc.AuthenticationInfo;
import org.apache.shiro.authc.AuthenticationToken;
import org.apache.shiro.authc.UsernamePasswordToken;
import org.apache.shiro.authc.credential.CredentialsMatcher;
import org.bouncycastle.crypto.generators.OpenBSDBCrypt;

/**
 * Password matcher for BCrypt
 *
 * @opensearch.experimental
 */
public class BCryptPasswordMatcher implements CredentialsMatcher {

    /**
     * Check if the provided authentication token and authentication info match one another
     * @param token   the {@code AuthenticationToken} submitted during the authentication attempt
     * @param info the {@code AuthenticationInfo} stored in the system.
     * @return A boolean showing whether the token credentials match the info or not.
     */
    @Override
    public boolean doCredentialsMatch(AuthenticationToken token, AuthenticationInfo info) {
        final UsernamePasswordToken userToken = (UsernamePasswordToken) token;
        return OpenBSDBCrypt.checkPassword((String) info.getCredentials(), userToken.getPassword());
    }

}
