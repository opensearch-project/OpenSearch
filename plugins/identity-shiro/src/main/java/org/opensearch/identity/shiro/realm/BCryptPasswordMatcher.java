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
import org.opensearch.SpecialPermission;

import java.nio.CharBuffer;
import java.security.AccessController;
import java.security.PrivilegedAction;

import com.password4j.BcryptFunction;
import com.password4j.Password;

import static org.opensearch.core.common.Strings.isNullOrEmpty;

/**
 * Password matcher for BCrypt
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
        return check(userToken.getPassword(), (String) info.getCredentials());
    }

    @SuppressWarnings("removal")
    private boolean check(char[] password, String hash) {
        if (password == null || password.length == 0) {
            throw new IllegalStateException("Password cannot be empty or null");
        }
        if (isNullOrEmpty(hash)) {
            throw new IllegalStateException("Hash cannot be empty or null");
        }
        CharBuffer passwordBuffer = CharBuffer.wrap(password);
        SecurityManager securityManager = System.getSecurityManager();
        if (securityManager != null) {
            securityManager.checkPermission(new SpecialPermission());
        }
        return AccessController.doPrivileged(
            (PrivilegedAction<Boolean>) () -> Password.check(passwordBuffer, hash).with(BcryptFunction.getInstanceFromHash(hash))
        );
    }

}
