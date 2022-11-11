/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.identity.internal;
import org.opensearch.authn.realm.InternalRealm;
import org.opensearch.identity.AccessTokenManager;
import org.opensearch.identity.AuthenticationManager;
import org.opensearch.authn.Subject;
import org.apache.shiro.SecurityUtils;
import org.apache.shiro.mgt.DefaultSecurityManager;
import org.apache.shiro.mgt.SecurityManager;

/**
 * Implementation of authentication manager that does not enforce authentication
 *
 * This class and related classes in this package will not return nulls or fail permissions checks
 *
 * @opensearch.internal
 */
public class InternalAuthenticationManager implements AuthenticationManager {

    public InternalAuthenticationManager() {
        final SecurityManager securityManager = new DefaultSecurityManager(InternalRealm.INSTANCE);
        SecurityUtils.setSecurityManager(securityManager);
    }

    public InternalAuthenticationManager(SecurityManager securityManager) {
        SecurityUtils.setSecurityManager(securityManager);
    }

    @Override
    public Subject getSubject() {
        return new InternalSubject(SecurityUtils.getSubject());
    }

    @Override
    public AccessTokenManager getAccessTokenManager() {
        return null;
    }
}
