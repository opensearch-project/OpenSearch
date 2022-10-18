/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.identity.noop;

import org.opensearch.identity.AccessTokenManager;
import org.opensearch.identity.AuthenticationManager;
import org.opensearch.identity.noop.InternalSubject;
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

    @Override
    public Subject getSubject() {
        return new InternalSubject(SecurityUtils.getSubject());
    }

    @Override
    public AccessTokenManager getAccessTokenManager() {
        return null;
    }
}
