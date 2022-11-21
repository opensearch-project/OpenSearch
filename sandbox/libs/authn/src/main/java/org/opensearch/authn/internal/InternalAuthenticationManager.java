/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.authn.internal;

import org.opensearch.authn.AccessTokenManager;
import org.opensearch.authn.AuthenticationManager;
import org.opensearch.authn.internal.InternalSubject;
import org.opensearch.authn.realm.InternalRealm;
import org.opensearch.authn.Subject;
import org.apache.shiro.SecurityUtils;
import org.apache.shiro.mgt.DefaultSecurityManager;
import org.apache.shiro.mgt.SecurityManager;

/**
 * Implementation of authentication manager that enforces authentication against internal idp
 *
 * This class and related classes in this package will not return nulls or fail permissions checks
 *
 * This class manages the subjects loaded via the realm, and provides current subject
 * when authenticating the incoming request
 * Checkout
 * and how the internal Identity system uses auth manager to get current subject to use for authentication
 *
 * @opensearch.internal
 */
public class InternalAuthenticationManager implements AuthenticationManager {

    /**
     * Security manager is loaded with default user set,
     * and this instantiation uses the default security manager
     */
    public InternalAuthenticationManager() {
        final SecurityManager securityManager = new DefaultSecurityManager(InternalRealm.INSTANCE);
        SecurityUtils.setSecurityManager(securityManager);
    }

    /**
     * Instantiates this Auth manager by setting the custom security Manager that is passed as an argument
     * @param securityManager the custom security manager (with realm instantiated in it)
     */
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
