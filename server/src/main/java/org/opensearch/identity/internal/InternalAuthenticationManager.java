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
import org.opensearch.rest.RestChannel;
import org.opensearch.rest.RestRequest;

/**
 * Implementation of authentication manager that does not enforce authentication
 *
 * This class and related classes in this package will not return nulls or fail permissions checks
 *
 * This class manages the subjects loaded via the realm, and provides current subject
 * when authenticating the incoming request
 * Checkout {@link org.opensearch.rest.RestController#authenticate(RestRequest, RestChannel)}
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
