/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.identity.shiro;

import java.security.Principal;
import java.util.Objects;

import org.opensearch.identity.Subject;
import org.opensearch.identity.tokens.AuthToken;

/**
 * Subject backed by Shiro
 *
 * @opensearch.experimental
 */
public class ShiroSubject implements Subject {
    private final AuthTokenHandler authTokenHandler;
    private final org.apache.shiro.subject.Subject shiroSubject;

    public ShiroSubject(final AuthTokenHandler authTokenHandler, final org.apache.shiro.subject.Subject subject) {
        this.authTokenHandler = authTokenHandler;
        this.shiroSubject = subject;
    }

    @Override
    public Principal getPrincipal() {
        final Object o = shiroSubject.getPrincipal();
        if (o == null) return null;
        if (o instanceof Principal) return (Principal) o;
        return () -> o.toString();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        final Subject that = (Subject) obj;
        return Objects.equals(getPrincipal(), that.getPrincipal());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getPrincipal());
    }

    @Override
    public String toString() {
        return "ShiroSubject(principal=" + getPrincipal() + ")";
    }

    /**
     * Logs the user in via authenticating the user against current Shiro realm
     */
    public void login(AuthToken authenticationToken) {
        final org.apache.shiro.authc.AuthenticationToken authToken = authTokenHandler.translateAuthToken(authenticationToken);
        shiroSubject.login(authToken);
    }
}
