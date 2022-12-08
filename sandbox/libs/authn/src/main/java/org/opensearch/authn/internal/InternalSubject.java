/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.authn.internal;

import java.security.Principal;
import java.util.Objects;

import org.apache.shiro.SecurityUtils;
import org.apache.shiro.session.Session;
import org.opensearch.authn.AuthenticationTokenHandler;
import org.opensearch.authn.tokens.AuthenticationToken;
import org.opensearch.authn.Subject;

/**
 * Implementation of subject that is always authenticated
 *
 * This class and related classes in this package will not return nulls or fail permissions checks
 *
 * @opensearch.internal
 */
public class InternalSubject implements Subject {
    private final org.apache.shiro.subject.Subject shiroSubject;

    public InternalSubject(org.apache.shiro.subject.Subject subject) {
        shiroSubject = subject;
    }

    @Override
    public Principal getPrincipal() {
        final Object o = shiroSubject.getPrincipal();

        if (o == null) {
            return null;
        }

        if (o instanceof Principal) {
            return (Principal) o;
        }

        return () -> o.toString();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        Subject that = (Subject) obj;
        return Objects.equals(getPrincipal(), that.getPrincipal());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getPrincipal());
    }

    @Override
    public String toString() {
        return "InternalSubject (principal=" + getPrincipal() + ")";
    }

    /**
     * Logs the user in via authenticating the user against current Shiro realm
     */
    public void login(AuthenticationToken authenticationToken) {
        org.apache.shiro.authc.AuthenticationToken authToken = AuthenticationTokenHandler.extractShiroAuthToken(authenticationToken);
        // Login via shiro realm.
        ensureUserIsLoggedOut();
        shiroSubject.login(authToken);
    }

    // Logout the user fully before continuing.
    private void ensureUserIsLoggedOut() {
        try {
            // Get the user if one is logged in.
            org.apache.shiro.subject.Subject currentUser = SecurityUtils.getSubject();
            if (currentUser == null) return;

            // Log the user out and kill their session if possible.
            currentUser.logout();
            Session session = currentUser.getSession(false);
            if (session == null) return;

            session.stop();
        } catch (Exception e) {
            // Ignore all errors, as we're trying to silently
            // log the user out.
        }
    }
}
