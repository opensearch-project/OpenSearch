/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.authn.internal;

import java.security.Principal;
import java.util.Objects;

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

        // If already authenticated, do not check login info again
        /*
            TODO: understand potential repercussions in following situations:
             1. How to handle this in password change situations
             2. Can two subjects in same environment have same principal name? if so the following check is invalid
         */
        if (this.isAuthenticated() && this.getPrincipal().getName().equals(authToken.getPrincipal())) {
            return;
        }
        // Login via shiro realm.
        shiroSubject.login(authToken);
    }

    /**
     * Logs out this subject
     *
     * TODO: test this method
     */
    @Override
    public void logout() {
        try {
            if (shiroSubject == null) return;
            shiroSubject.logout();
            // Get current session and kill it before proceeding to create a new session
            // TODO: need to study the impact of this
            Session session = shiroSubject.getSession(false);
            if (session == null) return;
            session.stop();
        } catch (Exception e) {
            // Ignore all errors, as we're trying to silently kill the session
        }
    }

    /**
     * A flag to indicate whether this subject is already authenticated
     *
     * @return true if authenticated, false otherwise
     */
    @Override
    public boolean isAuthenticated() {
        return shiroSubject.isAuthenticated();
    }

}
