/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.authn.internal;

import java.security.Principal;
import java.util.Objects;
import java.util.List;
import java.util.Collection;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.authn.AuthenticationTokenHandler;
import org.opensearch.authn.tokens.AuthenticationToken;
import org.opensearch.authn.Subject;
import org.opensearch.authn.UnauthorizedException;

/**
 * Implementation of subject that is always authenticated
 *
 * This class and related classes in this package will not return nulls or fail permissions checks
 *
 * @opensearch.internal
 */
public class InternalSubject implements Subject {

    private static final Logger LOG = LogManager.getLogger(InternalSubject.class);

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
        shiroSubject.login(authToken);
    }

    @Override
    public UnauthorizedException checkPermission(final List<String> permissions) {
        LOG.debug("Check for permission: " + permissions.stream().collect(Collectors.joining(", ")));

        final List<String> unauthorizedPermissions = permissions
            .stream()
            .filter(p -> !shiroSubject.isPermitted(p))
            .collect(Collectors.toList());

        if (unauthorizedPermissions.isEmpty()) {
            return null;
        }

        return new UnauthorizedException("Missing the following permissions: " + permissionsAsString(unauthorizedPermissions));
    }

    private static String permissionsAsString(final List<String> permissions) {
        return permissions.stream().collect(Collectors.joining(", "));
    }
}
