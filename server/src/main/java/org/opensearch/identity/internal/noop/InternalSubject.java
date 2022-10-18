/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.identity.noop;

import java.security.Principal;
import java.util.Objects;

import org.opensearch.authn.Subject;
import org.opensearch.authn.Principals;
import org.apache.shiro.authc.UsernamePasswordToken;

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
        return "InternalSubject(principal=" + getPrincipal() + ")";
    }

    /**
     * Logs the user in
     */
    void login(AuthenticationToken authenticationToken) {

        AuthenticationToken authToken;

        if (authenticationToken instanceof HttpHeaderToken) {
            final HttpHeaderToken headerToken = (HttpHeaderToken) authenticationToken;

            if (token.getHeaderValue().contains("Basic")) {
                final byte[] decodedAuthHeader = Base64.getDecoder().decode(token.getHeaderValue().substring("Basic".length()).trim());
                final String[] decodedUserNamePassword = decodedAuthHeader.toString().split(":");
        
                authToken = new UsernamePasswordToken(decodedUserNamePassword[0], decodedUserNamePassword[1]);
           }
        }


        shiroSubject.login(authToken);

        return; // Do nothing we are already logged in to nothing
    }
}
