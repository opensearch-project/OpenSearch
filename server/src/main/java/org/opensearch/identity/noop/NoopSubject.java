/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.identity.noop;

import org.opensearch.identity.NamedPrincipal;
import org.opensearch.identity.Subject;
import org.opensearch.identity.tokens.AuthToken;

import java.security.Principal;
import java.util.Objects;

/**
 * Implementation of subject that is always authenticated
 * <p>
 * This class and related classes in this package will not return nulls or fail permissions checks
 *
 * @opensearch.internal
 */
public class NoopSubject implements Subject {

    @Override
    public Principal getPrincipal() {
        return NamedPrincipal.UNAUTHENTICATED;
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
        return "NoopSubject(principal=" + getPrincipal() + ")";
    }

    /**
     * Logs the user in
     */
    @Override
    public void authenticate(AuthToken AuthToken) {
        // Do nothing as noop subject is always logged in
    }

    @Override
    public Session runAs() {
        return null;
    }
}
