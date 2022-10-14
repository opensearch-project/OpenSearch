/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.identity.noop;

import java.security.Principal;
import java.util.Objects;

import org.opensearch.authn.Subject;
import org.opensearch.authn.Principals;

/**
 * Implementation of subject that is always authenticated
 *
 * This class and related classes in this package will not return nulls or fail permissions checks
 *
 * @opensearch.internal
 */
public class NoopSubject implements Subject {

    @Override
    public Principal getPrincipal() {
        return Principals.UNAUTHENTICATED.getPrincipal();
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

}
