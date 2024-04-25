/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.identity;

import java.security.Principal;
import java.util.Objects;

/**
 * Create a principal from a string
 *
 * @opensearch.experimental
 */
public class NamedPrincipal implements Principal {

    /**
     * Represents a principal which has not been authenticated
     */
    public static final NamedPrincipal UNAUTHENTICATED = new NamedPrincipal("Unauthenticated");

    private final String name;

    /**
     * Creates a principal for an identity specified as a string
     * @param name A persistent string that represent an identity
     */
    public NamedPrincipal(final String name) {
        this.name = name;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public boolean equals(final Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        final Principal that = (Principal) obj;
        return Objects.equals(name, that.getName());
    }

    @Override
    public int hashCode() {
        return Objects.hash(name);
    }

    @Override
    public String toString() {
        return "NamedPrincipal(" + "name=" + name + ")";
    }
}
