/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.identity;

import java.security.Principal;

/**
 * Available OpenSearch internal principals
 *
 * @opensearch.experimental
 */
public class NativePrincipal {

    /**
     * Represents a principal which has not been authenticated
     */
    public static final NativePrincipal UNAUTHENTICATED = new NativePrincipal("Unauthenticated");

    private final Principal principal;

    NativePrincipal(final String principal) {
        this.principal = new NamedPrincipal(principal);
    }

    /**
     * Returns the underlying principal for this
     */
    public Principal getPrincipal() {
        return principal;
    }

}
